//! Cluster admission control for a SQL gateway.
//!
//! A gateway fronts several *groups* of engine clusters (Trino, StarRocks, ...).
//! Every incoming query must be admitted to one cluster of its target group
//! before it is forwarded, and each cluster accepts at most `max_running`
//! concurrent queries. The gateway handles thousands of queries per second,
//! so `try_acquire` sits on the hot path of every request.
//!
//! See README.md for the tasks.

use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock, RwLockWriteGuard};
use std::time::Duration;

use tokio::sync::Notify;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterConfig {
    pub name: String,
    pub max_running: u64,
}

impl ClusterConfig {
    pub fn new(name: impl Into<String>, max_running: u64) -> Self {
        Self {
            name: name.into(),
            max_running,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PoolConfig {
    pub groups: HashMap<String, Vec<ClusterConfig>>,
}

impl PoolConfig {
    pub fn group(mut self, name: impl Into<String>, clusters: Vec<ClusterConfig>) -> Self {
        self.groups.insert(name.into(), clusters);
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AcquireError {
    /// The group does not exist in the current config.
    UnknownGroup,
    /// Every cluster in the group is at capacity.
    Busy,
    /// `acquire_timeout` waited for its full duration without getting a slot.
    Timeout,
}

#[derive(Debug)]
// Keep cluster counters off the Arc reference-count cache line.
#[repr(align(128))]
struct ClusterState {
    name: String,
    max_running: AtomicU64,
    running: AtomicU64,
    admissions: Arc<AtomicU64>,
    waiters: Arc<Waiters>,
}

impl ClusterState {
    fn new(config: &ClusterConfig, admissions: Arc<AtomicU64>, waiters: Arc<Waiters>) -> Self {
        Self {
            name: config.name.clone(),
            max_running: AtomicU64::new(config.max_running),
            running: AtomicU64::new(0),
            admissions,
            waiters,
        }
    }

    fn running(&self) -> u64 {
        self.running.load(Ordering::Relaxed)
    }

    fn max_running(&self) -> u64 {
        self.max_running.load(Ordering::Relaxed)
    }

    fn available(&self) -> u64 {
        self.max_running().saturating_sub(self.running())
    }

    fn try_increment(&self) -> bool {
        let max = self.max_running();
        let mut current = self.running();
        while current < max {
            match self.running.compare_exchange_weak(
                current,
                current + 1,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(observed) => current = observed,
            }
        }
        false
    }

    fn decrement(&self) {
        self.running.fetch_sub(1, Ordering::Relaxed);
        self.waiters.notify.notify_one();
    }
}

#[derive(Debug, Default)]
struct Waiters {
    notify: Notify,
    queued: AtomicU64,
}

struct Queued<'a>(&'a Waiters);

impl Drop for Queued<'_> {
    fn drop(&mut self) {
        self.0.queued.fetch_sub(1, Ordering::Relaxed);
        // Hand off a wake-up if this caller exits without taking the freed slot.
        self.0.notify.notify_one();
    }
}

type Groups = HashMap<String, Vec<Arc<ClusterState>>>;

const CONFIG_SHARDS: usize = 32;
static NEXT_SHARD: AtomicUsize = AtomicUsize::new(0);
thread_local! {
    static CONFIG_SHARD: usize = NEXT_SHARD.fetch_add(1, Ordering::Relaxed) % CONFIG_SHARDS;
}

// Separate reader-lock counters, including on CPUs with 128-byte cache lines.
#[repr(align(128))]
struct ConfigShard(RwLock<Arc<Groups>>);

pub struct ClusterPool {
    groups: [ConfigShard; CONFIG_SHARDS],
    admissions: Mutex<HashMap<String, Arc<AtomicU64>>>,
    waiters: Mutex<HashMap<String, Arc<Waiters>>>,
}

impl ClusterPool {
    pub fn new(config: PoolConfig) -> Arc<Self> {
        let mut admissions = HashMap::new();
        let mut waiters = HashMap::new();
        let groups = Arc::new(build_groups(&config, &mut admissions, &mut waiters));
        Arc::new(Self {
            groups: std::array::from_fn(|_| ConfigShard(RwLock::new(Arc::clone(&groups)))),
            admissions: Mutex::new(admissions),
            waiters: Mutex::new(waiters),
        })
    }

    fn groups(&self) -> &RwLock<Arc<Groups>> {
        &self.groups[CONFIG_SHARD.with(|index| *index)].0
    }

    fn write_groups(&self) -> Vec<RwLockWriteGuard<'_, Arc<Groups>>> {
        // Every writer takes all shards in the same order before changing state.
        self.groups
            .iter()
            .map(|shard| shard.0.write().unwrap())
            .collect()
    }

    fn find(&self, group: &str, cluster: &str) -> Option<Arc<ClusterState>> {
        self.groups()
            .read()
            .unwrap()
            .get(group)?
            .iter()
            .find(|c| c.name == cluster)
            .cloned()
    }

    /// Admit one query to the least-loaded cluster of `group`, or fail fast with `Busy`.
    pub fn try_acquire(self: &Arc<Self>, group: &str) -> Result<Permit, AcquireError> {
        // Synchronize capacity changes with the check-and-increment operation.
        let groups = self.groups().read().unwrap();
        let clusters = groups.get(group).ok_or(AcquireError::UnknownGroup)?;

        let preferred = clusters
            .iter()
            .max_by_key(|state| state.available())
            .ok_or(AcquireError::Busy)?;
        let state = std::iter::once(preferred)
            .chain(
                clusters
                    .iter()
                    .filter(|state| !Arc::ptr_eq(state, preferred)),
            )
            .find(|state| state.try_increment())
            .cloned()
            .ok_or(AcquireError::Busy)?;
        drop(groups);
        let permit = Permit { state };

        permit.state.admissions.fetch_add(1, Ordering::Relaxed);

        Ok(permit)
    }

    /// Part 3: like `try_acquire`, but wait up to `wait` for a slot to free up.
    pub async fn acquire_timeout(
        self: &Arc<Self>,
        group: &str,
        wait: Duration,
    ) -> Result<Permit, AcquireError> {
        match self.try_acquire(group) {
            Err(AcquireError::Busy) => {}
            result => return result,
        }
        let started = tokio::time::Instant::now();
        let waiters = self.waiters.lock().unwrap().get(group).cloned().unwrap();

        tokio::time::timeout(wait, async {
            waiters.queued.fetch_add(1, Ordering::Relaxed);
            let _queued = Queued(&waiters);
            let notified = waiters.notify.notified();
            tokio::pin!(notified);

            loop {
                if started.elapsed() >= wait {
                    return Err(AcquireError::Timeout);
                }
                // Register before rechecking capacity to avoid a lost release.
                notified.as_mut().enable();
                match self.try_acquire(group) {
                    Err(AcquireError::Busy) => {}
                    result => return result,
                }
                notified.as_mut().await;
                notified.set(waiters.notify.notified());
            }
        })
        .await
        .unwrap_or(Err(AcquireError::Timeout))
    }

    /// Current running count for a cluster, or `None` if it is not in the current config.
    pub fn running(&self, group: &str, cluster: &str) -> Option<u64> {
        self.find(group, cluster).map(|c| c.running())
    }

    /// Number of callers currently waiting inside `acquire_timeout` for `group`.
    pub fn queued(&self, group: &str) -> u64 {
        self.waiters
            .lock()
            .unwrap()
            .get(group)
            .map(|waiters| waiters.queued.load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    /// Total queries ever admitted to a cluster (a metric scraped by Prometheus).
    pub fn admissions(&self, group: &str, cluster: &str) -> u64 {
        self.admissions
            .lock()
            .unwrap()
            .get(&format!("{group}/{cluster}"))
            .map(|count| count.load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    /// Admin API: change a cluster's capacity at runtime.
    pub fn set_max_running(&self, group: &str, cluster: &str, max_running: u64) {
        let groups = self.write_groups();
        let waiters = groups[0]
            .get(group)
            .and_then(|clusters| clusters.iter().find(|c| c.name == cluster))
            .map(|state| {
                state.max_running.store(max_running, Ordering::Relaxed);
                Arc::clone(&state.waiters)
            });
        drop(groups);
        if let Some(waiters) = waiters {
            waiters.notify.notify_waiters();
        }
    }

    /// Bonus: swap in a new config while queries are in flight.
    ///
    /// A cluster keeps its identity across reloads by (group, name). Clusters removed
    /// from the config take no new queries; clusters added to it take traffic at once.
    pub fn reload(&self, config: PoolConfig) {
        let mut shards = self.write_groups();
        let mut admissions = self.admissions.lock().unwrap();
        let mut waiters = self.waiters.lock().unwrap();
        let groups = Arc::new(build_groups(&config, &mut admissions, &mut waiters));
        for shard in &mut shards {
            **shard = Arc::clone(&groups);
        }
        let wake: Vec<_> = waiters.values().cloned().collect();
        drop(waiters);
        drop(admissions);
        drop(shards);
        for waiters in wake {
            waiters.notify.notify_waiters();
        }
    }

    /// Bonus: correct a cluster's running counter against the engine's own count.
    /// `fetch` asks the engine how many queries it is running; it is slow.
    pub async fn reconcile<F, Fut>(&self, group: &str, cluster: &str, fetch: F)
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = u64>,
    {
        let target = fetch().await;
        if let Some(state) = self.find(group, cluster) {
            state.running.store(target, Ordering::Relaxed);
            state.waiters.notify.notify_waiters();
        }
    }
}

fn build_groups(
    config: &PoolConfig,
    admissions: &mut HashMap<String, Arc<AtomicU64>>,
    waiters: &mut HashMap<String, Arc<Waiters>>,
) -> HashMap<String, Vec<Arc<ClusterState>>> {
    config
        .groups
        .iter()
        .map(|(group, clusters)| {
            // Preserve group notifications across config changes, including removal.
            let group_waiters = Arc::clone(waiters.entry(group.clone()).or_default());
            let states = clusters
                .iter()
                .map(|c| {
                    // Keep the lifetime metric even if a cluster is removed and re-added.
                    let count = admissions
                        .entry(format!("{group}/{}", c.name))
                        .or_insert_with(|| Arc::new(AtomicU64::new(0)));
                    Arc::new(ClusterState::new(
                        c,
                        Arc::clone(count),
                        Arc::clone(&group_waiters),
                    ))
                })
                .collect();
            (group.clone(), states)
        })
        .collect()
}

/// Proof of admission. The holder may run one query on `cluster()`.
/// Dropping the permit returns its slot, including on cancellation or unwinding.
pub struct Permit {
    state: Arc<ClusterState>,
}

impl Permit {
    pub fn cluster(&self) -> &str {
        &self.state.name
    }

    /// Give the slot back once the query has finished.
    pub fn release(self) {
        drop(self);
    }
}

impl Drop for Permit {
    fn drop(&mut self) {
        self.state.decrement();
    }
}
