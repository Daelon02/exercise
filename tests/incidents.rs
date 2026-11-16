use std::sync::{mpsc, Arc, Barrier};

use admission::{AcquireError, ClusterConfig, ClusterPool, PoolConfig};

fn pool(clusters: &[(&str, u64)]) -> Arc<ClusterPool> {
    ClusterPool::new(
        PoolConfig::default().group(
            "g",
            clusters
                .iter()
                .map(|(name, max)| ClusterConfig::new(*name, *max))
                .collect(),
        ),
    )
}

// Hold every permit until all attempts finish. Fresh pools keep a broken
// counter in one round from making a later round panic inside a barrier.
fn contention_rounds(clusters: &[(&str, u64)], workers: usize) -> Vec<(usize, u64, u64)> {
    let pools: Vec<_> = (0..512).map(|_| pool(clusters)).collect();
    let barrier = Barrier::new(workers + 1);
    let (tx, rx) = mpsc::channel();

    std::thread::scope(|scope| {
        for _ in 0..workers {
            let tx = tx.clone();
            let pools = &pools;
            let barrier = &barrier;
            scope.spawn(move || {
                for pool in pools {
                    barrier.wait();
                    let permit = pool.try_acquire("g");
                    tx.send(permit.is_ok()).unwrap();
                    barrier.wait();
                    if let Ok(permit) = permit {
                        permit.release();
                    }
                    barrier.wait();
                }
            });
        }
        drop(tx);

        pools
            .iter()
            .map(|pool| {
                barrier.wait();
                let admitted = (0..workers).filter(|_| rx.recv().unwrap()).count();
                let running = clusters
                    .iter()
                    .map(|(name, _)| pool.running("g", name).unwrap())
                    .sum();
                barrier.wait();
                barrier.wait();
                let remaining = clusters
                    .iter()
                    .map(|(name, _)| pool.running("g", name).unwrap())
                    .fold(0_u64, u64::saturating_add);
                (admitted, running, remaining)
            })
            .collect()
    })
}

#[test]
fn inc1_concurrent_admission_respects_cap_and_recovers_slots() {
    for (admitted, running, remaining) in contention_rounds(&[("a", 1)], 16) {
        assert_eq!(admitted, 1, "multiple queries acquired the single slot");
        assert_eq!(running, 1, "running count must match live permits");
        assert_eq!(remaining, 0, "releases must not wrap the running count");
    }
}

#[test]
fn inc2_capacity_cut_drains_existing_queries() {
    let pool = pool(&[("a", 40)]);
    let mut permits: Vec<_> = (0..40).map(|_| pool.try_acquire("g").unwrap()).collect();
    pool.set_max_running("g", "a", 10);
    assert_eq!(pool.running("g", "a"), Some(40));
    assert_eq!(pool.try_acquire("g").err(), Some(AcquireError::Busy));

    for _ in 0..30 {
        permits.pop().unwrap().release();
    }
    assert_eq!(pool.try_acquire("g").err(), Some(AcquireError::Busy));
    permits.pop().unwrap().release();
    pool.try_acquire("g").unwrap().release();
    permits.into_iter().for_each(|permit| permit.release());
    assert_eq!(pool.running("g", "a"), Some(0));

    let held = pool.try_acquire("g").unwrap();
    pool.set_max_running("g", "a", 0);
    assert_eq!(pool.try_acquire("g").err(), Some(AcquireError::Busy));
    held.release();
    assert_eq!(pool.try_acquire("g").err(), Some(AcquireError::Busy));
    pool.set_max_running("g", "a", 1);
    pool.try_acquire("g").unwrap().release();
}

#[test]
fn inc2_capacity_cut_keeps_other_clusters_available() {
    let pool = pool(&[("a", 40), ("b", 1)]);
    let held: Vec<_> = (0..20).map(|_| pool.try_acquire("g").unwrap()).collect();
    assert!(held.iter().all(|permit| permit.cluster() == "a"));
    pool.set_max_running("g", "a", 10);
    let fallback = pool.try_acquire("g").expect("b still has a free slot");
    assert_eq!(fallback.cluster(), "b");
    fallback.release();
    held.into_iter().for_each(|permit| permit.release());
}

#[test]
fn inc3_contended_selection_tries_other_free_clusters() {
    let clusters: Vec<_> = ["a", "b", "c", "d", "e", "f", "g", "h"]
        .into_iter()
        .map(|name| (name, 1))
        .collect();
    for (admitted, running, remaining) in contention_rounds(&clusters, clusters.len()) {
        assert_eq!(admitted, clusters.len(), "Busy despite free capacity");
        assert_eq!(running, clusters.len() as u64);
        assert_eq!(remaining, 0);
    }
}

#[test]
fn inc4_dropped_permit_returns_its_slot() {
    let pool = pool(&[("a", 1)]);
    drop(pool.try_acquire("g").unwrap());
    assert_eq!(pool.running("g", "a"), Some(0));
    pool.try_acquire("g").unwrap().release();
    assert_eq!(
        pool.running("g", "a"),
        Some(0),
        "release must decrement once"
    );
    assert_eq!(pool.admissions("g", "a"), 2);
}

#[test]
fn inc4_unwinding_returns_its_slot() {
    let pool = pool(&[("a", 1)]);
    let result = std::panic::catch_unwind(|| {
        let _permit = pool.try_acquire("g").unwrap();
        panic!("query failed");
    });
    assert!(result.is_err());
    assert_eq!(pool.running("g", "a"), Some(0));
}

#[tokio::test]
async fn inc4_cancelled_query_returns_its_slot() {
    let pool = pool(&[("a", 1)]);
    let (tx, rx) = tokio::sync::oneshot::channel();
    let task_pool = Arc::clone(&pool);
    let query = tokio::spawn(async move {
        let _permit = task_pool.try_acquire("g").unwrap();
        tx.send(()).unwrap();
        std::future::pending::<()>().await;
    });
    rx.await.unwrap();
    query.abort();
    assert!(query.await.unwrap_err().is_cancelled());
    assert_eq!(pool.running("g", "a"), Some(0));
    pool.try_acquire("g").unwrap().release();
}
