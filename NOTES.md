# Notes

## Part 1: Incidents

| Incident | Root cause | Reproducing test | Fix |
|----------|------------|------------------|-----|
| INC-1 | Admission used a separate load and store. Concurrent callers could both get the same slot, while one increment was lost. A concurrent release could also be overwritten. Extra releases then wrapped the counter to `u64::MAX`, wedging admission. | `inc1_concurrent_admission_respects_cap_and_recovers_slots` | Reserve with a compare-and-exchange loop that rechecks the observed count against the cap after every conflict. |
| INC-2 | When the cap dropped below the running count, unsigned subtraction panicked in debug and wrapped in release. The wrapped value made the over-cap cluster win selection, and its rejection hid free peers. | `inc2_capacity_cut_drains_existing_queries`, `inc2_capacity_cut_keeps_other_clusters_available` | Use saturating subtraction, let existing queries drain, and try other candidates. Synchronize capacity updates with reservation using the existing config lock. |
| INC-3 | Candidate availability was a snapshot. If another caller took the selected cluster's last slot, admission returned `Busy` without trying another cluster. INC-2's wrapped availability caused the same symptom. | `inc3_contended_selection_tries_other_free_clusters` | Try the cluster with the most free capacity first, then try the remaining clusters if reservation fails. Include initially full clusters, since a release may have occurred since the snapshot. Part 2 replaces the original candidate vector with this allocation-free scan. |
| INC-4 | Only explicit `release()` returned a slot. Dropping a permit on cancellation, disconnection, or panic leaked capacity. | `inc4_dropped_permit_returns_its_slot`, `inc4_unwinding_returns_its_slot`, `inc4_cancelled_query_returns_its_slot` | The permit owns the exact reserved cluster state. `Drop` decrements it; consuming `release()` delegates to `drop(self)`, so it cannot decrement twice. |

No public types, method names, or signatures changed; no dependencies were added.
The counter uses relaxed atomics because it protects a numeric count, not query
data. Admission holds a config read lock through reservation; capacity updates
take all config write locks (sharded in Part 2), so a concurrent reservation
cannot use a stale cap after an update completes. Admission does not take an
exclusive config lock. Lowering the
cap does not revoke existing permits; that cluster admits no new queries until
the count drops below the new cap. A zero cap disables new admissions.

The contention tests run 512 barrier-coordinated rounds with permits held until
all callers have attempted admission. These are stress regressions, not an
exhaustive proof of all interleavings. All seven tests failed against the original
source in debug. The free-peer capacity-cut test also failed against the original
release build with `Busy`, confirming the production symptom separately.

Validation after Part 1:

- `cargo test --test incidents --test smoke --no-fail-fast`: 10 passed.
- `cargo test --release --test incidents --test smoke --no-fail-fast`: 10 passed.
- `cargo test --no-fail-fast`: all incident and smoke tests pass; the 6 waiting
  tests still fail at the existing Part 3 `todo!` calls.
- `rustfmt --edition 2021 --check src/lib.rs tests/incidents.rs` and
  `git diff --check`: pass.

Scope: Parts 1–3. Distributed admission and the bonus fixes are not included.
`reload` still resets running counters and `reconcile` can still
overwrite concurrent accounting; holding the original state in a permit does
not by itself solve those bonus incidents.

## Part 2: Performance

Measured on 2026-11-16: Apple M3 Max, 14 physical / 14 logical cores, macOS
27.2.1, rustc 1.99.0. Thus the 16-thread case oversubscribes this machine.
Criterion 0.5.1, release profile, 100 samples, 3 seconds warm-up and 3 seconds
measurement per case. Runs were sequential, with no other tests or benchmark
processes launched alongside them. No dependencies were added.

The starter was reconstructed in a separate scratch directory from commit
`9f1efff`, using the same Cargo.lock as this checkout. Both the unchanged starter
and the Part 1 version were measured before editing the library for Part 2.
The supplied benchmark was not changed; its SHA-256 in both copies is
`4a85e2853ae90d9c2957a775c1599bcfe2b406576cd1dd5a649e4b5f7ffd3a71`.

Throughput is millions of benchmark elements per second (higher is better):

| Bench | Starter | Before Part 2 (Part 1) | After Part 2 | After / starter |
|-------|--------:|----------------------:|-------------:|----------------:|
| acquire_release/1 | 1.8275 | 7.6361 | 50.5603 | 27.67x |
| acquire_release/4 | 2.9344 | 2.1159 | 11.0747 | 3.77x |
| acquire_release/16 | 2.2183 | 3.3662 | 8.6828 | **3.91x** |

The 16-thread target is met: 3.91x the starter and 2.58x Part 1. Criterion's
95% throughput intervals for that case were [2.2012, 2.2453] M/s (starter) and
[8.3215, 9.0932] M/s (final). These are local measurements, not a portable
speed guarantee. The earlier sharded-reader trial also reached 8.4893 M/s.

Criterion's time estimates, in ns per iteration, were:

| Threads | Starter | Part 1 | Part 2 |
|--------:|--------:|-------:|-------:|
| 1 | 547.197 | 130.958 | 19.778 |
| 4 | 1363.145 | 1890.441 | 361.184 |
| 16 | 7212.799 | 4753.176 | 1842.719 |

Each benchmark iteration represents one attempt per worker, so throughput is
`threads / iteration_time`, not `1 / iteration_time`. The supplied benchmark
counts attempts and ignores `Busy`; the buggy starter numbers must not be read
as verified successful-query throughput. Correctness is checked separately.

Commands (using the appropriate manifest for the starter snapshot):

```sh
cargo bench --bench admission -- --save-baseline starter
cargo bench --bench admission -- --save-baseline part1
cargo bench --bench admission -- --save-baseline optimized
```

Changes and their measured effect, in implementation order:

1. **Per-cluster atomic admission metrics.** Replace the per-query global
   metric mutex, formatted key allocation, and hash-map mutation with
   `permit.state.admissions.fetch_add(1, Ordering::Relaxed)`. A registry retains
   the shared counters across reload, removal, and re-addition, preserving the
   existing lifetime-metric behavior. The registry mutex is now used only for
   configuration changes and metric reads. Alone this made 16-thread throughput
   worse: 3.3662 -> 1.6307 M/s. Removing this serialization exposed contention
   elsewhere; it was not a standalone performance win.
2. **Selection without heap allocation.** Remove the candidate vector and
   repeated max searches. Scan for the most free capacity, attempt it, then
   scan the remaining clusters on contention. Both success and exhausted-group
   paths are O(n), with no candidate allocation or per-query string allocation.
   Before splitting the config lock this measured 1.6064 M/s, indistinguishable
   from the atomic-metrics trial within their confidence intervals. It did not
   resolve the remaining bottleneck on its own.
3. **Split config-reader contention.** All threads previously updated one
   RwLock's reader bookkeeping even though they only read configuration. Keep
   32 independently locked references to the same immutable `Arc<Groups>`.
   Each thread selects one shard once; calls then read-lock only that shard.
   Align shards to 128 bytes to avoid sharing reader-lock cache lines. Together
   with steps 1–2 this increased the trial to 8.4893 M/s; the final full run
   measured 8.6828 M/s. This strongly supports shared reader bookkeeping as the
   remaining bottleneck, though no hardware-counter profiling was performed.

The shard tradeoff is roughly 4 KiB of locks/references per pool and taking all
32 write locks for rare admin updates. The configuration map and cluster states
are shared, not copied 32 times. Writers always acquire locks in index order
and hold every lock before changing a cap or publishing a new configuration.
That preserves Part 1's synchronization and avoids cycles between writers.
Threads may share a shard; this affects contention, not correctness. There is
still a read lock on admission, so this is not a lock-free design.

`find` now also borrows the group lookup and clones only the matching Arc, rather
than allocating a group string and cloning a whole vector. It is used by getters
and reconcile, not the acquire/release benchmark; no benchmark gain is attributed
to this cleanup.

Selection policy: choose the largest absolute `max_running - running`, saturated
at zero, preserving the original free-slot preference for unequal capacities.
Only if that reservation loses a race do we scan other clusters in config order.
This is roughly least-loaded under concurrency, not an atomic global snapshot or
a fairness guarantee. The fallback checks all remaining clusters and never
returns Busy merely because the first candidate filled. Equal-capacity held
queries spread with a running-count difference of at most one in the sequential
regression test.

Validation: 15 tests pass in both debug and release (7 incident, 5 new performance
regression, 3 smoke). New tests cover unequal/equal capacity selection, exact
metrics after 16,000 concurrent admissions, metric history across reload/removal,
and visibility of config updates to 64 reader threads. `cargo clippy --all-targets
-- -D warnings`, formatting checks on the changed Rust files, and `git diff
--check` pass. At the end of Part 2, the full suite failed only the six then
unimplemented waiting tests; these now pass with Part 3.

## Part 3: Waiting for a slot

`acquire_timeout` first tries immediate admission: free capacity and unknown
groups return without entering the queue. A busy caller then uses its group's
shared `Notify` and a single timeout covering all retries. The retry loop also
checks elapsed time, so continuously ready notifications cannot extend the wait
budget by keeping the inner future runnable.

The pinned notification future is enabled **before** rechecking capacity. A
release between that check and awaiting the notification therefore cannot be
lost. Every permit release decrements `running` and calls `notify_one`, which
wakes a waiter promptly without polling. Releases notify unconditionally:
checking a relaxed queue counter first could race the first arriving waiter.

A `Queued` guard increments/decrements the per-group count around the waiting
operation. Its destructor runs on success, timeout, panic, and future drop. It
also notifies another waiter: if this caller consumed a release notification
but then expired or was cancelled, the free slot must not be stranded. Queues
are independent between groups. This does not promise FIFO admission or prevent
new `try_acquire` calls from taking a slot ahead of a woken waiter.

Group waiter state is retained across config reloads, including removal and
re-addition. Capacity updates, reloads, and reconciliation broadcast a recheck;
notification callbacks run after the config/registry locks have been released.
Removing a group wakes its callers to receive `UnknownGroup`. These wakeups do
not fix the bonus running-counter identity or reconciliation races.

The existing six waiting tests now pass. Seven additional tests cover zero
wait, unknown groups, independent queues, multiple slots becoming available,
config changes, cancellation of an already-notified waiter, expiration of an
already-notified waiter, and preserving the deadline across repeated wakeups.
The handoff tests manually poll futures to force those interleavings; the
deadline tests use Tokio's paused clock.

Validation: all **28 tests** pass in debug and release with
`cargo test --no-fail-fast` and `cargo test --release --no-fail-fast`.
`cargo clippy --all-targets -- -D warnings`, formatting checks on the changed
Rust files, and `git diff --check` pass. No new dependencies or public API changes.

Performance follow-up on the same machine and supplied 16-thread benchmark:
unconditional notifications initially measured 6.7944 M attempts/s. A trial
using sequentially consistent registration/reservation plus conditional
notifications measured 6.6199 M/s; it did not show a benefit and was removed.
The retained implementation instead aligns cluster state to 128 bytes, keeping
its hot data separate from the Arc reference-count cache line. This costs
padding per cluster but avoids false sharing. The final measurement was
**10.8648 M attempts/s** (95% interval [10.503, 11.291]), **4.90x** the original
starter. This is a local measurement with the same attempt-counting caveat as
Part 2; no claim is made about end-to-end query throughput.

## Part 4: Global admission across 6 pods

## Bonus: INC-5 and INC-6

## Before shipping, I would still want

-
