# Notes

## Part 1: Incidents

| Incident | Root cause | Reproducing test | Fix |
|----------|------------|------------------|-----|
| INC-1 | Admission used a separate load and store. Concurrent callers could both get the same slot, while one increment was lost. A concurrent release could also be overwritten. Extra releases then wrapped the counter to `u64::MAX`, wedging admission. | `inc1_concurrent_admission_respects_cap_and_recovers_slots` | Reserve with a compare-and-exchange loop that rechecks the observed count against the cap after every conflict. |
| INC-2 | When the cap dropped below the running count, unsigned subtraction panicked in debug and wrapped in release. The wrapped value made the over-cap cluster win selection, and its rejection hid free peers. | `inc2_capacity_cut_drains_existing_queries`, `inc2_capacity_cut_keeps_other_clusters_available` | Use saturating subtraction, let existing queries drain, and try other candidates. Synchronize capacity updates with reservation using the existing config lock. |
| INC-3 | Candidate availability was a snapshot. If another caller took the selected cluster's last slot, admission returned `Busy` without trying another cluster. INC-2's wrapped availability caused the same symptom. | `inc3_contended_selection_tries_other_free_clusters` | Attempt candidates in descending snapshot free-capacity order, discarding each failed candidate. Include initially full clusters, since a release may have occurred since the snapshot. |
| INC-4 | Only explicit `release()` returned a slot. Dropping a permit on cancellation, disconnection, or panic leaked capacity. | `inc4_dropped_permit_returns_its_slot`, `inc4_unwinding_returns_its_slot`, `inc4_cancelled_query_returns_its_slot` | The permit owns the exact reserved cluster state. `Drop` decrements it; consuming `release()` delegates to `drop(self)`, so it cannot decrement twice. |

No public types, method names, or signatures changed; no dependencies were added.
The counter uses relaxed atomics because it protects a numeric count, not query
data. Admission holds the config read lock through reservation; capacity updates
take its write lock, so a concurrent reservation cannot use a stale cap after an
update completes. Admission does not take an exclusive config lock. Lowering the
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

Scope: Part 1 only. Performance benchmarks, waiting, distributed admission, and
the bonus fixes are not included. `reload` still resets counters and `reconcile`
can still overwrite concurrent accounting; holding the original state in a
permit does not by itself solve those bonus incidents.

## Part 2: Performance

Machine (CPU and core count):

| Bench | Before | After |
|-------|--------|-------|
| acquire_release/1  | | |
| acquire_release/4  | | |
| acquire_release/16 | | |

Changes, and why each one helps (or didn't):

-

Selection policy, and why:

## Part 3: Waiting for a slot

## Part 4: Global admission across 6 pods

## Bonus: INC-5 and INC-6

## Before shipping, I would still want

-
