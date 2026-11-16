use std::sync::{Arc, Barrier};

use admission::{AcquireError, ClusterConfig, ClusterPool, PoolConfig};

#[test]
fn selection_prefers_free_capacity_and_can_fill_the_group() {
    let pool = ClusterPool::new(PoolConfig::default().group(
        "g",
        vec![
            ClusterConfig::new("large", 8),
            ClusterConfig::new("small", 2),
        ],
    ));
    let mut permits = Vec::new();
    for _ in 0..6 {
        let permit = pool.try_acquire("g").unwrap();
        assert_eq!(permit.cluster(), "large");
        permits.push(permit);
    }
    for _ in 0..4 {
        permits.push(pool.try_acquire("g").unwrap());
    }
    assert_eq!(pool.running("g", "large"), Some(8));
    assert_eq!(pool.running("g", "small"), Some(2));
    assert_eq!(pool.try_acquire("g").err(), Some(AcquireError::Busy));
    drop(permits);
    assert_eq!(pool.admissions("g", "large"), 8);
    assert_eq!(pool.admissions("g", "small"), 2);
}

#[test]
fn equal_capacity_clusters_spread_held_queries_evenly() {
    let pool = ClusterPool::new(
        PoolConfig::default().group(
            "g",
            ["a", "b", "c", "d"]
                .into_iter()
                .map(|name| ClusterConfig::new(name, 8))
                .collect(),
        ),
    );
    let mut permits = Vec::new();
    for _ in 0..32 {
        permits.push(pool.try_acquire("g").unwrap());
        let counts: Vec<_> = ["a", "b", "c", "d"]
            .iter()
            .map(|name| pool.running("g", name).unwrap())
            .collect();
        assert!(counts.iter().max().unwrap() - counts.iter().min().unwrap() <= 1);
    }
}

#[test]
fn concurrent_admissions_are_counted_exactly() {
    let pool = ClusterPool::new(PoolConfig::default().group(
        "g",
        vec![ClusterConfig::new("a", 64), ClusterConfig::new("b", 64)],
    ));
    std::thread::scope(|scope| {
        for _ in 0..16 {
            let pool = Arc::clone(&pool);
            scope.spawn(move || {
                for _ in 0..1_000 {
                    pool.try_acquire("g").unwrap().release();
                }
            });
        }
    });
    assert_eq!(
        pool.admissions("g", "a") + pool.admissions("g", "b"),
        16_000
    );
    assert_eq!(pool.running("g", "a"), Some(0));
    assert_eq!(pool.running("g", "b"), Some(0));
    assert_eq!(pool.admissions("missing", "a"), 0);
}

#[test]
fn admissions_history_survives_reload_and_removal() {
    let config = PoolConfig::default()
        .group("g", vec![ClusterConfig::new("a", 1)])
        .group("other", vec![ClusterConfig::new("a", 1)]);
    let pool = ClusterPool::new(config.clone());
    pool.try_acquire("g").unwrap().release();
    pool.reload(config.clone());
    pool.try_acquire("g").unwrap().release();
    pool.reload(PoolConfig::default());
    assert_eq!(pool.admissions("g", "a"), 2);
    pool.reload(config);
    pool.try_acquire("g").unwrap().release();
    pool.try_acquire("other").unwrap().release();
    assert_eq!(pool.admissions("g", "a"), 3);
    assert_eq!(pool.admissions("other", "a"), 1);
}

#[test]
fn config_updates_reach_every_reader_shard() {
    let pool = ClusterPool::new(PoolConfig::default().group("g", vec![ClusterConfig::new("a", 1)]));
    let barrier = Barrier::new(65);
    std::thread::scope(|scope| {
        let readers: Vec<_> = (0..64)
            .map(|_| {
                scope.spawn(|| {
                    barrier.wait();
                    let disabled = pool.try_acquire("g").err() == Some(AcquireError::Busy);
                    barrier.wait();
                    barrier.wait();
                    let reloaded = pool.running("g", "a").is_none()
                        && pool.running("g", "b") == Some(0)
                        && pool.try_acquire("g").err() == Some(AcquireError::Busy);
                    barrier.wait();
                    (disabled, reloaded)
                })
            })
            .collect();
        pool.set_max_running("g", "a", 0);
        barrier.wait();
        barrier.wait();
        pool.reload(PoolConfig::default().group("g", vec![ClusterConfig::new("b", 0)]));
        barrier.wait();
        barrier.wait();
        for reader in readers {
            assert_eq!(reader.join().unwrap(), (true, true));
        }
    });
}
