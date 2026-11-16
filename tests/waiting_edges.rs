use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use admission::{AcquireError, ClusterConfig, ClusterPool, PoolConfig};

fn pool(max: u64) -> Arc<ClusterPool> {
    ClusterPool::new(PoolConfig::default().group("g", vec![ClusterConfig::new("a", max)]))
}

#[tokio::test]
async fn zero_wait_and_unknown_group_leave_no_queue() {
    let pool = pool(1);
    assert_eq!(pool.queued("missing"), 0);
    let held = pool.acquire_timeout("g", Duration::ZERO).await.unwrap();
    assert_eq!(
        pool.acquire_timeout("g", Duration::ZERO).await.err(),
        Some(AcquireError::Timeout)
    );
    assert_eq!(
        pool.acquire_timeout("missing", Duration::ZERO).await.err(),
        Some(AcquireError::UnknownGroup)
    );
    assert_eq!(pool.queued("g"), 0);
    drop(held);
}

#[tokio::test]
async fn cancelling_notified_waiter_hands_slot_to_another() {
    let pool = pool(1);
    let held = pool.try_acquire("g").unwrap();
    let mut first = Box::pin(pool.acquire_timeout("g", Duration::from_secs(10)));
    let mut second = Box::pin(pool.acquire_timeout("g", Duration::from_secs(10)));
    let mut cx = Context::from_waker(Waker::noop());
    assert!(first.as_mut().poll(&mut cx).is_pending());
    assert!(second.as_mut().poll(&mut cx).is_pending());
    assert_eq!(pool.queued("g"), 2);

    drop(held);
    drop(first);
    assert_eq!(pool.queued("g"), 1);
    let Poll::Ready(Ok(permit)) = second.as_mut().poll(&mut cx) else {
        panic!("the other waiter must get the released slot without another release");
    };
    assert_eq!(pool.queued("g"), 0);
    drop(permit);
    assert_eq!(pool.running("g", "a"), Some(0));
}

#[tokio::test(start_paused = true)]
async fn expired_notified_waiter_hands_slot_to_another() {
    let pool = pool(1);
    let held = pool.try_acquire("g").unwrap();
    let mut first = Box::pin(pool.acquire_timeout("g", Duration::from_millis(10)));
    let mut second = Box::pin(pool.acquire_timeout("g", Duration::from_secs(1)));
    let mut cx = Context::from_waker(Waker::noop());
    assert!(first.as_mut().poll(&mut cx).is_pending());
    assert!(second.as_mut().poll(&mut cx).is_pending());
    drop(held);
    tokio::time::advance(Duration::from_millis(10)).await;
    assert!(matches!(
        first.as_mut().poll(&mut cx),
        Poll::Ready(Err(AcquireError::Timeout))
    ));
    assert!(matches!(second.as_mut().poll(&mut cx), Poll::Ready(Ok(_))));
    assert_eq!(pool.queued("g"), 0);
}

#[tokio::test(start_paused = true)]
async fn notifications_do_not_restart_timeout() {
    let pool = pool(0);
    let started = tokio::time::Instant::now();
    let task_pool = Arc::clone(&pool);
    let waiter = tokio::spawn(async move {
        task_pool
            .acquire_timeout("g", Duration::from_millis(100))
            .await
    });
    tokio::task::yield_now().await;
    assert_eq!(pool.queued("g"), 1);
    for _ in 0..2 {
        tokio::time::advance(Duration::from_millis(40)).await;
        pool.set_max_running("g", "a", 0);
        tokio::task::yield_now().await;
        assert_eq!(pool.queued("g"), 1);
    }
    assert_eq!(waiter.await.unwrap().err(), Some(AcquireError::Timeout));
    assert_eq!(started.elapsed(), Duration::from_millis(100));
    assert_eq!(pool.queued("g"), 0);
}

#[tokio::test]
async fn increasing_capacity_wakes_all_newly_available_slots() {
    let pool = pool(0);
    let mut futures: Vec<_> = (0..3)
        .map(|_| Box::pin(pool.acquire_timeout("g", Duration::from_secs(10))))
        .collect();
    let mut cx = Context::from_waker(Waker::noop());
    for future in &mut futures {
        assert!(future.as_mut().poll(&mut cx).is_pending());
    }
    assert_eq!(pool.queued("g"), 3);
    pool.set_max_running("g", "a", 3);
    let mut permits = Vec::new();
    for future in &mut futures {
        let Poll::Ready(Ok(permit)) = future.as_mut().poll(&mut cx) else {
            panic!("capacity increase must make each waiter ready");
        };
        permits.push(permit);
    }
    assert_eq!(pool.running("g", "a"), Some(3));
    assert_eq!(pool.queued("g"), 0);
}

#[tokio::test]
async fn queues_are_independent_between_groups() {
    let pool = ClusterPool::new(
        PoolConfig::default()
            .group("g", vec![ClusterConfig::new("a", 1)])
            .group("other", vec![ClusterConfig::new("b", 1)]),
    );
    let held = pool.try_acquire("g").unwrap();
    let other = pool.try_acquire("other").unwrap();
    let mut waiter = Box::pin(pool.acquire_timeout("g", Duration::from_secs(10)));
    let mut cx = Context::from_waker(Waker::noop());
    assert!(waiter.as_mut().poll(&mut cx).is_pending());
    assert_eq!(pool.queued("g"), 1);
    assert_eq!(pool.queued("other"), 0);
    drop(other);
    assert!(waiter.as_mut().poll(&mut cx).is_pending());
    drop(held);
    assert!(matches!(waiter.as_mut().poll(&mut cx), Poll::Ready(Ok(_))));
    assert_eq!(pool.queued("g"), 0);
}

#[tokio::test]
async fn config_reload_wakes_waiters_and_keeps_their_queue() {
    let pool = pool(0);
    let mut waiter = Box::pin(pool.acquire_timeout("g", Duration::from_secs(10)));
    let mut cx = Context::from_waker(Waker::noop());
    assert!(waiter.as_mut().poll(&mut cx).is_pending());
    pool.reload(PoolConfig::default().group("g", vec![ClusterConfig::new("b", 0)]));
    assert_eq!(pool.queued("g"), 1);
    assert!(waiter.as_mut().poll(&mut cx).is_pending());
    pool.set_max_running("g", "b", 1);
    assert!(matches!(waiter.as_mut().poll(&mut cx), Poll::Ready(Ok(_))));

    let held = pool.try_acquire("g").unwrap();
    let mut removed = Box::pin(pool.acquire_timeout("g", Duration::from_secs(10)));
    assert!(removed.as_mut().poll(&mut cx).is_pending());
    pool.reload(PoolConfig::default());
    assert!(matches!(
        removed.as_mut().poll(&mut cx),
        Poll::Ready(Err(AcquireError::UnknownGroup))
    ));
    assert_eq!(pool.queued("g"), 0);
    drop(held);
}
