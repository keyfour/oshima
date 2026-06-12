//! Integration tests for the `oshima-tokio` (async task) backend.

mod common;
use common::*;
use oshima_tokio::prelude::*;

// ---------------------------------------------------------------------------
// Counter tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tokio_counter_add() {
    let addr = TokioRuntime::spawn(Counter::new(0));
    let result = addr.send(Add(5)).await.expect("send failed");
    assert_eq!(result, 5);
}

#[tokio::test]
async fn tokio_counter_sequential_messages() {
    let addr = TokioRuntime::spawn(Counter::new(0));

    assert_eq!(addr.send(Add(10)).await.unwrap(), 10);
    assert_eq!(addr.send(Add(5)).await.unwrap(), 15);
    assert_eq!(addr.send(Sub(3)).await.unwrap(), 12);
    assert_eq!(addr.send(GetValue).await.unwrap(), 12);
}

#[tokio::test]
async fn tokio_counter_reset() {
    let addr = TokioRuntime::spawn(Counter::new(100));
    addr.send(Reset).await.unwrap();
    assert_eq!(addr.send(GetValue).await.unwrap(), 0);
}

#[tokio::test]
async fn tokio_counter_saturating_sub() {
    let addr = TokioRuntime::spawn(Counter::new(3));
    let result = addr.send(Sub(100)).await.unwrap();
    assert_eq!(result, 0, "subtraction should saturate at zero");
}

// ---------------------------------------------------------------------------
// Multiple independent actors
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tokio_multiple_independent_actors() {
    let a = TokioRuntime::spawn(Counter::new(0));
    let b = TokioRuntime::spawn(Counter::new(100));

    a.send(Add(10)).await.unwrap();
    b.send(Sub(50)).await.unwrap();

    assert_eq!(a.send(GetValue).await.unwrap(), 10);
    assert_eq!(b.send(GetValue).await.unwrap(), 50);
}

// ---------------------------------------------------------------------------
// Cloned addresses
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tokio_cloned_addresses_share_actor() {
    let addr1 = TokioRuntime::spawn(Counter::new(0));
    let addr2 = addr1.clone();

    addr1.send(Add(7)).await.unwrap();
    addr2.send(Add(3)).await.unwrap();

    assert_eq!(addr1.send(GetValue).await.unwrap(), 10);
}

// ---------------------------------------------------------------------------
// Actor self-stop
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tokio_actor_stops_via_context() {
    let addr = TokioRuntime::spawn(Counter::new(0));
    addr.send(StopSelf).await.unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let result = addr.send(GetValue).await;
    assert!(result.is_err(), "actor should have stopped");
}

// ---------------------------------------------------------------------------
// do_send (async fire-and-forget)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tokio_do_send_fire_and_forget() {
    let addr = TokioRuntime::spawn(Counter::new(0));

    addr.do_send(Add(42)).await.unwrap();

    let value = addr.send(GetValue).await.unwrap();
    assert_eq!(value, 42);
}

// ---------------------------------------------------------------------------
// Echo actor
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tokio_echo_actor() {
    let addr = TokioRuntime::spawn(Echo);
    let msg = "Hello async Oshima!".to_string();
    let reply = addr.send(EchoMsg(msg.clone())).await.unwrap();
    assert_eq!(reply, msg);
}

// ---------------------------------------------------------------------------
// Accumulator
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tokio_accumulator_batch_and_flush() {
    let addr = TokioRuntime::spawn(Accumulator { items: vec![] });

    for i in 0..5 {
        addr.do_send(Push(i)).await.unwrap();
    }

    let items = addr.send(Flush).await.unwrap();
    assert_eq!(items, vec![0, 1, 2, 3, 4]);
}

// ---------------------------------------------------------------------------
// Concurrent async senders
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tokio_concurrent_senders() {
    let addr = TokioRuntime::spawn(Counter::new(0));

    let handles: Vec<_> = (0..10)
        .map(|_| {
            let a = addr.clone();
            tokio::spawn(async move {
                a.send(Add(1)).await.unwrap();
            })
        })
        .collect();

    for h in handles {
        h.await.unwrap();
    }

    let total = addr.send(GetValue).await.unwrap();
    assert_eq!(total, 10);
}

// ---------------------------------------------------------------------------
// try_send (non-async, non-blocking)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tokio_try_send_succeeds_when_room() {
    let addr = TokioRuntime::spawn(Counter::new(0));
    addr.try_send(Add(1)).expect("mailbox should have room");
    let value = addr.send(GetValue).await.unwrap();
    assert_eq!(value, 1);
}
