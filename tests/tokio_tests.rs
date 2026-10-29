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

// ---------------------------------------------------------------------------
// Error paths — exact SendError variants
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tokio_send_to_stopped_actor_returns_actor_stopped() {
    let addr = TokioRuntime::spawn(Counter::new(0));
    addr.send(StopSelf).await.unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let err = addr.send(GetValue).await.unwrap_err();
    assert_eq!(err, SendError::ActorStopped);
}

#[tokio::test]
async fn tokio_do_send_to_stopped_actor_returns_actor_stopped() {
    let addr = TokioRuntime::spawn(Counter::new(0));
    addr.send(StopSelf).await.unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let err = addr.do_send(Add(1)).await.unwrap_err();
    assert_eq!(err, SendError::ActorStopped);
}

#[tokio::test]
async fn tokio_try_send_to_stopped_actor_returns_actor_stopped() {
    let addr = TokioRuntime::spawn(Counter::new(0));
    addr.send(StopSelf).await.unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let err = addr.try_send(Add(1)).unwrap_err();
    assert_eq!(err, SendError::ActorStopped);
}

// ---------------------------------------------------------------------------
// Mailbox full — try_send on a bounded mailbox returns MailboxFull and the
// rejected message must never be processed
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tokio_try_send_full_mailbox_returns_mailbox_full() {
    let (actor, gate) = GatedCounter::new(0);
    let addr = TokioRuntime::spawn_with_capacity(actor, 1);

    // First send: the actor picks it up and blocks inside the handler.
    addr.try_send(GatedAdd(1))
        .expect("send must fit the empty mailbox");

    // Wait until the actor is blocked inside the first message handler,
    // so the mailbox (capacity 1) can be filled deterministically.
    while !gate.entered.load(std::sync::atomic::Ordering::SeqCst) {
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    }

    addr.try_send(GatedAdd(10))
        .expect("second send must fit capacity 1");

    let err = addr.try_send(GatedAdd(100)).unwrap_err();
    assert_eq!(err, SendError::MailboxFull);

    // Unblock the actor; Add(1) + Add(10) must be processed, the rejected
    // Add(100) must not.
    gate.release
        .store(true, std::sync::atomic::Ordering::SeqCst);
    assert_eq!(addr.send(GetValue).await.unwrap(), 11);
}

// ---------------------------------------------------------------------------
// Lifecycle hooks
// ---------------------------------------------------------------------------

async fn wait_for_stopped(log: &LifecycleLog) {
    for _ in 0..200 {
        if log.stopped.load(std::sync::atomic::Ordering::SeqCst) > 0 {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    panic!("actor did not stop within 2s");
}

#[tokio::test]
async fn tokio_lifecycle_hooks_run_on_self_stop() {
    let log = LifecycleLog::new();
    let addr = TokioRuntime::spawn(LifecycleActor { log: log.clone() });

    addr.send(StopSelf).await.unwrap();
    wait_for_stopped(&log).await;

    let (started, stopping, stopped, vetoes) = log.counts();
    assert_eq!((started, stopping, stopped, vetoes), (1, 1, 1, 0));
}

#[tokio::test]
async fn tokio_all_senders_dropped_stops_actor_gracefully() {
    let log = LifecycleLog::new();
    let addr = TokioRuntime::spawn(LifecycleActor { log: log.clone() });

    // Dropping the last sender closes the mailbox — the runtime must shut
    // the actor down through the full lifecycle.
    drop(addr);
    wait_for_stopped(&log).await;

    let (started, stopping, stopped, vetoes) = log.counts();
    assert_eq!((started, stopping, stopped, vetoes), (1, 1, 1, 0));
}

#[tokio::test]
async fn tokio_stopping_veto_resumes_then_stops() {
    let log = LifecycleLog::new();
    let addr = TokioRuntime::spawn(VetoCounter::new(log.clone()));

    addr.send(StopSelf).await.unwrap();
    wait_for_stopped(&log).await;

    let (started, stopping, stopped, vetoes) = log.counts();
    assert_eq!(started, 1);
    assert_eq!(vetoes, 1, "the first stopping() must veto the shutdown");
    assert_eq!(stopping, 2, "stopping() must be re-run after the veto");
    assert_eq!(stopped, 1, "stopped() must run exactly once");
}

// ---------------------------------------------------------------------------
// Panic resilience — a panicking handler must not wedge the runtime; later
// sends observe a disconnected mailbox (ActorStopped)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tokio_handler_panic_reports_actor_stopped() {
    let addr = TokioRuntime::spawn(PanicActor);

    addr.do_send(PanicNow).await.unwrap();

    // Give the task time to abort
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let err = addr.do_send(PanicNow).await.unwrap_err();
    assert_eq!(err, SendError::ActorStopped);
}
