//! Integration tests for the `oshima-sync` (threaded) backend.

mod common;
use common::*;
use oshima_sync::prelude::*;

// ---------------------------------------------------------------------------
// Counter tests
// ---------------------------------------------------------------------------

#[test]
fn sync_counter_add() {
    let addr = SyncRuntime::spawn(Counter::new(0));
    let result = addr.send(Add(5)).expect("send failed");
    assert_eq!(result, 5);
}

#[test]
fn sync_counter_sequential_messages() {
    let addr = SyncRuntime::spawn(Counter::new(0));

    assert_eq!(addr.send(Add(10)).unwrap(), 10);
    assert_eq!(addr.send(Add(5)).unwrap(), 15);
    assert_eq!(addr.send(Sub(3)).unwrap(), 12);
    assert_eq!(addr.send(GetValue).unwrap(), 12);
}

#[test]
fn sync_counter_reset() {
    let addr = SyncRuntime::spawn(Counter::new(100));
    addr.send(Reset).unwrap();
    assert_eq!(addr.send(GetValue).unwrap(), 0);
}

#[test]
fn sync_counter_saturating_sub() {
    let addr = SyncRuntime::spawn(Counter::new(3));
    let result = addr.send(Sub(100)).unwrap();
    assert_eq!(result, 0, "subtraction should saturate at zero");
}

// ---------------------------------------------------------------------------
// Multiple independent actors
// ---------------------------------------------------------------------------

#[test]
fn sync_multiple_independent_actors() {
    let a = SyncRuntime::spawn(Counter::new(0));
    let b = SyncRuntime::spawn(Counter::new(100));

    a.send(Add(10)).unwrap();
    b.send(Sub(50)).unwrap();

    assert_eq!(a.send(GetValue).unwrap(), 10);
    assert_eq!(b.send(GetValue).unwrap(), 50);
}

// ---------------------------------------------------------------------------
// Clone address and send from multiple handles
// ---------------------------------------------------------------------------

#[test]
fn sync_cloned_addresses_share_actor() {
    let addr1 = SyncRuntime::spawn(Counter::new(0));
    let addr2 = addr1.clone();

    addr1.send(Add(7)).unwrap();
    addr2.send(Add(3)).unwrap();

    // Both go to the same actor — total must be 10
    assert_eq!(addr1.send(GetValue).unwrap(), 10);
}

// ---------------------------------------------------------------------------
// Actor self-stop via ctx.stop()
// ---------------------------------------------------------------------------

#[test]
fn sync_actor_stops_via_context() {
    let addr = SyncRuntime::spawn(Counter::new(0));
    addr.send(StopSelf).unwrap();

    // Give the thread a moment to shut down
    std::thread::sleep(std::time::Duration::from_millis(50));

    // After stopping, the mailbox is disconnected
    let result = addr.send(GetValue);
    assert!(result.is_err(), "actor should have stopped");
}

// ---------------------------------------------------------------------------
// do_send (fire and forget)
// ---------------------------------------------------------------------------

#[test]
fn sync_do_send_fire_and_forget() {
    let addr = SyncRuntime::spawn(Counter::new(0));

    addr.do_send(Add(42)).unwrap();

    // Need to synchronise — send a blocking message to ensure do_send was processed
    let value = addr.send(GetValue).unwrap();
    assert_eq!(value, 42);
}

// ---------------------------------------------------------------------------
// Echo actor
// ---------------------------------------------------------------------------

#[test]
fn sync_echo_actor() {
    let addr = SyncRuntime::spawn(Echo);
    let msg = "Hello from Oshima!".to_string();
    let reply = addr.send(EchoMsg(msg.clone())).unwrap();
    assert_eq!(reply, msg);
}

// ---------------------------------------------------------------------------
// Accumulator — batch messages then flush
// ---------------------------------------------------------------------------

#[test]
fn sync_accumulator_batch_and_flush() {
    let addr = SyncRuntime::spawn(Accumulator { items: vec![] });

    for i in 0..5 {
        addr.do_send(Push(i)).unwrap();
    }

    let items = addr.send(Flush).unwrap();
    assert_eq!(items, vec![0, 1, 2, 3, 4]);
}

// ---------------------------------------------------------------------------
// Concurrent senders
// ---------------------------------------------------------------------------

#[test]
fn sync_concurrent_senders() {
    use std::thread;

    let addr = SyncRuntime::spawn(Counter::new(0));

    let handles: Vec<_> = (0..10)
        .map(|_| {
            let a = addr.clone();
            thread::spawn(move || {
                a.send(Add(1)).unwrap();
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    let total = addr.send(GetValue).unwrap();
    assert_eq!(total, 10, "all 10 increments must be reflected");
}

// ---------------------------------------------------------------------------
// Error paths — exact SendError variants
// ---------------------------------------------------------------------------

#[test]
fn sync_send_to_stopped_actor_returns_actor_stopped() {
    let addr = SyncRuntime::spawn(Counter::new(0));
    addr.send(StopSelf).unwrap();

    std::thread::sleep(std::time::Duration::from_millis(50));

    let err = addr.send(GetValue).unwrap_err();
    assert_eq!(err, SendError::ActorStopped);
}

#[test]
fn sync_do_send_to_stopped_actor_returns_actor_stopped() {
    let addr = SyncRuntime::spawn(Counter::new(0));
    addr.send(StopSelf).unwrap();

    std::thread::sleep(std::time::Duration::from_millis(50));

    let err = addr.do_send(Add(1)).unwrap_err();
    assert_eq!(err, SendError::ActorStopped);
}

// ---------------------------------------------------------------------------
// Mailbox full — do_send on a bounded mailbox returns MailboxFull and the
// rejected message must never be processed
// ---------------------------------------------------------------------------

#[test]
fn sync_do_send_full_mailbox_returns_mailbox_full() {
    let (actor, gate) = GatedCounter::new(0);
    let addr = SyncRuntime::spawn_with_capacity(actor, 1);

    // First send: the actor picks it up and blocks inside the handler.
    addr.do_send(GatedAdd(1))
        .expect("send must fit the empty mailbox");

    // Wait until the actor is blocked inside the first message handler,
    // so the mailbox (capacity 1) can be filled deterministically.
    while !gate.entered.load(std::sync::atomic::Ordering::SeqCst) {
        std::thread::sleep(std::time::Duration::from_millis(1));
    }

    addr.do_send(GatedAdd(10))
        .expect("second send must fit capacity 1");

    let err = addr.do_send(GatedAdd(100)).unwrap_err();
    assert_eq!(err, SendError::MailboxFull);

    // Unblock the actor; Add(1) + Add(10) must be processed, the rejected
    // Add(100) must not.
    gate.release
        .store(true, std::sync::atomic::Ordering::SeqCst);
    assert_eq!(addr.send(GetValue).unwrap(), 11);
}

// ---------------------------------------------------------------------------
// Lifecycle hooks
// ---------------------------------------------------------------------------

#[test]
fn sync_lifecycle_hooks_run_on_self_stop() {
    let log = LifecycleLog::new();
    let addr = SyncRuntime::spawn(LifecycleActor { log: log.clone() });

    addr.send(StopSelf).unwrap();
    log.wait_stopped();

    let (started, stopping, stopped, vetoes) = log.counts();
    assert_eq!((started, stopping, stopped, vetoes), (1, 1, 1, 0));
}

#[test]
fn sync_all_senders_dropped_stops_actor_gracefully() {
    let log = LifecycleLog::new();
    let addr = SyncRuntime::spawn(LifecycleActor { log: log.clone() });

    // Dropping the last sender disconnects the mailbox — the runtime must
    // shut the actor down through the full lifecycle.
    drop(addr);
    log.wait_stopped();

    let (started, stopping, stopped, vetoes) = log.counts();
    assert_eq!((started, stopping, stopped, vetoes), (1, 1, 1, 0));
}

#[test]
fn sync_stopping_veto_resumes_then_stops() {
    let log = LifecycleLog::new();
    let addr = SyncRuntime::spawn(VetoCounter::new(log.clone()));

    addr.send(StopSelf).unwrap();
    log.wait_stopped();

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

#[test]
fn sync_handler_panic_reports_actor_stopped() {
    let addr = SyncRuntime::spawn(PanicActor);

    addr.do_send(PanicNow).unwrap();

    // Give the thread time to unwind
    std::thread::sleep(std::time::Duration::from_millis(100));

    let err = addr.do_send(PanicNow).unwrap_err();
    assert_eq!(err, SendError::ActorStopped);
}
