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
    assert_eq!(addr.send(Add(5)).unwrap(),  15);
    assert_eq!(addr.send(Sub(3)).unwrap(),  12);
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

    let handles: Vec<_> = (0..10).map(|_| {
        let a = addr.clone();
        thread::spawn(move || {
            a.send(Add(1)).unwrap();
        })
    }).collect();

    for h in handles { h.join().unwrap(); }

    let total = addr.send(GetValue).unwrap();
    assert_eq!(total, 10, "all 10 increments must be reflected");
}
