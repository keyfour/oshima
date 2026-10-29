//! Shared actor definitions reused across both backend test suites.
//!
//! ZERO imports from any runtime crate — only oshima_core.
//! This file is the proof of concept: actors are defined once and
//! run on any backend chosen at the call site.

use oshima_core::prelude::*;

// ---------------------------------------------------------------------------
// Counter actor
// ---------------------------------------------------------------------------

pub struct Counter {
    pub value: u32,
}

impl Counter {
    pub fn new(v: u32) -> Self {
        Self { value: v }
    }
}

// ActorBase satisfies the marker bound required by ActorContext<A>.
impl ActorBase for Counter {}

// Messages
pub struct Add(pub u32);
impl Message for Add {
    type Result = u32;
}

pub struct Sub(pub u32);
impl Message for Sub {
    type Result = u32;
}

pub struct GetValue;
impl Message for GetValue {
    type Result = u32;
}

pub struct Reset;
impl Message for Reset {
    type Result = ();
}

pub struct StopSelf;
impl Message for StopSelf {
    type Result = ();
}

// Actor impl — generic over C, so the same impl works for every backend.
impl<C: ActorContext<Self>> Actor<C> for Counter {
    fn started(&mut self, _ctx: &mut C) {}
    fn stopped(&mut self, _ctx: &mut C) {}
}

impl<C: ActorContext<Self>> Handler<C, Add> for Counter {
    fn handle(&mut self, msg: Add, _ctx: &mut C) -> u32 {
        self.value += msg.0;
        self.value
    }
}

impl<C: ActorContext<Self>> Handler<C, Sub> for Counter {
    fn handle(&mut self, msg: Sub, _ctx: &mut C) -> u32 {
        self.value = self.value.saturating_sub(msg.0);
        self.value
    }
}

impl<C: ActorContext<Self>> Handler<C, GetValue> for Counter {
    fn handle(&mut self, _msg: GetValue, _ctx: &mut C) -> u32 {
        self.value
    }
}

impl<C: ActorContext<Self>> Handler<C, Reset> for Counter {
    fn handle(&mut self, _msg: Reset, _ctx: &mut C) {
        self.value = 0;
    }
}

impl<C: ActorContext<Self>> Handler<C, StopSelf> for Counter {
    fn handle(&mut self, _msg: StopSelf, ctx: &mut C) {
        ctx.stop();
    }
}

// ---------------------------------------------------------------------------
// Echo actor — bounces messages back to the sender
// ---------------------------------------------------------------------------

pub struct Echo;
impl ActorBase for Echo {}

pub struct EchoMsg(pub String);
impl Message for EchoMsg {
    type Result = String;
}

impl<C: ActorContext<Self>> Actor<C> for Echo {}

impl<C: ActorContext<Self>> Handler<C, EchoMsg> for Echo {
    fn handle(&mut self, msg: EchoMsg, _ctx: &mut C) -> String {
        msg.0
    }
}

// ---------------------------------------------------------------------------
// Accumulator actor — collects values, returns all on Flush
// ---------------------------------------------------------------------------

pub struct Accumulator {
    pub items: Vec<u32>,
}
impl ActorBase for Accumulator {}

pub struct Push(pub u32);
impl Message for Push {
    type Result = ();
}

pub struct Flush;
impl Message for Flush {
    type Result = Vec<u32>;
}

impl<C: ActorContext<Self>> Actor<C> for Accumulator {}

impl<C: ActorContext<Self>> Handler<C, Push> for Accumulator {
    fn handle(&mut self, msg: Push, _ctx: &mut C) {
        self.items.push(msg.0);
    }
}

impl<C: ActorContext<Self>> Handler<C, Flush> for Accumulator {
    fn handle(&mut self, _msg: Flush, _ctx: &mut C) -> Vec<u32> {
        std::mem::take(&mut self.items)
    }
}

// ---------------------------------------------------------------------------
// Lifecycle probe actor — records every lifecycle hook invocation
// ---------------------------------------------------------------------------

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

/// Shared log of lifecycle hook invocations, observable from the test thread.
#[derive(Debug, Default)]
pub struct LifecycleLog {
    pub started: AtomicUsize,
    pub stopping: AtomicUsize,
    pub stopped: AtomicUsize,
    pub vetoes: AtomicUsize,
}

impl LifecycleLog {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Returns `(started, stopping, stopped, vetoes)` counts.
    pub fn counts(&self) -> (usize, usize, usize, usize) {
        (
            self.started.load(Ordering::SeqCst),
            self.stopping.load(Ordering::SeqCst),
            self.stopped.load(Ordering::SeqCst),
            self.vetoes.load(Ordering::SeqCst),
        )
    }

    /// Spin-wait until `stopped` has been recorded (bails out after ~2s).
    #[allow(dead_code)] // used only by the threaded (sync) test suite
    pub fn wait_stopped(&self) {
        for _ in 0..2000 {
            if self.stopped.load(Ordering::SeqCst) > 0 {
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        panic!("actor did not stop within 2s");
    }
}

/// No-logic actor whose sole purpose is to exercise lifecycle hooks.
pub struct LifecycleActor {
    pub log: Arc<LifecycleLog>,
}

impl ActorBase for LifecycleActor {}

impl<C: ActorContext<Self>> Actor<C> for LifecycleActor {
    fn started(&mut self, _ctx: &mut C) {
        self.log.started.fetch_add(1, Ordering::SeqCst);
    }

    fn stopping(&mut self, _ctx: &mut C) -> Running {
        self.log.stopping.fetch_add(1, Ordering::SeqCst);
        Running::Stop
    }

    fn stopped(&mut self, _ctx: &mut C) {
        self.log.stopped.fetch_add(1, Ordering::SeqCst);
    }
}

impl<C: ActorContext<Self>> Handler<C, StopSelf> for LifecycleActor {
    fn handle(&mut self, _msg: StopSelf, ctx: &mut C) {
        ctx.stop();
    }
}

/// Counter that vetoes its first shutdown attempt, then stops on the retry.
/// Covers the `Running::Continue` veto branch of the runtimes' shutdown path.
pub struct VetoCounter {
    pub log: Arc<LifecycleLog>,
    vetoed: AtomicBool,
}

impl VetoCounter {
    pub fn new(log: Arc<LifecycleLog>) -> Self {
        Self {
            log,
            vetoed: AtomicBool::new(false),
        }
    }
}

impl ActorBase for VetoCounter {}

impl<C: ActorContext<Self>> Actor<C> for VetoCounter {
    fn started(&mut self, _ctx: &mut C) {
        self.log.started.fetch_add(1, Ordering::SeqCst);
    }

    fn stopping(&mut self, _ctx: &mut C) -> Running {
        self.log.stopping.fetch_add(1, Ordering::SeqCst);
        if !self.vetoed.swap(true, Ordering::SeqCst) {
            self.log.vetoes.fetch_add(1, Ordering::SeqCst);
            Running::Continue
        } else {
            Running::Stop
        }
    }

    fn stopped(&mut self, _ctx: &mut C) {
        self.log.stopped.fetch_add(1, Ordering::SeqCst);
    }
}

impl<C: ActorContext<Self>> Handler<C, StopSelf> for VetoCounter {
    fn handle(&mut self, _msg: StopSelf, ctx: &mut C) {
        ctx.stop();
    }
}

// ---------------------------------------------------------------------------
// Gated counter — `GatedAdd` handler blocks until released from the test,
// letting the test occupy the actor and fill the mailbox deterministically
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
pub struct HandlerGate {
    pub entered: AtomicBool,
    pub release: AtomicBool,
}

/// Counter used for mailbox-capacity (MailboxFull) tests.
pub struct GatedCounter {
    value: u32,
    gate: Arc<HandlerGate>,
}

pub struct GatedAdd(pub u32);
impl Message for GatedAdd {
    type Result = u32;
}

impl GatedCounter {
    /// Returns the actor and the gate shared with the test.
    pub fn new(value: u32) -> (Self, Arc<HandlerGate>) {
        let gate = Arc::new(HandlerGate::default());
        (
            Self {
                value,
                gate: gate.clone(),
            },
            gate,
        )
    }
}

impl ActorBase for GatedCounter {}

impl<C: ActorContext<Self>> Actor<C> for GatedCounter {}

impl<C: ActorContext<Self>> Handler<C, GatedAdd> for GatedCounter {
    fn handle(&mut self, msg: GatedAdd, _ctx: &mut C) -> u32 {
        self.gate.entered.store(true, Ordering::SeqCst);
        while !self.gate.release.load(Ordering::SeqCst) {
            std::thread::yield_now();
        }
        self.value += msg.0;
        self.value
    }
}

impl<C: ActorContext<Self>> Handler<C, GetValue> for GatedCounter {
    fn handle(&mut self, _msg: GetValue, _ctx: &mut C) -> u32 {
        self.value
    }
}

// ---------------------------------------------------------------------------
// Panic actor — handler panics; used to test crash resilience
// ---------------------------------------------------------------------------

pub struct PanicActor;
impl ActorBase for PanicActor {}

impl<C: ActorContext<Self>> Actor<C> for PanicActor {}

pub struct PanicNow;
impl Message for PanicNow {
    type Result = ();
}

impl<C: ActorContext<Self>> Handler<C, PanicNow> for PanicActor {
    fn handle(&mut self, _msg: PanicNow, _ctx: &mut C) {
        panic!("intentional test panic");
    }
}
