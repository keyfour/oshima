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
