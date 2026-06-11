//! Type-erased message envelopes used internally by runtime backends.
//!
//! An `Envelope` boxes a concrete message + its one-shot reply sender so
//! that a mailbox channel can carry `Box<dyn EnvelopeProxy<A, C>>` — a
//! single heterogeneous queue of any messages the actor handles.
//!
//! This module is `std`-only (requires heap allocation). `no_std` backends
//! use statically-typed, fixed-message-type queues instead.

use crate::traits::{ActorBase, ActorContext, Handler, Message};

/// Object-safe dispatch trait letting a runtime deliver a boxed message to an
/// actor without knowing the concrete message type at the call site.
#[cfg(feature = "std")]
pub trait EnvelopeProxy<A, C>: Send
where
    A: ActorBase,
    C: ActorContext<A>,
{
    /// Deliver the contained message to `actor`, routing the result to
    /// whatever reply channel is stored inside this envelope (if any).
    fn handle(self: Box<Self>, actor: &mut A, ctx: &mut C);
}

/// A concrete envelope wrapping message `M` destined for actor `A`.
///
/// Optionally holds a one-shot reply sender (`tx`) so callers can block on
/// the result. Fire-and-forget sends leave `tx` as `None`.
#[cfg(feature = "std")]
pub struct Envelope<A, C, M>
where
    A: Handler<C, M> + Send,
    C: ActorContext<A> + Send,
    M: Message,
{
    pub(crate) msg: Option<M>,
    pub(crate) tx: Option<std::sync::mpsc::SyncSender<M::Result>>,
    _phantom: core::marker::PhantomData<fn(A, C)>,
}

#[cfg(feature = "std")]
impl<A, C, M> Envelope<A, C, M>
where
    A: Handler<C, M> + Send,
    C: ActorContext<A> + Send,
    M: Message,
    M::Result: Send,
{
    /// Wrap a message without a reply channel (fire-and-forget).
    pub fn new(msg: M) -> Self {
        Self { msg: Some(msg), tx: None, _phantom: core::marker::PhantomData }
    }

    /// Wrap a message with a reply channel so the sender can wait for the result.
    pub fn with_reply(msg: M, tx: std::sync::mpsc::SyncSender<M::Result>) -> Self {
        Self { msg: Some(msg), tx: Some(tx), _phantom: core::marker::PhantomData }
    }
}

#[cfg(feature = "std")]
impl<A, C, M> EnvelopeProxy<A, C> for Envelope<A, C, M>
where
    A: Handler<C, M> + Send,
    C: ActorContext<A> + Send,
    M: Message + Send,
    M::Result: Send,
{
    fn handle(mut self: Box<Self>, actor: &mut A, ctx: &mut C) {
        if let Some(msg) = self.msg.take() {
            let result = actor.handle(msg, ctx);
            if let Some(tx) = self.tx.take() { let _ = tx.send(result); }
        }
    }
}

/// Stub type used on `no_std` targets where heap allocation is unavailable.
#[cfg(not(feature = "std"))]
pub struct Envelope<A, C, M>(core::marker::PhantomData<(A, C, M)>);

/// Stub trait for `no_std` builds.
#[cfg(not(feature = "std"))]
pub trait EnvelopeProxy<A, C> {}
