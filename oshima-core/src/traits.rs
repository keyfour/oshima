//! Core traits for the Oshima actor model.
//!
//! These traits define the actor contract. They have zero runtime dependencies
//! and are safe to use in `no_std` environments.

// ---------------------------------------------------------------------------
// Running
// ---------------------------------------------------------------------------

/// Describes whether an actor wishes to keep running after its `stopping`
/// lifecycle hook is called.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Running {
    /// The actor should continue running.
    Continue,
    /// The actor should stop after the current message.
    #[default]
    Stop,
}

// ---------------------------------------------------------------------------
// Message
// ---------------------------------------------------------------------------

/// A typed message that can be sent to an actor.
///
/// Every message carries an associated `Result` type — the value the actor
/// returns after handling it. For fire-and-forget messages use `Result = ()`.
///
/// # Example
/// ```rust
/// use oshima_core::Message;
///
/// struct Ping;
/// impl Message for Ping {
///     type Result = bool;
/// }
/// ```
pub trait Message: 'static {
    /// The type returned by the actor after handling this message.
    type Result: 'static;
}

// ---------------------------------------------------------------------------
// ActorBase + ActorContext
// ---------------------------------------------------------------------------

/// Sealed marker automatically implemented by all `Actor<C>` types.
/// Used to break the circular bound between `Actor` and `ActorContext`.
pub trait ActorBase: Sized + 'static {}

/// The execution context passed to actor lifecycle hooks and handlers.
///
/// Each runtime backend provides its own concrete `Context<A>` type that
/// implements this trait. Actors interact with the runtime (stopping
/// themselves, etc.) exclusively through this interface — keeping actor code
/// runtime-agnostic.
pub trait ActorContext<A: ActorBase> {
    /// Signal that this actor should stop after finishing the current message.
    fn stop(&mut self);

    /// Returns `true` if the actor is still in the running state.
    fn is_running(&self) -> bool;
}

// ---------------------------------------------------------------------------
// Actor
// ---------------------------------------------------------------------------

/// The core trait every actor must implement.
///
/// An actor is any Rust struct that implements this trait. Its associated
/// `Context` type is provided by whichever runtime backend is in use — the
/// actor itself stays completely backend-agnostic.
///
/// # Lifecycle
/// ```text
///  ┌─────────┐  started()  ┌─────────┐  stopping()  ┌──────────┐
///  │ Created │ ──────────► │ Running │ ────────────► │ Stopping │
///  └─────────┘             └─────────┘               └──────────┘
///                                                          │
///                                              stopped()   ▼
///                                                     ┌─────────┐
///                                                     │ Stopped │
///                                                     └─────────┘
/// ```
///
/// # Example
/// ```rust
/// use oshima_core::{Actor, ActorBase, ActorContext};
///
/// struct Counter { value: u32 }
///
/// impl ActorBase for Counter {}
///
/// impl<C: ActorContext<Self>> Actor<C> for Counter {
///     fn started(&mut self, _ctx: &mut C) {
///         println!("Counter started at {}", self.value);
///     }
/// }
/// ```
pub trait Actor<C: ActorContext<Self>>: ActorBase {
    /// Called once when the actor starts, before processing any messages.
    fn started(&mut self, _ctx: &mut C) {}

    /// Called when the actor is about to stop.
    /// Return [`Running::Continue`] to cancel the stop and stay alive.
    fn stopping(&mut self, _ctx: &mut C) -> Running {
        Running::Stop
    }

    /// Called after the actor has fully stopped. No more messages will arrive.
    fn stopped(&mut self, _ctx: &mut C) {}
}

// ---------------------------------------------------------------------------
// Handler
// ---------------------------------------------------------------------------

/// Implemented by an actor to declare it can process a message of type `M`.
///
/// An actor can handle any number of distinct message types — each gets its
/// own `Handler<C, M>` impl.
///
/// # Example
/// ```rust
/// use oshima_core::{Actor, ActorBase, ActorContext, Handler, Message};
///
/// struct Increment(u32);
/// impl Message for Increment { type Result = u32; }
///
/// struct Counter { value: u32 }
/// impl ActorBase for Counter {}
/// impl<C: ActorContext<Self>> Actor<C> for Counter {}
///
/// impl<C: ActorContext<Self>> Handler<C, Increment> for Counter {
///     fn handle(&mut self, msg: Increment, _ctx: &mut C) -> u32 {
///         self.value += msg.0;
///         self.value
///     }
/// }
/// ```
pub trait Handler<C, M>: Actor<C>
where
    C: ActorContext<Self>,
    M: Message,
{
    /// Process the message and return its result.
    fn handle(&mut self, msg: M, ctx: &mut C) -> M::Result;
}
