//! # Oshima
//!
//! A runtime-agnostic actor framework for Rust.
//!
//! Actors are defined once using traits from `oshima-core`.
//! The runtime backend is chosen via Cargo feature flags — actor logic never changes.
//!
//! ## Defining an actor (runtime-agnostic)
//!
//! ```rust
//! use oshima_core::prelude::*;
//!
//! // 1. Define messages
//! struct Add(u32);
//! impl Message for Add { type Result = u32; }
//!
//! struct Reset;
//! impl Message for Reset { type Result = (); }
//!
//! // 2. Define the actor struct
//! struct Counter { value: u32 }
//!
//! // 3. Implement ActorBase (required marker) + Actor generic over any context C
//! impl ActorBase for Counter {}
//!
//! impl<C: ActorContext<Self>> Actor<C> for Counter {
//!     fn started(&mut self, _ctx: &mut C) {
//!         println!("Counter started with value {}", self.value);
//!     }
//! }
//!
//! // 4. One Handler impl covers ALL backends — no duplication
//! impl<C: ActorContext<Self>> Handler<C, Add> for Counter {
//!     fn handle(&mut self, msg: Add, _ctx: &mut C) -> u32 {
//!         self.value += msg.0;
//!         self.value
//!     }
//! }
//!
//! impl<C: ActorContext<Self>> Handler<C, Reset> for Counter {
//!     fn handle(&mut self, _msg: Reset, _ctx: &mut C) {
//!         self.value = 0;
//!     }
//! }
//! ```
//!
//! ## Running with the sync backend
//!
//! ```rust,ignore
//! use oshima_sync::prelude::*;
//! let addr = SyncRuntime::spawn(Counter { value: 0 });
//! let result = addr.send(Add(10)).unwrap();
//! assert_eq!(result, 10);
//! ```
//!
//! ## Running with the Tokio backend
//!
//! ```rust,ignore
//! use oshima_tokio::prelude::*;
//! #[tokio::main]
//! async fn main() {
//!     let addr = TokioRuntime::spawn(Counter { value: 0 });
//!     let result = addr.send(Add(10)).await.unwrap();
//!     assert_eq!(result, 10);
//! }
//! ```

pub use oshima_core as core;

#[cfg(feature = "runtime-sync")]
pub use oshima_sync as sync;

#[cfg(feature = "runtime-tokio")]
pub use oshima_tokio as tokio;
