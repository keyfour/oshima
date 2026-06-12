#![cfg_attr(not(feature = "std"), no_std)]
#![forbid(unsafe_code)]
#![warn(missing_docs)]

//! # oshima-core
//!
//! The portable, runtime-agnostic heart of the Oshima actor framework.

pub mod envelope;
pub mod error;
pub mod traits;

pub use envelope::{Envelope, EnvelopeProxy};
pub use error::SendError;
pub use traits::{Actor, ActorBase, ActorContext, Handler, Message, Running};

/// Convenience re-export of everything needed to write actors.
pub mod prelude {
    pub use crate::error::SendError;
    pub use crate::traits::{Actor, ActorBase, ActorContext, Handler, Message, Running};
}
