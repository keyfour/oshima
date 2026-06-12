//! # oshima-sync
//!
//! A synchronous, thread-per-actor runtime backend for Oshima.
//!
//! Each actor runs on its own OS thread and processes messages sequentially
//! from a bounded MPSC channel. No async runtime required.
//!
//! # When to use
//! - Simple applications that don't need async I/O.
//! - CPU-bound actors where dedicating a full thread makes sense.
//! - Testing: easy to reason about, no executor setup needed.

pub mod addr;
pub mod context;
pub mod runtime;

pub use addr::SyncAddr;
pub use context::SyncContext;
pub use runtime::SyncRuntime;

/// Re-export core prelude alongside sync-specific types for convenience.
pub mod prelude {
    pub use crate::{SyncAddr, SyncContext, SyncRuntime};
    pub use oshima_core::prelude::*;
}
