//! Error types used across the Oshima framework.

use core::fmt;

/// Error returned when a message could not be delivered to an actor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendError {
    /// The actor's mailbox is full and not accepting new messages right now.
    MailboxFull,
    /// The actor has stopped; the message will never be processed.
    ActorStopped,
}

impl fmt::Display for SendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SendError::MailboxFull => write!(f, "actor mailbox is full"),
            SendError::ActorStopped => write!(f, "actor has stopped"),
        }
    }
}

#[cfg(feature = "std")]
impl std::error::Error for SendError {}
