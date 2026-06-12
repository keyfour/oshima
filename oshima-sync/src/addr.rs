use crate::context::SyncContext;
use oshima_core::envelope::{Envelope, EnvelopeProxy};
use oshima_core::error::SendError;
use oshima_core::traits::{Actor, ActorBase, Handler, Message};
use std::sync::mpsc::{self, SyncSender};

pub struct SyncAddr<A: ActorBase + Actor<SyncContext<A>> + Send> {
    pub(crate) tx: SyncSender<Box<dyn EnvelopeProxy<A, SyncContext<A>> + Send>>,
}

impl<A: ActorBase + Actor<SyncContext<A>> + Send> Clone for SyncAddr<A> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
        }
    }
}

impl<A: ActorBase + Actor<SyncContext<A>> + Send> SyncAddr<A> {
    /// Send a message and block the calling thread until the actor replies.
    pub fn send<M>(&self, msg: M) -> Result<M::Result, SendError>
    where
        A: Handler<SyncContext<A>, M>,
        M: Message + Send + 'static,
        M::Result: Send + 'static,
    {
        let (reply_tx, reply_rx) = mpsc::sync_channel(1);
        let envelope = Box::new(Envelope::<A, SyncContext<A>, M>::with_reply(msg, reply_tx));
        self.tx
            .send(envelope)
            .map_err(|_| SendError::ActorStopped)?;
        reply_rx.recv().map_err(|_| SendError::ActorStopped)
    }

    /// Fire-and-forget — returns immediately, does not wait for processing.
    pub fn do_send<M>(&self, msg: M) -> Result<(), SendError>
    where
        A: Handler<SyncContext<A>, M>,
        M: Message + Send + 'static,
        M::Result: Send + 'static,
    {
        let envelope = Box::new(Envelope::<A, SyncContext<A>, M>::new(msg));
        self.tx.try_send(envelope).map_err(|e| match e {
            mpsc::TrySendError::Full(_) => SendError::MailboxFull,
            mpsc::TrySendError::Disconnected(_) => SendError::ActorStopped,
        })
    }
}
