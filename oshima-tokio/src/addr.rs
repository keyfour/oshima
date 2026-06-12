use crate::context::TokioContext;
use oshima_core::error::SendError;
use oshima_core::traits::{Actor, ActorBase, Handler, Message};
use tokio::sync::{mpsc, oneshot};

pub(crate) trait TokioEnvelope<A>: Send
where
    A: ActorBase + Actor<TokioContext<A>> + Send,
{
    fn handle(self: Box<Self>, actor: &mut A, ctx: &mut TokioContext<A>);
}

pub(crate) struct TokioEnvelopeInner<A, M>
where
    A: ActorBase + Handler<TokioContext<A>, M> + Send,
    M: Message,
{
    pub msg: Option<M>,
    pub tx: Option<oneshot::Sender<M::Result>>,
    pub _actor: std::marker::PhantomData<A>,
}

impl<A, M> TokioEnvelope<A> for TokioEnvelopeInner<A, M>
where
    A: ActorBase + Handler<TokioContext<A>, M> + Send,
    M: Message + Send,
    M::Result: Send,
{
    fn handle(mut self: Box<Self>, actor: &mut A, ctx: &mut TokioContext<A>) {
        if let Some(msg) = self.msg.take() {
            let result = actor.handle(msg, ctx);
            if let Some(tx) = self.tx.take() {
                let _ = tx.send(result);
            }
        }
    }
}

pub struct TokioAddr<A>
where
    A: ActorBase + Actor<TokioContext<A>> + Send,
{
    pub(crate) tx: mpsc::Sender<Box<dyn TokioEnvelope<A> + Send>>,
}

impl<A> Clone for TokioAddr<A>
where
    A: ActorBase + Actor<TokioContext<A>> + Send,
{
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
        }
    }
}

impl<A> TokioAddr<A>
where
    A: ActorBase + Actor<TokioContext<A>> + Send,
{
    /// Send a message and `.await` the actor's response.
    pub async fn send<M>(&self, msg: M) -> Result<M::Result, SendError>
    where
        A: Handler<TokioContext<A>, M>,
        M: Message + Send + 'static,
        M::Result: Send + 'static,
    {
        let (reply_tx, reply_rx) = oneshot::channel();
        let envelope = Box::new(TokioEnvelopeInner::<A, M> {
            msg: Some(msg),
            tx: Some(reply_tx),
            _actor: std::marker::PhantomData,
        });
        self.tx
            .send(envelope)
            .await
            .map_err(|_| SendError::ActorStopped)?;
        reply_rx.await.map_err(|_| SendError::ActorStopped)
    }

    /// Async fire-and-forget — does not wait for the actor to process the message.
    pub async fn do_send<M>(&self, msg: M) -> Result<(), SendError>
    where
        A: Handler<TokioContext<A>, M>,
        M: Message + Send + 'static,
        M::Result: Send + 'static,
    {
        let envelope = Box::new(TokioEnvelopeInner::<A, M> {
            msg: Some(msg),
            tx: None,
            _actor: std::marker::PhantomData,
        });
        self.tx
            .send(envelope)
            .await
            .map_err(|_| SendError::ActorStopped)?;
        Ok(())
    }

    /// Non-blocking, non-async fire-and-forget.
    pub fn try_send<M>(&self, msg: M) -> Result<(), SendError>
    where
        A: Handler<TokioContext<A>, M>,
        M: Message + Send + 'static,
        M::Result: Send + 'static,
    {
        let envelope = Box::new(TokioEnvelopeInner::<A, M> {
            msg: Some(msg),
            tx: None,
            _actor: std::marker::PhantomData,
        });
        self.tx.try_send(envelope).map_err(|e| match e {
            mpsc::error::TrySendError::Full(_) => SendError::MailboxFull,
            mpsc::error::TrySendError::Closed(_) => SendError::ActorStopped,
        })
    }
}
