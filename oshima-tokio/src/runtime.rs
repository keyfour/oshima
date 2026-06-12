use crate::addr::{TokioAddr, TokioEnvelope};
use crate::context::TokioContext;
use oshima_core::traits::ActorContext;
use oshima_core::traits::{Actor, ActorBase, Running};
use tokio::sync::mpsc;

const DEFAULT_MAILBOX_SIZE: usize = 64;

pub struct TokioRuntime;

impl TokioRuntime {
    pub fn spawn<A>(actor: A) -> TokioAddr<A>
    where
        A: ActorBase + Actor<TokioContext<A>> + Send + 'static,
    {
        Self::spawn_with_capacity(actor, DEFAULT_MAILBOX_SIZE)
    }

    pub fn spawn_with_capacity<A>(mut actor: A, capacity: usize) -> TokioAddr<A>
    where
        A: ActorBase + Actor<TokioContext<A>> + Send + 'static,
    {
        let (tx, mut rx) = mpsc::channel::<Box<dyn TokioEnvelope<A> + Send>>(capacity);

        tokio::spawn(async move {
            let mut ctx = TokioContext::new();
            actor.started(&mut ctx);

            while ctx.is_running() {
                match rx.recv().await {
                    Some(envelope) => envelope.handle(&mut actor, &mut ctx),
                    None => {
                        ctx.set_stopping();
                        break;
                    }
                }
            }

            let decision = actor.stopping(&mut ctx);
            if decision == Running::Continue {
                ctx = TokioContext::new();
                while let Ok(envelope) = rx.try_recv() {
                    envelope.handle(&mut actor, &mut ctx);
                    if !ctx.is_running() {
                        break;
                    }
                }
                actor.stopping(&mut ctx);
            }

            actor.stopped(&mut ctx);
        });

        TokioAddr { tx }
    }
}
