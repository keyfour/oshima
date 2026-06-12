use crate::addr::SyncAddr;
use crate::context::SyncContext;
use oshima_core::envelope::EnvelopeProxy;
use oshima_core::traits::{Actor, ActorBase, ActorContext, Running};
use std::sync::mpsc;

const DEFAULT_MAILBOX_SIZE: usize = 64;

pub struct SyncRuntime;

impl SyncRuntime {
    /// Spawn an actor on a new OS thread with the default mailbox capacity.
    pub fn spawn<A>(actor: A) -> SyncAddr<A>
    where
        A: ActorBase + Actor<SyncContext<A>> + Send + 'static,
    {
        Self::spawn_with_capacity(actor, DEFAULT_MAILBOX_SIZE)
    }

    /// Spawn with a custom mailbox capacity.
    pub fn spawn_with_capacity<A>(mut actor: A, capacity: usize) -> SyncAddr<A>
    where
        A: ActorBase + Actor<SyncContext<A>> + Send + 'static,
    {
        let (tx, rx) =
            mpsc::sync_channel::<Box<dyn EnvelopeProxy<A, SyncContext<A>> + Send>>(capacity);

        std::thread::spawn(move || {
            let mut ctx = SyncContext::new();

            // Lifecycle: started
            actor.started(&mut ctx);

            // Message loop
            while ctx.is_running() {
                match rx.recv() {
                    Ok(envelope) => envelope.handle(&mut actor, &mut ctx),
                    Err(_) => {
                        ctx.set_stopping();
                        break;
                    }
                }
            }

            // Lifecycle: stopping — actor may veto the shutdown
            let decision = actor.stopping(&mut ctx);
            if decision == Running::Continue {
                ctx = SyncContext::new();
                while let Ok(envelope) = rx.try_recv() {
                    envelope.handle(&mut actor, &mut ctx);
                    if !ctx.is_running() {
                        break;
                    }
                }
                actor.stopping(&mut ctx); // second call is final
            }

            // Lifecycle: stopped
            actor.stopped(&mut ctx);
        });

        SyncAddr { tx }
    }
}
