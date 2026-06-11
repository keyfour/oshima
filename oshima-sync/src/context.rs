use oshima_core::traits::{ActorBase, ActorContext};

pub struct SyncContext<A: ActorBase + Send> {
    running: bool,
    _actor: std::marker::PhantomData<A>,
}

impl<A: ActorBase + Send> SyncContext<A> {
    pub(crate) fn new() -> Self {
        Self { running: true, _actor: std::marker::PhantomData }
    }
    pub(crate) fn set_stopping(&mut self) { self.running = false; }
}

// The key fix: ActorContext<A> does NOT require A: Actor<SyncContext<A>>.
// The circular constraint that caused E0275.
impl<A: ActorBase + Send> ActorContext<A> for SyncContext<A> {
    fn stop(&mut self) { self.running = false; }
    fn is_running(&self) -> bool { self.running }
}
