use oshima_core::traits::{ActorBase, ActorContext};

pub struct TokioContext<A: ActorBase + Send> {
    running: bool,
    _actor: std::marker::PhantomData<A>,
}

impl<A: ActorBase + Send> TokioContext<A> {
    pub(crate) fn new() -> Self {
        Self {
            running: true,
            _actor: std::marker::PhantomData,
        }
    }
    pub(crate) fn set_stopping(&mut self) {
        self.running = false;
    }
}

impl<A: ActorBase + Send> ActorContext<A> for TokioContext<A> {
    fn stop(&mut self) {
        self.running = false;
    }
    fn is_running(&self) -> bool {
        self.running
    }
}
