use std::{
    pin::Pin,
    task::{Poll, Waker},
};

use flume::{Receiver, Sender};
use futures::Stream;

pub trait Job<Output>: Future<Output = Output> + Sync + Send + 'static {}
impl<Output, T: Future<Output = Output> + Sync + Send + 'static> Job<Output> for T {}
pub trait IntoJob<Ctx, Output> {
    fn into_job(self, ctx: Ctx) -> impl Job<Output>;
}

type PinnedJob<Output> = Pin<Box<dyn Job<Output>>>;
type PendingJobs<Output> = Vec<PinnedJob<Output>>;

pub struct JobQueue<Input, Ctx, Output> {
    ctx: Ctx,
    task_tx: Sender<Input>,
    task_rx: Receiver<Input>,
    pending: PendingJobs<Output>,
    pending_temp: PendingJobs<Output>,
    ready: Vec<Output>,
    waker: Option<Waker>,
}

impl<Input, Ctx: Clone, Output: 'static> JobQueue<Input, Ctx, Output>
where
    Input: IntoJob<Ctx, Output>,
{
    pub fn new(ctx: Ctx) -> Self {
        let (task_tx, task_rx) = flume::unbounded();
        Self {
            ctx,
            task_tx,
            task_rx,
            pending: Vec::new(),
            pending_temp: Vec::new(),
            ready: Vec::new(),
            waker: None,
        }
    }

    pub fn add_job(&self, task: Input) {
        if self.task_tx.send(task).is_err() {
            panic!("Task queue full");
        }
        if let Some(waker) = &self.waker {
            waker.wake_by_ref();
        }
    }
}

impl<Input, Ctx: Clone, Output: 'static> Stream for JobQueue<Input, Ctx, Output>
where
    Input: IntoJob<Ctx, Output>,
    JobQueue<Input, Ctx, Output>: Unpin,
{
    type Item = Output;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let Self {
            ctx,
            pending,
            pending_temp,
            task_rx,
            waker,
            ready,
            ..
        } = &mut *self;

        while let Ok(task) = task_rx.try_recv() {
            pending.push(Box::pin(task.into_job(ctx.clone())));
        }

        for mut fut in pending.drain(..) {
            match fut.as_mut().poll(cx) {
                Poll::Ready(result) => ready.push(result),
                Poll::Pending => pending_temp.push(fut),
            }
        }
        std::mem::swap(pending, pending_temp);
        *waker = Some(cx.waker().clone());

        if let Some(result) = ready.pop() {
            if !ready.is_empty() {
                // Make sure we wake if we have more records ready
                cx.waker().wake_by_ref();
            }

            Poll::Ready(Some(result))
        } else {
            Poll::Pending
        }
    }
}
