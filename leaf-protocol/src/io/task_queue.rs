use crate::{
    Result,
    io::{IoTaskExt, LeafIo},
};
use std::{
    pin::Pin,
    sync::Arc,
    task::{Poll, Waker},
};

use beelay_core::io::{IoResult, IoTask};
use futures::Stream;

type PendingFutures = Vec<Pin<Box<dyn Future<Output = Result<IoResult>> + Sync + Send>>>;
pub struct TaskQueue<Io> {
    io: Arc<Io>,
    new_tasks: Arc<concurrent_queue::ConcurrentQueue<IoTask>>,
    pending: PendingFutures,
    pending_temp: PendingFutures,
    ready: Vec<Result<IoResult>>,
    waker: Option<Waker>,
}

impl<Io> TaskQueue<Io> {
    pub fn new(io: Arc<Io>) -> Self {
        Self {
            io,
            new_tasks: Arc::new(concurrent_queue::ConcurrentQueue::unbounded()),
            pending: Vec::new(),
            pending_temp: Vec::new(),
            ready: Vec::new(),
            waker: None,
        }
    }

    pub fn add_task(&self, task: IoTask) {
        self.new_tasks.push(task).expect("Task queue is full");
        if let Some(waker) = &self.waker {
            waker.wake_by_ref();
        }
    }
}

impl<Io: LeafIo> Stream for TaskQueue<Io> {
    type Item = Result<IoResult>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let Self {
            io,
            pending,
            pending_temp,
            new_tasks,
            waker,
            ready,
        } = &mut *self;

        for task in new_tasks.try_iter() {
            pending.push(Box::pin(task.into_future(io.clone())));
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
