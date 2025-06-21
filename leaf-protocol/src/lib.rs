use flume::{Sender, r#async::RecvStream};
use futures::StreamExt;
use std::{
    collections::{HashMap, VecDeque},
    future::Future,
    sync::Arc,
    task::Poll,
};

pub use anyhow::Result;
pub use beelay_core::StorageKey;
use beelay_core::{
    Beelay, Config, Event, EventResults, PeerId, Stopped, UnixTimestampMillis,
    io::{IoAction, IoResult, IoTask},
    loading::Step,
};
pub use ed25519_dalek::Signature;
use rand::rngs::ThreadRng;

use crate::io::{IoTaskExt, LeafIo, TaskQueue};

pub mod io;

pub struct Leaf {
    id: PeerId,
    events: Sender<LeafEvent>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
enum LeafEvent {
    Stop,
}

impl From<LeafEvent> for beelay_core::Event {
    fn from(val: LeafEvent) -> Self {
        match val {
            LeafEvent::Stop => Event::stop(),
        }
    }
}

impl Leaf {
    /// Instantiate a Leaf instance with the given IO adapter.
    ///
    /// This will return two types, the [`Leaf`] handle and the [`LeafRunner`]. The [`LeafRunner`]
    /// is a future that must be [`await`]ed on in order for Leaf to process it's events. Usually
    /// you will spawn this as an async task using your executor, or you will make it the last thing
    /// that you `await` in your program. The future will resolve once the Leaf peer has been
    /// stopped.
    ///
    /// > **⚠️ Important:** The returned [`LeafRunner`] is a [`Future`] not [`Sync`] or [`Send`] and
    /// > must be awaited on the same thread that it was created on. The [`Leaf`] instance, on the
    /// > other hand,  is a handle that can be cheaply cloned and is both [`Sync`] and [`Send`].
    pub async fn new<Io: LeafIo>(io: Io) -> Result<(Self, LeafRunner<Io>)> {
        let io = Arc::new(io);
        let now = UnixTimestampMillis::now;
        let rng = rand::thread_rng();
        let mut step = Beelay::load(Config::new(rng, io.public_key()), now());

        let mut startup_task_queue = VecDeque::new();
        let beelay = loop {
            match step {
                Step::Loading(l, io_tasks) => {
                    startup_task_queue.extend(io_tasks);
                    let next_task = startup_task_queue.pop_front().unwrap();
                    let result = next_task.into_future(io.clone()).await?;
                    step = l.handle_io_complete(now(), result)
                }
                Step::Loaded(beelay, io_tasks) => {
                    startup_task_queue.extend(io_tasks);
                    break beelay;
                }
            }
        };

        let task_queue = TaskQueue::new(io.clone());

        for task in startup_task_queue.drain(..) {
            task_queue.add_task(task);
        }

        let (sender, receiver) = flume::unbounded();
        let leaf = Self {
            id: beelay.peer_id(),
            events: sender,
        };
        let runner = LeafRunner {
            beelay,
            task_queue,
            events: receiver.into_stream(),
        };

        Ok((leaf, runner))
    }

    /// Get the Leaf Peer ID.
    pub fn id(&self) -> PeerId {
        self.id
    }

    /// Stop the leaf peer. This will cause [`run()`][Self::run] to return once the signal has been
    /// processed.
    pub fn stop(&self) {
        self.events.try_send(LeafEvent::Stop).ok();
    }
}

pub struct LeafRunner<Io> {
    beelay: Beelay<ThreadRng>,
    task_queue: TaskQueue<Io>,
    events: RecvStream<'static, LeafEvent>,
}

impl<Io: LeafIo> Future for LeafRunner<Io> {
    type Output = Result<()>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Self::Output> {
        let Self {
            beelay,
            task_queue,
            events,
        } = &mut *self;
        let now = UnixTimestampMillis::now;

        let handle_beelay_result = |result: EventResults, task_queue: &mut TaskQueue<Io>| {
            for task in result.new_tasks {
                task_queue.add_task(task);
            }
        };

        // If there is a new leaf event ready, then send that to Beelay
        if let Poll::Ready(Some(event)) = events.poll_next_unpin(cx) {
            match beelay.handle_event(now(), event.into()) {
                Ok(EventResults { stopped: true, .. }) | Err(Stopped) => {
                    return Poll::Ready(Ok(()));
                }
                Ok(result) => handle_beelay_result(result, task_queue),
            }
        }

        // If there is an IO result ready, then send that to Beelay, too
        if let Poll::Ready(Some(io_result)) = task_queue.poll_next_unpin(cx) {
            match io_result {
                Ok(io_result) => match beelay.handle_event(now(), Event::io_complete(io_result)) {
                    Ok(EventResults { stopped: true, .. }) | Err(Stopped) => {
                        return Poll::Ready(Ok(()));
                    }
                    Ok(result) => handle_beelay_result(result, task_queue),
                },
                Err(e) => tracing::error!("IO Error: {e}"),
            }
        }

        // Keep going forever until we get a stop result from Beelay
        Poll::Pending
    }
}
