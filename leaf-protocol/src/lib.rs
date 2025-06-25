use flume::Sender;
use std::{
    collections::{HashMap, VecDeque},
    future::Future,
    marker::PhantomData,
    sync::Arc,
};

pub use anyhow::Result;
use beelay_core::{
    doc_status::DocStatus, keyhive::KeyhiveEntityId, loading::Step, Beelay, CommitBundle, Config, DocumentId, PeerId, UnixTimestampMillis
};
pub use beelay_core::{Commit, CommitOrBundle, StorageKey};
pub use ed25519_dalek::Signature;

use crate::{
    io::LeafIo,
    job_queue::{IntoJob, JobQueue},
    runner::{LeafJob, LeafJobResult, LeafRunner},
};

#[cfg(feature = "loro")]
pub use loro;

mod doc;
pub mod io;
mod job_queue;
mod runner;

pub use doc::*;

pub struct Leaf<Doc> {
    id: PeerId,
    events: Sender<LeafEvent>,
    _phantom: PhantomData<Doc>,
}

impl<T> Clone for Leaf<T> {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            events: self.events.clone(),
            _phantom: self._phantom,
        }
    }
}

pub enum LeafId {
    Public,
    Group(PeerId),
    Doc(DocumentId),
}

impl From<LeafId> for KeyhiveEntityId {
    fn from(value: LeafId) -> Self {
        match value {
            LeafId::Public => Self::Public,
            LeafId::Group(peer_id) => Self::Group(peer_id),
            LeafId::Doc(doc_id) => Self::Doc(doc_id),
        }
    }
}

/// An event that is sent from the leaf handle to the leaf runner
enum LeafEvent {
    Stop,
    CreateDoc(oneshot::Sender<LeafResponse>, Commit, Vec<LeafId>),
    LoadDoc(oneshot::Sender<LeafResponse>, DocumentId),
    DocStatus(oneshot::Sender<LeafResponse>, DocumentId),
    AddCommits(DocumentId, Vec<Commit>),
    AddBundle(DocumentId, CommitBundle),
}

/// A response that is sent from the leaf runner to the leaf handle
enum LeafResponse {
    CreateDoc(Result<DocumentId>),
    LoadDoc(Option<Vec<CommitOrBundle>>),
    DocStatus(DocStatus),
}

impl<Doc: Document> Leaf<Doc> {
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
        let now = UnixTimestampMillis::now;
        let io = Arc::new(io);
        let rng = rand::thread_rng();

        // Start loading the Beelay instance
        let mut step = Beelay::load(Config::new(rng, io.public_key()), now());

        // Create a queue for the startup tasks
        let mut startup_task_queue = VecDeque::new();

        // Keep looping and executing the startup tasks until the Beelay instance has finished loading.
        let beelay = loop {
            match step {
                Step::Loading(l, io_tasks) => {
                    startup_task_queue.extend(io_tasks);
                    let next_job = LeafJob::IoTask(startup_task_queue.pop_front().unwrap());
                    let LeafJobResult::IoResult(result) = next_job.into_job(io.clone()).await?
                    else {
                        unreachable!("Invalid result")
                    };
                    step = l.handle_io_complete(now(), result)
                }
                Step::Loaded(beelay, io_tasks) => {
                    startup_task_queue.extend(io_tasks);
                    break beelay;
                }
            }
        };

        // Create our runtime task queue
        let task_queue = JobQueue::new(io.clone());

        // Add any tasks remaining after startup to the runtime queue
        for task in startup_task_queue.drain(..) {
            task_queue.add_job(LeafJob::IoTask(task));
        }

        // Create the event channel
        let (event_tx, event_rx) = flume::unbounded();

        // Create the leaf handle
        let leaf = Self {
            id: beelay.peer_id(),
            events: event_tx.clone(),
            _phantom: PhantomData,
        };

        // Create a runner to execute the leaf event loop
        let runner = LeafRunner {
            beelay,
            task_queue,
            event_rx: event_rx.into_stream(),
            event_tx,
            command_event_id_map: HashMap::default(),
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

    /// Create a new document
    pub async fn create_doc(&self, other_owners: Vec<LeafId>) -> Result<DocumentId> {
        let (responder, response) = oneshot::channel();
        self.events
            .send(LeafEvent::CreateDoc(
                responder,
                Doc::initial_commit(),
                other_owners,
            ))
            .ok();

        if let LeafResponse::CreateDoc(doc_id) =
            response.into_future().await.expect("channel error")
        {
            doc_id
        } else {
            panic!("Invalid response type")
        }
    }

    /// Load a document
    pub async fn load_doc(&self, doc_id: DocumentId) -> Result<Option<Doc>> {
        let (responder, response) = oneshot::channel();
        self.events.send(LeafEvent::LoadDoc(responder, doc_id)).ok();
        let events = self.events.clone();

        if let LeafResponse::LoadDoc(chunks) = response.into_future().await.expect("channel error")
        {
            chunks
                .map(|chunks| {
                    let doc = Doc::from_raw(chunks)?;
                    let events_ = events.clone();
                    doc.subscribe_to_commits(Box::new(move |commit| {
                        events_
                            .send(LeafEvent::AddCommits(doc_id, vec![commit]))
                            .ok();
                    }));
                    Ok(doc)
                })
                .transpose()
        } else {
            panic!("Invalid response type")
        }
    }

    pub async fn doc_status(&self, doc_id: DocumentId) -> DocStatus {
        let (responder, response) = oneshot::channel();
        self.events
            .send(LeafEvent::DocStatus(responder, doc_id))
            .ok();

        if let LeafResponse::DocStatus(status) =
            response.into_future().await.expect("channel error")
        {
            status
        } else {
            panic!("Invalid response type")
        }
    }
}
