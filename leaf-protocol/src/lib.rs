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
    Beelay, CommandId, CommandResult, Commit, CommitOrBundle, Config, DocumentId, Event,
    EventResults, PeerId, Stopped, UnixTimestampMillis,
    io::{IoAction, IoResult, IoTask},
    keyhive::KeyhiveEntityId,
    loading::Step,
};
pub use ed25519_dalek::Signature;
use rand::rngs::ThreadRng;

use crate::io::{IoTaskExt, LeafIo, TaskQueue};

pub mod io;

#[derive(Clone)]
pub struct Leaf {
    id: PeerId,
    events: Sender<LeafEvent>,
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

enum LeafEvent {
    Stop,
    CreateDoc(oneshot::Sender<LeafResponse>, Commit, Vec<LeafId>),
    LoadDoc(oneshot::Sender<LeafResponse>, DocumentId),
}

enum LeafResponse {
    CreateDoc(Result<DocumentId>),
    LoadDoc(Option<Vec<CommitOrBundle>>),
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

        let (event_tx, event_rx) = flume::unbounded();
        let leaf = Self {
            id: beelay.peer_id(),
            events: event_tx,
        };
        let runner = LeafRunner {
            beelay,
            task_queue,
            events: event_rx.into_stream(),
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
    pub async fn create_doc(
        &self,
        commit: Commit,
        other_owners: Vec<LeafId>,
    ) -> Result<DocumentId> {
        let (responder, response) = oneshot::channel();
        self.events
            .send(LeafEvent::CreateDoc(responder, commit, other_owners))
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
    pub async fn load_doc(&self, doc_id: DocumentId) -> Option<Vec<CommitOrBundle>> {
        let (responder, response) = oneshot::channel();
        self.events.send(LeafEvent::LoadDoc(responder, doc_id)).ok();

        if let LeafResponse::LoadDoc(doc_id) = response.into_future().await.expect("channel error")
        {
            doc_id
        } else {
            panic!("Invalid response type")
        }
    }
}

pub struct LeafRunner<Io> {
    beelay: Beelay<ThreadRng>,
    task_queue: TaskQueue<Io>,
    events: RecvStream<'static, LeafEvent>,
    command_event_id_map: HashMap<CommandId, oneshot::Sender<LeafResponse>>,
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
            command_event_id_map: command_responders,
        } = &mut *self;
        let now = UnixTimestampMillis::now;

        let handle_beelay_result = |result: EventResults,
                                    task_queue: &mut TaskQueue<Io>,
                                    command_responders: &mut HashMap<
            CommandId,
            oneshot::Sender<LeafResponse>,
        >| {
            for task in result.new_tasks {
                task_queue.add_task(task);
            }
            for (id, command) in result.completed_commands {
                // We ignore the case of a command that got interrupted because beelay is being stopped for now
                let Ok(command) = command else { continue };
                let Some(responder) = command_responders.remove(&id) else {
                    continue;
                };

                match command {
                    CommandResult::CreateDoc(document_id) => {
                        responder
                            .send(LeafResponse::CreateDoc(document_id.map_err(|e| e.into())))
                            .ok();
                    }
                    CommandResult::LoadDoc(commit_or_bundles) => {
                        responder
                            .send(LeafResponse::LoadDoc(commit_or_bundles))
                            .ok();
                    }
                    CommandResult::AddCommits(bundle_specs) => {}
                    CommandResult::AddBundle(_) => todo!(),
                    CommandResult::CreateStream(stream_id) => todo!(),
                    CommandResult::DisconnectStream => todo!(),
                    CommandResult::HandleRequest(endpoint_response) => todo!(),
                    CommandResult::HandleResponse => todo!(),
                    CommandResult::RegisterEndpoint(endpoint_id) => todo!(),
                    CommandResult::UnregisterEndpoint => todo!(),
                    CommandResult::Keyhive(keyhive_command_result) => todo!(),
                    CommandResult::QueryStatus(doc_status) => todo!(),
                    CommandResult::Stop => (),
                }
            }
        };

        // If there is a new leaf event ready, then send that to Beelay
        if let Poll::Ready(Some(event)) = events.poll_next_unpin(cx) {
            let beelay_event = match event {
                LeafEvent::Stop => Event::stop(),
                LeafEvent::CreateDoc(responder, commit, keyhive_entity_ids) => {
                    let (command, ev) = Event::create_doc(
                        commit,
                        keyhive_entity_ids.into_iter().map(Into::into).collect(),
                    );
                    command_responders.insert(command, responder);
                    ev
                }
                LeafEvent::LoadDoc(responder, doc_id) => {
                    let (command, ev) = Event::load_doc(doc_id);
                    command_responders.insert(command, responder);
                    ev
                }
            };
            match beelay.handle_event(now(), beelay_event) {
                Ok(EventResults { stopped: true, .. }) | Err(Stopped) => {
                    return Poll::Ready(Ok(()));
                }
                Ok(result) => handle_beelay_result(result, task_queue, command_responders),
            }
        }

        // If there is an IO result ready, then send that to Beelay, too
        if let Poll::Ready(Some(io_result)) = task_queue.poll_next_unpin(cx) {
            match io_result {
                Ok(io_result) => match beelay.handle_event(now(), Event::io_complete(io_result)) {
                    Ok(EventResults { stopped: true, .. }) | Err(Stopped) => {
                        return Poll::Ready(Ok(()));
                    }
                    Ok(result) => handle_beelay_result(result, task_queue, command_responders),
                },
                Err(e) => tracing::error!("IO Error: {e}"),
            }
        }

        // Keep going forever until we get a stop result from Beelay
        Poll::Pending
    }
}
