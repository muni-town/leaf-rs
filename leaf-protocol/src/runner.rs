use flume::{Sender, r#async::RecvStream};
use futures::StreamExt;
use std::{collections::HashMap, future::Future, sync::Arc, task::Poll};

pub use anyhow::Result;
use beelay_core::{
    Beelay, BundleSpec, CommandId, CommandResult, CommitBundle, CommitOrBundle, DocumentId, Event,
    EventResults, Stopped, UnixTimestampMillis,
    io::{IoAction, IoResult, IoTask},
};
use rand::rngs::ThreadRng;

use crate::{
    LeafEvent, LeafResponse,
    io::LeafIo,
    job_queue::{IntoJob, JobQueue},
};

impl<Io: LeafIo> IntoJob<Arc<Io>, Result<LeafJobResult>> for LeafJob {
    async fn into_job(self, io: Arc<Io>) -> Result<LeafJobResult> {
        let io = io.clone();
        match self {
            LeafJob::IoTask(io_task) => {
                let id = io_task.id();

                let result = match io_task.take_action() {
                    IoAction::Load { key } => IoResult::load(id, io.load(key).await?),
                    IoAction::LoadRange { prefix } => {
                        IoResult::load_range(id, io.load_range(prefix).await?)
                    }
                    IoAction::ListOneLevel { prefix } => {
                        IoResult::list_one_level(id, io.list_one_level(prefix).await?)
                    }
                    IoAction::Put { key, data } => {
                        io.put(key, data).await?;
                        IoResult::put(id)
                    }
                    IoAction::Delete { key } => {
                        io.delete(key).await?;
                        IoResult::delete(id)
                    }
                    IoAction::Sign { payload } => IoResult::sign(id, io.sign(payload).await?),
                };

                Ok(LeafJobResult::IoResult(result))
            }
            LeafJob::CreateBundles(bundle_specs) => {


                Ok(LeafJobResult::CreateBundles(todo!()))
            },
        }
    }
}

pub enum LeafJob {
    IoTask(IoTask),
    CreateBundles(Vec<BundleSpec>),
}
pub enum LeafJobResult {
    IoResult(IoResult),
    CreateBundles(Vec<(DocumentId, CommitBundle)>),
}

pub struct LeafRunner<Io> {
    pub(super) beelay: Beelay<ThreadRng>,
    pub(super) task_queue: JobQueue<LeafJob, Arc<Io>, Result<LeafJobResult>>,
    pub(super) event_rx: RecvStream<'static, LeafEvent>,
    pub(super) event_tx: Sender<LeafEvent>,
    pub(super) command_event_id_map: HashMap<CommandId, oneshot::Sender<LeafResponse>>,
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
            event_rx,
            event_tx,
            command_event_id_map: command_responders,
        } = &mut *self;
        let now = UnixTimestampMillis::now;

        // If there is a new leaf event ready, then send that to Beelay
        if let Poll::Ready(Some(event)) = event_rx.poll_next_unpin(cx) {
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
                LeafEvent::AddCommits(doc_id, commits) => {
                    let (_command, ev) = Event::add_commits(doc_id, commits);
                    ev
                }
                LeafEvent::DocStatus(responder, doc_id) => {
                    let (command, ev) = Event::query_status(doc_id);
                    command_responders.insert(command, responder);
                    ev
                }
                LeafEvent::AddBundle(doc_id, bundle) => Event::add_bundle(doc_id, bundle).1,
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
                Ok(job_result) => match job_result {
                    LeafJobResult::IoResult(io_result) => {
                        match beelay.handle_event(now(), Event::io_complete(io_result)) {
                            Ok(EventResults { stopped: true, .. }) | Err(Stopped) => {
                                return Poll::Ready(Ok(()));
                            }
                            Ok(result) => {
                                handle_beelay_result(result, task_queue, command_responders)
                            }
                        }
                    }
                    LeafJobResult::CreateBundles(bundles) => {
                        for (doc_id, bundle) in bundles {
                            event_tx.send(LeafEvent::AddBundle(doc_id, bundle)).ok();
                        }
                    }
                },
                Err(e) => tracing::error!("IO Error: {e}"),
            }
        }

        // Keep going forever until we get a stop result from Beelay
        Poll::Pending
    }
}

/// Helper function to handle the beelay [`EventResults`].
fn handle_beelay_result<Io: LeafIo>(
    result: EventResults,
    job_queue: &mut JobQueue<LeafJob, Arc<Io>, Result<LeafJobResult>>,
    command_responders: &mut HashMap<CommandId, oneshot::Sender<LeafResponse>>,
) {
    for task in result.new_tasks {
        job_queue.add_job(LeafJob::IoTask(task));
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
            CommandResult::AddCommits(bundle_specs) => match bundle_specs {
                Ok(bundle_specs) => job_queue.add_job(LeafJob::CreateBundles(bundle_specs)),
                Err(e) => {
                    tracing::error!("Could not add commits, data will be lost: {e}");
                }
            },
            CommandResult::AddBundle(result) => {
                if let Err(e) = result {
                    tracing::error!("Error adding budne: {e}");
                }
            }
            CommandResult::CreateStream(_stream_id) => todo!(),
            CommandResult::DisconnectStream => (),
            CommandResult::HandleRequest(_endpoint_response) => todo!(),
            CommandResult::HandleResponse => todo!(),
            CommandResult::RegisterEndpoint(_endpoint_id) => todo!(),
            CommandResult::UnregisterEndpoint => (),
            CommandResult::Keyhive(_keyhive_command_result) => todo!(),
            CommandResult::QueryStatus(doc_status) => {
                responder.send(LeafResponse::DocStatus(doc_status)).ok();
            }
            CommandResult::Stop => (),
        }
    }
}
