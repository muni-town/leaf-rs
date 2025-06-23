use crate::Result;

pub use beelay_core::{Commit, CommitOrBundle};

pub type DocumentSubscriber = Box<dyn Fn(beelay_core::Commit) + Sync + Send + 'static>;
pub trait Document: Default + Sized {
    fn initial_commit() -> Commit;
    fn add_commits(&self, chunks: Vec<CommitOrBundle>) -> Result<()>;
    fn subscribe_to_commits(&self, callback: Box<dyn Fn(Commit) + Sync + Send + 'static>);

    fn from_raw(chunks: Vec<CommitOrBundle>) -> Result<Self> {
        let doc = Self::default();
        doc.add_commits(chunks)?;
        Ok(doc)
    }
}

#[cfg(feature = "automerge")]
pub use self::automerge::*;
#[cfg(feature = "automerge")]
mod automerge {
    use std::sync::Arc;

    use crate::{DocumentSubscriber, Result};
    use anyhow::format_err;
    use automerge::transaction::CommitOptions;
    use automerge::{Automerge, Change};
    use beelay_core::{Commit, CommitHash, CommitOrBundle};
    use miniz_oxide::{
        deflate::{CompressionLevel, compress_to_vec},
        inflate::decompress_to_vec,
    };
    use parking_lot::RwLock;

    use super::Document;

    fn change_to_commit(change: &Change) -> Commit {
        Commit::new(
            Vec::new(),
            compress_to_vec(
                change.raw_bytes(),
                CompressionLevel::DefaultCompression as u8,
            ),
            CommitHash::from(change.hash().0),
        )
    }
    fn commit_to_change(commit: &Commit) -> Result<Change> {
        Ok(Change::from_bytes(
            decompress_to_vec(commit.contents()).map_err(|e| format_err!("{e}"))?,
        )?)
    }

    #[derive(Default)]
    pub struct AutomergeDoc {
        inner: Arc<RwLock<AutomergeDocInner>>,
    }

    #[derive(Default)]
    struct AutomergeDocInner {
        doc: Automerge,
        subscribers: Vec<DocumentSubscriber>,
    }

    impl AutomergeDoc {
        pub fn read<R, F: FnOnce(&Automerge) -> R>(&self, f: F) -> R {
            let mut inner = self.inner.write();
            f(&mut inner.doc)
        }
        pub fn change<R, F: FnOnce(&mut Automerge) -> R>(&self, f: F) -> R {
            let mut inner = self.inner.write();
            let version = inner.doc.get_heads();

            let r = f(&mut inner.doc);

            if !inner.subscribers.is_empty() {
                for change in inner.doc.get_changes(&version) {
                    let commit = change_to_commit(change);
                    for sub in &inner.subscribers {
                        sub(commit.clone())
                    }
                }
            }

            r
        }
    }

    impl Document for AutomergeDoc {
        fn initial_commit() -> beelay_core::Commit {
            let mut doc = Automerge::new();
            doc.empty_commit(CommitOptions::default());
            let c = doc.get_changes(&[]).into_iter().next().expect("no changes");
            change_to_commit(c)
        }

        fn add_commits(&self, chunks: Vec<CommitOrBundle>) -> Result<()> {
            for chunk in chunks {
                match chunk {
                    CommitOrBundle::Commit(commit) => {
                        let change = commit_to_change(&commit)?;
                        self.inner.write().doc.apply_changes([change].into_iter())?;
                    }
                    CommitOrBundle::Bundle(_commit_bundle) => {
                        todo!();
                    }
                }
            }
            Ok(())
        }

        fn subscribe_to_commits(&self, callback: DocumentSubscriber) {
            self.inner.write().subscribers.push(callback);
        }
    }
}

#[cfg(feature = "loro")]
mod loro {
    use crate::Result;
    use beelay_core::{CommitHash, CommitOrBundle};
    use loro::LoroDoc;

    use super::Document;

    impl Document for LoroDoc {
        fn initial_commit() -> beelay_core::Commit {
            let doc = LoroDoc::new();
            beelay_core::Commit::new(
                Vec::new(),
                doc.export(loro::ExportMode::Snapshot).expect("invalid doc"),
                CommitHash::from([0u8; 32]),
            )
        }

        fn subscribe_to_commits(
            &self,
            callback: Box<dyn Fn(beelay_core::Commit) + Sync + Send + 'static>,
        ) {
            self.subscribe_local_update(Box::new(move |commit| {
                callback(beelay_core::Commit::new(
                    Vec::new(),
                    commit.clone(),
                    CommitHash::from([0u8; 32]),
                ));
                true
            }))
            .detach();
        }

        fn add_commits(&self, chunks: Vec<beelay_core::CommitOrBundle>) -> Result<()> {
            for chunk in chunks {
                match chunk {
                    CommitOrBundle::Commit(commit) => {
                        self.import(commit.contents())?;
                    }
                    CommitOrBundle::Bundle(commit_bundle) => {
                        self.import(commit_bundle.bundled_commits())?;
                    }
                }
            }
            Ok(())
        }
    }
}
