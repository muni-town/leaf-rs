use crate::Result;

pub use beelay_core::{Commit, CommitOrBundle};

pub trait Document: Sized {
    fn initial_commit() -> Commit;
    fn from_raw(chunks: Vec<CommitOrBundle>) -> Result<Self>;
    fn add_commits(&self, chunks: Vec<CommitOrBundle>) -> Result<()>;
    fn subscribe_to_commits(&self, callback: Box<dyn Fn(Commit) + Sync + Send + 'static>);
}

#[cfg(feature = "automerge")]
mod automerge {
    use crate::Result;
    use crate::{RawCommit, RawCommitOrBundle};
    use automerge::Automerge;
    use automerge::transaction::CommitOptions;
    use beelay_core::CommitHash;

    use super::Document;

    impl Document for Automerge {
        fn initial_commit() -> beelay_core::Commit {
            let mut doc = Automerge::new();
            doc.empty_commit(CommitOptions::default());
            let c = doc.get_changes(&[]).into_iter().next().expect("no changes");

            RawCommit::new(
                Vec::new(),
                c.raw_bytes().into(),
                CommitHash::from(c.hash().0),
            )
        }

        fn from_raw(chunks: Vec<RawCommitOrBundle>) -> Result<Self> {
            todo!()
        }

        fn add_commits(&self, chunks: Vec<RawCommitOrBundle>) -> Result<()> {
            todo!()
        }

        fn subscribe_to_commits(
            &self,
            callback: Box<dyn Fn(beelay_core::Commit) + Sync + Send + 'static>,
        ) {
            todo!()
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

        fn from_raw(chunks: Vec<CommitOrBundle>) -> Result<Self> {
            let doc = LoroDoc::new();
            for chunk in chunks {
                match chunk {
                    CommitOrBundle::Commit(commit) => {
                        doc.import(commit.contents())?;
                    }
                    CommitOrBundle::Bundle(commit_bundle) => {
                        doc.import(commit_bundle.bundled_commits())?;
                    }
                }
            }
            Ok(doc)
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
