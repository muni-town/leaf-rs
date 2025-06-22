use crate::{RawCommit, RawCommitOrBundle, Result};

pub trait Document: Sized {
    fn initial_commit() -> RawCommit;
    fn from_raw(chunks: Vec<RawCommitOrBundle>) -> Result<Self>;
    fn add_commits(&self, chunks: Vec<RawCommitOrBundle>) -> Result<()>;
    fn subscribe_to_commits(&self, callback: Box<dyn Fn(RawCommit) + Sync + Send + 'static>);
}
