use automerge::{transaction::CommitOptions, Automerge};

fn main() {
    let mut doc = Automerge::new();
    doc.empty_commit(CommitOptions::default());
}