use std::str::FromStr;

use beelay_core::DocumentId;
use leaf_protocol::{io::native::NativeIo, *};

fn main() -> Result<()> {
    smol::block_on(run())?;
    Ok(())
}

async fn run() -> Result<()> {
    // Create a native IO adapter that stores its data in a local filesystem directory.
    let io = NativeIo::open("./data.gitignore").await?;
    // Create a leaf instance and its "runner" that is used to drive the async event loop.
    let (leaf, runner) = Leaf::new(io).await?;

    // Print the peer ID which is persisted across starts by storing the private key in the
    // data store.
    println!("Peer ID: {}", leaf.id());

    // Spawn an async task to load a document
    smol::spawn(async move {
        // You have to have created this doc first
        let doc = leaf
            .load_doc(
                DocumentId::from_str("2cdu3VYtG5z9FsogMFPXb57JVd3NsA1wpVc6x1CXzUtumahVwT").unwrap(),
            )
            .await;
        // Show its loaded data
        dbg!(doc);

        // Stop the Leaf node.
        leaf.stop();
    })
    .detach();

    // The Leaf node won't make any progress if you don't await the runner
    println!("Starting beelay service...");
    runner.await?;

    // Once `leaf.stop()` has been called the runner will finish and the program will exit.
    println!("Done");

    Ok(())
}
