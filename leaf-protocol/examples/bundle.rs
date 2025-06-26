use std::str::FromStr;

use automerge::{ObjId, ReadDoc, ScalarValue, transaction::Transactable};
use beelay_core::DocumentId;
use leaf_protocol::{io::native::NativeIo, *};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let doc_id = std::env::args()
        .nth(1)
        .map(|x| DocumentId::from_str(&x))
        .transpose()?;

    // Create a native IO adapter that stores its data in a local filesystem directory.
    let io = NativeIo::open("./data.gitignore").await?;
    // Create a leaf instance and its "runner" that is used to drive the async event loop.
    let (leaf, runner) = Leaf::<AutomergeDoc, _>::new(io).await?;

    // Print the peer ID which is persisted across starts by storing the private key in the
    // data store.
    println!("Peer ID: {}", leaf.id());

    // Spawn an async task to load a document
    tokio::spawn(async move {
        let leaf_ = leaf.clone();
        let result = async move {
            let id = leaf.create_doc(Vec::new()).await?;
            let doc = leaf.load_doc(id).await?.unwrap();
            doc.change(|doc| {
                doc.transact(|t| {
                    t.put(ObjId::Root, "age", ScalarValue::counter(0))?;
                    t.increment(ObjId::Root, "age", 1)?;
                    Ok::<_, anyhow::Error>(())
                })
                .unwrap();
            });
            for _ in 0..100000 {
                doc.change(|doc| {
                    doc.transact(|t| {
                        t.increment(ObjId::Root, "age", 1).unwrap();
                        Ok::<_, anyhow::Error>(())
                    })
                    .unwrap();

                    let age = doc.get(ObjId::Root, "age").unwrap();
                    // dbg!(&age.map(|x| x.0));
                });
            }

            Ok::<(), anyhow::Error>(())
        }
        .await;

        // Stop the Leaf node.
        leaf_.stop();

        if let Err(e) = result {
            eprintln!("Error: {e}");
        }
    });

    // The Leaf node won't make any progress if you don't await the runner
    println!("Starting beelay service...");
    runner.await?;

    // Once `leaf.stop()` has been called the runner will finish and the program will exit.
    println!("Done");

    Ok(())
}
