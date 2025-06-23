use std::str::FromStr;

use automerge::{ObjId, ReadDoc, ScalarValue, Value, transaction::Transactable};
use beelay_core::DocumentId;
use leaf_protocol::{io::native::NativeIo, *};

#[tokio::main]
async fn main() -> Result<()> {
    let doc_id = std::env::args()
        .nth(1)
        .map(|x| DocumentId::from_str(&x))
        .transpose()?;

    // Create a native IO adapter that stores its data in a local filesystem directory.
    let io = NativeIo::open("./data.gitignore").await?;
    // Create a leaf instance and its "runner" that is used to drive the async event loop.
    let (leaf, runner) = Leaf::<AutomergeDoc>::new(io).await?;

    // Print the peer ID which is persisted across starts by storing the private key in the
    // data store.
    println!("Peer ID: {}", leaf.id());

    // Spawn an async task to load a document
    tokio::spawn(async move {
        let leaf_ = leaf.clone();
        let result = async move {
            // Load doc if specified
            if let Some(doc_id) = doc_id {
                // You have to have created this doc first
                let doc = leaf.load_doc(doc_id).await?;
                if let Some(doc) = doc {
                    doc.change(|doc| {
                        doc.transact(|t| {
                            t.increment(ObjId::Root, "age", 1).unwrap();
                            Ok::<_, anyhow::Error>(())
                        })
                        .unwrap();

                        let age = doc.get(ObjId::Root, "age")?;

                        dbg!(&age.map(|x| x.0));

                        Ok::<_, anyhow::Error>(())
                    })?;
                } else {
                    anyhow::bail!("Doc not found")
                }
            } else {
                let doc_id = leaf.create_doc(Vec::new()).await?;
                dbg!(doc_id);
                let doc = leaf.load_doc(doc_id).await?.unwrap();

                doc.change(|doc| {
                    doc.transact(|t| {
                        t.put(ObjId::Root, "age", ScalarValue::counter(0))?;
                        t.increment(ObjId::Root, "age", 1)?;
                        Ok::<_, anyhow::Error>(())
                    })
                    .unwrap();

                    let age = doc.get(ObjId::Root, "age")?;

                    dbg!(&age.map(|x| x.0));

                    Ok::<_, anyhow::Error>(())
                })?;
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
