use std::time::Duration;

use beelay_core::{Commit, CommitHash};
use leaf_protocol::{io::native::NativeIo, *};

#[tokio::main]
async fn main() -> Result<()> {
    let io = NativeIo::open("./data.gitignore").await?;
    let (leaf, runner) = Leaf::new(io).await?;

    println!("{}", leaf.id());

    tokio::spawn(async move {
        let doc = leaf
            .create_doc(
                Commit::new(
                    Vec::new(),
                    Vec::new(),
                    CommitHash::try_from(&[0; 32][..]).unwrap(),
                ),
                Vec::new(),
            )
            .await
            .unwrap();
        dbg!(doc);

        tokio::time::sleep(Duration::from_secs(2)).await;
        leaf.stop();
    });

    println!("Starting beelay service...");

    runner.await?;
    println!("Done");

    Ok(())
}
