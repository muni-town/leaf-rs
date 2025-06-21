use std::{str::FromStr, time::Duration};

use beelay_core::{Commit, CommitHash, DocumentId};
use leaf_protocol::{io::native::NativeIo, *};

#[tokio::main]
async fn main() -> Result<()> {
    let io = NativeIo::open("./data.gitignore").await?;
    let (leaf, runner) = Leaf::new(io).await?;

    println!("{}", leaf.id());

    tokio::spawn(async move {
        let doc = leaf
            .load_doc(
                DocumentId::from_str("2cdu3VYtG5z9FsogMFPXb57JVd3NsA1wpVc6x1CXzUtumahVwT").unwrap(),
            )
            .await;
        dbg!(doc);
        // let doc = leaf
        //     .create_doc(
        //         Commit::new(
        //             Vec::new(),
        //             Vec::new(),
        //             CommitHash::try_from(&[0; 32][..]).unwrap(),
        //         ),
        //         Vec::new(),
        //     )
        //     .await
        //     .unwrap();
        // dbg!(doc);

        leaf.stop();
    });

    println!("Starting beelay service...");

    runner.await?;
    println!("Done");

    Ok(())
}
