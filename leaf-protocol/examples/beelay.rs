use std::time::Duration;

use leaf_protocol::{io::native::NativeIo, *};

#[tokio::main]
async fn main() -> Result<()> {
    let io = NativeIo::open("./data.gitignore").await?;
    let (leaf, runner) = Leaf::new(io).await?;

    println!("{}", leaf.id());

    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(2)).await;
        leaf.stop();
    });

    println!("Starting beelay service...");

    runner.await?;
    println!("Done");

    Ok(())
}
