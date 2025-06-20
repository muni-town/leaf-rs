
use leaf_protocol::{io::native::NativeIo, *};


fn main() {
    smol::block_on(run()).unwrap();
}

async fn run() -> Result<()> {
    let io = NativeIo::open("./data.gitignore").await?;
    let leaf = Leaf::new(io).await?;

    println!("{}", leaf.id());

    Ok(())
}
