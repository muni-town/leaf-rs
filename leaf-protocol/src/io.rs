use ed25519_dalek::VerifyingKey;

use crate::*;

pub use task_queue::*;

pub mod native;
mod task_queue;

pub trait IoTaskExt<Io: LeafIo> {
    fn into_future(self, io: Arc<Io>) -> impl Future<Output = Result<IoResult>> + Sync + Send;
}
impl<Io: LeafIo> IoTaskExt<Io> for IoTask {
    async fn into_future(self, io: Arc<Io>) -> Result<IoResult> {
        let id = self.id();
        let result = match self.take_action() {
            IoAction::Load { key } => IoResult::load(id, io.load(key).await?),
            IoAction::LoadRange { prefix } => {
                IoResult::load_range(id, io.load_range(prefix).await?)
            }
            IoAction::ListOneLevel { prefix } => {
                IoResult::list_one_level(id, io.list_one_level(prefix).await?)
            }
            IoAction::Put { key, data } => {
                io.put(key, data).await?;
                IoResult::put(id)
            }
            IoAction::Delete { key } => {
                io.delete(key).await?;
                IoResult::delete(id)
            }
            IoAction::Sign { payload } => IoResult::sign(id, io.sign(payload).await?),
        };
        Ok(result)
    }
}

pub trait LeafIo: Sync + Send + 'static {
    fn load(&self, key: StorageKey) -> impl Future<Output = Result<Option<Vec<u8>>>> + Send + Sync;
    fn load_range(
        &self,
        prefix: StorageKey,
    ) -> impl Future<Output = Result<HashMap<StorageKey, Vec<u8>>>> + Send + Sync;
    fn list_one_level(
        &self,
        prefix: StorageKey,
    ) -> impl Future<Output = Result<Vec<StorageKey>>> + Send + Sync;
    fn put(&self, key: StorageKey, data: Vec<u8>)
    -> impl Future<Output = Result<()>> + Send + Sync;
    fn delete(&self, key: StorageKey) -> impl Future<Output = Result<()>> + Send + Sync;
    fn sign(&self, data: Vec<u8>) -> impl Future<Output = Result<Signature>> + Send + Sync;
    fn public_key(&self) -> VerifyingKey;
}
