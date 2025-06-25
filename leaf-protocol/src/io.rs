use ed25519_dalek::VerifyingKey;

use crate::*;

pub mod native;

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
