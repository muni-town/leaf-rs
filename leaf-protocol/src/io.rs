use ed25519_dalek::VerifyingKey;

use crate::*;

pub use task_queue::*;

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

pub mod native {
    use blocking::unblock;
    use ed25519_dalek::{SigningKey, ed25519::signature::Signer};
    use fjall::{Config, Keyspace, PartitionCreateOptions};
    use smallvec::SmallVec;

    use super::*;
    use std::{collections::HashSet, path::Path};

    const BEELAY_PREFIX: &[u8] = b"beelay_";
    const SIGNING_KEY_KEY: &[u8] = b"signing_key";

    pub struct NativeIo {
        _keyspace: Keyspace,
        signing_key: SigningKey,
        partition: fjall::Partition,
    }

    impl NativeIo {
        pub async fn open<P: AsRef<Path>>(data_dir: P) -> Result<Self> {
            let config = Config::new(data_dir);
            unblock(move || {
                let mut rng = rand::thread_rng();
                let keyspace = config.open()?;
                let partition =
                    keyspace.open_partition("leaf", PartitionCreateOptions::default())?;

                let signing_key = if let Some(data) = partition.get(SIGNING_KEY_KEY)? {
                    SigningKey::try_from(&data[..])?
                } else {
                    let k = SigningKey::generate(&mut rng);
                    partition.insert(SIGNING_KEY_KEY, k.to_bytes())?;
                    k
                };

                Ok(NativeIo {
                    _keyspace: keyspace,
                    partition,
                    signing_key,
                })
            })
            .await
        }
    }

    trait StorageKeyExt {
        fn to_bytes(&self) -> Vec<u8>;
        fn from_bytes(bytes: &[u8]) -> Self;
    }
    impl StorageKeyExt for StorageKey {
        fn to_bytes(&self) -> Vec<u8> {
            let mut bytes = Vec::new();

            // Add our prefix to mark beelay storage keys
            bytes.extend_from_slice(BEELAY_PREFIX);

            for component in self.components() {
                let len: u8 = component
                    .len()
                    .try_into()
                    .expect("storage key path component longer than 255 bytes");
                bytes.push(len);
                bytes.extend_from_slice(component.as_bytes());
            }
            bytes
        }

        fn from_bytes(bytes: &[u8]) -> Self {
            // Load the namespace
            const ERR: &str = "Error parsing storage path component";
            let mut strings = Vec::new();
            let bytes = &mut bytes.iter().copied();

            // Make sure the prefix matches our beelay storage prefix
            assert_eq!(
                BEELAY_PREFIX,
                &bytes
                    .take(BEELAY_PREFIX.len())
                    .collect::<SmallVec<[u8; 7]>>()[..],
                "{ERR}",
            );

            loop {
                let Some(len) = bytes.next() else { break };
                let data = bytes.take(len as usize).collect::<Vec<_>>();
                strings.push(String::from_utf8(data).expect(ERR));
            }
            StorageKey::try_from(strings).expect(ERR)
        }
    }

    impl LeafIo for NativeIo {
        fn public_key(&self) -> VerifyingKey {
            self.signing_key.verifying_key()
        }

        async fn load(&self, key: StorageKey) -> Result<Option<Vec<u8>>> {
            let partition = self.partition.clone();
            let key = key.to_bytes();
            let data = unblock(move || partition.get(&key)).await?;
            Ok(data.map(|x| x.to_vec()))
        }

        async fn load_range(&self, prefix: StorageKey) -> Result<HashMap<StorageKey, Vec<u8>>> {
            let partition = self.partition.clone();
            let prefix_bytes = prefix.to_bytes();
            unblock(move || {
                let mut output = HashMap::new();
                for result in partition.prefix(&prefix_bytes) {
                    let (key, value) = result?;
                    let key = StorageKey::from_bytes(&key);
                    if prefix.is_prefix_of(&key) {
                        output.insert(key, value.to_vec());
                    }
                }

                Ok(output)
            })
            .await
        }

        async fn list_one_level(&self, prefix: StorageKey) -> Result<Vec<StorageKey>> {
            let partition = self.partition.clone();
            let prefix_bytes = prefix.to_bytes();
            unblock(move || {
                let mut output = HashSet::new();
                for result in partition.prefix(&prefix_bytes) {
                    let (key, _value) = result?;
                    let key = StorageKey::from_bytes(&key);
                    if let Some(key) = key.onelevel_deeper(&prefix) {
                        output.insert(key);
                    }
                }
                Ok(output.into_iter().collect())
            })
            .await
        }

        async fn put(&self, key: StorageKey, data: Vec<u8>) -> Result<()> {
            let partition = self.partition.clone();
            let key = key.to_bytes();
            unblock(move || partition.insert(key, data)).await?;
            Ok(())
        }

        async fn delete(&self, key: StorageKey) -> Result<()> {
            let partition = self.partition.clone();
            let key = key.to_bytes();
            unblock(move || partition.remove(key)).await?;
            Ok(())
        }

        async fn sign(&self, data: Vec<u8>) -> Result<Signature> {
            Ok(self.signing_key.sign(&data))
        }
    }
}
