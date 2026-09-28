use dialog_capability::{Command, Provider};
use dialog_common::{Blake3Hash, Buffer, ConditionalSync};
use dialog_storage::DialogStorageError;

use crate::DialogSearchTreeError;

/// Command for loading one tree node block by its content hash.
///
/// This is everything the tree needs from its environment. A block's
/// address is its content, so where the bytes come from (a local archive,
/// an unflushed delta, a cache, a remote the environment hydrates from) is
/// the provider's concern, and the tree asks only for the hash.
///
/// `Ok(None)` means the provider cannot reach the block. The tree checks
/// every block it loads against the hash it asked for, so a provider never
/// has to be trusted to return the right bytes.
pub struct Load;

impl Command for Load {
    type Input = Blake3Hash;
    type Output = Result<Option<Buffer>, DialogSearchTreeError>;
}

/// Loads the block stored under `hash` through `env`, refusing bytes that do
/// not hash to it.
///
/// A corrupt block raises rather than reading as absent, so `None` means
/// the block is genuinely not reachable.
pub(crate) async fn load<Env>(
    env: &Env,
    hash: &Blake3Hash,
) -> Result<Option<Buffer>, DialogSearchTreeError>
where
    Env: Provider<Load> + ConditionalSync,
{
    match env.execute(hash.clone()).await? {
        Some(buffer) if buffer.blake3_hash() != hash => Err(DialogSearchTreeError::Storage(
            DialogStorageError::Verification(
                "Retrieved bytes did not match the provided hash".to_string(),
            ),
        )),
        loaded => Ok(loaded),
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use async_trait::async_trait;
    use dialog_capability::Provider;
    use dialog_common::{Blake3Hash, Buffer};

    use super::{Load, load};
    use crate::DialogSearchTreeError;

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    /// Answers every load with the same bytes, whatever hash was asked for.
    struct Answering(Option<Buffer>);

    #[cfg_attr(not(target_arch = "wasm32"), async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
    impl Provider<Load> for Answering {
        async fn execute(&self, _: Blake3Hash) -> Result<Option<Buffer>, DialogSearchTreeError> {
            Ok(self.0.clone())
        }
    }

    #[dialog_common::test]
    async fn it_returns_the_block_that_hashes_to_what_was_asked() -> Result<()> {
        let block = Buffer::from(b"a block".to_vec());
        let hash = block.blake3_hash().clone();

        let loaded = load(&Answering(Some(block.clone())), &hash).await?;

        assert_eq!(loaded, Some(block));
        Ok(())
    }

    /// A provider handing back other bytes is a corrupt block, not an
    /// absent one: the load fails rather than reading as `None`.
    #[dialog_common::test]
    async fn it_refuses_bytes_that_do_not_hash_to_what_was_asked() -> Result<()> {
        let asked = Buffer::from(b"the block asked for".to_vec());
        let other = Buffer::from(b"some other block".to_vec());

        let loaded = load(&Answering(Some(other)), asked.blake3_hash()).await;

        assert!(
            matches!(loaded, Err(DialogSearchTreeError::Storage(_))),
            "mismatched bytes must fail the load: {loaded:?}"
        );
        Ok(())
    }

    #[dialog_common::test]
    async fn it_reads_an_unreachable_block_as_absent() -> Result<()> {
        let asked = Buffer::from(b"never stored".to_vec());

        let loaded = load(&Answering(None), asked.blake3_hash()).await?;

        assert_eq!(loaded, None);
        Ok(())
    }
}
