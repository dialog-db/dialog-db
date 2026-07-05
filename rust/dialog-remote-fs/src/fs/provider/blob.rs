//! Blob providers for FS-remote.
//!
//! Delegates the streaming blob capability to the verified
//! [`FileSystem`](dialog_storage::provider::FileSystem) the authorization
//! carries, exactly as the archive providers do. All blob I/O — ranged reads
//! and digest-verified single-part imports — lives in `dialog_storage`.

use dialog_capability::{ForkInvocation, Provider};
use dialog_effects::blob::{BlobError, BlobReader, BlobWriter, Import, Read};

use crate::fs::Fs;

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<ForkInvocation<Fs, Read>> for Fs {
    async fn execute(&self, input: ForkInvocation<Fs, Read>) -> Result<BlobReader, BlobError> {
        Provider::<Read>::execute(input.authorization.filesystem(), input.capability).await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<ForkInvocation<Fs, Import>> for Fs {
    async fn execute(&self, input: ForkInvocation<Fs, Import>) -> Result<BlobWriter, BlobError> {
        Provider::<Import>::execute(input.authorization.filesystem(), input.capability).await
    }
}
