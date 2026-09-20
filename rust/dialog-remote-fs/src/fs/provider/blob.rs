//! Blob providers for FS-remote.
//!
//! Delegates the streaming blob capability to the verified
//! [`FileSystem`](dialog_storage::provider::FileSystem) the authorization
//! carries, exactly as the archive providers do. All blob I/O — ranged reads
//! and digest-verified single-part imports — lives in `dialog_storage`.
//!
//! The transfer meter counts each open as one request; the bytes that then
//! move through the returned reader/writer are outside the invocation and are
//! not metered here.

use dialog_capability::{ForkInvocation, Provider};
use dialog_effects::blob::{BlobError, BlobReader, BlobWriter, Import, Read};

use crate::fs::Fs;
use crate::fs::simulation::{self, Traffic};

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<ForkInvocation<Fs, Read>> for Fs {
    async fn execute(&self, input: ForkInvocation<Fs, Read>) -> Result<BlobReader, BlobError> {
        let flight = simulation::begin(Traffic::BlobRead).await;
        let result =
            Provider::<Read>::execute(input.authorization.filesystem(), input.capability).await;
        flight.complete(0).await;
        result
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<ForkInvocation<Fs, Import>> for Fs {
    async fn execute(&self, input: ForkInvocation<Fs, Import>) -> Result<BlobWriter, BlobError> {
        let flight = simulation::begin(Traffic::BlobImport).await;
        let result =
            Provider::<Import>::execute(input.authorization.filesystem(), input.capability).await;
        flight.complete(0).await;
        result
    }
}
