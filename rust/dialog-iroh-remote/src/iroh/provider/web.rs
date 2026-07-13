//! Wasm provider stubs.
//!
//! The iroh transport is native-only for now; on wasm the [`Iroh`] site
//! still composes (addresses parse, serialize, and authorize), but
//! executing an effect fails with an execution error. When iroh's browser
//! support fits this workspace's wasm targets, these stubs are replaced by
//! the real transport.

use dialog_capability::{ForkInvocation, Provider};
use dialog_effects::archive::{self, ArchiveError};
use dialog_effects::blob::{self, BlobError, BlobReader, BlobWriter};
use dialog_effects::memory::{self, Edition, MemoryError, Version};

use crate::Iroh;

const UNSUPPORTED: &str = "the iroh remote transport is not available on this target yet";

#[async_trait::async_trait(?Send)]
impl Provider<ForkInvocation<Iroh, archive::Get>> for Iroh {
    async fn execute(
        &self,
        _input: ForkInvocation<Iroh, archive::Get>,
    ) -> Result<Option<Vec<u8>>, ArchiveError> {
        Err(ArchiveError::ExecutionError(UNSUPPORTED.to_string()))
    }
}

#[async_trait::async_trait(?Send)]
impl Provider<ForkInvocation<Iroh, archive::Put>> for Iroh {
    async fn execute(
        &self,
        _input: ForkInvocation<Iroh, archive::Put>,
    ) -> Result<(), ArchiveError> {
        Err(ArchiveError::ExecutionError(UNSUPPORTED.to_string()))
    }
}

#[async_trait::async_trait(?Send)]
impl Provider<ForkInvocation<Iroh, archive::Import>> for Iroh {
    async fn execute(
        &self,
        _input: ForkInvocation<Iroh, archive::Import>,
    ) -> Result<(), ArchiveError> {
        Err(ArchiveError::ExecutionError(UNSUPPORTED.to_string()))
    }
}

#[async_trait::async_trait(?Send)]
impl Provider<ForkInvocation<Iroh, memory::Resolve>> for Iroh {
    async fn execute(
        &self,
        _input: ForkInvocation<Iroh, memory::Resolve>,
    ) -> Result<Option<Edition<Vec<u8>>>, MemoryError> {
        Err(MemoryError::Storage(UNSUPPORTED.to_string()))
    }
}

#[async_trait::async_trait(?Send)]
impl Provider<ForkInvocation<Iroh, memory::Publish>> for Iroh {
    async fn execute(
        &self,
        _input: ForkInvocation<Iroh, memory::Publish>,
    ) -> Result<Version, MemoryError> {
        Err(MemoryError::Storage(UNSUPPORTED.to_string()))
    }
}

#[async_trait::async_trait(?Send)]
impl Provider<ForkInvocation<Iroh, memory::Retract>> for Iroh {
    async fn execute(
        &self,
        _input: ForkInvocation<Iroh, memory::Retract>,
    ) -> Result<(), MemoryError> {
        Err(MemoryError::Storage(UNSUPPORTED.to_string()))
    }
}

#[async_trait::async_trait(?Send)]
impl Provider<ForkInvocation<Iroh, blob::Read>> for Iroh {
    async fn execute(
        &self,
        _input: ForkInvocation<Iroh, blob::Read>,
    ) -> Result<BlobReader, BlobError> {
        Err(BlobError::ExecutionError(UNSUPPORTED.to_string()))
    }
}

#[async_trait::async_trait(?Send)]
impl Provider<ForkInvocation<Iroh, blob::Import>> for Iroh {
    async fn execute(
        &self,
        _input: ForkInvocation<Iroh, blob::Import>,
    ) -> Result<BlobWriter, BlobError> {
        Err(BlobError::ExecutionError(UNSUPPORTED.to_string()))
    }
}
