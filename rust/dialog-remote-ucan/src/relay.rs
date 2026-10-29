//! Performing a UCAN site's operations through something other than its
//! own requests: a [`Relay`].
//!
//! A site whose code runs where it has no network of its own, such as a
//! guest kernel in a component whose embedder reaches the network for it,
//! still proves each operation as any other: the fork is authorized and
//! invoked as usual. The relay then carries the invocation, its capability
//! and the address it is for to whatever performs it, and answers as the
//! site would.

use std::fmt::Debug;

use dialog_capability::ForkInvocation;
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_effects::archive::{ArchiveError, Get, Put};
use dialog_effects::blob::{BlobError, BlobReader, BlobWriter, Import, Read};
use dialog_effects::memory::{Edition, MemoryError, Publish, Resolve, Retract, Version};

use crate::site::UcanSite;

/// What performs a [`UcanSite`]'s operations in place of its requests.
///
/// Each call takes an operation as the site was asked to perform it, and
/// answers as the site answers it. A relayed site follows no cell: a watch
/// is refused, and the cell is read by resolving it again.
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
pub trait Relay: Debug + ConditionalSend + ConditionalSync {
    /// A block of an archive's catalog.
    async fn get(
        &self,
        invocation: ForkInvocation<UcanSite, Get>,
    ) -> Result<Option<Vec<u8>>, ArchiveError>;

    /// Store a block in an archive's catalog.
    async fn put(&self, invocation: ForkInvocation<UcanSite, Put>) -> Result<(), ArchiveError>;

    /// What a cell holds now, and its version.
    async fn resolve(
        &self,
        invocation: ForkInvocation<UcanSite, Resolve>,
    ) -> Result<Option<Edition<Vec<u8>>>, MemoryError>;

    /// Write a cell, if it is at the version the invocation expects.
    async fn publish(
        &self,
        invocation: ForkInvocation<UcanSite, Publish>,
    ) -> Result<Version, MemoryError>;

    /// Empty a cell, if it is at the version the invocation expects.
    async fn retract(
        &self,
        invocation: ForkInvocation<UcanSite, Retract>,
    ) -> Result<(), MemoryError>;

    /// A blob's bytes, or the range the invocation asks for.
    async fn read(
        &self,
        invocation: ForkInvocation<UcanSite, Read>,
    ) -> Result<BlobReader, BlobError>;

    /// A writer for a blob, which stores it once finished.
    async fn import(
        &self,
        invocation: ForkInvocation<UcanSite, Import>,
    ) -> Result<BlobWriter, BlobError>;
}
