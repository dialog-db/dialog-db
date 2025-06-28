//! Traits for artifact storage and querying.
//!
//! This module defines the core traits that enable querying and modification
//! of artifacts in the triple store, providing both read-only and mutable
//! interfaces.

use async_trait::async_trait;
use dialog_common::ConditionalSend;
use dialog_storage::Blake3Hash;
use futures_util::Stream;

use crate::{
    Artifact, ArtifactSelector, DialogArtifactsError, Instruction, artifacts::selector::Constrained,
};

/// A trait that may be implemented by anything that is capable of querying
/// [`Artifact`]s.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait ArtifactStore
where
    Self: Sized,
{
    /// Query for [`Artifact`]s that match the given [`ArtifactSelector`].
    /// Results are provided as a [`Stream`], implying that they are produced
    /// from the implementation lazily.
    ///
    /// For additional details, see the documentation for [`ArtifactSelector`].
    fn select(
        &self,
        selector: ArtifactSelector<Constrained>,
    ) -> impl Stream<Item = Result<Artifact, DialogArtifactsError>> + 'static + ConditionalSend;
}

/// A trait that may be implemented by anything that is capable of
/// of storing [`Artifact`]s.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait ArtifactStoreMut: ArtifactStore {
    /// Commit one or more [`Artifact`]s to storage. Implementors should take care
    /// to ensure that commits are transactional and resilient to unexpected
    /// halts and other such failure modes.
    async fn commit<Instructions>(
        &mut self,
        instructions: Instructions,
    ) -> Result<Blake3Hash, DialogArtifactsError>
    where
        Instructions: Stream<Item = Instruction> + ConditionalSend;
}

/// An extension trait that has a blanket implementation for all implementors of
/// [`ArtifactStoreMut`], to add convenience methods to those implementors
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait ArtifactStoreMutExt: ArtifactStoreMut {
    /// A wrapper for [`ArtifactStoreMut::commit`] that accepts an
    /// [`IntoIterator`] instead of a [`Stream`] (and performs the conversion
    /// internally).
    async fn commit<Instructions>(
        &mut self,
        instructions: Instructions,
    ) -> Result<Blake3Hash, DialogArtifactsError>
    where
        Instructions: IntoIterator<Item = Instruction> + ConditionalSend,
        Instructions::IntoIter: ConditionalSend,
    {
        ArtifactStoreMut::commit(self, futures_util::stream::iter(instructions)).await
    }
}

impl<A> ArtifactStoreMutExt for A where A: ArtifactStoreMut {}
