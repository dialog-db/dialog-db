use async_trait::async_trait;
use dialog_common::ConditionalSend;
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
    async fn commit<I>(&mut self, instructions: I) -> Result<(), DialogArtifactsError>
    where
        I: IntoIterator<Item = Instruction> + ConditionalSend,
        I::IntoIter: ConditionalSend;
}
