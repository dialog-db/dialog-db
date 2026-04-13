//! Format-agnostic export trait for artifacts.
//!
//! Implementations live in separate crates (e.g., `dialog-csv`).

use async_trait::async_trait;

use crate::{Artifact, DialogArtifactsError};

/// Writes artifacts to some output format.
///
/// Implementors handle the format-specific serialization of each artifact.
/// The caller is responsible for iterating over artifacts and calling
/// [`Exporter::write`] for each one, followed by [`Exporter::close`] to
/// finalize the output.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait Exporter {
    /// Write a single artifact to the output.
    async fn write(&mut self, artifact: &Artifact) -> Result<(), DialogArtifactsError>;

    /// Finalize the output. Called after all artifacts have been written.
    async fn close(&mut self) -> Result<(), DialogArtifactsError>;
}
