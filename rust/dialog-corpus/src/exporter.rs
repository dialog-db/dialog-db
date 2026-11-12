//! Format-agnostic export trait for facts.
//!
//! Implementations live in separate crates (e.g., `dialog-csv`).

use async_trait::async_trait;

use crate::{DialogCorpusError, Fact};

/// Writes facts to some output format.
///
/// Implementors handle the format-specific serialization of each fact.
/// The caller is responsible for iterating over facts and calling
/// [`Exporter::write`] for each one, followed by [`Exporter::close`] to
/// finalize the output.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait Exporter {
    /// Write a single fact to the output.
    async fn write(&mut self, fact: &Fact) -> Result<(), DialogCorpusError>;

    /// Finalize the output. Called after all facts have been written.
    async fn close(&mut self) -> Result<(), DialogCorpusError>;
}
