use async_trait::async_trait;
use dialog_artifacts::Exporter;
use dialog_artifacts::{Artifact, DialogArtifactsError};
use tokio::io::AsyncWrite;

use crate::row::CsvRow;

/// Exports artifacts as CSV rows.
///
/// Columns: `the`, `of`, `as` (value type), `is`, `cause`.
pub struct CsvExporter<W: AsyncWrite + Unpin> {
    writer: csv_async::AsyncSerializer<W>,
}

impl<W: AsyncWrite + Unpin> CsvExporter<W> {
    /// Create a new CSV exporter writing to the given writer.
    pub fn new(writer: W) -> Self {
        Self {
            writer: csv_async::AsyncSerializer::from_writer(writer),
        }
    }
}

impl<W: AsyncWrite + Unpin> From<W> for CsvExporter<W> {
    fn from(writer: W) -> Self {
        Self::new(writer)
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
// bare-send-ok: csv_async bounds its writers on real Send on every target
impl<W: AsyncWrite + Unpin + Send> Exporter for CsvExporter<W> {
    async fn write(&mut self, artifact: &Artifact) -> Result<(), DialogArtifactsError> {
        let row = CsvRow::from(artifact);
        self.writer
            .serialize(row)
            .await
            .map_err(|e| DialogArtifactsError::Export(e.to_string()))
    }

    async fn close(&mut self) -> Result<(), DialogArtifactsError> {
        // Push everything the serializer buffered down to the underlying
        // writer. Without this the tail of an export sits in the internal
        // buffer, so a reader that opens the destination right after close
        // sees a truncated file — the `it_roundtrips_via_file` flake, where
        // release-mode timing dropped the last row.
        self.writer
            .flush()
            .await
            .map_err(|e| DialogArtifactsError::Export(e.to_string()))
    }
}
