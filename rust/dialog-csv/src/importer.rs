use dialog_artifacts::{Artifact, DialogArtifactsError};
use dialog_common::ConditionalSend;
use futures_util::{Stream, StreamExt};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::AsyncRead;

use crate::row::CsvRow;

/// The imported row stream: boxed `Send` on native so the importer
/// can cross threads, unboxed of that requirement on wasm.
#[cfg(not(target_arch = "wasm32"))]
// bare-send-ok: dyn bounds cannot carry ConditionalSend; this is the cfg'd native alias
type ArtifactRows = Pin<Box<dyn Stream<Item = Result<Artifact, DialogArtifactsError>> + Send>>;

/// The imported row stream (see the native alias).
#[cfg(target_arch = "wasm32")]
type ArtifactRows = Pin<Box<dyn Stream<Item = Result<Artifact, DialogArtifactsError>>>>;

/// Imports artifacts from CSV rows.
///
/// Expects columns: `the`, `of`, `as` (value type), `is`, `cause`.
///
/// Implements [`Stream`] yielding one [`Artifact`] per CSV row.
pub struct CsvImporter {
    inner: ArtifactRows,
}

impl CsvImporter {
    /// Create a new CSV importer reading from the given reader.
    pub fn new<R: AsyncRead + Unpin + ConditionalSend + 'static>(reader: R) -> Self {
        let deserializer = csv_async::AsyncReaderBuilder::new().create_deserializer(reader);
        let stream = deserializer
            .into_deserialize::<CsvRow>()
            .map(|result| match result {
                Ok(row) => Artifact::try_from(row),
                Err(e) => Err(DialogArtifactsError::Export(e.to_string())),
            });
        Self {
            inner: Box::pin(stream),
        }
    }
}

impl<R: AsyncRead + Unpin + ConditionalSend + 'static> From<R> for CsvImporter {
    fn from(reader: R) -> Self {
        Self::new(reader)
    }
}

impl Stream for CsvImporter {
    type Item = Result<Artifact, DialogArtifactsError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}
