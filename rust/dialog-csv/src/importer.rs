use dialog_artifacts::{Artifact, DialogArtifactsError};
use futures_util::{Stream, StreamExt};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::AsyncRead;

use crate::row::CsvRow;

/// Imports artifacts from CSV rows.
///
/// Expects columns: `the`, `of`, `as` (value type), `is`, `cause`.
///
/// Implements [`Stream`] yielding one [`Artifact`] per CSV row.
///
/// Readers are bounded on real `Send` (not `ConditionalSend`)
/// because `csv_async` requires it on every target.
pub struct CsvImporter {
    // bare-send-ok: dyn bound over a csv_async stream, which is Send on every target
    inner: Pin<Box<dyn Stream<Item = Result<Artifact, DialogArtifactsError>> + Send>>,
}

impl CsvImporter {
    /// Create a new CSV importer reading from the given reader.
    // bare-send-ok: csv_async bounds its readers on real Send on every target
    pub fn new<R: AsyncRead + Unpin + Send + 'static>(reader: R) -> Self {
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

// bare-send-ok: csv_async bounds its readers on real Send on every target
impl<R: AsyncRead + Unpin + Send + 'static> From<R> for CsvImporter {
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
