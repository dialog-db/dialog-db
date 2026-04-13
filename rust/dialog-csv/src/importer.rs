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
pub struct CsvImporter {
    inner: Pin<Box<dyn Stream<Item = Result<Artifact, DialogArtifactsError>> + Send>>,
}

impl CsvImporter {
    /// Create a new CSV importer reading from the given reader.
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
