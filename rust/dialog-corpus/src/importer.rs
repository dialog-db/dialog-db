//! Format-agnostic import trait for artifacts.
//!
//! Implementations live in separate crates (e.g., `dialog-csv`).

use futures_util::Stream;

use crate::{Artifact, DialogCorpusError};

/// Reads artifacts from some input format.
///
/// An importer is simply a stream of artifacts. Implementations handle
/// the format-specific deserialization internally.
pub trait Importer: Stream<Item = Result<Artifact, DialogCorpusError>> {}

impl<T> Importer for T where T: Stream<Item = Result<Artifact, DialogCorpusError>> {}
