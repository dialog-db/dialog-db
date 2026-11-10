//! Format-agnostic import trait for facts.
//!
//! Implementations live in separate crates (e.g., `dialog-csv`).

use futures_util::Stream;

use crate::{DialogCorpusError, Fact};

/// Reads facts from some input format.
///
/// An importer is simply a stream of facts. Implementations handle
/// the format-specific deserialization internally.
pub trait Importer: Stream<Item = Result<Fact, DialogCorpusError>> {}

impl<T> Importer for T where T: Stream<Item = Result<Fact, DialogCorpusError>> {}
