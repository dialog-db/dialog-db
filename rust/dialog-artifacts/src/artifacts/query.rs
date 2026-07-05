use dialog_capability::Command;
use futures_util::Stream;
use std::marker::PhantomData;
use std::pin::Pin;

use crate::selector::Constrained;
use crate::{Artifact, ArtifactSelector, DialogArtifactsError};

/// A boxed stream of artifact query results.
#[cfg(not(target_arch = "wasm32"))]
pub type ArtifactStream<'a> =
    Pin<Box<dyn Stream<Item = Result<Artifact, DialogArtifactsError>> + Send + 'a>>;

/// A boxed stream of artifact query results.
#[cfg(target_arch = "wasm32")]
pub type ArtifactStream<'a> =
    Pin<Box<dyn Stream<Item = Result<Artifact, DialogArtifactsError>> + 'a>>;

/// Command for selecting artifacts from a source.
///
/// The lifetime parameter `'a` ties the output stream to the provider,
/// allowing the stream to borrow from the environment.
pub struct Select<'a> {
    _borrow: PhantomData<&'a ()>,
}

impl<'a> Command for Select<'a> {
    type Input = ArtifactSelector<Constrained>;
    type Output = Result<ArtifactStream<'a>, DialogArtifactsError>;
}
