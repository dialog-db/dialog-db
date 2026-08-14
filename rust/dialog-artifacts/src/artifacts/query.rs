use dialog_capability::Command;
use futures_util::Stream;
use std::marker::PhantomData;
use std::pin::Pin;

use crate::selector::Constrained;
use crate::{ArtifactSelector, ArtifactView, DialogArtifactsError};

/// A boxed stream of artifact query results, as borrowed-access
/// [`ArtifactView`]s: read fields off each row, or call
/// [`ArtifactView::to_owned`] where ownership is genuinely needed.
#[cfg(not(target_arch = "wasm32"))]
pub type ArtifactStream<'a> =
    Pin<Box<dyn Stream<Item = Result<ArtifactView, DialogArtifactsError>> + Send + 'a>>;

/// A boxed stream of artifact query results, as borrowed-access
/// [`ArtifactView`]s: read fields off each row, or call
/// [`ArtifactView::to_owned`] where ownership is genuinely needed.
#[cfg(target_arch = "wasm32")]
pub type ArtifactStream<'a> =
    Pin<Box<dyn Stream<Item = Result<ArtifactView, DialogArtifactsError>> + 'a>>;

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
