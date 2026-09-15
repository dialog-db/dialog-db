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

/// How confident the requester is that a preloaded range will be read.
///
/// The distinction is scheduling, not semantics: `Likely` work (a range
/// the evaluation has committed to, a spine) is served before `Maybe`
/// work (leaf frontiers, ranges a decision point may abandon).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Likelihood {
    /// The evaluation will read this range unless it fails first.
    Likely,
    /// The evaluation may read this range; a decision point ahead may
    /// abandon it.
    Maybe,
}

/// A hint that a selector's blocks should replicate ahead of demand.
#[derive(Debug, Clone)]
pub struct PreloadRequest {
    /// The selector whose backing blocks are wanted.
    pub selector: ArtifactSelector<Constrained>,
    /// How the request ranks against other speculative work.
    pub likelihood: Likelihood,
}

/// Command hinting that a selector's backing blocks will probably be
/// needed, so replication may fetch them ahead of demand.
///
/// Purely advisory: a provider may do nothing, and no outcome is
/// reported (the output is `()`), because a preload that fails must
/// surface as nothing — the demand read that actually needs the data
/// owns the error.
pub struct Preload;

impl Command for Preload {
    type Input = PreloadRequest;
    type Output = ();
}
