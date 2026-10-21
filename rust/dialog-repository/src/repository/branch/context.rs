//! The full causal context of a head, wherever it is written down.
//!
//! A head carries its causal context in one of three shapes. Since the
//! context region, it carries the active set and a spill summary, and the
//! whole map is in its tree's context region. Before that it carried the
//! whole map inline, and before that nothing. Every gate a commit or a
//! pull runs needs the whole map, so this resolves it once per head, from
//! the branch's memo when it can, and remembers it there.

use std::sync::Arc;

use dialog_artifacts::history::{Context, ContextCache, History, context_in};
use dialog_artifacts::tree::ArtifactNodeCache;
use dialog_artifacts::{ArchiveReader, DialogArtifactsError};
use dialog_common::Blake3Hash as NodeHash;

use crate::{Index, Revision};

/// The full context `head` publishes, or `None` for a head minted before
/// heads carried one.
///
/// Answered from `contexts` when remembered. A head carrying its whole
/// context inline (one minted before the region, or one that spills
/// nothing) is taken as it is, at no read. Otherwise the head's tree's
/// context region is read through `store` (its root plus the few leaves
/// the clustered region occupies), and accepted only when it projects to
/// exactly what the head publishes ([`Revision::check_context`]).
/// Whatever is resolved is remembered under the head's version.
pub(crate) async fn published_context<S>(
    head: &Revision,
    contexts: &ContextCache,
    store: &S,
    cache: &ArtifactNodeCache,
) -> Result<Option<Arc<Context>>, DialogArtifactsError>
where
    S: ArchiveReader + Clone,
{
    let version = head.version();
    if let Some(context) = contexts.cached(&version).await {
        return Ok(Some(context));
    }
    let context = match head.inline_context() {
        Some(context) => Arc::new(context.clone()),
        None if head.context.is_none() => return Ok(None),
        None => {
            let tree =
                Index::from_hash_with_cache(NodeHash::from(*head.tree.hash()), cache.clone());
            let context = context_in(&tree, store).await?;
            head.check_context(&context)?;
            Arc::new(context)
        }
    };
    contexts.insert(version, context.clone());
    Ok(Some(context))
}

/// The full context of `head`: the one it publishes (see
/// [`published_context`]), or, for a head minted before heads carried
/// one, the one the O(ancestry) walk over `history` derives.
pub(crate) async fn full_context<S, H>(
    head: &Revision,
    contexts: &ContextCache,
    store: &S,
    cache: &ArtifactNodeCache,
    history: &H,
) -> Result<Arc<Context>, DialogArtifactsError>
where
    S: ArchiveReader + Clone,
    H: History,
{
    match published_context(head, contexts, store, cache).await? {
        Some(context) => Ok(context),
        None => contexts.context_of(&head.version(), history).await,
    }
}

/// What the context region of `head`'s tree holds, given `context`, the
/// head's full context: the whole of it for a head that carries a spill,
/// and nothing for a head minted before the region existed.
pub(crate) fn region_of<'a>(head: &Revision, context: &'a Context) -> Option<&'a Context> {
    head.spill.is_some().then_some(context)
}

/// The full context of an upstream head whose spill equals ours, without
/// a read: what we spill is what it spills, so its full context is ours
/// with the active sets exchanged. `None` when the spills differ, or when
/// either head does not carry one.
pub(crate) fn exchanged_context(
    ours: &Revision,
    our_context: &Context,
    theirs: &Revision,
) -> Option<Context> {
    match (&ours.context, &ours.spill, &theirs.context, &theirs.spill) {
        (Some(our_active), Some(our_spill), Some(their_active), Some(their_spill))
            if our_spill == their_spill =>
        {
            Some(our_context.exchange_active(our_active, their_active))
        }
        _ => None,
    }
}
