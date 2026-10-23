//! The full causal context of a head, wherever it is written down.
//!
//! A head minted since the context region existed carries no context: its
//! tree's region holds it, exact for every origin except the head's own,
//! which the head derives from its edition. A head minted before carried
//! its whole context inline, and one minted before that carried nothing.
//! Every gate a commit or a merge runs on a head's context needs the whole
//! map, so this resolves it once per head, from the branch's memo when it
//! can, and remembers it there.

use std::sync::Arc;

use dialog_artifacts::history::{
    Context, ContextCache, Held, History, RevisionRecord, Version, Watermark, context_in,
    region_entry, region_writes,
};
use dialog_artifacts::tree::ArtifactNodeCache;
use dialog_artifacts::{ArchiveReader, Datum, DialogArtifactsError, Key, State, context_entry};
use dialog_common::Blake3Hash as NodeHash;
use dialog_search_tree::Cache;

use crate::{Index, Revision};

#[cfg(test)]
use crate::repository::source::SourceRef;
#[cfg(test)]
use crate::{Hydrate, NetworkedIndex};
#[cfg(test)]
use dialog_artifacts::history::context_of;
#[cfg(test)]
use dialog_capability::Provider;
#[cfg(test)]
use dialog_common::ConditionalSync;
#[cfg(test)]
use dialog_effects::archive::{Get as ArchiveGet, Put as ArchivePut};
#[cfg(test)]
use dialog_effects::blob::Read as BlobRead;
#[cfg(test)]
use dialog_effects::memory::Resolve;

/// The full context `head` publishes, or `None` for a head minted before
/// heads carried one.
///
/// Answered from `contexts` when remembered. A head carrying its whole
/// context inline (one minted before the region) is taken as it is, at no
/// read. Otherwise the head's tree's context region is read through
/// `store` (its root plus the few leaves the clustered region occupies)
/// and the head's own entry derived from its edition
/// ([`Revision::context_from_region`]), which refuses a region no head
/// minted. Whatever is resolved is remembered under the head's version.
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
        None if !head.region => return Ok(None),
        None => {
            let tree =
                Index::from_hash_with_cache(NodeHash::from(*head.tree.hash()), cache.clone());
            Arc::new(head.context_from_region(context_in(&tree, store).await?)?)
        }
    };
    contexts.insert(version, context.clone());
    Ok(Some(context))
}

/// Whether `head` has seen `version`, answered as cheaply as the head's
/// shape allows, or `None` for a head minted before heads carried a
/// context, whose answer only the ancestry walk gives.
///
/// A head's context holds its own version and is closed under ancestry,
/// so this is also whether the head has seen everything `version`'s head
/// had. The memo answers when it holds the head's context, and so does an
/// inline context. Otherwise, for a version of the head's own origin the
/// editions answer, since an origin's revisions form one chain. For any
/// other origin, the head's record answers when `records` holds it and it
/// names `version` as a parent or a skip link: the head built on it, as a
/// commit on another origin's head does, and a pull of that same head
/// after it is then answered at no read. Otherwise the region holds the
/// origin's watermark exactly, and one point read of that entry answers
/// (the tree's root and the path to its leaf).
pub(crate) async fn head_observes<S>(
    head: &Revision,
    version: &Version,
    contexts: &ContextCache,
    records: &Cache<Version, RevisionRecord>,
    store: &S,
    cache: &ArtifactNodeCache,
) -> Result<Option<bool>, DialogArtifactsError>
where
    S: ArchiveReader + Clone,
{
    if let Some(context) = contexts.cached(&head.version()).await {
        return Ok(Some(context.observes(version)));
    }
    if let Some(context) = head.inline_context() {
        return Ok(Some(context.observes(version)));
    }
    if !head.region {
        return Ok(None);
    }
    if version.origin == head.origin() {
        return Ok(Some(version.edition <= head.edition));
    }
    if records
        .get_cached(&head.version())
        .is_some_and(|record| record.parents.contains(version) || record.skips.contains(version))
    {
        return Ok(Some(true));
    }
    let tree = Index::from_hash_with_cache(NodeHash::from(*head.tree.hash()), cache.clone());
    Ok(Some(
        region_entry(&tree, &version.origin, store)
            .await?
            .is_some_and(|watermark| watermark.edition >= version.edition),
    ))
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

/// What a commit needs to know about the head it mints, `version`, built
/// on `parent`: the count it signs, the region entries it writes, and its
/// full context when that came at no read.
pub(crate) struct Successor {
    /// How many of its own origin's revisions the new head's ancestry
    /// holds, itself included.
    pub(crate) count: u64,
    /// The context region entries the new head's tree must gain.
    pub(crate) region: Vec<(Key, State<Datum>)>,
    /// The new head's full context, for the memo, when the parent's was
    /// in it (or the parent's shape made it free). `None` otherwise: the
    /// first reader that needs it reads the region then.
    pub(crate) context: Option<Arc<Context>>,
}

/// Work out the [`Successor`] of `parent` minted as `version`, reading as
/// little as the parent's shape allows.
///
/// When the memo holds the parent's full context, or the parent is a head
/// minted before the region (whose tree holds no entries, so the whole map
/// is written once), the successor's context is the parent's plus
/// `version`, and [`region_writes`] decides the entries, as before.
///
/// Otherwise the parent is a region head, and the commit does not
/// materialize its context. If the parent carries its own count:
///
/// - on the parent's own origin the commit writes nothing (the stored
///   entry where the chain began still derives the new head's) and signs
///   the parent's count plus one, at no read;
/// - on another origin's head (a takeover) it writes the two entries whose
///   chains change hands: the parent origin's exact watermark, which the
///   parent carries, and its own, one past what the parent holds of the
///   commit's origin, which one point read of that entry of the parent's
///   region answers.
///
/// The point read is the only source of that count. The line cannot know
/// it from its own history: a head it adopts can hold revisions of its
/// origin its own head does not, once the line has been reset to an
/// earlier head, so no record kept on the line is exact for every head it
/// later takes over. The read is one root-to-leaf path of the region,
/// a subset of what materializing the parent's context reads.
///
/// A region head minted before heads carried their count takes the first
/// path: its count is only in its region.
pub(crate) async fn successor<S, H>(
    parent: Option<&Revision>,
    version: Version,
    contexts: &ContextCache,
    store: &S,
    cache: &ArtifactNodeCache,
    history: impl FnOnce() -> H,
) -> Result<Successor, DialogArtifactsError>
where
    S: ArchiveReader + Clone,
    H: History,
{
    let Some(parent) = parent else {
        let mut context = Context::new();
        context.record(version);
        return Ok(Successor {
            count: 1,
            region: region_writes(version, &context, &[None]),
            context: Some(Arc::new(context)),
        });
    };
    let remembered = contexts.cached(&parent.version()).await;
    if remembered.is_none()
        && parent.region
        && let Some(theirs) = parent.own_watermark()
    {
        if parent.origin() == version.origin {
            return Ok(Successor {
                count: theirs.count.saturating_add(1),
                region: Vec::new(),
                context: None,
            });
        }
        let tree = Index::from_hash_with_cache(NodeHash::from(*parent.tree.hash()), cache.clone());
        let held = region_entry(&tree, &version.origin, store).await?;
        let ours = Watermark {
            edition: version.edition,
            count: held.map_or(0, |held| held.count).saturating_add(1),
        };
        ours.check()?;
        return Ok(Successor {
            count: ours.count,
            region: vec![
                context_entry(&parent.origin(), &theirs),
                context_entry(&version.origin, &ours),
            ],
            context: None,
        });
    }
    let before = match remembered {
        Some(context) => context,
        None => full_context(parent, contexts, store, cache, &history()).await?,
    };
    let mut context = Context::clone(&before);
    context.record(version);
    let region = region_writes(version, &context, &[held(parent, &before)]);
    Ok(Successor {
        count: context.get(&version.origin).map_or(1, |own| own.count),
        region,
        context: Some(Arc::new(context)),
    })
}

/// The count `head` signs for its own origin, given `context`, its full
/// causal context, which holds the head's own version.
pub(crate) fn own_count(head: &Revision, context: &Context) -> u64 {
    context
        .get(&head.origin())
        .map_or(1, |watermark| watermark.count)
}

/// What `head`'s tree holds in its context region, for deciding the region
/// writes of a tree minted from it (`history::region_writes`), given
/// `context`, the head's full context: `None` for a head minted before the
/// region existed, whose tree holds no entries.
pub(crate) fn held<'a>(head: &Revision, context: &'a Context) -> Option<Held<'a>> {
    head.region.then_some(Held {
        head: head.version(),
        context,
    })
}

/// The full context of `source`'s head, resolved as a commit or a pull
/// resolves it: for tests that assert on a head's context, which the head
/// itself no longer carries.
#[cfg(test)]
pub(crate) async fn head_context<'a, Env>(
    source: impl Into<SourceRef<'a>>,
    env: &Env,
) -> Result<Arc<Context>, DialogArtifactsError>
where
    Env: Provider<ArchiveGet>
        + Provider<ArchivePut>
        + Provider<BlobRead>
        + Provider<Resolve>
        + Provider<Hydrate>
        + ConditionalSync
        + 'static,
{
    let source = source.into();
    let head = source
        .revision()
        .ok_or_else(|| DialogArtifactsError::InvalidKey("the line has no head".into()))?;
    let store = NetworkedIndex::new(env, source.archive().index(), source.fallback());
    full_context(
        &head,
        &source.contexts(),
        &store,
        &source.node_cache(),
        &source.history(env),
    )
    .await
}

/// Check what `source`'s head keeps in its tree's context region, as a
/// reader that remembers nothing would: the head carries no context and
/// says its tree holds one, the region read raw and brought up to the head
/// equals the ancestry walk, and so does what the memo holds for the head
/// when it holds anything. Returns the region's raw entries and the walked
/// context, for tests that compare regions across heads.
#[cfg(test)]
pub(crate) async fn region_agrees<'a, Env>(
    source: impl Into<SourceRef<'a>>,
    env: &Env,
    what: &str,
) -> Result<(Context, Context), DialogArtifactsError>
where
    Env: Provider<ArchiveGet>
        + Provider<ArchivePut>
        + Provider<BlobRead>
        + Provider<Resolve>
        + Provider<Hydrate>
        + ConditionalSync
        + 'static,
{
    let source = source.into();
    let head = source
        .revision()
        .ok_or_else(|| DialogArtifactsError::InvalidKey("the line has no head".into()))?;
    assert!(
        head.region && head.context.is_none(),
        "{what}: the head keeps its context in its tree's region"
    );
    let store = NetworkedIndex::new(env, source.archive().index(), source.fallback());
    let tree = Index::from_hash_with_cache(NodeHash::from(*head.tree.hash()), source.node_cache());
    let raw = context_in(&tree, &store).await?;
    let read = head.context_from_region(raw.clone())?;
    let walked = context_of(&head.version(), &source.history(env)).await?;
    assert_eq!(
        read, walked,
        "{what}: the region reads as the ancestry walk"
    );
    assert_eq!(
        head.own_watermark().as_ref(),
        walked.get(&head.origin()),
        "{what}: the head carries its own watermark's count"
    );
    if let Some(memo) = source.contexts().cached(&head.version()).await {
        assert_eq!(*memo, walked, "{what}: the memo holds the ancestry walk");
    }
    Ok((raw, walked))
}
