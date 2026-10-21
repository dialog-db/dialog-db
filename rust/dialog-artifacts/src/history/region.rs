//! Reading and writing the causal context a tree carries in its context
//! region (see [`key::context`](crate::key)).
//!
//! The region is the source of truth for the context of the head that
//! names the tree: the head carries only its active set and a digest of
//! the rest. Every operation that mints a head keeps the two in step: the
//! region of the head's tree holds the head's full context, and the head's
//! active set and spill summary are the projection of that region.

use dialog_common::ConditionalSync;
use futures_util::StreamExt as _;

use crate::merge::context_scope;
use crate::tree::ArtifactTree;
use crate::{
    ArchiveReader, Datum, DialogArtifactsError, Key, State, context_entry, read_context_entry,
};

use super::Context;

/// The full causal context `tree` carries in its context region.
///
/// Every entry is checked on the way in ([`read_context_entry`]): the
/// tree may be one another replica minted, and a hostile entry is refused
/// rather than folded into a context. A tree minted before the region
/// existed reads as the empty context; callers tell that case apart by
/// the head, which says whether its tree carries a region at all.
pub async fn context_in<S>(tree: &ArtifactTree, store: &S) -> Result<Context, DialogArtifactsError>
where
    S: ArchiveReader + Clone + ConditionalSync,
{
    let [range] = context_scope();
    let entries = tree.stream_range(range, store);
    futures_util::pin_mut!(entries);
    let mut context = Vec::new();
    while let Some(entry) = entries.next().await {
        let entry = entry?;
        context.push(read_context_entry(&entry.key, &entry.value)?);
    }
    Ok(context.into_iter().collect())
}

/// The region writes that make a tree hold `context`, given what its
/// region may hold now.
///
/// `held` lists every context the tree's region may have been copied from
/// (`None` for a tree minted before the region existed, which holds no
/// entries at all). An origin is written when any of them may hold it at
/// a different watermark, so the result is exact whichever one the region
/// actually came from, and a merge whose tree stitches regions from both
/// sides passes both. In the steady state that is one entry per commit:
/// its own origin's.
pub fn context_writes(context: &Context, held: &[Option<&Context>]) -> Vec<(Key, State<Datum>)> {
    context
        .iter()
        .filter(|(origin, watermark)| {
            held.iter().any(|held| match held {
                None => true,
                Some(held) => held.get(origin) != Some(*watermark),
            })
        })
        .map(|(origin, watermark)| context_entry(origin, watermark))
        .collect()
}
