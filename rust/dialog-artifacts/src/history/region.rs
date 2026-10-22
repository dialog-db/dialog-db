//! Reading and writing the causal context a tree carries in its context
//! region (see [`key::context`](crate::key)).
//!
//! The region is where a head's causal context lives: the head carries
//! none. It holds every origin's watermark exactly except the head's own,
//! whose entry is where the head's chain of commits began; the reader
//! brings that one up to the head from the head's edition (see
//! `Revision::context_from_region` in `dialog-capability`). A commit on a
//! head of its own origin therefore writes nothing to the region, and
//! every head that breaks the chain (a merge, or a commit on another
//! origin's head) writes the entries it changes. [`region_writes`] is the
//! one place that decides which those are.

use dialog_common::ConditionalSync;
use futures_util::StreamExt as _;

use crate::merge::context_scope;
use crate::tree::ArtifactTree;
use crate::{
    ArchiveReader, Datum, DialogArtifactsError, Key, State, context_entry, read_context_entry,
};

use super::{Context, Origin, Version, Watermark};

/// The entries `tree` holds in its context region, as stored.
///
/// Every entry is checked on the way in ([`read_context_entry`]): the
/// tree may be one another replica minted, and a hostile entry is refused
/// rather than folded into a context. The entry for the head's own origin
/// is where its chain began, not the head's watermark, so this is the
/// head's context only once the head has derived that entry. A tree
/// minted before the region existed reads as the empty context; callers
/// tell that case apart by the head, which says whether its tree carries
/// a region at all.
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

/// A tree whose context region a newly minted tree starts from: the
/// version of the head that names it, and that head's full context.
///
/// That is all a region's entries follow from: every origin's entry is the
/// head's watermark for it, except the head's own origin, whose stored
/// entry is only known to derive the head's watermark.
#[derive(Debug, Clone, Copy)]
pub struct Held<'a> {
    /// The version of the head whose tree this is.
    pub head: Version,
    /// That head's full causal context.
    pub context: &'a Context,
}

/// The region writes a tree minted for `head` needs so that its region
/// reads back as `context`, the head's full causal context.
///
/// `held` lists every tree the new tree's region may have come from:
/// the parent's for a commit, the side a merge started from, both sides
/// for a graft that stitches the two regions together, and `None` for a
/// tree minted before the region existed, which holds no entries at all.
/// An entry is written unless every one of them already makes the reader
/// derive it, so the result is exact whichever one a range of the region
/// actually came from.
///
/// For a commit on a head of its own origin that is nothing: the stored
/// entry where the chain began still derives the new head's. A commit on
/// another origin's head writes two entries, the parent origin's (its
/// chain ends, so its entry must now be exact) and its own; a merge writes
/// its own entry, since its edition jumps past both parents, and every
/// entry its sides hold differently.
pub fn region_writes(
    head: Version,
    context: &Context,
    held: &[Option<Held<'_>>],
) -> Vec<(Key, State<Datum>)> {
    context
        .iter()
        .filter(|(origin, watermark)| {
            held.iter()
                .any(|held| !derives(*held, &head, origin, watermark))
        })
        .map(|(origin, watermark)| context_entry(origin, watermark))
        .collect()
}

/// Whether the region of `held`'s tree, read for `head`, already gives
/// `watermark` as `origin`'s entry.
fn derives(held: Option<Held<'_>>, head: &Version, origin: &Origin, watermark: &Watermark) -> bool {
    let Some(held) = held else {
        return false;
    };
    let ours = *origin == head.origin;
    if *origin == held.head.origin {
        // Stored is wherever the held head's chain began; all that is known
        // is that it derives the held head's watermark. Read for a head of
        // the same origin it derives one more revision per edition since;
        // read for another head it is taken as stored, which is that
        // watermark only by chance, so it is written.
        let Some(at_held) = held.context.get(origin) else {
            return false;
        };
        return ours && held.head.edition <= head.edition && *watermark == along(at_held, head);
    }
    // Stored exactly as the held head's watermark for it.
    let Some(stored) = held.context.get(origin) else {
        return false;
    };
    if ours {
        stored.edition <= head.edition && *watermark == along(stored, head)
    } else {
        stored == watermark
    }
}

/// `stored` brought up to `head` along `head`'s own chain: one edition and
/// one revision per commit.
fn along(stored: &Watermark, head: &Version) -> Watermark {
    Watermark {
        edition: head.edition,
        count: stored
            .count
            .saturating_add(head.edition.value() - stored.edition.value()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::history::Edition;
    use crate::read_context_entry;

    fn origin(seed: u8) -> Origin {
        Origin::from([seed; 32])
    }

    fn version(seed: u8, edition: u64) -> Version {
        Version::new(origin(seed), Edition::new(edition))
    }

    /// The reader's rule, for checking what the writes leave behind: the
    /// stored entries, with `head`'s own brought up to it.
    fn read(head: &Version, stored: &Context) -> Context {
        let mut context = stored.clone();
        let own = *stored
            .get(&head.origin)
            .expect("the head's origin is stored");
        context.set(head.origin, along(&own, head));
        context
    }

    /// `stored` with `writes` applied, as the tree's region holds it after
    /// the batch.
    fn apply(stored: &Context, writes: Vec<(Key, State<Datum>)>) -> Context {
        let mut region = stored.clone();
        for (key, value) in writes {
            let (origin, watermark) = read_context_entry(&key, &value).expect("a region entry");
            region.set(origin, watermark);
        }
        region
    }

    /// A chain of commits by one writer, each on the one before: the first
    /// commit on an empty tree writes its entry, and every commit after it
    /// writes nothing, while the region still reads as each head's context.
    #[test]
    fn it_writes_nothing_along_a_writers_own_chain() {
        let mut stored = Context::new();
        let mut context = Context::new();
        let mut previous: Option<Version> = None;
        for edition in 0..5 {
            let head = version(1, edition);
            let parent = previous.map(|head| (head, context.clone()));
            context.record(head);
            let held = parent.as_ref().map(|(head, context)| Held {
                head: *head,
                context,
            });
            let writes = region_writes(head, &context, &[held]);
            if edition == 0 {
                assert_eq!(writes.len(), 1, "the first commit writes its entry");
            } else {
                assert!(
                    writes.is_empty(),
                    "commit {edition} on its own head writes nothing"
                );
            }
            stored = apply(&stored, writes);
            assert_eq!(read(&head, &stored), context);
            previous = Some(head);
        }
    }

    /// A commit on another origin's head writes exactly two entries, the
    /// parent origin's (its chain ends there) and its own; then the next
    /// commit by the first writer, on that head, writes two again. This
    /// is the interleaving a reader that only patched would get wrong.
    #[test]
    fn it_writes_both_origins_where_a_chain_changes_hands() {
        let mut stored = Context::new();
        let mut context = Context::new();
        let mut parent: Option<(Version, Context)> = None;
        let steps = [(1, 0), (1, 1), (2, 2), (2, 3), (1, 4)];
        for (seed, edition) in steps {
            let head = version(seed, edition);
            context.record(head);
            let held = parent.as_ref().map(|(head, context)| Held {
                head: *head,
                context,
            });
            let writes = region_writes(head, &context, &[held]);
            let expected = match parent {
                Some((previous, _)) if previous.origin == head.origin => 0,
                Some(_) => 2,
                None => 1,
            };
            assert_eq!(writes.len(), expected, "the commit at edition {edition}");
            stored = apply(&stored, writes);
            assert_eq!(read(&head, &stored), context, "after edition {edition}");
            parent = Some((head, context.clone()));
        }
    }

    /// A merge writes its own entry, which jumps past both parents, and
    /// every entry the sides hold differently; with both sides held (a
    /// graft), an entry either side differs in is written. Whichever side
    /// a range of the region came from, it reads as the merged context.
    #[test]
    fn it_writes_a_merge_exactly_from_either_side() {
        let base = {
            let mut context = Context::new();
            context.record(version(1, 0));
            context.record(version(2, 1));
            context
        };
        let mut ours = base.clone();
        ours.record(version(1, 2));
        let ours_head = version(1, 2);
        let mut theirs = base.clone();
        theirs.record(version(3, 5));
        let theirs_head = version(3, 5);

        let merge = version(1, 6);
        let mut merged = ours.clone();
        merged.merge(&theirs);
        merged.record(merge);

        let held = [
            Some(Held {
                head: ours_head,
                context: &ours,
            }),
            Some(Held {
                head: theirs_head,
                context: &theirs,
            }),
        ];
        let writes = region_writes(merge, &merged, &held);
        // A region read as ours or as theirs: each side's own entry stored
        // where its chain began (here, at the side's head).
        for side in [&ours, &theirs] {
            let after = apply(side, writes.clone());
            assert_eq!(read(&merge, &after), merged);
        }
        let written: Vec<Origin> = writes
            .iter()
            .map(|(key, value)| read_context_entry(key, value).unwrap().0)
            .collect();
        assert!(written.contains(&origin(1)), "the merge's own entry");
        assert!(written.contains(&origin(3)), "theirs, which ours lacks");
    }

    /// A tree minted before the region existed holds no entries, so a head
    /// minted on it writes its whole context, once.
    #[test]
    fn it_writes_the_whole_context_atop_a_tree_without_a_region() {
        let mut context = Context::new();
        for seed in 0..10 {
            context.record(version(seed, u64::from(seed)));
        }
        let head = version(9, 9);
        assert_eq!(region_writes(head, &context, &[None]).len(), 10);
    }
}
