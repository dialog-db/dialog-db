//! The sync base a pull merges from (#673).
//!
//! A merge integrates only what the upstream gained since the sync base,
//! taking everything the base holds as already in the branch's head. So it
//! runs from the base only while the head holds the revision whose tree the
//! base is. Two shapes broke that: a head kept against a rival merge, which
//! records the rival's tree as the base without holding it, and a reset to
//! an older head, which moves the head behind the base. Each test runs its
//! writers as sessions of their own, each with a branch of its own.

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

use std::collections::{BTreeSet, HashMap};
use std::env;

use anyhow::Result;
use dialog_artifacts::history::{Edition, Origin, Version, context_of};
use dialog_artifacts::{Artifact, ArtifactSelector, Instruction, Pick, Value};
use dialog_capability::Subject;
use dialog_effects::authority::{Identify, OperatorExt as _};
use dialog_peer::helpers::{test_session_with_peer, unique_name};
use futures_util::{StreamExt as _, stream};

use crate::repository::branch::upstream::Bases;
use crate::{HeldCaches, RepositoryExt as _, Revision, TreeReference};

fn title(of: &str) -> Artifact {
    Artifact {
        the: "post/title".parse().expect("attribute parses"),
        of: of.parse().expect("entity parses"),
        is: Value::String("body".to_string()),
        cause: None,
    }
}

fn retract_one(of: &str) -> Instruction {
    Instruction::Retract(title(of))
}

fn assert_one(of: &str) -> Instruction {
    Instruction::Assert(
        Artifact {
            the: "post/title".parse().expect("attribute parses"),
            of: of.parse().expect("entity parses"),
            is: Value::String("body".to_string()),
            cause: None,
        },
        dialog_artifacts::Pick::All,
    )
}

/// A session of `$profile`'s peer under `$tag`: a writer with an origin of
/// its own.
macro_rules! session {
    ($profile:expr, $tag:expr) => {
        $profile
            .session($tag)
            .space($profile.state())
            .allow(Subject::any())
            .await?
    };
}

/// Open branch `$branch` of space `$name` through `$env`.
macro_rules! line {
    ($profile:expr, $name:expr, $env:expr, $branch:expr) => {
        $profile
            .space($name.clone())
            .open()
            .perform($env)
            .await?
            .branch($branch)
            .open()
            .perform($env)
            .await?
    };
}

/// Commit one fact about `$of` to `$branch` through `$env`.
macro_rules! commit {
    ($branch:expr, $env:expr, $of:expr) => {
        $branch
            .commit(stream::iter(vec![assert_one(&format!("{}", $of))]))
            .perform($env)
            .await?
    };
}

/// Retract the fact about `$of` from `$branch` through `$env`.
macro_rules! retract {
    ($branch:expr, $env:expr, $of:expr) => {
        $branch
            .commit(stream::iter(vec![retract_one(&format!("{}", $of))]))
            .perform($env)
            .await?
    };
}

/// What `$branch`'s bases cell is made to hold before a pull, as `$record`
/// says (see [`Record`]).
macro_rules! rewrite_bases {
    ($branch:expr, $env:expr, $record:expr) => {{
        let bases = match $record {
            Record::Kept => None,
            Record::Missing => Some(Bases::default()),
            Record::Stale => {
                let mut bases = $branch.based();
                for synced in $branch.tracked().synced {
                    let elsewhere = Version::new(Origin::from([9; 32]), Edition::GENESIS);
                    bases.record(
                        synced.target,
                        TreeReference::from([9; 32]),
                        elsewhere,
                        elsewhere,
                    );
                }
                Some(bases)
            }
        };
        if let Some(bases) = bases {
            $branch.bases().checkpoint().publish(bases, $env).await?;
        }
    }};
}

/// What a test leaves of a branch's bases cell before the pull it checks.
#[derive(Clone, Copy)]
enum Record {
    /// As the syncs wrote it.
    Kept,
    /// Empty, as a release before the cell, or `upgrade`, leaves it.
    Missing,
    /// Naming other trees than the tracking cell does, as an older release
    /// that moved the tracking cell, or a crash between the two writes,
    /// leaves it.
    Stale,
}

/// The entities `$branch` holds a title for, read through `$env`.
macro_rules! titles {
    ($branch:expr, $env:expr) => {{
        let rows = $branch
            .claims()
            .select(ArtifactSelector::new().the("post/title".parse()?))
            .to_owned()
            .perform($env)
            .await?
            .collect::<Vec<_>>()
            .await;
        let mut titles = BTreeSet::new();
        for row in rows {
            titles.insert(row?.of.to_string());
        }
        titles
    }};
}

/// The live claims of `post/title` and `post/state` in `$branch`, read
/// through `$env`.
macro_rules! claims {
    ($branch:expr, $env:expr) => {{
        let mut claims = BTreeSet::new();
        for the in ["post/title", "post/state"] {
            let rows = $branch
                .claims()
                .select(ArtifactSelector::new().the(the.parse()?))
                .to_owned()
                .perform($env)
                .await?
                .collect::<Vec<_>>()
                .await;
            for row in rows {
                let row = row?;
                let Value::String(is) = row.is else {
                    panic!("every modeled value is a string");
                };
                claims.insert(Claim {
                    the: the.to_string(),
                    of: row.of.to_string(),
                    is,
                });
            }
        }
        claims
    }};
}

/// Whether `$branch`'s head has a complete history: its ancestry walks to
/// the end, every revision on the way holding its record and claims.
macro_rules! walkable {
    ($branch:expr, $env:expr) => {{
        let head = $branch.revision().expect("the branch has a head");
        context_of(&head.version(), &$branch.history($env))
            .await
            .map(|_| ())
            .map_err(|error| error.to_string())
    }};
}

/// The origin `$branch` mints under through `$env`.
macro_rules! origin {
    ($branch:expr, $env:expr) => {{
        let authority = Identify.perform($env).await?;
        $branch
            .commit_identity(authority.profile(), &authority.did())
            .1
    }};
}

/// Two writers each merge the same two commits at once, so each holds a
/// merge the other has not seen: rival merges. The greater keeps its head;
/// the lesser has already built on its own before it meets the rival. The
/// next pull of the greater from the lesser must bring in the lesser
/// merge's record, which the lesser's later head names as a parent.
///
/// Which rival is the greater is decided by origin, the merges having the
/// same edition, so the roles are dealt by origin rather than left to the
/// identities a run happens to draw. `greater_keeps` deals them so that the
/// writer that later pulls the lesser's line holds the greater merge (the
/// shape that dropped the record); otherwise it holds the lesser, and
/// adopts the greater's head by root.
async fn rival_merges(greater_keeps: bool) -> Result<()> {
    let (writer, profile) = test_session_with_peer().await;
    let name = unique_name("repo");
    profile.space(name.clone()).open().perform(&writer).await?;
    let envs = [
        session!(profile, b"one"),
        session!(profile, b"two"),
        session!(profile, b"three"),
    ];
    let lines = [
        line!(profile, name, &envs[0], "one"),
        line!(profile, name, &envs[1], "two"),
        line!(profile, name, &envs[2], "three"),
    ];
    let mut by_origin: Vec<(Origin, usize)> = Vec::new();
    for at in 0..3 {
        by_origin.push((origin!(lines[at], &envs[at]), at));
    }
    by_origin.sort();
    // `committer` commits twice; `lesser` and `greater` mint the rivals.
    let (committer, lesser, greater) = (by_origin[0].1, by_origin[1].1, by_origin[2].1);
    // `keeper` holds a rival and later pulls the other's line; `builder`
    // holds the other rival and builds on it first.
    let (builder, keeper) = if greater_keeps {
        (lesser, greater)
    } else {
        (greater, lesser)
    };
    let (c, b, k) = (&lines[committer], &lines[builder], &lines[keeper]);
    let (ce, be, ke) = (&envs[committer], &envs[builder], &envs[keeper]);

    commit!(c, ce, "post:a");
    commit!(k, ke, "post:b");
    b.pull().from(c).perform(be).await?;
    b.pull().from(k).perform(be).await?;
    let builder_merge = b.revision().expect("the builder's merge");
    k.pull().from(c).perform(ke).await?;
    let keeper_merge = k.revision().expect("the keeper's merge");
    assert_eq!(
        builder_merge.edition, keeper_merge.edition,
        "the rivals have one edition, so their origins decide"
    );
    commit!(c, ce, "post:a2");

    // The keeper meets the builder's rival.
    k.pull().from(b).perform(ke).await?;
    let settled = k.revision().expect("the keeper's head");
    if greater_keeps {
        assert_eq!(settled, keeper_merge, "the greater rival keeps its head");
    } else {
        assert_eq!(
            settled, builder_merge,
            "the lesser rival adopts the greater's head"
        );
    }

    // The builder builds on its rival before it meets the other.
    b.pull().from(c).perform(be).await?;
    // The keeper pulls the builder's line again.
    k.pull().from(b).perform(ke).await?;

    assert_eq!(walkable!(k, ke), Ok(()), "the keeper's history is whole");
    let expected: BTreeSet<String> = ["post:a", "post:b", "post:a2"]
        .iter()
        .map(|of| {
            of.parse::<dialog_artifacts::Entity>()
                .map(|of| of.to_string())
        })
        .collect::<Result<_, _>>()?;
    assert_eq!(titles!(k, ke), expected, "the keeper holds every fact");

    // A writer that never held the builder's rival can vouch for every
    // revision the keeper's head brings in.
    let prepared = c.pull().from(k).prepare(ce).await?;
    prepared.novelty(ce).await?;
    prepared.commit(ce).await?;
    assert_eq!(walkable!(c, ce), Ok(()), "the committer's history is whole");
    assert_eq!(titles!(c, ce), expected, "the committer holds every fact");
    Ok(())
}

#[dialog_common::test]
async fn it_brings_in_the_record_of_a_rival_merge_the_greater_side_kept_its_head_against()
-> Result<()> {
    rival_merges(true).await
}

#[dialog_common::test]
async fn it_keeps_the_history_whole_when_the_lesser_rival_adopts_the_greater() -> Result<()> {
    rival_merges(false).await
}

/// A line reset to its first commit, behind the sync base it recorded with
/// an upstream, then pulling that upstream again: the upstream's head was
/// built on the line's first commit, so it holds everything the line holds,
/// and is adopted by root. Merging from the stale base would have kept the
/// line's first commit and dropped the upstream's own first commit, which
/// only the base held. `separate_caches` gives each writer caches of its
/// own, as two processes over one store would have. `record` says what
/// w1's bases cell holds at the pull.
async fn reset_behind_the_base(separate_caches: bool, record: Record) -> Result<()> {
    let (writer, profile) = test_session_with_peer().await;
    let name = unique_name("repo");
    profile.space(name.clone()).open().perform(&writer).await?;
    let one = session!(profile, b"one");
    let two = session!(profile, b"two");
    if separate_caches {
        HeldCaches::new().hold(&one);
        HeldCaches::new().hold(&two);
    }
    let w1 = line!(profile, name, &one, "one");
    let w2 = line!(profile, name, &two, "two");

    commit!(w2, &two, "post:two");
    commit!(w1, &one, "post:one");
    let first = w1.revision().expect("w1's first commit");
    w1.pull().from(&w2).perform(&one).await?;
    let merge = w1.revision().expect("w1's merge");
    w2.pull().from(&w1).perform(&two).await?;
    assert_eq!(w2.revision().as_ref(), Some(&merge), "w2 fast-forwards");

    w1.reset(first).perform(&one).await?;
    rewrite_bases!(w1, &one, record);
    w1.pull().from(&w2).perform(&one).await?;

    assert_eq!(
        w1.revision().as_ref(),
        Some(&merge),
        "the upstream's head holds all of w1's, so it is adopted"
    );
    assert_eq!(walkable!(w1, &one), Ok(()), "w1's history is whole");
    let expected: BTreeSet<String> = ["post:one", "post:two"]
        .iter()
        .map(|of| {
            of.parse::<dialog_artifacts::Entity>()
                .map(|of| of.to_string())
        })
        .collect::<Result<_, _>>()?;
    assert_eq!(titles!(w1, &one), expected, "w1 holds both facts");
    Ok(())
}

#[dialog_common::test]
async fn it_does_not_merge_from_a_base_a_reset_left_ahead_of_the_head() -> Result<()> {
    reset_behind_the_base(false, Record::Kept).await
}

#[dialog_common::test]
async fn it_does_not_merge_from_a_base_a_reset_left_ahead_of_the_head_with_separate_caches()
-> Result<()> {
    reset_behind_the_base(true, Record::Kept).await
}

/// The same, with no revision recorded for the bad base: unknown is not
/// trusted.
#[dialog_common::test]
async fn it_does_not_merge_from_an_unrecorded_base_a_reset_left_ahead_of_the_head() -> Result<()> {
    reset_behind_the_base(false, Record::Missing).await
}

/// The same, with the bases cell naming another tree than the tracking
/// cell, as an older release or a crash between the two writes leaves it.
#[dialog_common::test]
async fn it_does_not_merge_from_a_stale_base_a_reset_left_ahead_of_the_head() -> Result<()> {
    reset_behind_the_base(false, Record::Stale).await
}

/// A line reset behind its base, to an older head of the upstream's own
/// line, with a commit of its own on top: the upstream has not seen that
/// commit, so a real merge runs, and it runs from no base. Merging from the
/// stale base would have dropped what only the base held. The reset target
/// sits above every edition the line minted, so its next commit mints a
/// version it never minted before.
#[dialog_common::test]
async fn it_merges_from_no_base_when_the_head_does_not_hold_it() -> Result<()> {
    no_base(Record::Kept).await
}

/// The same with no revision recorded for the base, as after an upgrade:
/// the first merging pull runs from no base once.
#[dialog_common::test]
async fn it_merges_from_no_base_when_the_base_revision_is_unrecorded() -> Result<()> {
    no_base(Record::Missing).await
}

/// The same with a stale bases cell.
#[dialog_common::test]
async fn it_merges_from_no_base_when_the_base_revision_is_stale() -> Result<()> {
    no_base(Record::Stale).await
}

async fn no_base(record: Record) -> Result<()> {
    let (writer, profile) = test_session_with_peer().await;
    let name = unique_name("repo");
    profile.space(name.clone()).open().perform(&writer).await?;
    let one = session!(profile, b"one");
    let two = session!(profile, b"two");
    let w1 = line!(profile, name, &one, "one");
    let w2 = line!(profile, name, &two, "two");

    commit!(w1, &one, "post:a");
    w2.pull().from(&w1).perform(&two).await?;
    commit!(w2, &two, "post:b");
    let b = w2.revision().expect("B");
    commit!(w2, &two, "post:c");
    w1.pull().from(&w2).perform(&one).await?;
    assert_eq!(w1.revision(), w2.revision(), "w1 fast-forwards to C");
    commit!(w2, &two, "post:d");

    // Behind the base (C), at B, above w1's only mint (A).
    w1.reset(b).perform(&one).await?;
    commit!(w1, &one, "post:e");
    rewrite_bases!(w1, &one, record);
    w1.pull().from(&w2).perform(&one).await?;

    assert_eq!(walkable!(w1, &one), Ok(()), "w1's history is whole");
    let expected: BTreeSet<String> = ["post:a", "post:b", "post:c", "post:d", "post:e"]
        .iter()
        .map(|of| {
            of.parse::<dialog_artifacts::Entity>()
                .map(|of| of.to_string())
        })
        .collect::<Result<_, _>>()?;
    assert_eq!(titles!(w1, &one), expected, "w1 holds every fact");
    Ok(())
}

/// The upstream's head moved behind the base, not ours: w2 resets to a head
/// of w3's that never held w2's C, which w1 synced at. Merging from C would
/// read c, which C holds and the upstream lacks, as removed upstream, and
/// remove it from w1, though no writer retracted it. The upstream does not
/// hold C, so the merge runs from no base and w1 keeps c. The reset target
/// sits above w2's last mint, so no version is reused.
#[dialog_common::test]
async fn it_merges_from_no_base_when_the_upstream_does_not_hold_it() -> Result<()> {
    upstream_reset(true, Record::Kept).await
}

/// The same with w1 still at C, so its tree is the base.
#[dialog_common::test]
async fn it_merges_from_no_base_when_the_upstream_does_not_hold_our_tree() -> Result<()> {
    upstream_reset(false, Record::Kept).await
}

/// The same with no revision recorded for the base. Our tree being the base
/// is no reason to trust it: the base's revision is then our head, and the
/// upstream does not hold it.
#[dialog_common::test]
async fn it_merges_from_no_base_when_the_upstream_does_not_hold_our_unrecorded_tree() -> Result<()>
{
    upstream_reset(false, Record::Missing).await
}

/// `commit_on_top` has w1 commit x after syncing at C; `record` says what
/// w1's bases cell holds at the pull.
async fn upstream_reset(commit_on_top: bool, record: Record) -> Result<()> {
    let (writer, profile) = test_session_with_peer().await;
    let name = unique_name("repo");
    profile.space(name.clone()).open().perform(&writer).await?;
    let one = session!(profile, b"one");
    let two = session!(profile, b"two");
    let three = session!(profile, b"three");
    let w1 = line!(profile, name, &one, "one");
    let w2 = line!(profile, name, &two, "two");
    let w3 = line!(profile, name, &three, "three");

    commit!(w1, &one, "post:a");
    w2.pull().from(&w1).perform(&two).await?;
    w3.pull().from(&w1).perform(&three).await?;
    commit!(w2, &two, "post:c");
    let c = w2.revision().expect("C");
    w1.pull().from(&w2).perform(&one).await?;
    assert_eq!(w1.revision().as_ref(), Some(&c), "w1 fast-forwards to C");
    commit!(w3, &three, "post:y");
    commit!(w3, &three, "post:z");
    let z = w3.revision().expect("Z");
    assert!(z.edition > c.edition, "Z sits above w2's last mint");

    w2.reset(z).perform(&two).await?;
    let mut facts = vec!["post:a", "post:c", "post:y", "post:z"];
    if commit_on_top {
        commit!(w1, &one, "post:x");
        facts.push("post:x");
    }
    rewrite_bases!(w1, &one, record);
    w1.pull().from(&w2).perform(&one).await?;

    assert_eq!(walkable!(w1, &one), Ok(()), "w1's history is whole");
    let expected: BTreeSet<String> = facts
        .iter()
        .map(|of| {
            of.parse::<dialog_artifacts::Entity>()
                .map(|of| of.to_string())
        })
        .collect::<Result<_, _>>()?;
    assert_eq!(titles!(w1, &one), expected, "w1 keeps c");
    Ok(())
}

/// A replica that never resets keeps its own facts when another resets and
/// pushes. w2 is the shared upstream. w3 pushes its fact; w1 pulls it,
/// resets to its own earlier head, pushes, commits and pushes again. Before,
/// the first push rewound w2 and the second published a head without w3's
/// fact, and w3's next pull merged from its base and removed its own fact.
/// Now both pushes are refused, w2 keeps w3's head, and w3 keeps every fact.
#[dialog_common::test]
async fn it_keeps_a_replica_that_never_reset_whole_when_another_resets_and_pushes() -> Result<()> {
    let (writer, profile) = test_session_with_peer().await;
    let name = unique_name("repo");
    profile.space(name.clone()).open().perform(&writer).await?;
    let one = session!(profile, b"one");
    let two = session!(profile, b"two");
    let three = session!(profile, b"three");
    let w1 = line!(profile, name, &one, "one");
    let w2 = line!(profile, name, &two, "two");
    let w3 = line!(profile, name, &three, "three");

    commit!(w2, &two, "post:a");
    w1.pull().from(&w2).perform(&one).await?;
    let b = w1.revision().expect("B");
    commit!(w3, &three, "post:w3");
    w3.pull().from(&w2).perform(&three).await?;
    w3.push().to(&w2).perform(&three).await?;
    let c = w3.revision().expect("C");
    w1.pull().from(&w2).perform(&one).await?;

    w1.reset(b).perform(&one).await?;
    let pushed = w1.push().to(&w2).perform(&one).await;
    assert!(
        matches!(pushed, Err(crate::PushError::NonFastForward { .. })),
        "a push from behind the base is refused: {pushed:?}"
    );
    commit!(w1, &one, "post:d");
    let pushed = w1.push().to(&w2).perform(&one).await;
    assert!(
        matches!(pushed, Err(crate::PushError::NonFastForward { .. })),
        "a push of a head without the upstream's is refused: {pushed:?}"
    );
    let w2 = line!(profile, name, &two, "two");
    assert_eq!(w2.revision().as_ref(), Some(&c), "the upstream keeps C");

    commit!(w3, &three, "post:e");
    w3.pull().from(&w2).perform(&three).await?;
    assert_eq!(walkable!(w3, &three), Ok(()), "w3's history is whole");
    let expected: BTreeSet<String> = ["post:a", "post:w3", "post:e"]
        .iter()
        .map(|of| {
            of.parse::<dialog_artifacts::Entity>()
                .map(|of| of.to_string())
        })
        .collect::<Result<_, _>>()?;
    assert_eq!(titles!(w3, &three), expected, "w3 keeps its own fact");
    Ok(())
}

/// The greater rival can push onto the lesser once it has pulled it. The
/// greater keeps its head against the lesser merge and records the lesser's
/// tree as its base without holding that revision: the two merges hold the
/// same data, and differ only by their own revision records. A push of the
/// greater's head over the lesser's, which no head has built on, is the
/// settlement the lesser reaches by pulling, and drops only the lesser
/// merge's own record, which nothing names. The push guard accepts it: the
/// base is the upstream's head, and our head holds the head that held the
/// base when the pull recorded it.
#[dialog_common::test]
async fn it_lets_the_greater_rival_push_onto_the_lesser_after_a_pull() -> Result<()> {
    let (writer, profile) = test_session_with_peer().await;
    let name = unique_name("repo");
    profile.space(name.clone()).open().perform(&writer).await?;
    let envs = [
        session!(profile, b"one"),
        session!(profile, b"two"),
        session!(profile, b"three"),
    ];
    let lines = [
        line!(profile, name, &envs[0], "one"),
        line!(profile, name, &envs[1], "two"),
        line!(profile, name, &envs[2], "three"),
    ];
    let mut by_origin: Vec<(Origin, usize)> = Vec::new();
    for at in 0..3 {
        by_origin.push((origin!(lines[at], &envs[at]), at));
    }
    by_origin.sort();
    let (committer, lesser, greater) = (by_origin[0].1, by_origin[1].1, by_origin[2].1);
    let (c, b, k) = (&lines[committer], &lines[lesser], &lines[greater]);
    let (ce, be, ke) = (&envs[committer], &envs[lesser], &envs[greater]);
    commit!(c, ce, "post:a");
    commit!(k, ke, "post:b");
    b.pull().from(c).perform(be).await?;
    b.pull().from(k).perform(be).await?;
    k.pull().from(c).perform(ke).await?;
    let keeper_merge = k.revision().expect("the greater merge");
    k.pull().from(b).perform(ke).await?;
    assert_eq!(
        k.revision().as_ref(),
        Some(&keeper_merge),
        "the greater keeps its head"
    );
    k.push().to(b).perform(ke).await?;
    let b = line!(profile, name, be, NAMES[lesser]);
    assert_eq!(
        b.revision().as_ref(),
        Some(&keeper_merge),
        "the lesser now holds the greater merge"
    );
    assert_eq!(walkable!(b, be), Ok(()), "the lesser's history is whole");
    let expected: BTreeSet<String> = ["post:a", "post:b"]
        .iter()
        .map(|of| {
            of.parse::<dialog_artifacts::Entity>()
                .map(|of| of.to_string())
        })
        .collect::<Result<_, _>>()?;
    assert_eq!(titles!(b, be), expected, "the lesser holds every fact");
    Ok(())
}

/// A merge from no base keeps a retraction: a fact the upstream retracted
/// stays retracted in a line that still holds the assertion. The line
/// resets behind its base to a head holding the fact and commits on top,
/// so the upstream has not seen its head and a real merge runs from no
/// base. Every claim and record the upstream holds is integrated, its
/// retraction's coverage record among them, and that record retires the
/// line's copy of the assertion, as it does in any merge.
#[dialog_common::test]
async fn it_keeps_a_retraction_when_it_merges_from_no_base() -> Result<()> {
    let (writer, profile) = test_session_with_peer().await;
    let name = unique_name("repo");
    profile.space(name.clone()).open().perform(&writer).await?;
    let one = session!(profile, b"one");
    let two = session!(profile, b"two");
    let w1 = line!(profile, name, &one, "one");
    let w2 = line!(profile, name, &two, "two");

    commit!(w1, &one, "post:a");
    w2.pull().from(&w1).perform(&two).await?;
    commit!(w2, &two, "post:f");
    w1.pull().from(&w2).perform(&one).await?;
    let holding = w1.revision().expect("w1 holds f");
    retract!(w2, &two, "post:f");
    w1.pull().from(&w2).perform(&one).await?;
    assert_eq!(w1.revision(), w2.revision(), "w1 takes the retraction");
    commit!(w2, &two, "post:x");

    // Behind the base, at a head that still asserts f, above w1's only
    // mint, with a commit on top the upstream has not seen.
    w1.reset(holding).perform(&one).await?;
    commit!(w1, &one, "post:g");
    w1.pull().from(&w2).perform(&one).await?;

    assert_eq!(walkable!(w1, &one), Ok(()), "w1's history is whole");
    let expected: BTreeSet<String> = ["post:a", "post:g", "post:x"]
        .iter()
        .map(|of| {
            of.parse::<dialog_artifacts::Entity>()
                .map(|of| of.to_string())
        })
        .collect::<Result<_, _>>()?;
    assert_eq!(titles!(w1, &one), expected, "f stays retracted");
    Ok(())
}

/// A push after a reset behind the base does not rewind the upstream. w1
/// fast-forwards to w2's C, resets to B, and pushes: the upstream still
/// sits at the sync base, so the tree check alone would take the push for
/// a fast-forward and move w2 back to B, losing c. The push is refused,
/// since w1's head does not hold the upstream's.
#[dialog_common::test]
async fn it_does_not_push_a_head_a_reset_left_behind_the_base() -> Result<()> {
    let (writer, profile) = test_session_with_peer().await;
    let name = unique_name("repo");
    profile.space(name.clone()).open().perform(&writer).await?;
    let one = session!(profile, b"one");
    let two = session!(profile, b"two");
    let w1 = line!(profile, name, &one, "one");
    let w2 = line!(profile, name, &two, "two");
    commit!(w1, &one, "post:a");
    w2.pull().from(&w1).perform(&two).await?;
    commit!(w2, &two, "post:b");
    let b = w2.revision().expect("B");
    commit!(w2, &two, "post:c");
    w1.pull().from(&w2).perform(&one).await?;
    w1.reset(b).perform(&one).await?;
    let (head, tracked, based) = (w1.revision(), w1.tracked(), w1.based());
    let pushed = w1.push().to(&w2).perform(&one).await;
    assert!(
        matches!(pushed, Err(crate::PushError::NonFastForward { .. })),
        "the push is refused: {pushed:?}"
    );
    let w1 = line!(profile, name, &one, "one");
    assert_eq!(w1.revision(), head, "the refusal leaves our head");
    assert_eq!(w1.tracked(), tracked, "the refusal records no sync");
    assert_eq!(w1.based(), based, "the refusal records no base");
    let w2 = line!(profile, name, &two, "two");
    let c = "post:c".parse::<dialog_artifacts::Entity>()?.to_string();
    assert!(titles!(w2, &two).contains(&c), "the upstream keeps c");
    Ok(())
}

/// Random interleavings of commit, conflicting single-valued writes,
/// retract, re-assert, pull, mutual pull (which breeds rival merges), push
/// and reset across three writers.
///
/// The model keeps, per head, its live claims and the claims its history
/// has retired. A claim is an attribute, an entity and a value, written by
/// one write: a value asserted again after it was retracted is a new claim,
/// which nothing that retired the first one retires. Facts are of two
/// attributes: `post/title`, written under `Pick::All`, whose writes
/// append, and `post/state`, written under `Pick::Last` to one of three
/// entities, so writers race on them. A write under `Last` retires the claim
/// its pick returns, and concurrent writes leave several live claims for
/// the next write to choose among; which one it retires is read from the
/// writer's head after the write, not modeled. A retraction retires a live
/// claim of either attribute, and a re-assertion writes again a value its
/// writer's history retired. A head's history retired what its parents'
/// histories retired plus what its own write retired.
///
/// What is checked:
/// - after every step the acting writer's head has a complete history, and
///   holds exactly the model's live claims;
/// - after every pull the puller holds exactly the claims either side held
///   before it, less every claim either side's history retired: no claim is
///   lost, none that was retired comes back, and a re-asserted value
///   survives a merge with a history that retired its earlier claim;
/// - a push either lands, and the upstream then holds the pusher's head, or
///   is refused as a non-fast-forward and leaves the upstream as it was; a
///   push that lands never drops a claim the upstream held that the
///   pusher's history had not retired, which a rewind would;
/// - no writer stays refused: after a refused push the writer pulls its
///   target, and its next push lands.
///
/// A reset moves a writer to a head it has held, or to the other writer's
/// current head, but only to one that holds the revision the writer last
/// minted. Every revision of one origin then stays on one chain, which
/// every context assumes: a context keeps one watermark per origin, so
/// holding an origin's edition n reads as holding all its revisions up to
/// n. A reset to a head without the writer's last mint, followed by a
/// commit, breaks that (#682): below the last mint the commit reuses a
/// version, and above it (a takeover of another writer's line) it forks
/// the origin, after which a context that holds one fork claims the other
/// and a merge screens the other's records out as already seen. This test
/// reproduced the second as a history hole before the floor was this one.
/// A drawn target that does not hold the last mint is undone and counted.
#[dialog_common::test]
async fn it_keeps_histories_whole_and_facts_kept_under_random_interleavings() -> Result<()> {
    let seeds: u64 = env::var("DIALOG_SYNC_BASE_INTERLEAVINGS")
        .ok()
        .and_then(|seeds| seeds.parse().ok())
        .unwrap_or(100);
    let steps = 32;
    let mut seen = Census::default();
    for seed in 0..seeds {
        let (writer, profile) = test_session_with_peer().await;
        let name = unique_name("repo");
        profile.space(name.clone()).open().perform(&writer).await?;
        let envs = [
            session!(profile, b"one"),
            session!(profile, b"two"),
            session!(profile, b"three"),
        ];
        let mut lines = [
            line!(profile, name, &envs[0], NAMES[0]),
            line!(profile, name, &envs[1], NAMES[1]),
            line!(profile, name, &envs[2], NAMES[2]),
        ];
        let mut held: [Vec<Revision>; 3] = [Vec::new(), Vec::new(), Vec::new()];
        // Per head, its live claims and the claims its history retired,
        // each claim with the write that made it.
        let mut live: HashMap<String, Written> = HashMap::new();
        let mut retired: HashMap<String, Written> = HashMap::new();
        let key = |head: &Revision| format!("{:?}", (head.version(), &head.tree));
        let of = |map: &HashMap<String, Written>, head: Option<&Revision>| {
            head.and_then(|head| map.get(&key(head)).cloned())
                .unwrap_or_default()
        };
        let mut writes = 0u64;
        let mut minted: [Option<Version>; 3] = [None, None, None];
        let mut state = seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
        let mut next = |bound: u64| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state % bound
        };
        for step in 0..steps {
            let at = next(3) as usize;
            let other = (at + 1 + next(2) as usize) % 3;
            let behind = match (lines[at].revision(), minted[at]) {
                (Some(head), Some(last)) => head.edition < last.edition,
                (None, Some(_)) => true,
                _ => false,
            };
            let op = next(11);
            let what = format!("seed {seed} step {step} writer {at} op {op}");
            let mut pullers = Vec::new();
            // A refused push to retry once the pusher has pulled its target.
            let mut retry = None;
            // A write: the instruction, and the claim it must leave live,
            // if any.
            let write = match op {
                0 | 1 if !behind => {
                    let claim = Claim::title(&format!("post:{seed}-{step}"));
                    Some((claim.assert(), Some(claim)))
                }
                7 if !behind => {
                    let claim =
                        Claim::state(&format!("post:k{}", next(3)), &format!("{seed}-{step}"));
                    seen.conflicting += 1;
                    Some((claim.assert(), Some(claim)))
                }
                6 if !behind => {
                    let held_now: Vec<Claim> = project(&of(&live, lines[at].revision().as_ref()))
                        .into_iter()
                        .collect();
                    if held_now.is_empty() {
                        None
                    } else {
                        let claim = held_now[next(held_now.len() as u64) as usize].clone();
                        seen.retracts += 1;
                        Some((claim.retract(), None))
                    }
                }
                10 if !behind => {
                    let head = lines[at].revision();
                    let held_now = project(&of(&live, head.as_ref()));
                    let gone: Vec<Claim> = project(&of(&retired, head.as_ref()))
                        .into_iter()
                        .filter(|claim| !held_now.contains(claim))
                        .collect();
                    if gone.is_empty() {
                        None
                    } else {
                        let claim = gone[next(gone.len() as u64) as usize].clone();
                        seen.reasserts += 1;
                        Some((claim.assert(), Some(claim)))
                    }
                }
                _ => None,
            };
            if let Some((instruction, kept)) = write {
                let parent = lines[at].revision();
                let before = of(&live, parent.as_ref());
                lines[at]
                    .commit(stream::iter(vec![instruction]))
                    .perform(&envs[at])
                    .await?;
                let head = lines[at].revision().expect("a write leaves a head");
                minted[at] = Some(head.version());
                let after = claims!(lines[at], &envs[at]);
                let gone: Written = before
                    .iter()
                    .filter(|(claim, _)| !after.contains(claim))
                    .cloned()
                    .collect();
                let mut now: Written = before.difference(&gone).cloned().collect();
                if let Some(kept) = kept {
                    writes += 1;
                    now.insert((kept, writes));
                }
                let mut history = of(&retired, parent.as_ref());
                history.extend(gone);
                live.insert(key(&head), now);
                retired.insert(key(&head), history);
            }
            match op {
                2 => {
                    // A head this writer has held, or the other writer's
                    // current head: taking over its line.
                    let floor = minted[at];
                    let theirs = lines[other].revision();
                    let mut targets: Vec<&Revision> = held[at].iter().collect();
                    targets.extend(theirs.as_ref().filter(|head| !held[at].contains(head)));
                    let targets: Vec<&Revision> = targets
                        .into_iter()
                        .filter(|head| floor.is_none_or(|floor| head.edition >= floor.edition))
                        .collect();
                    if !targets.is_empty() {
                        let target = targets[next(targets.len() as u64) as usize].clone();
                        let previous = lines[at].revision();
                        lines[at].reset(target.clone()).perform(&envs[at]).await?;
                        // Read once the target is the head, so the history
                        // walks the target's own tree. A target that does
                        // not hold the writer's last mint is a takeover, and
                        // is undone: see the doc comment.
                        let takeover = match &minted[at] {
                            Some(own) => {
                                !context_of(&target.version(), &lines[at].history(&envs[at]))
                                    .await?
                                    .observes(own)
                            }
                            None => false,
                        };
                        if let (true, Some(previous)) = (takeover, previous) {
                            lines[at].reset(previous).perform(&envs[at]).await?;
                            seen.declined += 1;
                        } else {
                            seen.resets += 1;
                        }
                    }
                }
                3 => {
                    // Both pull each other: when both moved since they last
                    // met, both mint a merge of the same two heads.
                    pullers.push((at, other));
                    pullers.push((other, at));
                    seen.rivals += 1;
                }
                8 | 9 => {
                    let Some(ours) = lines[at].revision() else {
                        continue;
                    };
                    let before = lines[other].revision();
                    let theirs = of(&live, before.as_ref());
                    let pushed = lines[at].push().to(&lines[other]).perform(&envs[at]).await;
                    // The upstream as its own writer opens it again.
                    lines[other] = line!(profile, name, &envs[other], NAMES[other]);
                    let upstream = &lines[other];
                    let after = upstream.revision();
                    match pushed {
                        // Our head is the base we last synced at: nothing
                        // to send, and the upstream is left as it was.
                        Ok(_) if after == before && after.as_ref() != Some(&ours) => {
                            seen.idle += 1;
                        }
                        Ok(_) => {
                            assert_eq!(
                                after.as_ref(),
                                Some(&ours),
                                "{what}: a push lands our head"
                            );
                            let covered = of(&retired, Some(&ours));
                            let now = claims!(upstream, &envs[other]);
                            let dropped: Vec<&Claim> = theirs
                                .difference(&covered)
                                .map(|(claim, _)| claim)
                                .filter(|claim| !now.contains(claim))
                                .collect();
                            assert!(
                                dropped.is_empty(),
                                "{what}: the push rewound {other}, dropping {dropped:?}"
                            );
                            seen.pushes += 1;
                        }
                        Err(crate::PushError::NonFastForward { .. }) => {
                            assert_eq!(
                                after, before,
                                "{what}: a refused push leaves {other} as it was"
                            );
                            seen.refused += 1;
                            pullers.push((at, other));
                            retry = Some((at, other));
                        }
                        Err(error) => panic!("{what}: push from {at} to {other} failed: {error}"),
                    }
                    if let Some(after) = after
                        && !held[other].contains(&after)
                    {
                        held[other].push(after);
                    }
                }
                4 | 5 => pullers.push((at, other)),
                _ => {}
            }
            for (to, from) in pullers {
                let Some(theirs) = lines[from].revision() else {
                    continue;
                };
                let before = lines[to].revision();
                let mut covered = of(&retired, before.as_ref());
                covered.extend(of(&retired, Some(&theirs)));
                let mut expected = of(&live, before.as_ref());
                expected.extend(of(&live, Some(&theirs)));
                let expected: Written = expected.difference(&covered).cloned().collect();
                lines[to]
                    .pull()
                    .from(&lines[from])
                    .perform(&envs[to])
                    .await?;
                seen.pulls += 1;
                let after = lines[to].revision().expect("a pull leaves a head");
                if Some(&after) != before.as_ref() && after != theirs {
                    minted[to] = Some(after.version());
                    seen.merges += 1;
                }
                assert_eq!(
                    walkable!(lines[to], &envs[to]),
                    Ok(()),
                    "{what}: writer {to}'s history after pulling {from}"
                );
                assert_eq!(
                    claims!(lines[to], &envs[to]),
                    project(&expected),
                    "{what}: writer {to} holds what it and {from} held, less what either retired"
                );
                live.insert(key(&after), expected);
                retired.insert(key(&after), covered);
                if !held[to].contains(&after) {
                    held[to].push(after);
                }
            }
            // No writer stays refused: once it has pulled its target, and
            // the target has not moved since, its push lands.
            if let Some((at, other)) = retry {
                let pushed = lines[at].push().to(&lines[other]).perform(&envs[at]).await;
                assert!(
                    pushed.is_ok(),
                    "{what}: after pulling {other}, writer {at}'s push lands: {pushed:?}"
                );
                lines[other] = line!(profile, name, &envs[other], NAMES[other]);
                let (ours, theirs) = (lines[at].revision(), lines[other].revision());
                assert_eq!(
                    theirs.as_ref().map(|head| &head.tree),
                    ours.as_ref().map(|head| &head.tree),
                    "{what}: after pulling {other}, writer {at}'s push lands its head"
                );
                if let Some(theirs) = theirs
                    && !held[other].contains(&theirs)
                {
                    held[other].push(theirs);
                }
                seen.recovered += 1;
            }
            if let Some(head) = lines[at].revision() {
                assert_eq!(
                    walkable!(lines[at], &envs[at]),
                    Ok(()),
                    "{what}: writer {at}'s history"
                );
                assert_eq!(
                    claims!(lines[at], &envs[at]),
                    project(&of(&live, Some(&head))),
                    "{what}: writer {at} holds the model's claims"
                );
                if !held[at].contains(&head) {
                    held[at].push(head);
                }
            }
        }
    }
    eprintln!("{seeds} seeds: {seen:?}");
    assert!(
        seen.pulls > 0
            && seen.rivals > 0
            && seen.merges > 0
            && seen.resets > 0
            && seen.declined > 0
            && seen.retracts > 0
            && seen.reasserts > 0
            && seen.conflicting > 0
            && seen.pushes > 0
            && seen.refused > 0
            && seen.recovered > 0,
        "every kind of step ran: {seen:?}"
    );
    Ok(())
}

/// Claims as the interleaving test models them, each with the write that
/// made it.
type Written = BTreeSet<(Claim, u64)>;

/// The claims of `written`, as a branch reads them.
fn project(written: &Written) -> BTreeSet<Claim> {
    written.iter().map(|(claim, _)| claim.clone()).collect()
}

/// The interleaving test's branches, one per writer.
const NAMES: [&str; 3] = ["one", "two", "three"];

/// What the interleaving test ran, so it can assert it ran each kind.
#[derive(Debug, Default)]
struct Census {
    pulls: usize,
    rivals: usize,
    merges: usize,
    resets: usize,
    declined: usize,
    retracts: usize,
    conflicting: usize,
    pushes: usize,
    refused: usize,
    idle: usize,
    reasserts: usize,
    recovered: usize,
}

/// A claim as the interleaving test models it: attribute, entity, value.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Claim {
    the: String,
    of: String,
    is: String,
}

impl Claim {
    fn title(of: &str) -> Self {
        Self::new("post/title", of, "body")
    }

    fn state(of: &str, is: &str) -> Self {
        Self::new("post/state", of, is)
    }

    fn new(the: &str, of: &str, is: &str) -> Self {
        Self {
            the: the.to_string(),
            of: of
                .parse::<dialog_artifacts::Entity>()
                .expect("entity parses")
                .to_string(),
            is: is.to_string(),
        }
    }

    fn artifact(&self) -> Artifact {
        Artifact {
            the: self.the.parse().expect("attribute parses"),
            of: self.of.parse().expect("entity parses"),
            is: Value::String(self.is.clone()),
            cause: None,
        }
    }

    /// The assertion of this claim under its attribute's pick.
    fn assert(&self) -> Instruction {
        let pick = if self.the == "post/state" {
            Pick::Last
        } else {
            Pick::All
        };
        Instruction::Assert(self.artifact(), pick)
    }

    fn retract(&self) -> Instruction {
        Instruction::Retract(self.artifact())
    }
}
