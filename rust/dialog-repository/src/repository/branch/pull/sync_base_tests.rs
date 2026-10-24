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
use dialog_artifacts::history::{Edition, Origin, context_of};
use dialog_artifacts::{Artifact, ArtifactSelector, Instruction, Value};
use dialog_capability::Subject;
use dialog_effects::authority::{Identify, OperatorExt as _};
use dialog_peer::helpers::{test_session_with_peer, unique_name};
use futures_util::{StreamExt as _, stream};

use crate::{HeldCaches, RepositoryExt as _, Revision};

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
/// own, as two processes over one store would have.
async fn reset_behind_the_base(separate_caches: bool) -> Result<()> {
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
    reset_behind_the_base(false).await
}

#[dialog_common::test]
async fn it_does_not_merge_from_a_base_a_reset_left_ahead_of_the_head_with_separate_caches()
-> Result<()> {
    reset_behind_the_base(true).await
}

/// A line reset behind its base, to an older head of the upstream's own
/// line, with a commit of its own on top: the upstream has not seen that
/// commit, so a real merge runs, and it runs from no base. Merging from the
/// stale base would have dropped what only the base held. The reset target
/// sits above every edition the line minted, so its next commit mints a
/// version it never minted before.
#[dialog_common::test]
async fn it_merges_from_no_base_when_the_head_does_not_hold_it() -> Result<()> {
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

/// Random interleavings of commit, retract, pull, mutual pull (which breeds
/// rival merges), reset and takeover across three writers. After every
/// step the acting writer's head has a complete history. After every pull
/// the puller holds exactly the facts either side held before it, less
/// those retracted anywhere in either side's history: no fact is lost, and
/// none a retraction covered comes back. A reset moves a
/// writer to a head it has held, its own or another's, at or above the
/// edition it last minted at: every revision it mints afterwards, a commit
/// or a pull's merge, is then above everything it minted before, so no
/// version is minted twice. A reset further back would have the writer
/// mint a second revision under a version it already used, which no merge
/// can tell apart from the first.
#[dialog_common::test]
async fn it_keeps_histories_whole_and_facts_kept_under_random_interleavings() -> Result<()> {
    let seeds: u64 = env::var("DIALOG_SYNC_BASE_INTERLEAVINGS")
        .ok()
        .and_then(|seeds| seeds.parse().ok())
        .unwrap_or(6);
    let steps = 32;
    let (mut pulls, mut rivals, mut resets, mut merges, mut retracts) = (0, 0, 0, 0, 0);
    for seed in 0..seeds {
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
        let mut held: [Vec<Revision>; 3] = [Vec::new(), Vec::new(), Vec::new()];
        // The facts retracted anywhere in each head's history.
        let mut retracted: HashMap<String, BTreeSet<String>> = HashMap::new();
        let key = |head: &Revision| format!("{:?}", (head.version(), &head.tree));
        let mut minted: [Option<Edition>; 3] = [None, None, None];
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
                (Some(head), Some(last)) => head.edition < last,
                (None, Some(_)) => true,
                _ => false,
            };
            let op = next(7);
            let what = format!("seed {seed} step {step} writer {at} op {op}");
            let mut pullers = Vec::new();
            match op {
                0 | 1 if !behind => {
                    let parent = lines[at].revision();
                    commit!(lines[at], &envs[at], format!("post:{seed}-{step}"));
                    let head = lines[at].revision().expect("a commit leaves a head");
                    minted[at] = Some(head.edition);
                    let inherited = parent
                        .and_then(|parent| retracted.get(&key(&parent)).cloned())
                        .unwrap_or_default();
                    retracted.insert(key(&head), inherited);
                }
                6 if !behind => {
                    let held_now: Vec<String> = titles!(lines[at], &envs[at]).into_iter().collect();
                    if !held_now.is_empty() {
                        let fact = held_now[next(held_now.len() as u64) as usize].clone();
                        let parent = lines[at]
                            .revision()
                            .expect("a line holding a fact has a head");
                        retract!(lines[at], &envs[at], fact);
                        let head = lines[at].revision().expect("a retraction leaves a head");
                        minted[at] = Some(head.edition);
                        let mut inherited =
                            retracted.get(&key(&parent)).cloned().unwrap_or_default();
                        inherited.insert(fact);
                        retracted.insert(key(&head), inherited);
                        retracts += 1;
                    }
                }
                2 if !held[at].is_empty() => {
                    let floor = minted[at];
                    let targets: Vec<&Revision> = held[at]
                        .iter()
                        .filter(|head| floor.is_none_or(|floor| head.edition >= floor))
                        .collect();
                    if !targets.is_empty() {
                        let target = targets[next(targets.len() as u64) as usize].clone();
                        lines[at].reset(target).perform(&envs[at]).await?;
                        resets += 1;
                    }
                }
                3 => {
                    // Both pull each other: when both moved since they last
                    // met, both mint a merge of the same two heads.
                    pullers.push((at, other));
                    pullers.push((other, at));
                    rivals += 1;
                }
                _ => pullers.push((at, other)),
            }
            for (to, from) in pullers {
                let Some(theirs) = lines[from].revision() else {
                    continue;
                };
                let before = lines[to].revision();
                let mut expected = titles!(lines[to], &envs[to]);
                expected.extend(titles!(lines[from], &envs[from]));
                let mut covered = before
                    .as_ref()
                    .and_then(|before| retracted.get(&key(before)).cloned())
                    .unwrap_or_default();
                covered.extend(retracted.get(&key(&theirs)).cloned().unwrap_or_default());
                expected.retain(|fact| !covered.contains(fact));
                lines[to]
                    .pull()
                    .from(&lines[from])
                    .perform(&envs[to])
                    .await?;
                pulls += 1;
                let after = lines[to].revision().expect("a pull leaves a head");
                if Some(&after) != before.as_ref() && after != theirs {
                    minted[to] = Some(after.edition);
                    merges += 1;
                }
                assert_eq!(
                    walkable!(lines[to], &envs[to]),
                    Ok(()),
                    "{what}: writer {to}'s history after pulling {from}"
                );
                assert_eq!(
                    titles!(lines[to], &envs[to]),
                    expected,
                    "{what}: writer {to} holds what it and {from} held, less what either retracted"
                );
                retracted.entry(key(&after)).or_default().extend(covered);
                if !held[to].contains(&after) {
                    held[to].push(after);
                }
            }
            if let Some(head) = lines[at].revision() {
                assert_eq!(
                    walkable!(lines[at], &envs[at]),
                    Ok(()),
                    "{what}: writer {at}'s history"
                );
                if !held[at].contains(&head) {
                    held[at].push(head);
                }
            }
        }
    }
    eprintln!(
        "{seeds} seeds: {pulls} pulls ({rivals} mutual), {merges} merges, {resets} resets, {retracts} retractions"
    );
    assert!(pulls > 0 && rivals > 0 && merges > 0 && resets > 0 && retracts > 0);
    Ok(())
}
