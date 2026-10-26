//! The causal context in the tree: a head carries none, and its tree's
//! context region holds every origin's watermark exactly except the head's
//! own, which the head's edition brings up to date.
//!
//! One test per way a head is minted or installed, each checking the region
//! the way a reader that remembers nothing would read it
//! ([`region_agrees`]): brought up to the head, it equals the ancestry walk
//! and the memo. Then what the design buys and what it costs: a writer's
//! own chain of commits writes nothing to the region, a follower
//! fast-forwards at zero reads, and a fast-forward across origins reads the
//! upstream's region.

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

use std::env;

use anyhow::Result;

use dialog_artifacts::history::{EDITION_CEILING, Edition, Origin, Watermark};
use dialog_artifacts::{Artifact, DialogArtifactsError, Instruction, Value, context_entry};
use dialog_capability::Subject;
use dialog_effects::authority::Attest;
use dialog_effects::storage::Location;
use dialog_peer::helpers::{open_peer, test_session_with_peer, test_storage, unique_name};
use futures_util::stream;

use super::super::context::{head_observes, region_agrees};
use crate::helpers::Counting;
use crate::{Branch, NetworkedIndex, PullError, RepositoryExt as _, Revision};
use dialog_artifacts::history::ContextCache;
use dialog_artifacts::history::context_of;
use dialog_artifacts::tree::ArtifactNodeCache;
use dialog_search_tree::Cache;

fn artifact(of: &str) -> Artifact {
    Artifact {
        the: "post/title".parse().expect("attribute parses"),
        of: of.parse().expect("entity parses"),
        is: Value::String("body".to_string()),
        cause: None,
    }
}

fn assert_one(of: &str) -> Instruction {
    Instruction::Assert(artifact(of), dialog_artifacts::Pick::All)
}

pub mod note {
    #[derive(dialog_query::Attribute, Clone, PartialEq, Eq, PartialOrd, Ord)]
    pub struct Body(pub String);
}

/// A note, for the transaction paths, which take statements.
#[derive(dialog_query::Concept, Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Note {
    pub this: dialog_query::Entity,
    pub body: note::Body,
}

fn note(body: &str) -> Result<Note> {
    Ok(Note {
        this: dialog_query::Entity::new()?,
        body: note::Body(body.into()),
    })
}

/// Commit one fact to `$branch` through `$env`.
macro_rules! commit {
    ($branch:expr, $env:expr, $of:expr) => {
        $branch
            .commit(stream::iter(vec![assert_one(&format!("{}", $of))]))
            .perform($env)
            .await?
    };
}

/// `count` passing writers, each a fresh branch that adopts the previous
/// writer's head (the first adopts `from`'s, when given) and commits once:
/// `count` more origins in the lineage of the branch returned, which is the
/// last writer.
macro_rules! passing_writers {
    ($repo:expr, $env:expr, $from:expr, $count:expr) => {{
        let mut previous: Option<Branch> = $from;
        let tag = unique_name("writer");
        for i in 0..$count {
            let writer = $repo
                .branch(format!("{tag}-{i}"))
                .open()
                .perform($env)
                .await?;
            if let Some(previous) = &previous {
                writer.pull().from(previous).perform($env).await?;
            }
            writer
                .commit(stream::iter(vec![Instruction::Assert(
                    Artifact {
                        the: "writer/mark".parse().expect("attribute parses"),
                        of: format!("mark:{tag}-{i}").parse().expect("entity parses"),
                        is: Value::String("passing".to_string()),
                        cause: None,
                    },
                    dialog_artifacts::Pick::All,
                )]))
                .perform($env)
                .await?;
            previous = Some(writer);
        }
        previous.expect("at least one writer")
    }};
}

/// A head minted by `$operator` from `$template` with `$edit` applied,
/// validly signed, installed as `$branch`'s head.
macro_rules! plant_signed {
    ($branch:expr, $operator:expr, $template:expr, $edit:expr) => {{
        let mut head: Revision = $template.clone();
        let edit: &dyn Fn(&mut Revision) = &$edit;
        edit(&mut head);
        head.signature = Attest::new(head.payload()).perform($operator).await?;
        $branch.reset(head.clone()).perform($operator).await?;
        head
    }};
}

/// Along one writer's chain the region is written once, by the chain's
/// first commit; every commit after it on its own head leaves the
/// region's entries exactly as they were, and the region still reads as
/// each head's context. That is the per-commit cost the design removes.
#[dialog_common::test]
async fn it_writes_nothing_to_the_region_along_a_writers_own_chain() -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = profile
        .space(unique_name("repo"))
        .open()
        .perform(&operator)
        .await?;
    let main = repo.branch("main").open().perform(&operator).await?;

    commit!(main, &operator, "post:0");
    let (first, _) = region_agrees(&main, &operator, "the first commit").await?;
    assert_eq!(first.len(), 1, "the first commit writes its own entry");
    for i in 1..4 {
        commit!(main, &operator, format!("post:{i}"));
        let (raw, _) = region_agrees(&main, &operator, "a commit on its own head").await?;
        assert_eq!(
            raw, first,
            "commit {i} on its own head leaves the region as it was"
        );
    }
    Ok(())
}

/// Where a chain changes hands the region is written: A commits, B commits
/// on A's head, A commits again on B's. Each commit on another origin's
/// head writes that origin's final entry and its own, and the region reads
/// as each head's context, the case a reader that only patched the head's
/// own entry would get wrong.
#[dialog_common::test]
async fn it_writes_both_origins_where_a_chain_changes_hands() -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = profile
        .space(unique_name("repo"))
        .open()
        .perform(&operator)
        .await?;
    let a = repo.branch("a").open().perform(&operator).await?;
    let b = repo.branch("b").open().perform(&operator).await?;

    commit!(a, &operator, "post:a0");
    commit!(a, &operator, "post:a1");
    region_agrees(&a, &operator, "A's chain").await?;
    let a_origin = a.revision().expect("A has a head").origin();

    b.pull().from(&a).perform(&operator).await?;
    commit!(b, &operator, "post:b0");
    let (after_b, walked) = region_agrees(&b, &operator, "B on A's head").await?;
    assert_eq!(walked.len(), 2);
    assert_eq!(
        after_b.get(&a_origin),
        walked.get(&a_origin),
        "A's chain ended, so its entry is exact"
    );
    commit!(b, &operator, "post:b1");
    region_agrees(&b, &operator, "B's chain").await?;

    a.pull().from(&b).perform(&operator).await?;
    commit!(a, &operator, "post:a2");
    region_agrees(&a, &operator, "A again, on B's head").await?;
    commit!(a, &operator, "post:a3");
    region_agrees(&a, &operator, "A's chain again").await?;
    Ok(())
}

/// After every way a pull lands (fast-forward adoption, nothing new, the
/// screened merge, the replay, the graft, and the reverse pull), the head's
/// tree carries the head's context in its region, and so does a commit on
/// a merge.
#[dialog_common::test]
async fn it_keeps_the_region_exact_in_every_merge_direction() -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = profile
        .space(unique_name("repo"))
        .open()
        .perform(&operator)
        .await?;

    let seed = passing_writers!(repo, &operator, None, 5);
    let main = repo.branch("main").open().perform(&operator).await?;
    main.pull().from(&seed).perform(&operator).await?;
    commit!(main, &operator, "post:0");
    region_agrees(&main, &operator, "a commit on a passing writer's head").await?;

    let feature = repo.branch("feature").open().perform(&operator).await?;
    feature.set_upstream(&main).perform(&operator).await?;
    feature.pull().perform(&operator).await?;
    commit!(main, &operator, "post:1");
    feature.pull().perform(&operator).await?;
    assert_eq!(feature.revision(), main.revision(), "feature adopted main");
    region_agrees(&feature, &operator, "a fast-forward").await?;

    commit!(feature, &operator, "post:2");
    main.pull().from(&feature).perform(&operator).await?;
    let before = feature.revision();
    feature.pull().perform(&operator).await?;
    assert_eq!(feature.revision(), before, "nothing new keeps the head");
    region_agrees(&feature, &operator, "nothing new").await?;

    for i in 0..4 {
        commit!(feature, &operator, format!("post:f{i}"));
    }
    commit!(main, &operator, "post:m0");
    feature.pull().perform(&operator).await?;
    region_agrees(&feature, &operator, "the screened merge").await?;

    for i in 0..4 {
        commit!(main, &operator, format!("post:m{}", i + 1));
    }
    commit!(feature, &operator, "post:f9");
    feature.pull().perform(&operator).await?;
    region_agrees(&feature, &operator, "the replay").await?;

    for i in 0..(super::SMALL_DIVERGENCE + 2) {
        commit!(main, &operator, format!("post:gm{i}"));
        commit!(feature, &operator, format!("post:gf{i}"));
    }
    feature.pull().perform(&operator).await?;
    region_agrees(&feature, &operator, "the graft").await?;

    main.pull().from(&feature).perform(&operator).await?;
    region_agrees(&main, &operator, "the reverse pull").await?;
    commit!(main, &operator, "post:after");
    region_agrees(&main, &operator, "a commit on a merge").await?;
    Ok(())
}

/// The other paths that mint or install a head: an amend of a staged
/// commit that took over another origin's head (the two entries the
/// takeover wrote must survive the amend), a reset to another branch's
/// head followed by a commit, and the commit after adopting a head by
/// root. A head installed from imported blocks is installed by the same
/// reset.
#[dialog_common::test]
async fn it_keeps_the_region_exact_through_amend_reset_and_adoption() -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = profile
        .space(unique_name("repo"))
        .open()
        .perform(&operator)
        .await?;
    let main = repo.branch("main").open().perform(&operator).await?;
    commit!(main, &operator, "post:0");
    commit!(main, &operator, "post:1");

    let amended = repo.branch("amended").open().perform(&operator).await?;
    amended.pull().from(&main).perform(&operator).await?;
    let batch = amended
        .transaction()
        .assert(note("t0")?)
        .commit()
        .perform(&operator)
        .await?;
    let batch = batch
        .assert(note("t1")?)
        .commit()
        .amend()
        .perform(&operator)
        .await?;
    batch.publish().perform(&operator).await?;
    region_agrees(&amended, &operator, "an amended takeover").await?;
    commit!(amended, &operator, "post:t2");
    region_agrees(&amended, &operator, "a commit after an amend").await?;

    let reset = repo.branch("reset").open().perform(&operator).await?;
    commit!(reset, &operator, "post:r0");
    reset
        .reset(main.revision().expect("main has a head"))
        .perform(&operator)
        .await?;
    region_agrees(&reset, &operator, "a reset").await?;
    commit!(reset, &operator, "post:r1");
    region_agrees(&reset, &operator, "a commit after a reset").await?;

    let adopted = repo.branch("adopted").open().perform(&operator).await?;
    adopted.set_upstream(&main).perform(&operator).await?;
    adopted.pull().perform(&operator).await?;
    assert_eq!(adopted.revision(), main.revision());
    commit!(adopted, &operator, "post:a0");
    region_agrees(&adopted, &operator, "a commit after adoption by root").await?;
    Ok(())
}

/// A replica following an upstream another session writes, whose head is
/// the one it adopted last time, fast-forwards at zero block reads: the two
/// heads share an origin, and an origin's revisions form one chain, so the
/// upstream's edition says it has seen ours. Nothing new is answered from
/// our own context, also at zero reads.
#[dialog_common::test]
async fn it_fast_forwards_a_follower_at_zero_reads() -> Result<()> {
    let (writer, profile) = test_session_with_peer().await;
    let name = unique_name("repo");
    let repo = profile.space(name.clone()).open().perform(&writer).await?;
    let main = repo.branch("main").open().perform(&writer).await?;
    commit!(main, &writer, "post:0");
    let feature = repo.branch("feature").open().perform(&writer).await?;
    feature.set_upstream(&main).perform(&writer).await?;
    feature.pull().perform(&writer).await?;

    let device = profile
        .session(b"device")
        .space(profile.state())
        .allow(Subject::any())
        .await?;
    let device = Counting::new(device);
    let local = profile.space(name).open().perform(&device).await?;
    let synced = local.branch("feature").open().perform(&device).await?;

    commit!(main, &writer, "post:1");
    device.reset();
    synced.pull().perform(&device).await?;
    assert_eq!(synced.revision(), main.revision(), "a fast-forward");
    assert_eq!(
        device.block_reads(),
        0,
        "a follower fast-forwards at zero reads: {:?}",
        device.snapshot()
    );

    let relay = repo.branch("relay").open().perform(&writer).await?;
    relay.pull().from(&main).perform(&writer).await?;
    let relayed = local.branch("relay").open().perform(&device).await?;
    let before = synced.revision();
    device.reset();
    synced.pull().from(&relayed).perform(&device).await?;
    assert_eq!(synced.revision(), before, "nothing new keeps the head");
    assert_eq!(
        device.block_reads(),
        0,
        "nothing new is answered from our own context: {:?}",
        device.snapshot()
    );
    region_agrees(&synced, &device, "a follower's head").await?;
    Ok(())
}

/// A fast-forward across origins reads the upstream's region. We follow
/// main, whose head another writer then built on, so our head and the
/// upstream's are of different origins and whether the upstream has seen
/// ours is one entry of its context. The read is the upstream tree's root
/// and the path to the region's leaf, which the design accepts in exchange
/// for heads that carry no context; a follower whose upstream's head shares
/// its origin reads nothing (`it_fast_forwards_a_follower_at_zero_reads`).
#[dialog_common::test]
async fn it_reads_the_upstreams_region_to_fast_forward_across_origins() -> Result<()> {
    let (writer, profile) = test_session_with_peer().await;
    let name = unique_name("repo");
    let repo = profile.space(name.clone()).open().perform(&writer).await?;
    let main = repo.branch("main").open().perform(&writer).await?;
    commit!(main, &writer, "post:0");

    let device = profile
        .session(b"device")
        .space(profile.state())
        .allow(Subject::any())
        .await?;
    let device = Counting::new(device);
    let local = profile.space(name).open().perform(&device).await?;
    let ours = local.branch("ours").open().perform(&device).await?;
    let upstream = local.branch("main").open().perform(&device).await?;
    ours.set_upstream(&upstream).perform(&device).await?;
    ours.pull().perform(&device).await?;
    assert_eq!(ours.revision(), main.revision(), "we follow main");

    // A third writer builds on main's head, and main takes its head.
    let third = repo.branch("third").open().perform(&writer).await?;
    third.pull().from(&main).perform(&writer).await?;
    commit!(third, &writer, "post:third");
    main.pull().from(&third).perform(&writer).await?;
    assert_eq!(
        main.revision(),
        third.revision(),
        "main took the third head"
    );
    assert_ne!(
        main.revision().expect("main has a head").origin(),
        ours.revision().expect("we have a head").origin()
    );

    device.reset();
    ours.pull().perform(&device).await?;
    assert_eq!(ours.revision(), main.revision(), "a fast-forward");
    let reads = device.block_reads();
    assert!(
        (1..=4).contains(&reads),
        "a fast-forward across origins reads the upstream's root and region leaf, \
         no more (got {reads}): {:?}",
        device.snapshot()
    );
    Ok(())
}

/// A deletion survives a pull from a stale peer when the receiver knows of
/// it only through its region: the deleter's origin is not the receiver's
/// own, so the screen reads its watermark from the region. Run through a
/// fresh session, so nothing is remembered.
#[dialog_common::test]
async fn it_does_not_resurrect_a_deleted_fact_whose_deleter_is_only_in_the_region() -> Result<()> {
    use dialog_query::attribute::Relation;
    use dialog_query::query::Output as _;
    use dialog_query::{AttributeQuery, Claim, Term, the};

    let (operator, profile) = test_session_with_peer().await;
    let name = unique_name("repo");
    let repo = profile
        .space(name.clone())
        .open()
        .perform(&operator)
        .await?;

    let main = repo.branch("main").open().perform(&operator).await?;
    commit!(main, &operator, "post:1");
    let feature = repo.branch("feature").open().perform(&operator).await?;
    feature.set_upstream(&main).perform(&operator).await?;
    feature.pull().perform(&operator).await?;
    let peer = repo.branch("peer").open().perform(&operator).await?;
    peer.set_upstream(&main).perform(&operator).await?;
    peer.pull().perform(&operator).await?;

    feature
        .commit(stream::iter(vec![Instruction::Retract(artifact("post:1"))]))
        .perform(&operator)
        .await?;
    let deleter = feature.revision().expect("feature has a head").origin();
    let last = passing_writers!(repo, &operator, Some(feature.clone()), 3);

    let titles = |branch: Branch, env| async move {
        let rows: Vec<Claim> = branch
            .query()
            .select(AttributeQuery::from(
                Term::<Relation>::from(the!("post/title"))
                    .of(Term::<dialog_artifacts::Entity>::var("of"))
                    .is(Term::<String>::var("is")),
            ))
            .perform(env)
            .try_vec()
            .await?;
        anyhow::Ok(rows.len())
    };

    let cold = profile
        .session(b"cold")
        .space(profile.state())
        .allow(Subject::any())
        .await?;
    let repo = profile.space(name).open().perform(&cold).await?;
    let last = repo.branch(last.name()).open().perform(&cold).await?;
    let peer = repo.branch("peer").open().perform(&cold).await?;
    let head = last.revision().expect("the last writer has a head");
    assert!(head.region && head.context.is_none());
    assert_ne!(head.origin(), deleter, "the deleter is not the head's own");

    assert_eq!(titles(last.clone(), &cold).await?, 0);
    assert_eq!(titles(peer.clone(), &cold).await?, 1, "peer still holds it");
    last.pull().from(&peer).perform(&cold).await?;
    assert_eq!(
        titles(last.clone(), &cold).await?,
        0,
        "a stale copy must not resurrect a fact deleted by an origin only the region holds"
    );
    peer.pull().from(&last).perform(&cold).await?;
    assert_eq!(
        titles(peer.clone(), &cold).await?,
        0,
        "the deletion reaches the stale peer"
    );
    Ok(())
}

/// Heads minted before the region (the whole context inline, or no
/// context at all) still pull as upstreams and as local heads, and the
/// first head minted on one of them writes the whole map into its tree's
/// region once.
#[dialog_common::test]
async fn it_pulls_and_commits_across_heads_minted_before_the_region() -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = profile
        .space(unique_name("repo"))
        .open()
        .perform(&operator)
        .await?;

    let last = passing_writers!(repo, &operator, None, 4);
    let current = last.revision().expect("the last writer has a head");
    let (_, full) = region_agrees(&last, &operator, "the template").await?;

    let inline = repo.branch("inline").open().perform(&operator).await?;
    let legacy = plant_signed!(inline, &operator, current, |head: &mut Revision| {
        head.context = Some(full.clone());
        head.region = false;
        head.count = None;
    });
    legacy.verify().expect("an inline head still verifies");

    let feature = repo.branch("feature").open().perform(&operator).await?;
    commit!(feature, &operator, "post:ours");
    feature.pull().from(&inline).perform(&operator).await?;
    region_agrees(&feature, &operator, "a merge with an inline upstream").await?;

    commit!(inline, &operator, "post:inline");
    let (raw, walked) = region_agrees(&inline, &operator, "a commit on an inline head").await?;
    assert_eq!(raw.len(), walked.len(), "every origin is written");

    let other = repo
        .branch("other-inline")
        .open()
        .perform(&operator)
        .await?;
    plant_signed!(other, &operator, current, |head: &mut Revision| {
        head.context = Some(full.clone());
        head.region = false;
        head.count = None;
    });
    commit!(feature, &operator, "post:ours-again");
    other.pull().from(&feature).perform(&operator).await?;
    region_agrees(&other, &operator, "an inline head pulling a region head").await?;

    let bare = repo.branch("bare").open().perform(&operator).await?;
    plant_signed!(bare, &operator, current, |head: &mut Revision| {
        head.context = None;
        head.region = false;
        head.count = None;
    });
    let plain = repo.branch("plain").open().perform(&operator).await?;
    commit!(plain, &operator, "post:plain");
    plain.pull().from(&bare).perform(&operator).await?;
    region_agrees(&plain, &operator, "a merge with a pre-context upstream").await?;

    // A region head minted before heads carried their own count: a commit
    // on it reads the region for the count, and the head it mints carries
    // one.
    let uncounted = repo.branch("uncounted").open().perform(&operator).await?;
    let planted = plant_signed!(uncounted, &operator, current, |head: &mut Revision| {
        head.count = None;
    });
    planted
        .verify()
        .expect("an uncounted region head still verifies");
    assert!(planted.region && planted.own_watermark().is_none());
    let taker = repo.branch("taker").open().perform(&operator).await?;
    taker.pull().from(&uncounted).perform(&operator).await?;
    commit!(taker, &operator, "post:taker");
    region_agrees(&taker, &operator, "a takeover of an uncounted head").await?;
    commit!(uncounted, &operator, "post:uncounted");
    region_agrees(&uncounted, &operator, "a commit on its own uncounted head").await?;
    Ok(())
}

/// Validly signed heads whose tree's region no head could have minted are
/// refused at pull with the specific error, and the puller's head stays:
/// a region without the head's own origin, and one holding an entry at
/// the edition ceiling or counting more revisions than its edition leaves
/// room for. Pulled through a fresh session, so the region is read.
#[dialog_common::test]
async fn it_refuses_to_pull_a_head_whose_region_no_head_minted() -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let name = unique_name("repo");
    let repo = profile
        .space(name.clone())
        .open()
        .perform(&operator)
        .await?;

    let main = repo.branch("main").open().perform(&operator).await?;
    commit!(main, &operator, "post:0");
    let template = main.revision().expect("main has a head");

    let missing = repo.branch("missing").open().perform(&operator).await?;
    let stranger: dialog_artifacts::Entity =
        format!("branch:{}", unique_name("stranger")).parse()?;
    plant_signed!(missing, &operator, template, |head: &mut Revision| {
        head.branch = stranger.clone();
    });

    let mut cases = vec![(
        "a region without the head's own origin",
        "missing".to_string(),
    )];
    for (why, watermark) in [
        (
            "an entry at the edition ceiling",
            Watermark {
                edition: Edition::new(EDITION_CEILING),
                count: 1,
            },
        ),
        (
            "an entry counting more revisions than its edition leaves room for",
            Watermark {
                edition: Edition::new(3),
                count: 9,
            },
        ),
    ] {
        let evil = unique_name("evil");
        repo.branch(evil.clone())
            .open()
            .perform(&operator)
            .await?
            .commit(stream::iter(vec![assert_one("post:evil")]))
            .with_entries(vec![context_entry(&Origin::from([0xEE; 32]), &watermark)])
            .perform(&operator)
            .await?;
        cases.push((why, evil));
    }

    let cold = profile
        .session(b"cold")
        .space(profile.state())
        .allow(Subject::any())
        .await?;
    let repo = profile.space(name).open().perform(&cold).await?;
    let puller = repo.branch("puller").open().perform(&cold).await?;
    commit!(puller, &cold, "post:ours");
    let ours = puller.revision();

    for (why, from) in cases {
        let upstream = repo.branch(from).open().perform(&cold).await?;
        let pulled = puller.pull().from(&upstream).perform(&cold).await;
        assert!(
            matches!(
                pulled,
                Err(PullError::Artifact(DialogArtifactsError::InvalidKey(_)))
            ),
            "{why} must be refused; got {pulled:?}"
        );
        assert_eq!(puller.revision(), ours, "{why}: the head stays");
    }
    Ok(())
}

/// A joiner's first commit on a head it adopted by root from another
/// origin (the claim a join makes) reads nothing of that head's context
/// region. The head carries its own count, which is the adopted origin's
/// exact watermark, and the joiner's own entry is one point read of the
/// region, which a joiner whose origin the head never saw finds absent.
/// What the commit reads is the data path its facts integrate into, as
/// any commit does; the region's whole map is never read.
#[dialog_common::test]
async fn it_takes_over_an_adopted_head_without_reading_its_region() -> Result<()> {
    let (writer, profile) = test_session_with_peer().await;
    let name = unique_name("repo");
    let repo = profile.space(name.clone()).open().perform(&writer).await?;
    let main = repo.branch("main").open().perform(&writer).await?;
    commit!(main, &writer, "post:0");
    let last = passing_writers!(repo, &writer, Some(main.clone()), 12);
    main.pull().from(&last).perform(&writer).await?;

    let device = profile
        .session(b"joiner")
        .space(profile.state())
        .allow(Subject::any())
        .await?;
    let device = Counting::new(device);
    let local = profile.space(name).open().perform(&device).await?;
    let upstream = local.branch("main").open().perform(&device).await?;
    let joiner = local.branch("joiner").open().perform(&device).await?;
    joiner.set_upstream(&upstream).perform(&device).await?;
    joiner.pull().perform(&device).await?;
    let adopted = joiner.revision().expect("the joiner adopted main's head");
    assert_eq!(Some(&adopted), main.revision().as_ref(), "adopted by root");

    device.reset();
    commit!(joiner, &device, "post:claim");
    let takeover = device.block_reads();
    eprintln!("takeover reads {takeover}: {:?}", device.snapshot());

    let head = joiner.revision().expect("the joiner committed");
    assert_ne!(head.origin(), adopted.origin(), "a takeover");
    assert_eq!(
        head.own_watermark(),
        Some(Watermark {
            edition: head.edition,
            count: 1,
        }),
        "the joiner's first revision on this branch"
    );
    region_agrees(&joiner, &device, "a takeover of an adopted head").await?;
    Ok(())
}

/// A pull right after a takeover commit, from a branch still at the head
/// the commit took over, is answered from the new head's record at no
/// read: the record names that head as its parent, so the new head has
/// seen it, without the point read of the region an observes across
/// origins otherwise pays. So is the pull after the push, whose upstream
/// is then our own head.
#[dialog_common::test]
async fn it_answers_observes_after_a_takeover_at_zero_reads() -> Result<()> {
    let (writer, profile) = test_session_with_peer().await;
    let name = unique_name("repo");
    let repo = profile.space(name.clone()).open().perform(&writer).await?;
    let main = repo.branch("main").open().perform(&writer).await?;
    commit!(main, &writer, "post:0");
    let last = passing_writers!(repo, &writer, Some(main.clone()), 12);
    main.pull().from(&last).perform(&writer).await?;

    let device = profile
        .session(b"joiner")
        .space(profile.state())
        .allow(Subject::any())
        .await?;
    let device = Counting::new(device);
    let local = profile.space(name).open().perform(&device).await?;
    let upstream = local.branch("main").open().perform(&device).await?;
    let joiner = local.branch("joiner").open().perform(&device).await?;
    joiner.set_upstream(&upstream).perform(&device).await?;
    joiner.pull().perform(&device).await?;
    let adopted = joiner.revision();
    commit!(joiner, &device, "post:claim");
    let head = joiner.revision();

    // A branch the joiner never synced with, at the head it took over: the
    // pull has no sync base to short-circuit on, and asks whether our head
    // has seen theirs.
    let mirror = local.branch("mirror").open().perform(&device).await?;
    mirror.pull().from(&upstream).perform(&device).await?;
    assert_eq!(mirror.revision(), adopted);
    device.reset();
    joiner.pull().from(&mirror).perform(&device).await?;
    assert_eq!(joiner.revision(), head, "nothing new keeps the head");
    assert_eq!(
        device.block_reads(),
        0,
        "the takeover's record answers that it built on that head: {:?}",
        device.snapshot()
    );

    // The same question asked of a reader with no blocks remembered, so
    // the region's leaf the commit read is not cached: the record answers
    // it at no read, and without the record it is the region's point read.
    let ours = head.clone().expect("the joiner committed");
    let theirs = adopted
        .clone()
        .expect("the joiner adopted a head")
        .version();
    let store = NetworkedIndex::new(&device, joiner.archive().index(), joiner.fallback());
    device.reset();
    let seen = head_observes(
        &ours,
        &theirs,
        &ContextCache::new(),
        &joiner.records(),
        &store,
        &ArtifactNodeCache::new(),
    )
    .await?;
    assert_eq!(seen, Some(true));
    assert_eq!(
        device.block_reads(),
        0,
        "the record answers: {:?}",
        device.snapshot()
    );
    let seen = head_observes(
        &ours,
        &theirs,
        &ContextCache::new(),
        &Cache::new(),
        &store,
        &ArtifactNodeCache::new(),
    )
    .await?;
    assert_eq!(seen, Some(true));
    assert!(
        device.block_reads() > 0,
        "without the record the region answers, at a read"
    );

    joiner.push().perform(&device).await?;
    device.reset();
    joiner.pull().perform(&device).await?;
    assert_eq!(joiner.revision(), head, "the pushed head is our own");
    assert_eq!(
        device.block_reads(),
        0,
        "a pull after the push reads nothing: {:?}",
        device.snapshot()
    );
    Ok(())
}

/// Check that `$branch`'s head signs its own origin's count as the
/// ancestry walk counts it, and that its region reads as its context.
macro_rules! assert_exact_count {
    ($branch:expr, $env:expr, $what:expr) => {{
        let head = $branch.revision().expect("the branch has a head");
        let walked = context_of(&head.version(), &$branch.history($env))
            .await
            .map_err(|error| anyhow::anyhow!("{}: walking the ancestry: {error}", $what))?;
        assert_eq!(
            head.own_watermark().as_ref(),
            walked.get(&head.origin()),
            "{}: the signed count is the ancestry's",
            $what
        );
        region_agrees($branch, $env, &$what).await?;
    }};
}

/// A session of `$profile`'s peer under `$tag`: a writer with an origin of
/// its own and a store of its own, so no memo it holds stands in for a
/// region read.
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

/// Two writers alternating, each fast-forwarding to the other's head and
/// committing on it: every commit after each writer's first is a takeover,
/// and each signs the count the ancestry walk gives, read from the
/// adopted head's region.
#[dialog_common::test]
async fn it_alternates_writers_with_exact_counts() -> Result<()> {
    let (writer, profile) = test_session_with_peer().await;
    let name = unique_name("repo");
    let repo = profile.space(name.clone()).open().perform(&writer).await?;
    let seed = repo.branch("main").open().perform(&writer).await?;
    commit!(seed, &writer, "post:seed");
    let last = passing_writers!(repo, &writer, Some(seed.clone()), 12);
    seed.pull().from(&last).perform(&writer).await?;

    let alice = session!(profile, b"alice");
    let bob = session!(profile, b"bob");
    let a = line!(profile, name, &alice, "alice");
    let b = line!(profile, name, &bob, "bob");
    a.pull().from(&seed).perform(&alice).await?;
    commit!(a, &alice, "post:a0");
    assert_exact_count!(&a, &alice, "the first takeover");

    for round in 1..=10 {
        let (writer, env, from) = match round % 2 {
            1 => (&b, &bob, &a),
            _ => (&a, &alice, &b),
        };
        writer.pull().from(from).perform(env).await?;
        assert_eq!(writer.revision(), from.revision(), "a fast-forward");
        commit!(writer, env, format!("post:{round}"));
        assert_exact_count!(writer, env, format!("round {round}"));
    }
    Ok(())
}

/// A line reset to a head it adopted earlier, after committing on top of
/// it, then fast-forwarded to a head that holds that later commit: the
/// takeover counts every revision of the line's origin the new head holds,
/// including the one the reset left behind. A count kept on the line from
/// when it first adopted the earlier head would say one fewer.
#[dialog_common::test]
async fn it_counts_a_takeover_exactly_after_a_reset_to_an_adopted_head() -> Result<()> {
    let (writer, profile) = test_session_with_peer().await;
    let name = unique_name("repo");
    profile.space(name.clone()).open().perform(&writer).await?;
    let alice = session!(profile, b"alice");
    let bob = session!(profile, b"bob");
    let a = line!(profile, name, &alice, "a");
    let b = line!(profile, name, &bob, "b");

    commit!(a, &alice, "post:a1");
    b.pull().from(&a).perform(&bob).await?;
    commit!(b, &bob, "post:x");
    let x = b.revision().expect("X");
    a.pull().from(&b).perform(&alice).await?;
    commit!(a, &alice, "post:a2");
    b.pull().from(&a).perform(&bob).await?;
    commit!(b, &bob, "post:u");
    let u = b.revision().expect("U");

    a.reset(x).perform(&alice).await?;
    a.pull().from(&b).perform(&alice).await?;
    assert_eq!(a.revision().as_ref(), Some(&u), "a fast-forward to U");
    assert!(
        a.contexts().cached(&u.version()).await.is_none(),
        "no memo of U's context stands in for its region"
    );
    commit!(a, &alice, "post:a3");
    assert_exact_count!(&a, &alice, "a takeover after a reset");
    let head = a.revision().expect("the takeover");
    assert_eq!(head.own_watermark().map(|own| own.count), Some(3));
    Ok(())
}

/// A line reset to its own earlier head, then fast-forwarded to a head
/// built on the commit the reset left behind: the takeover counts that
/// commit too. The line's own head, its latest revision until the reset,
/// says one fewer, so a count taken from the head the line moved from
/// would understate. In one process this pull computes and remembers the
/// adopted head's context, which the commit then uses;
/// `it_counts_a_takeover_exactly_after_a_reset_and_a_restart` takes that
/// memory away.
#[dialog_common::test]
async fn it_counts_a_takeover_exactly_after_a_reset_to_its_own_head() -> Result<()> {
    let (writer, profile) = test_session_with_peer().await;
    let name = unique_name("repo");
    profile.space(name.clone()).open().perform(&writer).await?;
    let alice = session!(profile, b"alice");
    let bob = session!(profile, b"bob");
    let a = line!(profile, name, &alice, "a");
    let b = line!(profile, name, &bob, "b");

    // a has pulled from b before, so its next pull from b has a sync
    // base and fast-forwards without computing b's whole context.
    commit!(b, &bob, "post:b0");
    a.pull().from(&b).perform(&alice).await?;
    commit!(a, &alice, "post:a1");
    let l1 = a.revision().expect("L1");
    commit!(a, &alice, "post:a2");
    b.pull().from(&a).perform(&bob).await?;
    commit!(b, &bob, "post:u");
    let u = b.revision().expect("U");

    a.reset(l1).perform(&alice).await?;
    a.pull().from(&b).perform(&alice).await?;
    assert_eq!(a.revision().as_ref(), Some(&u), "a fast-forward to U");
    commit!(a, &alice, "post:a3");
    assert_exact_count!(&a, &alice, "a takeover after a reset to our own head");
    let head = a.revision().expect("the takeover");
    assert_eq!(head.own_watermark().map(|own| own.count), Some(3));
    Ok(())
}

/// The same rewind as above, with the replica restarted between the pull
/// and the commit, so nothing remembered in memory from the pull (the
/// adopted head's context the pull computed) stands in for the region: a
/// count the line kept from the head it moved from would be all the
/// commit had, and would understate.
#[dialog_common::test]
async fn it_counts_a_takeover_exactly_after_a_reset_and_a_restart() -> Result<()> {
    let storage = test_storage().await;
    let location = Location::profile(unique_name("restart"));
    let profile = open_peer(storage.clone(), location.clone()).await?;
    let name = unique_name("repo");
    let writer = session!(profile, b"test");
    profile.space(name.clone()).open().perform(&writer).await?;
    let alice = session!(profile, b"alice");
    let bob = session!(profile, b"bob");
    let a = line!(profile, name, &alice, "a");
    let b = line!(profile, name, &bob, "b");

    commit!(b, &bob, "post:b0");
    a.pull().from(&b).perform(&alice).await?;
    commit!(a, &alice, "post:a1");
    let l1 = a.revision().expect("L1");
    commit!(a, &alice, "post:a2");
    b.pull().from(&a).perform(&bob).await?;
    commit!(b, &bob, "post:u");
    let u = b.revision().expect("U");
    a.reset(l1).perform(&alice).await?;
    a.pull().from(&b).perform(&alice).await?;
    assert_eq!(a.revision().as_ref(), Some(&u), "a's head is U");

    let restarted = open_peer(storage, location).await?;
    let alice = session!(restarted, b"alice");
    let a = line!(restarted, name, &alice, "a");
    assert_eq!(
        a.revision().as_ref(),
        Some(&u),
        "the head survives the restart"
    );
    assert!(
        a.contexts().cached(&u.version()).await.is_none(),
        "a restarted replica remembers no context"
    );
    commit!(a, &alice, "post:a3");
    assert_exact_count!(&a, &alice, "a takeover after a reset and a restart");
    let head = a.revision().expect("the takeover");
    assert_eq!(head.own_watermark().map(|own| own.count), Some(3));
    Ok(())
}

/// A takeover after the line's head was moved without appending to it, by
/// each way a head moves so on a line that has one: an amend, a reset to
/// another line's older head, and a pull that merges (minting under the
/// line's origin). Each head that results, and each takeover that follows,
/// signs the walked count. Adoption by root moves only a line with no
/// head; `it_takes_over_an_adopted_head_without_reading_its_region` covers
/// it.
#[dialog_common::test]
async fn it_counts_a_takeover_exactly_after_every_head_move() -> Result<()> {
    let (writer, profile) = test_session_with_peer().await;
    let name = unique_name("repo");
    profile.space(name.clone()).open().perform(&writer).await?;
    let alice = session!(profile, b"alice");
    let bob = session!(profile, b"bob");
    let a = line!(profile, name, &alice, "a");
    let b = line!(profile, name, &bob, "b");

    commit!(b, &bob, "post:b0");
    a.pull().from(&b).perform(&alice).await?;
    let older = a.revision().expect("b's first head");

    // An amend replaces the head with a revision of the same version.
    let batch = a
        .transaction()
        .assert(note("t0")?)
        .commit()
        .perform(&alice)
        .await?;
    let batch = batch
        .assert(note("t1")?)
        .commit()
        .amend()
        .perform(&alice)
        .await?;
    batch.publish().perform(&alice).await?;
    assert_exact_count!(&a, &alice, "an amended takeover");
    b.pull().from(&a).perform(&bob).await?;
    commit!(b, &bob, "post:b1");
    a.pull().from(&b).perform(&alice).await?;
    commit!(a, &alice, "post:after-amend");
    assert_exact_count!(&a, &alice, "a takeover after an amend");

    // A reset to b's older head, then a fast-forward to b's newest.
    b.pull().from(&a).perform(&bob).await?;
    commit!(b, &bob, "post:b2");
    a.reset(older).perform(&alice).await?;
    assert_exact_count!(&a, &alice, "a reset to an older head");
    a.pull().from(&b).perform(&alice).await?;
    assert_exact_count!(&a, &alice, "a pull after a reset");
    commit!(a, &alice, "post:after-reset");
    assert_exact_count!(&a, &alice, "a takeover after a reset to an older head");

    // A pull that merges mints under a's origin; b takes that over, and a
    // takes b's head back over.
    commit!(a, &alice, "post:a-side");
    commit!(b, &bob, "post:b-side");
    a.pull().from(&b).perform(&alice).await?;
    assert_exact_count!(&a, &alice, "a merge");
    b.pull().from(&a).perform(&bob).await?;
    commit!(b, &bob, "post:b4");
    assert_exact_count!(&b, &bob, "b's takeover of a's merge");
    a.pull().from(&b).perform(&alice).await?;
    commit!(a, &alice, "post:after-merge");
    assert_exact_count!(&a, &alice, "a takeover after a merge");
    Ok(())
}

/// Random interleavings of commit, pull, merge, takeover and restart
/// across three writers of one space: at every head a writer mints or
/// adopts, the signed count is the walked count. A restart reopens the
/// peer over the same storage, so commits after it have no context
/// remembered from the pulls before it and take the region path.
///
/// Two known holes in the history are kept out of what this checks, both
/// #673. Settling rival merges can leave a head whose ancestry walk fails,
/// and three writers reach that by commits and pulls alone: such a head
/// has no walked count to compare against, so it is counted and skipped,
/// and the test asserts most heads were checked. Resets are left to the
/// deterministic tests above, since a pull after one leaves such a head
/// every time in this crate's own tests.
#[dialog_common::test]
async fn it_signs_walked_counts_under_random_interleavings() -> Result<()> {
    let seeds: u64 = env::var("DIALOG_COUNT_INTERLEAVINGS")
        .ok()
        .and_then(|seeds| seeds.parse().ok())
        .unwrap_or(6);
    let steps = 32;
    let (mut commits, mut takeovers, mut merges, mut restarts) = (0, 0, 0, 0);
    let (mut checked, mut holes) = (0u64, 0u64);
    // Whether `line`'s head has a walkable ancestry: false for a head
    // carrying #673's hole, which no count can be checked against.
    async fn walkable<Env>(line: &Branch, env: &Env) -> Result<bool>
    where
        Env: crate::ResolveEnv,
    {
        let head = line.revision().expect("the branch has a head");
        match context_of(&head.version(), &line.history(env)).await {
            Ok(_) => Ok(true),
            Err(DialogArtifactsError::IncompleteHistory(_)) => Ok(false),
            Err(error) => Err(error.into()),
        }
    }
    for seed in 0..seeds {
        let storage = test_storage().await;
        let location = Location::profile(unique_name("mix"));
        let mut profile = open_peer(storage.clone(), location.clone()).await?;
        let name = unique_name("repo");
        let writer = session!(profile, b"test");
        profile.space(name.clone()).open().perform(&writer).await?;
        let tags: [&'static [u8]; 3] = [b"one", b"two", b"three"];
        let mut envs = [
            session!(profile, tags[0]),
            session!(profile, tags[1]),
            session!(profile, tags[2]),
        ];
        let mut lines = [
            line!(profile, name, &envs[0], "one"),
            line!(profile, name, &envs[1], "two"),
            line!(profile, name, &envs[2], "three"),
        ];
        let mut state = seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
        let mut next = |bound: u64| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state % bound
        };
        for step in 0..steps {
            let op = next(6);
            if op == 5 {
                profile = open_peer(storage.clone(), location.clone()).await?;
                envs = [
                    session!(profile, tags[0]),
                    session!(profile, tags[1]),
                    session!(profile, tags[2]),
                ];
                lines = [
                    line!(profile, name, &envs[0], "one"),
                    line!(profile, name, &envs[1], "two"),
                    line!(profile, name, &envs[2], "three"),
                ];
                restarts += 1;
                continue;
            }
            let at = next(3) as usize;
            let (line, env) = (&lines[at], &envs[at]);
            let what = format!("seed {seed} step {step} writer {at} op {op}");
            if op < 2 {
                let parent = line.revision();
                commit!(line, env, format!("post:{seed}-{step}"));
                let head = line.revision().expect("a commit leaves a head");
                commits += 1;
                if parent.is_some_and(|parent| parent.origin() != head.origin()) {
                    takeovers += 1;
                }
                if walkable(line, env).await? {
                    assert_exact_count!(line, env, what);
                    checked += 1;
                } else {
                    holes += 1;
                }
            } else {
                let from = &lines[(at + 1 + next(2) as usize) % 3];
                let Some(theirs) = from.revision() else {
                    continue;
                };
                let before = line.revision();
                line.pull().from(from).perform(env).await?;
                let after = line.revision().expect("a pull leaves a head");
                // A head that is neither ours before nor theirs is a
                // merge, minted under this writer's origin.
                if Some(&after) != before.as_ref() && after != theirs {
                    merges += 1;
                }
                if walkable(line, env).await? {
                    assert_exact_count!(line, env, what);
                    checked += 1;
                } else {
                    holes += 1;
                }
            }
        }
    }
    eprintln!(
        "{seeds} seeds: {commits} commits ({takeovers} takeovers), {merges} merges, {restarts} restarts; {checked} heads checked, {holes} skipped for #673"
    );
    assert!(takeovers > 0 && merges > 0 && restarts > 0);
    assert!(
        holes * 4 < checked,
        "most heads are checked: {checked} checked, {holes} skipped"
    );
    Ok(())
}
