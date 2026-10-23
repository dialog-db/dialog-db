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

use anyhow::Result;

use dialog_artifacts::history::{EDITION_CEILING, Edition, Origin, Watermark};
use dialog_artifacts::{Artifact, DialogArtifactsError, Instruction, Value, context_entry};
use dialog_capability::Subject;
use dialog_effects::authority::Attest;
use dialog_peer::helpers::{test_session_with_peer, unique_name};
use futures_util::stream;

use super::super::context::{head_observes, region_agrees};
use crate::helpers::Counting;
use crate::{Branch, BranchReference, NetworkedIndex, PullError, RepositoryExt as _, Revision};
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

/// Two writers alternating, each fast-forwarding to the other's head and
/// committing on it: every commit after each writer's first is a takeover
/// that signs its own count from what its line recorded when it adopted
/// that head (the head built on the writer's own last commit), not from
/// the head's region. What the line records is checked against the
/// ancestry walk before every such commit, since a count it got wrong
/// would be signed into the head. The commits read nothing; that both
/// sessions share one store's node cache here is why the region's point
/// read would cost nothing either, which the soak's separate replicas
/// measure instead.
#[dialog_common::test]
async fn it_alternates_writers_on_what_their_lines_recorded() -> Result<()> {
    let (writer, profile) = test_session_with_peer().await;
    let name = unique_name("repo");
    let repo = profile.space(name.clone()).open().perform(&writer).await?;
    let seed = repo.branch("main").open().perform(&writer).await?;
    commit!(seed, &writer, "post:seed");
    let last = passing_writers!(repo, &writer, Some(seed.clone()), 12);
    seed.pull().from(&last).perform(&writer).await?;

    let session = |tag: &'static [u8]| {
        let profile = profile.clone();
        async move {
            profile
                .session(tag)
                .space(profile.state())
                .allow(Subject::any())
                .await
        }
    };
    let alice = Counting::new(session(b"alice").await?);
    let bob = Counting::new(session(b"bob").await?);
    let a = profile
        .space(name.clone())
        .open()
        .perform(&alice)
        .await?
        .branch("alice")
        .open()
        .perform(&alice)
        .await?;
    let b = profile
        .space(name.clone())
        .open()
        .perform(&bob)
        .await?
        .branch("bob")
        .open()
        .perform(&bob)
        .await?;
    a.pull().from(&seed).perform(&alice).await?;
    commit!(a, &alice, "post:a0");
    let mut origins = [a.revision().map(|head| head.origin()), None];

    for round in 1..=10 {
        let (writer, env, from, slot) = match round % 2 {
            1 => (&b, &bob, &a, 1),
            _ => (&a, &alice, &b, 0),
        };
        writer.pull().from(from).perform(env).await?;
        let adopted = writer.revision().expect("the writer adopted a head");
        assert_eq!(Some(&adopted), from.revision().as_ref(), "a fast-forward");
        if let Some(origin) = origins[slot] {
            let known = BranchReference::from(writer).own_in_head();
            known.resolve().perform(env).await?;
            let recorded = known
                .content()
                .and_then(|known| known.in_head(&adopted.version(), &origin))
                .expect("the line recorded its own count in the head it adopted");
            let walked = context_of(&adopted.version(), &writer.history(env)).await?;
            assert_eq!(
                Some(&recorded),
                walked.get(&origin),
                "round {round}: the recorded count is the ancestry's"
            );
            env.reset();
        }
        commit!(writer, env, format!("post:{round}"));
        if origins[slot].is_some() {
            assert_eq!(
                env.block_reads(),
                0,
                "round {round}: a takeover on a recorded head reads nothing: {:?}",
                env.snapshot()
            );
        }
        origins[slot] = writer.revision().map(|head| head.origin());
        region_agrees(writer, env, "an alternating takeover").await?;
    }
    Ok(())
}
