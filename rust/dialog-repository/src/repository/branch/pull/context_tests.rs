//! The causal context once it outgrows a head: more origins than the
//! active set holds, so every head here spills, and the whole map lives in
//! the tree's context region.
//!
//! Every test builds its history with more than [`ACTIVE_CONTEXT`]
//! origins, one per passing writer branch, so the spill is never empty.

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

use anyhow::Result;
use std::iter::once;

use dialog_artifacts::history::{
    ACTIVE_CONTEXT, Context, EDITION_CEILING, Edition, HistoryError, Origin, Spill, Version,
    Watermark, context_in, context_of,
};
use dialog_artifacts::{Artifact, DialogArtifactsError, Instruction, Value};
use dialog_capability::Subject;
use dialog_common::Blake3Hash as NodeHash;
use dialog_effects::authority::Attest;
use dialog_peer::helpers::{test_session_with_peer, unique_name};
use futures_util::stream;

use crate::helpers::Counting;
use crate::{Branch, Index, NetworkedIndex, PullError, RepositoryExt as _, Revision};

/// One way to reshape a head before it is signed.
type Edit<'a> = Box<dyn Fn(&mut Revision) + 'a>;

/// More origins than a head carries inline, with room for the writers a
/// test adds on top.
const CROWD: usize = ACTIVE_CONTEXT + 8;

fn assert_one(the: &str, of: &str, value: &str) -> Instruction {
    Instruction::Assert(
        Artifact {
            the: the.parse().expect("attribute parses"),
            of: of.parse().expect("entity parses"),
            is: Value::String(value.to_string()),
            cause: None,
        },
        dialog_artifacts::Pick::All,
    )
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
                .commit(stream::iter(vec![assert_one(
                    "writer/mark",
                    &format!("mark:{tag}-{i}"),
                    "passing",
                )]))
                .perform($env)
                .await?;
            previous = Some(writer);
        }
        previous.expect("at least one writer")
    }};
}

/// What `branch`'s head says about its context, checked three ways: the
/// context region of its tree equals the ancestry walk, the head is the
/// projection of it, and so is what the memo remembers when it remembers.
macro_rules! assert_region_agrees {
    ($branch:expr, $env:expr, $what:expr) => {{
        let branch: &Branch = $branch;
        let head = branch.revision().expect("the branch has a head");
        let store = NetworkedIndex::new($env, branch.archive().index(), None);
        let tree =
            Index::from_hash_with_cache(NodeHash::from(*head.tree.hash()), branch.node_cache());
        let region = context_in(&tree, &store).await?;
        let walked = context_of(&head.version(), &branch.history($env)).await?;
        assert_eq!(
            region, walked,
            "{}: the region is the head's context",
            $what
        );
        head.check_context(&region)
            .unwrap_or_else(|error| panic!("{}: the head projects the region: {error}", $what));
        assert!(
            head.spill.is_some_and(|spill| !spill.is_empty()),
            "{}: the head spills",
            $what
        );
        if let Some(memo) = branch.contexts().cached(&head.version()).await {
            assert_eq!(*memo, walked, "{}: the memo is the head's context", $what);
        }
        walked
    }};
}

/// A head whose ancestry holds more origins than the active set carries
/// publishes its active set and spill, and its tree's context region is the
/// whole context: equal to the ancestry walk, projecting to the head.
/// Tampering with the spill breaks the signature like any other field.
#[dialog_common::test]
async fn it_publishes_the_active_set_and_spill_digest() -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let name = unique_name("repo");
    let repo = profile
        .space(name.clone())
        .open()
        .perform(&operator)
        .await?;

    let last = passing_writers!(repo, &operator, None, CROWD);
    let head = last.revision().expect("the last writer has a head");
    assert_eq!(
        head.context.as_ref().map(Context::len),
        Some(ACTIVE_CONTEXT)
    );
    assert_eq!(
        head.spill.map(|spill| spill.count),
        Some((CROWD - ACTIVE_CONTEXT) as u64)
    );
    head.verify().expect("the head verifies");
    let context = assert_region_agrees!(&last, &operator, "a commit");
    assert_eq!(context.len(), CROWD);

    let mut tampered = head.clone();
    tampered.spill = tampered.spill.map(|mut spill| {
        spill.count += 1;
        spill
    });
    assert!(
        matches!(tampered.verify(), Err(HistoryError::InvalidSignature(_))),
        "a tampered spill must fail head verification"
    );
    Ok(())
}

/// After every way a pull lands (fast-forward adoption, nothing new, the
/// screened merge, the replay, the graft), the head's tree carries the
/// head's whole context in its region, and the head projects it.
#[dialog_common::test]
async fn it_keeps_the_region_equal_to_the_head_context_in_every_merge_direction() -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let name = unique_name("repo");
    let repo = profile
        .space(name.clone())
        .open()
        .perform(&operator)
        .await?;

    let seed = passing_writers!(repo, &operator, None, CROWD);
    let main = repo.branch("main").open().perform(&operator).await?;
    main.pull().from(&seed).perform(&operator).await?;
    main.commit(stream::iter(vec![assert_one(
        "post/title",
        "post:0",
        "main",
    )]))
    .perform(&operator)
    .await?;
    assert_region_agrees!(&main, &operator, "a commit by a new writer");

    // Fast-forward: feature tracks main and has nothing of its own.
    let feature = repo.branch("feature").open().perform(&operator).await?;
    feature.set_upstream(&main).perform(&operator).await?;
    feature.pull().perform(&operator).await?;
    main.commit(stream::iter(vec![assert_one(
        "post/title",
        "post:1",
        "main",
    )]))
    .perform(&operator)
    .await?;
    feature.pull().perform(&operator).await?;
    assert_eq!(feature.revision(), main.revision(), "feature adopted main");
    assert_region_agrees!(&feature, &operator, "a fast-forward");

    // Nothing new: main adopts feature's commit, and feature pulls it back.
    feature
        .commit(stream::iter(vec![assert_one(
            "post/title",
            "post:2",
            "feature",
        )]))
        .perform(&operator)
        .await?;
    main.pull().from(&feature).perform(&operator).await?;
    let before = feature.revision();
    feature.pull().perform(&operator).await?;
    assert_eq!(feature.revision(), before, "nothing new keeps the head");
    assert_region_agrees!(&feature, &operator, "nothing new");

    // The screened merge: feature has more than main since the sync base.
    for i in 0..4 {
        feature
            .commit(stream::iter(vec![assert_one(
                "post/title",
                &format!("post:f{i}"),
                "feature",
            )]))
            .perform(&operator)
            .await?;
    }
    main.commit(stream::iter(vec![assert_one(
        "post/title",
        "post:m0",
        "main",
    )]))
    .perform(&operator)
    .await?;
    feature.pull().perform(&operator).await?;
    assert_region_agrees!(&feature, &operator, "the screened merge");

    // The replay: main has more than feature since the sync base.
    for i in 0..4 {
        main.commit(stream::iter(vec![assert_one(
            "post/title",
            &format!("post:m{}", i + 1),
            "main",
        )]))
        .perform(&operator)
        .await?;
    }
    feature
        .commit(stream::iter(vec![assert_one(
            "post/title",
            "post:f9",
            "feature",
        )]))
        .perform(&operator)
        .await?;
    feature.pull().perform(&operator).await?;
    assert_region_agrees!(&feature, &operator, "the replay");

    // The graft: both sides past the small-divergence threshold, with a
    // sync base.
    for i in 0..(super::SMALL_DIVERGENCE + 2) {
        main.commit(stream::iter(vec![assert_one(
            "post/title",
            &format!("post:gm{i}"),
            "main",
        )]))
        .perform(&operator)
        .await?;
        feature
            .commit(stream::iter(vec![assert_one(
                "post/title",
                &format!("post:gf{i}"),
                "feature",
            )]))
            .perform(&operator)
            .await?;
    }
    feature.pull().perform(&operator).await?;
    assert_region_agrees!(&feature, &operator, "the graft");

    // And the other way round, main pulling everything feature merged.
    main.pull().from(&feature).perform(&operator).await?;
    assert_region_agrees!(&main, &operator, "the reverse pull");
    Ok(())
}

/// A device pulling an upstream another device writes, whose writers all
/// stay active, reads no block for the context even though both heads
/// spill: the spills are equal, so the upstream's whole context is ours
/// with the active sets exchanged. When a new writer enters the upstream's
/// active set, the pull reads the upstream's region and still
/// fast-forwards.
///
/// The puller is a session of its own, so it remembers only what it
/// resolved itself; the upstream's heads are minted by the other session.
#[dialog_common::test]
async fn it_pulls_a_stable_writer_set_without_reading_the_context() -> Result<()> {
    let (writer, profile) = test_session_with_peer().await;
    let name = unique_name("repo");
    let repo = profile.space(name.clone()).open().perform(&writer).await?;

    let seed = passing_writers!(repo, &writer, None, CROWD);
    let main = repo.branch("main").open().perform(&writer).await?;
    main.pull().from(&seed).perform(&writer).await?;
    main.commit(stream::iter(vec![assert_one(
        "post/title",
        "post:0",
        "main",
    )]))
    .perform(&writer)
    .await?;
    let feature = repo.branch("feature").open().perform(&writer).await?;
    feature.set_upstream(&main).perform(&writer).await?;
    feature.pull().perform(&writer).await?;

    let device = profile
        .session(b"device")
        .space(profile.state())
        .allow(Subject::any())
        .await?;
    let device = Counting::new(device);
    let local = profile.space(name.clone()).open().perform(&device).await?;
    let synced = local.branch("feature").open().perform(&device).await?;

    // The writer on main is active, so its commits keep the spill.
    let spill = main.revision().and_then(|head| head.spill);
    let commit = |i: usize| {
        let main = main.clone();
        let writer = &writer;
        async move {
            main.commit(stream::iter(vec![assert_one(
                "post/title",
                &format!("post:{i}"),
                "main",
            )]))
            .perform(writer)
            .await?;
            anyhow::Ok(main.revision().expect("main has a head"))
        }
    };
    let moved = commit(1).await?;
    assert_eq!(moved.spill, spill, "an active writer keeps the spill");

    // The first pull reads the device's own head's context, once.
    synced.pull().perform(&device).await?;
    assert_eq!(synced.revision(), Some(moved), "a fast-forward");

    let moved = commit(2).await?;
    device.reset();
    synced.pull().perform(&device).await?;
    assert_eq!(synced.revision(), Some(moved.clone()), "a fast-forward");
    assert_eq!(
        device.block_reads(),
        0,
        "a stable writer set fast-forwards at zero reads: {:?}",
        device.snapshot()
    );
    assert!(
        synced.contexts().cached(&moved.version()).await.is_some(),
        "the exchanged context is remembered for the adopted head"
    );

    // Nothing new: the upstream moves to a head the device already holds.
    let merged = commit(3).await?;
    synced.pull().perform(&device).await?;
    feature.pull().from(&main).perform(&writer).await?;
    let relay = repo.branch("relay").open().perform(&writer).await?;
    relay.pull().from(&main).perform(&writer).await?;
    assert_eq!(relay.revision(), Some(merged.clone()));
    device.reset();
    synced
        .pull()
        .from(&local.branch("relay").open().perform(&device).await?)
        .perform(&device)
        .await?;
    assert_eq!(
        synced.revision(),
        Some(merged),
        "nothing new keeps the head"
    );
    assert_eq!(
        device.block_reads(),
        0,
        "nothing new is detected at zero reads: {:?}",
        device.snapshot()
    );

    // A new writer enters main's active set: the spills now differ, so the
    // pull reads main's region, and still adopts main's head.
    let newcomer = repo.branch("newcomer").open().perform(&writer).await?;
    newcomer.pull().from(&main).perform(&writer).await?;
    newcomer
        .commit(stream::iter(vec![assert_one(
            "post/title",
            "post:4",
            "newcomer",
        )]))
        .perform(&writer)
        .await?;
    main.pull().from(&newcomer).perform(&writer).await?;
    let moved = main.revision().expect("main has a head");
    assert_ne!(
        moved.spill,
        synced.revision().and_then(|head| head.spill),
        "the newcomer pushed an origin out of main's active set"
    );
    device.reset();
    synced.pull().perform(&device).await?;
    assert_eq!(synced.revision(), Some(moved), "still a fast-forward");
    assert!(
        device.block_reads() > 0,
        "a changed spill reads the upstream's region"
    );
    assert_region_agrees!(&synced, &device, "an adopted head after a region read");
    Ok(())
}

/// A deletion survives a pull from a stale peer even when the receiver's
/// knowledge of the deleted fact lives only in its spill: the fact's origin
/// and the deleter's have both been pushed out of the active set by later
/// writers, so the screen has to consult the context region to drop the
/// stale copy. Run through a fresh session, so nothing is remembered.
#[dialog_common::test]
async fn it_does_not_resurrect_a_deleted_fact_whose_origin_spilled() -> Result<()> {
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
    main.commit(stream::iter(vec![assert_one(
        "post/title",
        "post:1",
        "Hej",
    )]))
    .perform(&operator)
    .await?;
    let feature = repo.branch("feature").open().perform(&operator).await?;
    feature.set_upstream(&main).perform(&operator).await?;
    feature.pull().perform(&operator).await?;
    let peer = repo.branch("peer").open().perform(&operator).await?;
    peer.set_upstream(&main).perform(&operator).await?;
    peer.pull().perform(&operator).await?;

    feature
        .commit(stream::iter(vec![Instruction::Retract(Artifact {
            the: "post/title".parse()?,
            of: "post:1".parse()?,
            is: Value::String("Hej".to_string()),
            cause: None,
        })]))
        .perform(&operator)
        .await?;

    // Enough later writers to push both main's and feature's origins out.
    let last = passing_writers!(repo, &operator, Some(feature.clone()), ACTIVE_CONTEXT + 1);
    let head = last.revision().expect("the last writer has a head");
    let active = head
        .context
        .clone()
        .expect("the head carries its active set");
    let main_origin = main.revision().expect("main has a head").origin();
    let feature_origin = feature.revision().expect("feature has a head").origin();
    assert!(
        !active.observes_origin(&main_origin) && !active.observes_origin(&feature_origin),
        "both origins spilled"
    );

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
    let repo = profile.space(name.clone()).open().perform(&cold).await?;
    let last = repo.branch(last.name()).open().perform(&cold).await?;
    let peer = repo.branch("peer").open().perform(&cold).await?;
    assert!(
        last.contexts().cached(&head.version()).await.is_none(),
        "the fresh session remembers nothing"
    );

    assert_eq!(titles(last.clone(), &cold).await?, 0);
    assert_eq!(titles(peer.clone(), &cold).await?, 1, "peer still holds it");
    last.pull().from(&peer).perform(&cold).await?;
    assert_eq!(
        titles(last.clone(), &cold).await?,
        0,
        "a stale copy must not resurrect a fact whose origin spilled"
    );
    peer.pull().from(&last).perform(&cold).await?;
    assert_eq!(
        titles(peer.clone(), &cold).await?,
        0,
        "the deletion reaches the stale peer"
    );
    Ok(())
}

/// A head minted by `operator`'s session on `evil`'s branch with `edit`
/// applied, validly signed, planted as `evil`'s head.
macro_rules! plant_signed {
    ($evil:expr, $operator:expr, $template:expr, $edit:expr) => {{
        let mut head: Revision = $template.clone();
        let edit: &dyn Fn(&mut Revision) = &$edit;
        edit(&mut head);
        head.signature = Attest::new(head.payload()).perform($operator).await?;
        $evil.reset(head.clone()).perform($operator).await?;
        head
    }};
}

/// Validly signed heads whose published context no map can produce, or
/// whose tree's context region disagrees with what the head publishes, are
/// refused at pull with the specific error, and the puller's head stays.
#[dialog_common::test]
async fn it_refuses_to_pull_a_head_with_an_impossible_context() -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let name = unique_name("repo");
    let repo = profile
        .space(name.clone())
        .open()
        .perform(&operator)
        .await?;

    let last = passing_writers!(repo, &operator, None, CROWD);
    let template = last.revision().expect("the last writer has a head");
    let full = context_of(&template.version(), &last.history(&operator)).await?;

    let feature = repo.branch("feature").open().perform(&operator).await?;
    feature
        .commit(stream::iter(vec![assert_one(
            "post/title",
            "post:0",
            "ours",
        )]))
        .perform(&operator)
        .await?;
    let ours = feature.revision();

    // A version names one revision, so a head that reuses a known version
    // is answered from the memo as the head it names. The heads that must
    // reach the region check carry a branch identity of their own, and so a
    // version nothing remembers.
    let hostile: dialog_artifacts::Entity = format!("branch:{}", unique_name("hostile")).parse()?;

    // The template's active set with one entry swapped for a hostile one.
    let hostile_watermark = |watermark: Watermark| {
        let template = template.clone();
        move |head: &mut Revision| {
            let active = template.context.clone().expect("the template is projected");
            let dropped = *active.iter().next().expect("an active entry").0;
            let active: Context = active
                .iter()
                .filter(|(origin, _)| **origin != dropped)
                .map(|(origin, mark)| (*origin, *mark))
                .chain(once((Origin::from([0xEE; 32]), watermark)))
                .collect();
            head.publish_projection(active, template.spill.expect("the template spills"));
        }
    };

    let cases: Vec<(&str, Edit<'_>)> = vec![
        (
            "more active watermarks than allowed",
            Box::new({
                let full = full.clone();
                move |head: &mut Revision| {
                    head.context = Some(full.clone());
                    head.spill = Some(Spill::empty());
                }
            }),
        ),
        (
            "an active watermark at the edition ceiling",
            Box::new(hostile_watermark(Watermark {
                edition: Edition::new(EDITION_CEILING),
                count: 1,
            })),
        ),
        (
            "an active watermark counting more revisions than its edition holds",
            Box::new(hostile_watermark(Watermark {
                edition: Edition::new(template.edition.value()),
                count: template.edition.value() + 2,
            })),
        ),
        (
            "a spill its tree's region does not hold",
            Box::new({
                let hostile = hostile.clone();
                move |head: &mut Revision| {
                    head.branch = hostile.clone();
                    head.spill = head.spill.map(|mut spill| {
                        spill.digest = [0xAB; 32];
                        spill
                    });
                }
            }),
        ),
    ];

    for (why, edit) in cases {
        let evil = repo
            .branch(unique_name("evil"))
            .open()
            .perform(&operator)
            .await?;
        plant_signed!(evil, &operator, template, |head: &mut Revision| edit(head));
        let pulled = feature.pull().from(&evil).perform(&operator).await;
        assert!(
            matches!(
                pulled,
                Err(PullError::Artifact(DialogArtifactsError::InvalidKey(_)))
            ),
            "{why} must be refused; got {pulled:?}"
        );
        assert_eq!(feature.revision(), ours, "{why}: the head stays");
    }

    // The case the projection check exists for: a head that agrees with
    // itself but not with its tree's region. Its spill matches a region
    // built from a different map.
    let mut raised = full.clone();
    raised.record(Version::new(
        *full.iter().next().expect("an origin").0,
        Edition::new(template.edition.value()),
    ));
    let (active, spill) = raised.project(ACTIVE_CONTEXT);
    let evil = repo
        .branch(unique_name("evil"))
        .open()
        .perform(&operator)
        .await?;
    plant_signed!(evil, &operator, template, |head: &mut Revision| {
        head.branch = hostile.clone();
        head.publish_projection(active.clone(), spill)
    });
    let pulled = feature.pull().from(&evil).perform(&operator).await;
    assert!(
        matches!(
            pulled,
            Err(PullError::Artifact(DialogArtifactsError::InvalidKey(_)))
        ),
        "a head disagreeing with its region must be refused; got {pulled:?}"
    );
    assert_eq!(feature.revision(), ours, "the head stays");
    Ok(())
}

/// Heads minted before the context region (the whole context inline, or
/// no context at all) still pull, on either side of the pull, and the
/// first head minted on top of one writes the whole context into its
/// tree's region.
#[dialog_common::test]
async fn it_pulls_and_commits_across_heads_minted_before_the_region() -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let name = unique_name("repo");
    let repo = profile
        .space(name.clone())
        .open()
        .perform(&operator)
        .await?;

    let last = passing_writers!(repo, &operator, None, CROWD);
    let current = last.revision().expect("the last writer has a head");
    let full = context_of(&current.version(), &last.history(&operator)).await?;

    // The same head, as a release before the region signed it: the whole
    // context inline, no spill.
    let inline = repo.branch("inline").open().perform(&operator).await?;
    let legacy = plant_signed!(inline, &operator, current, |head: &mut Revision| {
        head.context = Some(full.clone());
        head.spill = None;
    });
    legacy.verify().expect("an inline head still verifies");

    // Pulled as an upstream into a branch with a head of its own.
    let feature = repo.branch("feature").open().perform(&operator).await?;
    feature
        .commit(stream::iter(vec![assert_one(
            "post/title",
            "post:0",
            "ours",
        )]))
        .perform(&operator)
        .await?;
    feature.pull().from(&inline).perform(&operator).await?;
    assert_region_agrees!(&feature, &operator, "a merge with an inline upstream");

    // Committed on as the local head.
    inline
        .commit(stream::iter(vec![assert_one(
            "post/title",
            "post:1",
            "inline",
        )]))
        .perform(&operator)
        .await?;
    assert_region_agrees!(&inline, &operator, "a commit on an inline head");

    // And with no context at all, as the first releases signed heads.
    let bare = repo.branch("bare").open().perform(&operator).await?;
    plant_signed!(bare, &operator, current, |head: &mut Revision| {
        head.context = None;
        head.spill = None;
    });
    let other = repo.branch("other").open().perform(&operator).await?;
    other
        .commit(stream::iter(vec![assert_one(
            "post/title",
            "post:2",
            "other",
        )]))
        .perform(&operator)
        .await?;
    other.pull().from(&bare).perform(&operator).await?;
    assert_region_agrees!(&other, &operator, "a merge with a pre-context upstream");
    Ok(())
}
