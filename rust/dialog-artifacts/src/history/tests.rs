//! Tests reproducing the scenarios illustrated in `notes/version-control.md`

use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::Result;
use dialog_storage::MemoryStorageBackend;
use ed25519_dalek::SigningKey;

use crate::tree::{ArtifactTree, ArtifactTreeExt as _};
use crate::{Artifact, Attribute, DialogArtifactsError, Entity, Instruction, Value};

use super::{
    Authority, Causality, Cause, Claim, Edition, History, MemoryHistory, Origin, Revision,
    SKIP_ATTRIBUTE, TreeHistory, Version, causality, common_ancestor, extend_skips,
};

#[cfg(target_arch = "wasm32")]
use wasm_bindgen_test::wasm_bindgen_test;
#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

fn signing_key(seed: u8) -> SigningKey {
    SigningKey::from_bytes(&[seed; 32])
}

fn authority_of(key: &SigningKey) -> Authority {
    Authority::from(key.verifying_key())
}

fn tree(seed: u8) -> [u8; 32] {
    [seed; 32]
}

fn name_attribute() -> Attribute {
    Attribute::from_str("profile/name").unwrap()
}

fn name_claim(of: &Entity, value: &str, cause: Cause) -> Claim {
    Claim {
        the: name_attribute(),
        of: of.clone(),
        is: Value::String(value.into()),
        cause,
    }
}

/// A revision on top of the given parents, issued by `key` against `subject`
fn revise(subject: &Entity, key: &SigningKey, parents: &[&Revision], seed: u8) -> Revision {
    Revision::issue(
        tree(seed),
        subject.clone(),
        authority_of(key),
        parents.iter().map(|parent| parent.version()).collect(),
        key,
    )
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[cfg_attr(not(target_arch = "wasm32"), test)]
fn it_derives_editions_from_the_revision_dag() -> Result<()> {
    let repo = Entity::new()?;
    let alice = signing_key(1);
    let bob = signing_key(2);

    // Conflict Detection Illustrated: Alice commits twice, Bob commits once,
    // then Bob pulls Alice's work before committing again
    let genesis = revise(&repo, &bob, &[], 0);
    assert_eq!(genesis.edition(), Edition::GENESIS);
    genesis.verify()?;

    let a1 = revise(&repo, &alice, &[&genesis], 1);
    let a2 = revise(&repo, &alice, &[&a1], 2);
    let b1 = revise(&repo, &bob, &[&genesis], 3);

    assert_eq!(a1.edition(), Edition::new(1));
    assert_eq!(a2.edition(), Edition::new(2));
    assert_eq!(b1.edition(), Edition::new(1));

    // Bob pulls: his next revision references both heads and lands at
    // max(2, 1) + 1 = 3
    let b3 = revise(&repo, &bob, &[&a2, &b1], 4);
    assert_eq!(b3.edition(), Edition::new(3));
    b3.verify()?;

    Ok(())
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[cfg_attr(not(target_arch = "wasm32"), test)]
fn it_derives_distinct_origins_per_repository() -> Result<()> {
    let repo_a = Entity::new()?;
    let repo_b = Entity::new()?;
    let key = signing_key(1);

    let in_a = revise(&repo_a, &key, &[], 0);
    let in_b = revise(&repo_b, &key, &[], 0);

    // The same principal acting on two different repositories produces two
    // distinct origins, so identical editions do not collide on merge
    assert_ne!(in_a.origin(), in_b.origin());
    assert_eq!(in_a.edition(), in_b.edition());
    assert_ne!(in_a.version(), in_b.version());

    Ok(())
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[cfg_attr(not(target_arch = "wasm32"), test)]
fn it_verifies_signatures_and_rejects_tampering() -> Result<()> {
    let repo = Entity::new()?;
    let key = signing_key(1);

    let genesis = revise(&repo, &key, &[], 0);
    let revision = revise(&repo, &key, &[&genesis], 1);
    revision.verify()?;

    // Tamper with the edition: structural rule violation
    let mut tampered = serde_json::to_value(&revision)?;
    tampered["edition"] = serde_json::json!(7);
    let tampered: Revision = serde_json::from_value(tampered)?;
    assert!(matches!(
        tampered.verify(),
        Err(DialogArtifactsError::InvalidSignature(_))
    ));

    // Tamper with the tree: signature no longer covers the payload
    let mut tampered = serde_json::to_value(&revision)?;
    tampered["tree"] = serde_json::to_value(tree(9).to_vec())?;
    let tampered: Revision = serde_json::from_value(tampered)?;
    assert!(matches!(
        tampered.verify(),
        Err(DialogArtifactsError::InvalidSignature(_))
    ));

    // Content addresses differ for different content, and are stable
    assert_eq!(revision.reference(), revision.clone().reference());
    assert_ne!(revision.reference(), genesis.reference());

    Ok(())
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
async fn it_detects_concurrent_claims_and_supersession() -> Result<()> {
    let repo = Entity::new()?;
    let entity = Entity::new()?;
    let alice = signing_key(1);
    let bob = signing_key(2);

    let genesis = revise(&repo, &bob, &[], 0);
    let a1 = revise(&repo, &alice, &[&genesis], 1);
    let a2 = revise(&repo, &alice, &[&a1], 2);
    let b1 = revise(&repo, &bob, &[&genesis], 3);

    // Alice writes the name twice; Bob writes it once, concurrently
    let alice_first = name_claim(&entity, "Alicia", Cause::genesis());
    let alice_second = name_claim(&entity, "Alice", Cause::from(a1.version()));
    let bob_first = name_claim(&entity, "Robert", Cause::genesis());

    let mut history = MemoryHistory::default();
    history.record(&a1.version(), alice_first.clone());
    history.record(&a2.version(), alice_second.clone());
    history.record(&b1.version(), bob_first.clone());

    // Same claim: causally equal
    assert_eq!(
        causality(
            (&alice_second, &a2.version()),
            (&alice_second, &a2.version()),
            &history
        )
        .await?,
        Causality::Equal
    );

    // Same edition, different origin: concurrent by inspection (tier 0)
    assert_eq!(
        causality(
            (&alice_first, &a1.version()),
            (&bob_first, &b1.version()),
            &history
        )
        .await?,
        Causality::Concurrent
    );

    // Same origin: ordered by edition (tier 0)
    assert_eq!(
        causality(
            (&alice_second, &a2.version()),
            (&alice_first, &a1.version()),
            &history
        )
        .await?,
        Causality::Supersedes
    );

    // Different edition and origin: tier 2 traversal finds only A:1 at
    // edition 1, which does not match B:1's version — concurrent
    assert_eq!(
        causality(
            (&alice_second, &a2.version()),
            (&bob_first, &b1.version()),
            &history
        )
        .await?,
        Causality::Concurrent
    );

    // Bob pulls Alice's work and deliberately supersedes both concurrent
    // claims; his cause lists both, so tier 1 resolves in O(1)
    let b3 = revise(&repo, &bob, &[&a2, &b1], 4);
    let bob_resolution = name_claim(&entity, "Bob", Cause::new(vec![a2.version(), b1.version()]));
    history.record(&b3.version(), bob_resolution.clone());

    assert_eq!(
        causality(
            (&bob_resolution, &b3.version()),
            (&alice_second, &a2.version()),
            &history
        )
        .await?,
        Causality::Supersedes
    );
    assert_eq!(
        causality(
            (&alice_second, &a2.version()),
            (&bob_resolution, &b3.version()),
            &history
        )
        .await?,
        Causality::Superseded
    );

    Ok(())
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
async fn it_traverses_multi_cause_lineages() -> Result<()> {
    let repo = Entity::new()?;
    let entity = Entity::new()?;
    let alice = signing_key(1);
    let bob = signing_key(2);
    let carol = signing_key(3);

    let genesis = revise(&repo, &alice, &[], 0);
    let a1 = revise(&repo, &alice, &[&genesis], 1);
    let b1 = revise(&repo, &bob, &[&genesis], 2);

    // Alice and Bob write concurrently; Carol resolves both; a later claim
    // builds on Carol's resolution
    let alice_claim = name_claim(&entity, "Alice", Cause::genesis());
    let bob_claim = name_claim(&entity, "Robert", Cause::genesis());

    let c2 = revise(&repo, &carol, &[&a1, &b1], 3);
    let carol_resolution = name_claim(
        &entity,
        "Carol",
        Cause::new(vec![a1.version(), b1.version()]),
    );

    let c3 = revise(&repo, &carol, &[&c2], 4);
    let carol_followup = name_claim(&entity, "Caroline", Cause::from(c2.version()));

    let mut history = MemoryHistory::default();
    history.record(&a1.version(), alice_claim.clone());
    history.record(&b1.version(), bob_claim.clone());
    history.record(&c2.version(), carol_resolution.clone());
    history.record(&c3.version(), carol_followup.clone());

    // The follow-up's history is a DAG: traversal must branch through the
    // resolution's multi-entry cause to find both concurrent ancestors
    assert_eq!(
        causality(
            (&carol_followup, &c3.version()),
            (&alice_claim, &a1.version()),
            &history
        )
        .await?,
        Causality::Supersedes
    );
    assert_eq!(
        causality(
            (&carol_followup, &c3.version()),
            (&bob_claim, &b1.version()),
            &history
        )
        .await?,
        Causality::Supersedes
    );

    // A claim from an origin outside the DAG remains concurrent
    let mallory = signing_key(4);
    let m1 = revise(&repo, &mallory, &[&genesis], 5);
    let mallory_claim = name_claim(&entity, "Mallory", Cause::genesis());
    history.record(&m1.version(), mallory_claim.clone());

    assert_eq!(
        causality(
            (&carol_followup, &c3.version()),
            (&mallory_claim, &m1.version()),
            &history
        )
        .await?,
        Causality::Concurrent
    );

    Ok(())
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
async fn it_blocks_on_incomplete_replication() -> Result<()> {
    let repo = Entity::new()?;
    let entity = Entity::new()?;
    let alice = signing_key(1);
    let bob = signing_key(2);

    let genesis = revise(&repo, &alice, &[], 0);
    let a1 = revise(&repo, &alice, &[&genesis], 1);
    let a2 = revise(&repo, &alice, &[&a1], 2);
    let a3 = revise(&repo, &alice, &[&a2], 3);
    let b1 = revise(&repo, &bob, &[&genesis], 4);

    let alice_second = name_claim(&entity, "Alice", Cause::from(a1.version()));
    let alice_third = name_claim(&entity, "Ally", Cause::from(a2.version()));
    let bob_first = name_claim(&entity, "Robert", Cause::genesis());

    // The intermediate claim at A:2 has not been replicated
    let mut history = MemoryHistory::default();
    history.record(&a3.version(), alice_third.clone());
    history.record(&b1.version(), bob_first.clone());

    assert!(matches!(
        causality(
            (&alice_third, &a3.version()),
            (&bob_first, &b1.version()),
            &history
        )
        .await,
        Err(DialogArtifactsError::IncompleteHistory(_))
    ));

    // Once the missing claim arrives, resolution proceeds
    history.record(&a2.version(), alice_second.clone());
    assert_eq!(
        causality(
            (&alice_third, &a3.version()),
            (&bob_first, &b1.version()),
            &history
        )
        .await?,
        Causality::Concurrent
    );

    Ok(())
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
async fn it_supports_collaborators_joining() -> Result<()> {
    let bob_repo = Entity::new()?;
    let bob = signing_key(1);
    let carol = signing_key(2);

    let genesis = revise(&bob_repo, &bob, &[], 0);
    let b1 = revise(&bob_repo, &bob, &[&genesis], 1);
    let b2 = revise(&bob_repo, &bob, &[&b1], 2);

    // Carol joins with no prior history: her first commit references Bob's
    // head and lands at edition 3 = max(2, 0) + 1
    let c3 = revise(&bob_repo, &carol, &[&b2], 3);
    assert_eq!(c3.edition(), Edition::new(3));

    // Carol joins with prior history: her pre-join revisions in her own
    // repository keep their editions and origins; her first post-join
    // revision merges both heads
    let carol_repo = Entity::new()?;
    let carol_genesis = revise(&carol_repo, &carol, &[], 0);
    let c1 = revise(&carol_repo, &carol, &[&carol_genesis], 1);
    let c2 = revise(&carol_repo, &carol, &[&c1], 2);

    let merge = revise(&bob_repo, &carol, &[&b2, &c2], 4);
    assert_eq!(merge.edition(), Edition::new(3));
    assert_ne!(c2.origin(), merge.origin());

    Ok(())
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
async fn it_forks_and_merges_across_repositories() -> Result<()> {
    let bob_repo = Entity::new()?;
    let alice_repo = Entity::new()?;
    let bob = signing_key(1);
    let alice = signing_key(2);

    let mut history = MemoryHistory::default();

    // Fork and Merge Illustrated: Bob commits twice, Alice forks at his
    // edition 2, both continue independently, then Alice merges Bob
    let genesis = revise(&bob_repo, &bob, &[], 0);
    let b1 = revise(&bob_repo, &bob, &[&genesis], 1);
    let b2 = revise(&bob_repo, &bob, &[&b1], 2);

    let a3 = revise(&alice_repo, &alice, &[&b2], 3);
    let a4 = revise(&alice_repo, &alice, &[&a3], 4);
    assert_eq!(a3.edition(), Edition::new(3));
    assert_eq!(a4.edition(), Edition::new(4));

    let b3 = revise(&bob_repo, &bob, &[&b2], 5);
    let b4 = revise(&bob_repo, &bob, &[&b3], 6);

    // The merge is the revision A:5: its cause references both lineages and
    // its edition is max(4, 4) + 1 = 5
    let a5 = revise(&alice_repo, &alice, &[&a4, &b4], 7);
    assert_eq!(a5.edition(), Edition::new(5));
    assert!(a5.cause().contains(&a4.version()));
    assert!(a5.cause().contains(&b4.version()));

    for revision in [&genesis, &b1, &b2, &a3, &a4, &b3, &b4, &a5] {
        revision.verify()?;
        history.record_revision(revision)?;
    }

    // The common ancestor of the two pre-merge heads is Bob's edition 2
    assert_eq!(
        common_ancestor(&a4.version(), &b4.version(), &history).await?,
        Some(b2.version())
    );

    // Each repository maintains its own lineage under its own DID, in a
    // total order consistent with causality
    let bob_lineage = history.revisions(&bob_repo);
    assert_eq!(
        bob_lineage
            .iter()
            .map(|(version, _)| version.edition)
            .collect::<Vec<_>>(),
        vec![
            Edition::new(0),
            Edition::new(1),
            Edition::new(2),
            Edition::new(3),
            Edition::new(4)
        ]
    );

    let alice_lineage = history.revisions(&alice_repo);
    assert_eq!(
        alice_lineage
            .iter()
            .map(|(version, _)| version.edition)
            .collect::<Vec<_>>(),
        vec![Edition::new(3), Edition::new(4), Edition::new(5)]
    );

    // Independent lineages share no history
    let stranger_repo = Entity::new()?;
    let stranger = signing_key(9);
    let s0 = revise(&stranger_repo, &stranger, &[], 0);
    history.record_revision(&s0)?;
    assert_eq!(
        common_ancestor(&a5.version(), &s0.version(), &history).await?,
        None
    );

    Ok(())
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[cfg_attr(not(target_arch = "wasm32"), test)]
fn it_orders_versions_by_causal_depth() -> Result<()> {
    let origin_a = Origin::from([1u8; 32]);
    let origin_b = Origin::from([2u8; 32]);

    let mut versions = vec![
        Version::new(origin_b, Edition::new(300)),
        Version::new(origin_a, Edition::new(4)),
        Version::new(origin_b, Edition::new(4)),
        Version::new(origin_a, Edition::new(0)),
    ];
    versions.sort();

    assert_eq!(
        versions
            .iter()
            .map(|version| version.edition.value())
            .collect::<Vec<_>>(),
        vec![0, 4, 4, 300]
    );

    // Key encoding preserves the ordering lexicographically
    let mut encoded = versions
        .iter()
        .map(|version| version.key_bytes())
        .collect::<Vec<_>>();
    let sorted = encoded.clone();
    encoded.sort();
    assert_eq!(encoded, sorted);

    // Round trip
    for version in versions {
        assert_eq!(Version::from_key_bytes(&version.key_bytes())?, version);
    }

    Ok(())
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
async fn it_records_multiple_values_per_attribute() -> Result<()> {
    let repo = Entity::new()?;
    let entity = Entity::new()?;
    let alice = signing_key(1);

    let genesis = revise(&repo, &alice, &[], 0);
    let a1 = revise(&repo, &alice, &[&genesis], 1);

    // A cardinality-many attribute written twice in one revision: both
    // claims are recorded under the same (version, entity, attribute)
    let first = name_claim(&entity, "Alice", Cause::genesis());
    let second = name_claim(&entity, "Alicia", Cause::genesis());

    let mut history = MemoryHistory::default();
    history.record(&a1.version(), first.clone());
    history.record(&a1.version(), second.clone());

    let claims = history
        .claims_at(&a1.version(), &entity, &name_attribute())
        .await?;
    assert_eq!(claims.len(), 2);

    Ok(())
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
async fn it_records_history_in_the_artifact_tree() -> Result<()> {
    use dialog_search_tree::Delta;
    use dialog_storage::{CborEncoder, Storage, StorageBackend as _};
    use futures_util::stream;

    let mut store = Storage {
        encoder: CborEncoder,
        backend: MemoryStorageBackend::default(),
    };

    let entity = Entity::new()?;
    let the: Attribute = "post/title".parse()?;
    let title = |value: &str| Artifact {
        the: the.clone(),
        of: entity.clone(),
        is: Value::String(value.into()),
        cause: None,
    };

    let first = Version::new(Origin::from([7u8; 32]), Edition::new(0));
    let second = Version::new(Origin::from([8u8; 32]), Edition::new(1));
    let third = Version::new(Origin::from([8u8; 32]), Edition::new(2));

    // Persist each version-tagged batch so the next edit can read the
    // spine back — data and history land in the same tree, so one root
    // covers both.
    let mut tree = ArtifactTree::empty();
    let apply = async |tree: &mut ArtifactTree,
                       store: &mut Storage<
        CborEncoder,
        MemoryStorageBackend<dialog_storage::Blake3Hash, Vec<u8>>,
    >,
                       version: Version,
                       instruction: Instruction|
           -> Result<()> {
        let mut delta = Delta::zero();
        tree.apply_versioned(
            store,
            &mut delta,
            Some(version),
            stream::iter(vec![instruction]),
        )
        .await?;
        for (digest, buffer) in delta.flush() {
            store.set(*digest.as_bytes(), buffer.into_vec()).await?;
        }
        Ok(())
    };

    apply(
        &mut tree,
        &mut store,
        first,
        Instruction::Assert(title("Hej")),
    )
    .await?;
    apply(
        &mut tree,
        &mut store,
        second,
        Instruction::Replace(title("Hi")),
    )
    .await?;

    // The replacement's record supersedes the first claim, detectable via
    // the tiered conflict detection over the same tree
    let history = TreeHistory::new(tree.clone(), store.clone());
    let records = history.records().await?;
    assert_eq!(records.len(), 2);
    let (first_version, hej) = &records[0];
    let (second_version, hi) = &records[1];
    assert_eq!(*first_version, first);
    assert_eq!(*second_version, second);
    assert!(hej.claim().cause.is_genesis());
    assert!(hi.claim().cause.contains(&first));
    assert_eq!(
        causality((hi.claim(), &second), (hej.claim(), &first), &history).await?,
        Causality::Supersedes
    );

    // Retraction records the assertion it withdraws, and the data region
    // reflects the retraction while the history region keeps all records
    apply(
        &mut tree,
        &mut store,
        third,
        Instruction::Retract(title("Hi")),
    )
    .await?;
    let history = TreeHistory::new(tree.clone(), store.clone());
    let records = history.records().await?;
    assert_eq!(records.len(), 3);
    let (_, retraction) = &records[2];
    assert!(!retraction.is_assertion());
    assert!(retraction.claim().cause.contains(&second));
    assert!(
        tree.select_data(store.clone(), &entity, &the)
            .await?
            .is_empty()
    );

    Ok(())
}

/// A [`History`] wrapper counting revision DAG reads, to assert the skip
/// links actually shrink traversals rather than merely not breaking them.
struct CountingHistory<'a> {
    inner: &'a MemoryHistory,
    reads: AtomicUsize,
}

impl<'a> CountingHistory<'a> {
    fn new(inner: &'a MemoryHistory) -> Self {
        Self {
            inner,
            reads: AtomicUsize::new(0),
        }
    }

    fn reads(&self) -> usize {
        self.reads.load(Ordering::SeqCst)
    }
}

impl History for CountingHistory<'_> {
    async fn claims_at(
        &self,
        version: &Version,
        of: &Entity,
        the: &Attribute,
    ) -> Result<Vec<Claim>, DialogArtifactsError> {
        self.inner.claims_at(version, of, the).await
    }

    async fn revision_at(&self, version: &Version) -> Result<Vec<Claim>, DialogArtifactsError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.inner.revision_at(version).await
    }

    async fn skips_at(&self, version: &Version) -> Result<Vec<Claim>, DialogArtifactsError> {
        self.inner.skips_at(version).await
    }
}

/// Record the skip table claims for a revision, the way a branch commit
/// does (see `dialog-repository`'s `Revision::records`).
fn record_skips(
    history: &mut MemoryHistory,
    revision: &Revision,
    skips: &[(u32, Version)],
) -> Result<()> {
    for (level, target) in skips {
        history.record(
            &revision.version(),
            Claim {
                the: Attribute::from_str(SKIP_ATTRIBUTE)?,
                of: revision.entity()?,
                is: Value::UnsignedInt(u128::from(*level)),
                cause: Cause::from(*target),
            },
        );
    }
    Ok(())
}

/// Skip links let `common_ancestor` leap over long linear runs: a head far
/// ahead of the other descends to the other's causal depth in
/// logarithmically many reads, and the result is exactly what the stepwise
/// walk would find.
#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
async fn it_finds_ancestors_in_logarithmic_reads_via_skip_links() -> Result<()> {
    let repo = Entity::new()?;
    let alice = signing_key(1);
    let bob = signing_key(2);

    // A long linear run by Alice, each revision recording the skip table
    // lifted from its parent's.
    let mut history = MemoryHistory::default();
    let mut chain = vec![revise(&repo, &alice, &[], 0)];
    history.record_revision(&chain[0])?;
    for _ in 1..=128 {
        let parent = chain.last().expect("chain is nonempty");
        let skips = extend_skips(&history, &parent.version()).await?;
        let next = revise(&repo, &alice, &[parent], 1);
        history.record_revision(&next)?;
        record_skips(&mut history, &next, &skips)?;
        chain.push(next);
    }

    // Bob forked early and made one commit of his own.
    let fork = revise(&repo, &bob, &[&chain[5]], 2);
    history.record_revision(&fork)?;

    let counting = CountingHistory::new(&history);
    let ancestor = common_ancestor(
        &chain.last().expect("chain is nonempty").version(),
        &fork.version(),
        &counting,
    )
    .await?;
    assert_eq!(
        ancestor,
        Some(chain[5].version()),
        "the accelerated traversal finds the exact fork point"
    );
    assert!(
        counting.reads() < 30,
        "the 123-revision gap should be leapt, not walked: {} reads",
        counting.reads()
    );

    Ok(())
}

/// A leap must never cross a merge: ancestry entering the run through a
/// merge's second parent stays reachable, because merges record no skip
/// table and the chains recorded after one never lift across it.
#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
async fn it_never_leaps_over_a_merge() -> Result<()> {
    let repo = Entity::new()?;
    let alice = signing_key(1);
    let bob = signing_key(2);

    let mut history = MemoryHistory::default();
    let extend = async |history: &mut MemoryHistory,
                        parents: &[&Revision],
                        key: &ed25519_dalek::SigningKey|
           -> Result<Revision> {
        let skips = match parents {
            [parent] => extend_skips(&*history, &parent.version()).await?,
            _ => Vec::new(),
        };
        let next = revise(&repo, key, parents, 3);
        history.record_revision(&next)?;
        record_skips(history, &next, &skips)?;
        Ok(next)
    };

    // Alice's chain, with a dormant branch by Bob hanging off revision 2;
    // the dormant work merges back in at revision 10, and the chain runs
    // long past the merge.
    let mut chain = vec![revise(&repo, &alice, &[], 0)];
    history.record_revision(&chain[0])?;
    for _ in 1..=10 {
        let next = extend(&mut history, &[chain.last().expect("nonempty")], &alice).await?;
        chain.push(next);
    }
    let dormant = extend(&mut history, &[&chain[2]], &bob).await?;
    let merge = extend(
        &mut history,
        &[chain.last().expect("nonempty"), &dormant],
        &alice,
    )
    .await?;
    chain.push(merge);
    for _ in 0..40 {
        let next = extend(&mut history, &[chain.last().expect("nonempty")], &alice).await?;
        chain.push(next);
    }

    // Bob continues the dormant line without ever seeing the merge. The
    // only meeting point is his dormant revision, reachable from Alice's
    // head solely through the merge's second parent.
    let bob_head = extend(&mut history, &[&dormant], &bob).await?;

    let ancestor = common_ancestor(
        &chain.last().expect("nonempty").version(),
        &bob_head.version(),
        &history,
    )
    .await?;
    assert_eq!(
        ancestor,
        Some(dormant.version()),
        "ancestry through the merge's second parent must not be leapt over"
    );

    Ok(())
}
