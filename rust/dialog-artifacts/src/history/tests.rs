//! Tests reproducing the scenarios illustrated in `notes/version-control.md`

use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::Result;
use dialog_storage::MemoryStorageBackend;
use ed25519_dalek::SigningKey;

use crate::tree::{ArtifactTree, ArtifactTreeExt as _};
use crate::{Artifact, Attribute, DialogArtifactsError, Entity, Instruction, Value};

use super::{
    Authority, Causality, CausalityCache, Cause, Claim, Edition, History, MemoryHistory, Origin,
    Revision, RevisionRecord, TreeHistory, Version, causality, common_ancestor, extend_skips, log,
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

    // Every revision's record is retrievable and carries its parents
    let merged = history
        .revision_record(&a5.version())
        .await?
        .expect("the merge's record is retrievable");
    assert!(merged.parents.contains(&a4.version()));
    assert!(merged.parents.contains(&b4.version()));
    assert_eq!(merged.parents.len(), 2);
    assert_eq!(merged.lineage, alice_repo);

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

/// A [`History`] wrapper counting revision record reads, to assert the
/// skip links actually shrink traversals rather than merely not breaking
/// them.
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
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.inner.claims_at(version, of, the).await
    }

    async fn revision_record(
        &self,
        version: &Version,
    ) -> Result<Option<RevisionRecord>, DialogArtifactsError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.inner.revision_record(version).await
    }
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
        history.record_skips(&next.version(), skips);
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
        history.record_skips(&next.version(), skips);
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

/// A batch that asserts and retracts the same fact leaves one history
/// record — the retraction, degenerated to a genesis retraction since a
/// record must not claim itself as its cause — and no data.
#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
async fn it_collapses_a_same_batch_assert_and_retract() -> Result<()> {
    use dialog_search_tree::Delta;
    use dialog_storage::{CborEncoder, Storage, StorageBackend as _};
    use futures_util::stream;

    let mut store = Storage {
        encoder: CborEncoder,
        backend: MemoryStorageBackend::default(),
    };

    let entity = Entity::new()?;
    let the: Attribute = "post/title".parse()?;
    let title = Artifact {
        the: the.clone(),
        of: entity.clone(),
        is: Value::String("Hej".into()),
        cause: None,
    };
    let version = Version::new(Origin::from([7u8; 32]), Edition::new(0));

    let mut tree = ArtifactTree::empty();
    let mut delta = Delta::zero();
    let changed = tree
        .apply_versioned(
            &mut store,
            &mut delta,
            Some(version),
            stream::iter(vec![
                Instruction::Assert(title.clone()),
                Instruction::Retract(title),
            ]),
        )
        .await?;
    assert!(changed);
    for (digest, buffer) in delta.flush() {
        store.set(*digest.as_bytes(), buffer.into_vec()).await?;
    }

    let history = TreeHistory::new(tree.clone(), store.clone());
    let records = history.records().await?;
    assert_eq!(records.len(), 1, "the retraction overwrites the assertion");
    let (recorded_version, record) = &records[0];
    assert_eq!(*recorded_version, version);
    assert!(!record.is_assertion());
    assert!(
        record.claim().cause.is_genesis(),
        "a record must not claim its own version as its cause"
    );
    assert!(
        tree.select_data(store.clone(), &entity, &the)
            .await?
            .is_empty()
    );

    Ok(())
}

/// A replacement over a cardinality-many anomaly — several values standing
/// at one (entity, attribute), one of them already the replacement's value
/// — supersedes exactly the different-valued claims: they are removed and
/// listed as the record's cause, while the same-valued claim survives at
/// its original version.
#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
async fn it_supersedes_only_different_values_when_replacing_many() -> Result<()> {
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
    let second = Version::new(Origin::from([7u8; 32]), Edition::new(1));
    let third = Version::new(Origin::from([7u8; 32]), Edition::new(2));

    let mut tree = ArtifactTree::empty();
    let apply = async |tree: &mut ArtifactTree,
                       store: &mut Storage<
        CborEncoder,
        MemoryStorageBackend<dialog_storage::Blake3Hash, Vec<u8>>,
    >,
                       version: Version,
                       instruction: Instruction|
           -> Result<bool> {
        let mut delta = Delta::zero();
        let changed = tree
            .apply_versioned(
                store,
                &mut delta,
                Some(version),
                stream::iter(vec![instruction]),
            )
            .await?;
        for (digest, buffer) in delta.flush() {
            store.set(*digest.as_bytes(), buffer.into_vec()).await?;
        }
        Ok(changed)
    };

    // Two values stand at the same (entity, attribute) — assertions are
    // additive, so this is the cardinality-many shape.
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
        Instruction::Assert(title("Hi")),
    )
    .await?;

    // Replacing with one of the standing values repairs the anomaly:
    // the different-valued claim is superseded, the same-valued one stays.
    let changed = apply(
        &mut tree,
        &mut store,
        third,
        Instruction::Replace(title("Hi")),
    )
    .await?;
    assert!(changed, "superseding a standing value is a change");

    let data = tree.select_data(store.clone(), &entity, &the).await?;
    assert_eq!(data.len(), 1);
    assert_eq!(
        data[0].version,
        Some(second),
        "the surviving claim keeps its original version"
    );

    let history = TreeHistory::new(tree.clone(), store.clone());
    let records = history.records().await?;
    assert_eq!(records.len(), 3);
    let (_, replacement) = &records[2];
    assert!(replacement.claim().cause.contains(&first));
    assert!(
        !replacement.claim().cause.contains(&second),
        "the surviving same-valued claim is not superseded"
    );

    // And replaying the exact same replacement is now a pure no-op.
    let changed = apply(
        &mut tree,
        &mut store,
        Version::new(Origin::from([7u8; 32]), Edition::new(3)),
        Instruction::Replace(title("Hi")),
    )
    .await?;
    assert!(!changed, "re-replacing the only standing value is a no-op");

    Ok(())
}

/// History keys truncate entity and attribute to raw heads; queries must
/// disambiguate collisions against the stored record. Two attributes
/// sharing the 57-byte head — and two entities sharing the 32-byte URI
/// head — recorded at the same version must not bleed into each other's
/// `claims_at`.
#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
async fn it_disambiguates_truncated_history_keys_in_queries() -> Result<()> {
    use dialog_search_tree::Delta;
    use dialog_storage::{CborEncoder, Storage, StorageBackend as _};
    use futures_util::stream;

    let mut store = Storage {
        encoder: CborEncoder,
        backend: MemoryStorageBackend::default(),
    };

    let head = "test/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let long_x: Attribute = format!("{head}x").parse()?;
    let long_y: Attribute = format!("{head}y").parse()?;
    let shared = "test:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let left: Entity = format!("{shared}left").parse()?;
    let right: Entity = format!("{shared}right").parse()?;

    let version = Version::new(Origin::from([7u8; 32]), Edition::new(0));
    let claim = |of: &Entity, the: &Attribute, value: &str| {
        Instruction::Assert(Artifact {
            the: the.clone(),
            of: of.clone(),
            is: Value::String(value.into()),
            cause: None,
        })
    };

    let mut tree = ArtifactTree::empty();
    let mut delta = Delta::zero();
    tree.apply_versioned(
        &mut store,
        &mut delta,
        Some(version),
        stream::iter(vec![
            claim(&left, &long_x, "left-x"),
            claim(&left, &long_y, "left-y"),
            claim(&right, &long_x, "right-x"),
        ]),
    )
    .await?;
    for (digest, buffer) in delta.flush() {
        store.set(*digest.as_bytes(), buffer.into_vec()).await?;
    }

    let history = TreeHistory::new(tree.clone(), store.clone());
    let x_claims = history.claims_at(&version, &left, &long_x).await?;
    assert_eq!(x_claims.len(), 1, "head-sharing neighbors are filtered out");
    assert_eq!(x_claims[0].is, Value::String("left-x".into()));
    assert_eq!(x_claims[0].the, long_x);

    let y_claims = history.claims_at(&version, &left, &long_y).await?;
    assert_eq!(y_claims.len(), 1);
    assert_eq!(y_claims[0].is, Value::String("left-y".into()));

    let right_claims = history.claims_at(&version, &right, &long_x).await?;
    assert_eq!(right_claims.len(), 1);
    assert_eq!(right_claims[0].is, Value::String("right-x".into()));

    Ok(())
}

/// A [`RevisionRecord`] is bound to its issuer and its slot: the version it
/// was recorded under is derivable from the record's own contents, and the
/// signature covers every other field. Tampering with any of them — or
/// replaying a valid record at another version — fails verification.
#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
async fn it_verifies_signed_revision_records() -> Result<()> {
    use base58::ToBase58 as _;
    use ed25519_dalek::Signer as _;

    let key = signing_key(7);
    let did_key = {
        let mut bytes = vec![0xed, 0x01];
        bytes.extend_from_slice(key.verifying_key().as_bytes());
        format!("did:key:z{}", bytes.to_base58())
    };

    let lineage = Entity::new()?;
    let parent = Version::new(Origin::from([3u8; 32]), Edition::new(4));
    let mut record = RevisionRecord {
        format: super::REVISION_RECORD_FORMAT,
        lineage,
        issuer: did_key.clone(),
        authority: did_key.clone(),
        parents: vec![parent],
        skips: vec![parent],
        signature: Vec::new(),
    };
    record.signature = key.sign(&record.payload()?).to_bytes().to_vec();

    // The derived version reflects the record's own contents: origin from
    // (lineage, issuer), edition from the parents.
    let version = record.version();
    assert_eq!(version.edition, Edition::new(5));
    record.verify(&version)?;

    // A valid record replayed at a different slot is rejected.
    let elsewhere = Version::new(version.origin, Edition::new(9));
    assert!(record.verify(&elsewhere).is_err());

    // Tampering with a signed field breaks the signature.
    let mut tampered = record.clone();
    tampered.skips = Vec::new();
    assert!(matches!(
        tampered.verify(&tampered.version()),
        Err(DialogArtifactsError::InvalidSignature(_))
    ));

    // Reattributing the record to another key changes the derived origin
    // *and* fails the signature; verifying at the reattributed slot still
    // fails because the original issuer's signature does not verify under
    // the new issuer's key.
    let other = signing_key(8);
    let mut reattributed = record.clone();
    reattributed.issuer = {
        let mut bytes = vec![0xed, 0x01];
        bytes.extend_from_slice(other.verifying_key().as_bytes());
        format!("did:key:z{}", bytes.to_base58())
    };
    assert!(reattributed.verify(&reattributed.version()).is_err());

    // An issuer that names no resolvable key cannot vouch for anything.
    let mut unresolvable = record.clone();
    unresolvable.issuer = "did:web:example.com".to_string();
    assert!(unresolvable.verify(&unresolvable.version()).is_err());

    Ok(())
}

/// The durable history reader refuses records that don't vouch for
/// themselves: an unsigned (or badly signed) record planted in the tree at
/// a revision entity errors out of `revision_record`, while a properly
/// signed one is returned.
#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
async fn it_refuses_forged_revision_records_in_the_tree() -> Result<()> {
    use base58::ToBase58 as _;
    use dialog_search_tree::Delta;
    use dialog_storage::{CborEncoder, Storage, StorageBackend as _};
    use ed25519_dalek::Signer as _;

    let mut store = Storage {
        encoder: CborEncoder,
        backend: MemoryStorageBackend::default(),
    };

    let key = signing_key(7);
    let did_key = {
        let mut bytes = vec![0xed, 0x01];
        bytes.extend_from_slice(key.verifying_key().as_bytes());
        format!("did:key:z{}", bytes.to_base58())
    };
    let mut signed = RevisionRecord {
        format: super::REVISION_RECORD_FORMAT,
        lineage: Entity::new()?,
        issuer: did_key,
        authority: "did:web:example.com".to_string(),
        parents: Vec::new(),
        skips: Vec::new(),
        signature: Vec::new(),
    };
    let forged = RevisionRecord {
        lineage: Entity::new()?,
        ..signed.clone()
    };
    signed.signature = key.sign(&signed.payload()?).to_bytes().to_vec();

    let mut tree = ArtifactTree::empty();
    let mut delta = Delta::zero();
    tree.record(&mut store, &mut delta, signed.entries()?)
        .await?;
    tree.record(&mut store, &mut delta, forged.entries()?)
        .await?;
    for (digest, buffer) in delta.flush() {
        store.set(*digest.as_bytes(), buffer.into_vec()).await?;
    }

    let history = TreeHistory::new(tree, store);
    assert_eq!(
        history.revision_record(&signed.version()).await?,
        Some(signed),
        "a properly signed record reads back"
    );
    assert!(
        matches!(
            history.revision_record(&forged.version()).await,
            Err(DialogArtifactsError::InvalidSignature(_))
        ),
        "an unsigned record planted in the tree is refused"
    );

    Ok(())
}

/// `log` lists the revisions reachable from a head, newest first, in
/// reverse topological order — every revision before any of its
/// ancestors, concurrent revisions ordered deterministically. The limit
/// caps the walk from the newest end, and a replication hole truncates
/// the walk instead of failing it.
#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
async fn it_logs_ancestry_newest_first() -> Result<()> {
    let repo = Entity::new()?;
    let alice = signing_key(1);
    let bob = signing_key(2);

    // A diamond with a tip: alice and bob fork from genesis, merge, and
    // alice commits once more on top.
    let genesis = revise(&repo, &alice, &[], 0);
    let a1 = revise(&repo, &alice, &[&genesis], 1);
    let b1 = revise(&repo, &bob, &[&genesis], 2);
    let merge = revise(&repo, &alice, &[&a1, &b1], 3);
    let tip = revise(&repo, &alice, &[&merge], 4);

    let mut history = MemoryHistory::default();
    for revision in [&genesis, &a1, &b1, &merge, &tip] {
        history.record_revision(revision)?;
    }

    let entries = log(&tip.version(), &history, usize::MAX).await?;
    let versions: Vec<_> = entries.iter().map(|(version, _)| *version).collect();
    // The concurrent pair shares an edition; the tie breaks by origin.
    let mut concurrent = [a1.version(), b1.version()];
    concurrent.sort();
    assert_eq!(
        versions,
        vec![
            tip.version(),
            merge.version(),
            concurrent[1],
            concurrent[0],
            genesis.version(),
        ],
        "newest first, every revision before its ancestors"
    );

    // The limit caps the walk from the newest end.
    let top = log(&tip.version(), &history, 2).await?;
    let versions: Vec<_> = top.iter().map(|(version, _)| *version).collect();
    assert_eq!(versions, vec![tip.version(), merge.version()]);

    // A hole where genesis's record should be truncates the walk after
    // everything still reachable.
    let mut sparse = MemoryHistory::default();
    for revision in [&a1, &b1, &merge, &tip] {
        sparse.record_revision(revision)?;
    }
    let entries = log(&tip.version(), &sparse, usize::MAX).await?;
    assert_eq!(
        entries.len(),
        4,
        "a replication hole truncates the walk instead of failing it"
    );

    Ok(())
}

/// A causal verdict between two fixed claims is immutable, so the memo
/// answers repeat questions — in either argument order — without touching
/// the history index again.
#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
async fn it_memoizes_causality_verdicts() -> Result<()> {
    let repo = Entity::new()?;
    let entity = Entity::new()?;
    let alice = signing_key(1);
    let bob = signing_key(2);

    // The conflict-detection scenario: Bob resolves Alice's and his own
    // concurrent writes with a merge claim.
    let genesis = revise(&repo, &bob, &[], 0);
    let a1 = revise(&repo, &alice, &[&genesis], 1);
    let a2 = revise(&repo, &alice, &[&a1], 2);
    let b1 = revise(&repo, &bob, &[&genesis], 3);
    let b3 = revise(&repo, &bob, &[&a2, &b1], 4);

    let alice_first = name_claim(&entity, "Alicia", Cause::genesis());
    let alice_second = name_claim(&entity, "Alice", Cause::from(a1.version()));
    let bob_resolution = name_claim(&entity, "Bob", Cause::new(vec![a2.version(), b1.version()]));

    let mut history = MemoryHistory::default();
    history.record(&a1.version(), alice_first.clone());
    history.record(&a2.version(), alice_second.clone());
    history.record(&b3.version(), bob_resolution.clone());

    // Resolving b3's claim against a1's needs a tier-2 walk: a1 is not in
    // b3's direct cause, so the traversal reads the index.
    let counting = CountingHistory::new(&history);
    let memo = CausalityCache::new();
    assert_eq!(
        memo.causality(
            (&bob_resolution, &b3.version()),
            (&alice_first, &a1.version()),
            &counting
        )
        .await?,
        Causality::Supersedes
    );
    let walked = counting.reads();
    assert!(walked > 0, "the first resolution walks the DAG");

    // Same question again: answered from memory.
    assert_eq!(
        memo.causality(
            (&bob_resolution, &b3.version()),
            (&alice_first, &a1.version()),
            &counting
        )
        .await?,
        Causality::Supersedes
    );
    assert_eq!(counting.reads(), walked, "the memo answers without reads");

    // The mirrored question shares the entry, reoriented.
    assert_eq!(
        memo.causality(
            (&alice_first, &a1.version()),
            (&bob_resolution, &b3.version()),
            &counting
        )
        .await?,
        Causality::Superseded
    );
    assert_eq!(
        counting.reads(),
        walked,
        "the inverse also comes from memory"
    );

    Ok(())
}

/// Common ancestry between two fixed revisions is likewise immutable —
/// including the "no shared history" outcome — so both memoize.
#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
async fn it_memoizes_common_ancestors() -> Result<()> {
    let repo = Entity::new()?;
    let alice = signing_key(1);
    let bob = signing_key(2);

    let genesis = revise(&repo, &bob, &[], 0);
    let a1 = revise(&repo, &alice, &[&genesis], 1);
    let a2 = revise(&repo, &alice, &[&a1], 2);
    let b1 = revise(&repo, &bob, &[&genesis], 3);

    let mut history = MemoryHistory::default();
    for revision in [&genesis, &a1, &a2, &b1] {
        history.record_revision(revision)?;
    }

    let counting = CountingHistory::new(&history);
    let memo = CausalityCache::new();

    let ancestor = memo
        .common_ancestor(&a2.version(), &b1.version(), &counting)
        .await?;
    assert_eq!(ancestor, Some(genesis.version()));
    let walked = counting.reads();
    assert!(walked > 0, "the first resolution walks the DAG");

    // Repeat and mirrored questions come from memory.
    assert_eq!(
        memo.common_ancestor(&a2.version(), &b1.version(), &counting)
            .await?,
        Some(genesis.version())
    );
    assert_eq!(
        memo.common_ancestor(&b1.version(), &a2.version(), &counting)
            .await?,
        Some(genesis.version())
    );
    assert_eq!(counting.reads(), walked, "the memo answers without reads");

    // Disjoint lineages: `None` is a definitive verdict and memoizes too.
    let elsewhere = Entity::new()?;
    let foreign = revise(&elsewhere, &alice, &[], 0);
    history.record_revision(&foreign)?;
    let counting = CountingHistory::new(&history);

    assert_eq!(
        memo.common_ancestor(&genesis.version(), &foreign.version(), &counting)
            .await?,
        None
    );
    let walked = counting.reads();
    assert_eq!(
        memo.common_ancestor(&foreign.version(), &genesis.version(), &counting)
            .await?,
        None
    );
    assert_eq!(counting.reads(), walked, "no shared history never re-walks");

    Ok(())
}

/// `IncompleteHistory` is the one revisable outcome — replication can
/// complete the chain — so it must never be memoized: after the missing
/// claims arrive, the same cache resolves the definitive verdict.
#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
async fn it_does_not_memoize_incomplete_history() -> Result<()> {
    let repo = Entity::new()?;
    let entity = Entity::new()?;
    let alice = signing_key(1);
    let bob = signing_key(2);
    let carol = signing_key(3);

    let genesis = revise(&repo, &bob, &[], 0);
    let a1 = revise(&repo, &alice, &[&genesis], 1);
    let a2 = revise(&repo, &alice, &[&a1], 2);
    let b1 = revise(&repo, &bob, &[&genesis], 3);
    let b3 = revise(&repo, &bob, &[&a2, &b1], 4);
    let c1 = revise(&repo, &carol, &[&genesis], 5);

    let alice_second = name_claim(&entity, "Alice", Cause::from(a1.version()));
    let bob_resolution = name_claim(&entity, "Bob", Cause::new(vec![a2.version(), b1.version()]));
    let carol_first = name_claim(&entity, "Caroline", Cause::genesis());

    // Alice's claim at a2 has not replicated yet: the walk from b3 toward
    // c1 dead-ends there.
    let mut history = MemoryHistory::default();
    history.record(&b3.version(), bob_resolution.clone());
    history.record(&c1.version(), carol_first.clone());

    let memo = CausalityCache::new();
    assert!(matches!(
        memo.causality(
            (&bob_resolution, &b3.version()),
            (&carol_first, &c1.version()),
            &history
        )
        .await,
        Err(DialogArtifactsError::IncompleteHistory(_))
    ));

    // The missing claim replicates; the same cache now resolves — the
    // error was not remembered as an answer.
    history.record(&a2.version(), alice_second.clone());
    assert_eq!(
        memo.causality(
            (&bob_resolution, &b3.version()),
            (&carol_first, &c1.version()),
            &history
        )
        .await?,
        Causality::Concurrent
    );

    Ok(())
}
