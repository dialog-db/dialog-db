//! Tests reproducing the scenarios illustrated in `notes/version-control.md`

use std::str::FromStr;

use anyhow::Result;
use ed25519_dalek::SigningKey;

use crate::{Attribute, DialogArtifactsError, Entity, Value};

use super::{
    Authority, Causality, Cause, Claim, Edition, History, MemoryHistory, Origin, Revision, Version,
    causality, common_ancestor,
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
    let bob_resolution = name_claim(
        &entity,
        "Bob",
        Cause::new(vec![a2.version(), b1.version()]),
    );
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
