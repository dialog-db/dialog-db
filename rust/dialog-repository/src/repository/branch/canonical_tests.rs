//! A write refuses an entity this dialog would not read back.
//!
//! Every entity this dialog stores is a URI in its canonical rendering,
//! holding no whitespace or control character: what a read admits as
//! stored. Every way of building an [`Entity`] checks that, save
//! `Uri::stored_unchecked`, which adopts a string as it is, for a string
//! already checked. A write is where such an entity would enter the
//! store, so a write refuses it, as the entity of a claim or as its
//! value, whether it asserts or retracts, and writes nothing.

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

use anyhow::Result;
use dialog_capability::Subject;
use dialog_capability::identity::Uri;
use dialog_corpus::{Cardinality, DialogCorpusError, Entity, Fact, Instruction, Value};
use dialog_effects::storage::Location;
use dialog_peer::PeerSpace;
use dialog_peer::helpers::{open_peer, test_storage, unique_name};
use dialog_storage::provider::storage::Storage;
use futures_util::stream;

use crate::{CommitError, RepositoryExt as _};

/// An entity holding `uri` as it is, unchecked, as a peer that does not
/// check what it builds could hold it.
fn unchecked(uri: &str) -> Result<Entity> {
    Ok(Entity::try_from(Uri::stored_unchecked(uri))?)
}

/// The instructions that write `fact`, each named: an assertion under
/// each cardinality, and a retraction.
fn writes(fact: &Fact) -> [(&'static str, Instruction); 3] {
    [
        (
            "cardinality-many assert",
            Instruction::Assert(fact.clone(), Cardinality::Many),
        ),
        (
            "cardinality-one assert",
            Instruction::Assert(fact.clone(), Cardinality::One),
        ),
        ("retract", Instruction::Retract(fact.clone())),
    ]
}

/// The facts that hold `entity`: as the entity of a claim, and as its
/// value.
fn holding(entity: &Entity) -> Result<[Fact; 2]> {
    Ok([
        Fact {
            the: "item/name".parse()?,
            of: entity.clone(),
            is: Value::String("box".into()),
            cause: None,
        },
        Fact {
            the: "item/owner".parse()?,
            of: "item:1".parse()?,
            is: Value::Entity(entity.clone()),
            cause: None,
        },
    ])
}

/// Every write of an entity stored as `Ref:qq` (a URI whose canonical
/// rendering is `ref:qq`) or as `user:a b` (holding a space), as the
/// entity of a claim or as its value, asserted or retracted, on a branch
/// over `storage`, is refused as an invalid entity, and leaves the branch
/// at the head it had.
async fn refuses_an_uncanonical_entity_on<S: PeerSpace>(storage: Storage<S>) -> Result<()> {
    let profile = open_peer(storage, Location::profile(unique_name("canonical"))).await?;
    let operator = profile
        .session(b"test")
        .space(profile.state())
        .allow(Subject::any())
        .await?;
    let repository = profile
        .space(unique_name("canonical"))
        .open()
        .perform(&operator)
        .await?;
    let branch = repository.branch("main").open().perform(&operator).await?;
    branch
        .commit(stream::iter(vec![Instruction::Assert(
            Fact {
                the: "item/kind".parse()?,
                of: "item:1".parse()?,
                is: Value::String("box".into()),
                cause: None,
            },
            Cardinality::One,
        )]))
        .perform(&operator)
        .await?;
    let head = branch.revision();

    for uri in ["Ref:qq", "user:a b"] {
        for fact in holding(&unchecked(uri)?)? {
            for (write, instruction) in writes(&fact) {
                let result = branch
                    .commit(stream::iter(vec![instruction]))
                    .perform(&operator)
                    .await;
                assert!(
                    matches!(
                        result,
                        Err(CommitError::Fact(DialogCorpusError::InvalidEntity(_)))
                    ),
                    "{uri:?} {write}: expected the entity refused, got {result:?}"
                );
                assert_eq!(
                    branch.revision(),
                    head,
                    "{uri:?} {write}: a refused write moves no head"
                );
            }
        }
    }
    Ok(())
}

/// [`refuses_an_uncanonical_entity_on`] the volatile provider, on every
/// target.
#[dialog_common::test]
async fn a_write_refuses_an_uncanonical_entity() -> Result<()> {
    refuses_an_uncanonical_entity_on(test_storage().await).await
}

/// [`refuses_an_uncanonical_entity_on`] the filesystem provider. Native
/// only: `Storage::temp()` is a directory on disk. On wasm the volatile
/// provider runs it, in [`a_write_refuses_an_uncanonical_entity`].
#[cfg(not(target_arch = "wasm32"))]
#[dialog_common::test]
async fn a_write_refuses_an_uncanonical_entity_on_the_filesystem() -> Result<()> {
    use dialog_peer::helpers::test_owned;

    refuses_an_uncanonical_entity_on(test_owned(Storage::temp()).await).await
}
