//! A claim whose stored value this replica cannot read yet, beside readable
//! claims of its cell.
//!
//! Such a claim can only come from another writer, such as another
//! implementation this replica syncs with: a typed write cannot produce
//! it. Only the value can be bad without moving the claim out of the
//! cell, since a cell is keyed by the entity's and the attribute's bytes,
//! so a bad value is the one kind that stands beside readable claims.
//! Three are pinned here: text that is not UTF-8, an entity value that is
//! not a URI, and an entity value that is a URI but not in its canonical
//! rendering, which reading would have to rewrite.
//!
//! Every read and write treats such a claim as if it did not exist: it
//! takes no part in a cell's election or in any query, a write to its
//! cell lands as it would without it, and a cardinality-one write evicts
//! it, as it replaces any claim of the cell it reads.

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

use anyhow::{Context as _, Result};
use dialog_capability::{Provider, Subject};
use dialog_common::Blake3Hash as NodeHash;
use dialog_common::ConditionalSync;
use dialog_corpus::history::Version;
use dialog_corpus::{
    AttributeKey, Cardinality, Datum, EntityKey, EntityKeyPart, Fact, Instruction, Key,
    KeyViewConstruct, KeyViewMut as _, State, ValueKey,
};
use dialog_effects::storage::Location;
use dialog_peer::helpers::{open_peer, test_storage, unique_name};
use dialog_peer::{Peer, PeerSpace, Session};
use dialog_query::attribute::Relation;
use dialog_query::query::Output as _;
use dialog_query::types::Any;
use dialog_query::{AttributeQuery, Claim, Entity, Term, Value};
use dialog_search_tree::PersistentNode;
use dialog_search_tree::nodes::{Preload, Read};
use dialog_storage::provider::storage::Storage;
use futures_util::{StreamExt as _, stream};

use crate::{Branch, RepositoryExt as _};

/// The entity every test here writes the cell of.
pub(super) const ITEM: &str = "item:1";

/// The version `branch`'s next commit stands at: its head's successor,
/// under the head's origin, which is this replica's.
pub(super) fn next_version(branch: &Branch) -> Result<Version> {
    let head = branch
        .revision()
        .context("the branch has a head")?
        .version();
    Ok(Version::new(head.origin, head.edition.successor()))
}

/// The index entries a commit writes for `fact`, standing at `version`,
/// with `marker` in the value's bytes replaced by `replacement`, which is
/// as long: the keys still say the value's type, and this replica cannot
/// read the value yet.
pub(super) fn undecodable(
    fact: &Fact,
    marker: &[u8],
    replacement: &[u8],
    version: &Version,
) -> Result<Vec<(Key, State<Datum>)>> {
    anyhow::ensure!(marker.len() == replacement.len(), "same length");
    let manifest = dialog_search_tree::Manifest::default();
    let mut datum = Datum::for_fact(fact);
    datum.absorb_versions([version]);
    [
        EntityKey::from_fact(fact, &manifest).into_key(),
        AttributeKey::from_fact(fact, &manifest).into_key(),
        ValueKey::from_fact(fact, &manifest).into_key(),
    ]
    .into_iter()
    .map(|key| {
        let mut bytes = key.as_ref().to_vec();
        let at = bytes
            .windows(marker.len())
            .position(|window| window == marker)
            .context("the key holds the value's bytes")?;
        bytes[at..at + marker.len()].copy_from_slice(replacement);
        Ok((Key::from(bytes), State::Added(datum.clone())))
    })
    .collect()
}

/// `the` of [`ITEM`] as `is`.
pub(super) fn claim(the: &str, is: Value) -> Result<Fact> {
    Ok(Fact {
        the: the.parse()?,
        of: ITEM.parse()?,
        is,
        cause: None,
    })
}

/// What a read of `the` of [`ITEM`] answers on `branch`: the cell's one
/// elected claim under `Cardinality::One`, every claim under `None`.
async fn read<Env>(
    branch: &Branch,
    the: &str,
    cardinality: Option<Cardinality>,
    env: &Env,
) -> Result<Vec<Value>>
where
    Env: crate::ResolveEnv,
{
    let rows: Vec<Claim> = branch
        .query()
        .select(AttributeQuery::new(
            Term::from(the.parse::<Relation>()?),
            Term::from(ITEM.parse::<Entity>()?),
            Term::<Any>::var("is"),
            Term::var("cause"),
            cardinality,
        ))
        .perform(env)
        .try_vec()
        .await?;
    Ok(rows.into_iter().map(|row| row.is().clone()).collect())
}

/// What a cardinality-one read of `the` of [`ITEM`] answers on `branch`.
pub(super) async fn read_one<Env>(branch: &Branch, the: &str, env: &Env) -> Result<Vec<Value>>
where
    Env: crate::ResolveEnv,
{
    read(branch, the, Some(Cardinality::One), env).await
}

/// How many claims of `the` of [`ITEM`] `branch`'s head stores, readable
/// or not: read off the head's tree, as no query sees an unread one.
pub(super) async fn stored<Env>(branch: &Branch, the: &str, operator: &Env) -> Result<usize>
where
    Env: Provider<Read<PersistentNode<Key, State<Datum>>>>
        + Provider<Preload<PersistentNode<Key, State<Datum>>>>
        + ConditionalSync,
{
    let head = branch.revision().context("the branch has a head")?;
    let tree = crate::Index::at(branch.subject(), NodeHash::from(*head.tree.hash()));
    let the: dialog_corpus::Relation = the.parse()?;
    let of: dialog_corpus::Entity = ITEM.parse()?;
    let range = <EntityKey<Key> as KeyViewConstruct>::min()
        .set_entity(EntityKeyPart::from(&of))
        .set_attribute(dialog_corpus::AttributeKeyPart::from(&the))
        .into_key()
        ..=<EntityKey<Key> as KeyViewConstruct>::max()
            .set_entity(EntityKeyPart::from(&of))
            .set_attribute(dialog_corpus::AttributeKeyPart::from(&the))
            .into_key();
    let entries = tree.stream_range(range, operator);
    futures_util::pin_mut!(entries);
    let mut count = 0;
    while let Some(entry) = entries.next().await {
        if let State::Added(_) = entry?.value {
            count += 1;
        }
    }
    Ok(count)
}

/// A claim this replica cannot read yet: `value`, its bytes `marker` replaced by
/// `replacement`.
#[derive(Clone)]
struct Bad {
    value: Value,
    marker: &'static [u8],
    replacement: &'static [u8],
}

/// Text whose bytes are not UTF-8.
fn not_utf8() -> Bad {
    Bad {
        value: Value::String("QZ".into()),
        marker: b"QZ",
        replacement: b"\xff\xfe",
    }
}

/// An entity value that is not a URI.
fn not_a_uri() -> Result<Bad> {
    Ok(Bad {
        value: entity("ref:qq")?,
        marker: b"ref:qq",
        replacement: b"r f:qq",
    })
}

/// An entity value stored as `Ref:qq`, a URI whose canonical rendering
/// is `ref:qq`.
fn not_canonical() -> Result<Bad> {
    Ok(Bad {
        value: entity("ref:qq")?,
        marker: b"ref:qq",
        replacement: b"Ref:qq",
    })
}

fn entity(uri: &str) -> Result<Value> {
    Ok(Value::Entity(uri.parse()?))
}

/// Where the bad claim of a cell stands among its claims, in the order a
/// commit wrote them.
#[derive(Clone, Copy)]
enum Stands {
    /// The readable claim, as a cardinality-one assertion, then the bad
    /// one: the bad claim is the newer.
    Newer,
    /// The bad claim, then the readable one, as a cardinality-many
    /// assertion, which reads no claim of the cell: the readable claim
    /// is the newer.
    Older,
    /// The bad claim alone.
    Alone,
}

/// A branch on `storage` whose cell of `the` holds `readable` and `bad`
/// as `stands` says, and the operator it was written through.
async fn cell<S: PeerSpace>(
    storage: Storage<S>,
    the: &str,
    readable: Value,
    bad: Bad,
    stands: Stands,
) -> Result<(Branch, Peer<S, Session>)> {
    let profile = open_peer(storage, Location::profile(unique_name("undecodable"))).await?;
    let operator = profile
        .session(b"test")
        .space(profile.state())
        .allow(Subject::any())
        .await?;
    let repository = profile
        .space(unique_name("undecodable"))
        .open()
        .perform(&operator)
        .await?;
    let branch = repository.branch("main").open().perform(&operator).await?;
    // A head, so the bad claim's commit has a version to stand at.
    branch
        .commit(stream::iter(vec![Instruction::Assert(
            claim("item/kind", Value::String("box".into()))?,
            Cardinality::One,
        )]))
        .perform(&operator)
        .await?;
    let readable = |cardinality| -> Result<Instruction> {
        Ok(Instruction::Assert(
            claim(the, readable.clone())?,
            cardinality,
        ))
    };
    if let Stands::Newer = stands {
        branch
            .commit(stream::iter(vec![readable(Cardinality::One)?]))
            .perform(&operator)
            .await?;
    }
    branch
        .commit(stream::iter(Vec::<Instruction>::new()))
        .with_entries(undecodable(
            &claim(the, bad.value)?,
            bad.marker,
            bad.replacement,
            &next_version(&branch)?,
        )?)
        .perform(&operator)
        .await?;
    if let Stands::Older = stands {
        branch
            .commit(stream::iter(vec![readable(Cardinality::Many)?]))
            .perform(&operator)
            .await?;
    }
    Ok((branch, operator))
}

/// What a cardinality-one read of a volatile cell of `the`, holding
/// `readable` and `bad` as `stands` says, answers.
async fn answer_of(the: &str, readable: Value, bad: Bad, stands: Stands) -> Result<Vec<Value>> {
    let (branch, operator) = cell(test_storage().await, the, readable, bad, stands).await?;
    read_one(&branch, the, &operator).await
}

/// An entity value that is not a URI, newer than the readable one, takes
/// no part in the election: the cell answers the readable value.
#[dialog_common::test]
async fn a_newer_entity_value_that_is_not_a_uri_is_passed_over() -> Result<()> {
    let answer = answer_of(
        "item/owner",
        entity("user:alice")?,
        not_a_uri()?,
        Stands::Newer,
    )
    .await?;
    assert_eq!(answer, vec![entity("user:alice")?]);
    Ok(())
}

/// An entity value that is not a URI, older than the readable one: the
/// cell answers the readable value.
#[dialog_common::test]
async fn an_older_entity_value_that_is_not_a_uri_is_passed_over() -> Result<()> {
    let answer = answer_of(
        "item/owner",
        entity("user:alice")?,
        not_a_uri()?,
        Stands::Older,
    )
    .await?;
    assert_eq!(answer, vec![entity("user:alice")?]);
    Ok(())
}

/// An entity value stored as `Ref:qq`, newer than the readable one, is
/// passed over: the cell answers the readable value, not `ref:qq`, which
/// no claim of the cell stores (a retraction of `ref:qq` would not reach
/// the stored claim, and a claim of `ref:qq` written beside it would be a
/// second claim reading as the same value).
#[dialog_common::test]
async fn a_newer_entity_value_not_in_canonical_rendering_is_passed_over() -> Result<()> {
    let answer = answer_of(
        "item/owner",
        entity("user:alice")?,
        not_canonical()?,
        Stands::Newer,
    )
    .await?;
    assert_eq!(answer, vec![entity("user:alice")?]);
    Ok(())
}

/// Text that is not UTF-8, newer than the readable text, is passed over:
/// the cell answers the readable text, and the read does not fail.
#[dialog_common::test]
async fn a_newer_text_that_is_not_utf8_is_passed_over() -> Result<()> {
    let answer = answer_of(
        "item/size",
        Value::String("large".into()),
        not_utf8(),
        Stands::Newer,
    )
    .await?;
    assert_eq!(answer, vec![Value::String("large".into())]);
    Ok(())
}

/// Text that is not UTF-8, older than the readable text: the cell answers
/// the readable text.
#[dialog_common::test]
async fn an_older_text_that_is_not_utf8_is_passed_over() -> Result<()> {
    let answer = answer_of(
        "item/size",
        Value::String("large".into()),
        not_utf8(),
        Stands::Older,
    )
    .await?;
    assert_eq!(answer, vec![Value::String("large".into())]);
    Ok(())
}

/// A cell holding only a claim this replica cannot read yet answers nothing, and
/// the read does not fail.
#[dialog_common::test]
async fn a_cell_holding_only_an_unread_claim_answers_nothing() -> Result<()> {
    for bad in [not_utf8(), not_a_uri()?, not_canonical()?] {
        let the = match bad.value {
            Value::String(_) => "item/size",
            _ => "item/owner",
        };
        let answer = answer_of(the, Value::String("unused".into()), bad, Stands::Alone).await?;
        assert_eq!(answer, Vec::<Value>::new(), "{the}");
    }
    Ok(())
}

/// A read of every claim of a cell passes over the one no reader can
/// read, and answers the rest.
#[dialog_common::test]
async fn a_read_of_every_claim_passes_over_an_unread_one() -> Result<()> {
    for bad in [not_utf8(), not_a_uri()?, not_canonical()?] {
        let (the, readable) = match bad.value {
            Value::String(_) => ("item/size", Value::String("large".into())),
            _ => ("item/owner", entity("user:alice")?),
        };
        let (branch, operator) = cell(
            test_storage().await,
            the,
            readable.clone(),
            bad,
            Stands::Newer,
        )
        .await?;
        assert_eq!(read(&branch, the, None, &operator).await?, vec![readable]);
    }
    Ok(())
}

/// A cardinality-one write to a cell on `storage` holding a readable
/// claim and a newer one this replica cannot read yet lands, reads back, and
/// evicts the unread claim with the readable one it replaces: the
/// head stores the written claim alone.
async fn a_cardinality_one_write_evicts_an_unread_claim_on<S: PeerSpace>(
    storage: Storage<S>,
) -> Result<()> {
    let (branch, operator) = cell(
        storage,
        "item/size",
        Value::String("large".into()),
        not_utf8(),
        Stands::Newer,
    )
    .await?;
    assert_eq!(stored(&branch, "item/size", &operator).await?, 2);
    branch
        .commit(stream::iter(vec![Instruction::Assert(
            claim("item/size", Value::String("fine".into()))?,
            Cardinality::One,
        )]))
        .perform(&operator)
        .await?;
    assert_eq!(
        read_one(&branch, "item/size", &operator).await?,
        vec![Value::String("fine".into())]
    );
    assert_eq!(
        stored(&branch, "item/size", &operator).await?,
        1,
        "the write evicts the unread claim"
    );
    Ok(())
}

/// [`a_cardinality_one_write_evicts_an_unread_claim_on`] the volatile
/// provider, on every target.
#[dialog_common::test]
async fn a_cardinality_one_write_evicts_an_unread_claim() -> Result<()> {
    a_cardinality_one_write_evicts_an_unread_claim_on(test_storage().await).await
}

/// [`a_cardinality_one_write_evicts_an_unread_claim_on`] the
/// filesystem provider. Native only: `Storage::temp()` is a directory on
/// disk. On wasm the volatile provider runs it, in
/// [`a_cardinality_one_write_evicts_an_unread_claim`].
#[cfg(not(target_arch = "wasm32"))]
#[dialog_common::test]
async fn a_cardinality_one_write_evicts_an_unread_claim_on_the_filesystem() -> Result<()> {
    use dialog_peer::helpers::test_owned;

    let storage = test_owned(Storage::temp()).await;
    a_cardinality_one_write_evicts_an_unread_claim_on(storage).await
}

/// A cardinality-many write beside a claim this replica cannot read yet lands and
/// reads back, and leaves the claim in place: a cardinality-many write
/// reads no claim of its cell, so it replaces none.
#[dialog_common::test]
async fn a_cardinality_many_write_beside_an_unread_claim_lands() -> Result<()> {
    let (branch, operator) = cell(
        test_storage().await,
        "item/size",
        Value::String("large".into()),
        not_utf8(),
        Stands::Newer,
    )
    .await?;
    branch
        .commit(stream::iter(vec![Instruction::Assert(
            claim("item/size", Value::String("more".into()))?,
            Cardinality::Many,
        )]))
        .perform(&operator)
        .await?;
    assert_eq!(
        read_one(&branch, "item/size", &operator).await?,
        vec![Value::String("more".into())]
    );
    assert_eq!(stored(&branch, "item/size", &operator).await?, 3);
    Ok(())
}

/// A retraction of a readable claim beside one this replica cannot read yet lands:
/// the cell then answers nothing, and still stores the unread claim,
/// which the retraction does not name.
#[dialog_common::test]
async fn a_retraction_beside_an_unread_claim_lands() -> Result<()> {
    let (branch, operator) = cell(
        test_storage().await,
        "item/size",
        Value::String("large".into()),
        not_utf8(),
        Stands::Newer,
    )
    .await?;
    branch
        .commit(stream::iter(vec![Instruction::Retract(claim(
            "item/size",
            Value::String("large".into()),
        )?)]))
        .perform(&operator)
        .await?;
    assert_eq!(
        read_one(&branch, "item/size", &operator).await?,
        Vec::<Value>::new()
    );
    assert_eq!(stored(&branch, "item/size", &operator).await?, 1);
    Ok(())
}
