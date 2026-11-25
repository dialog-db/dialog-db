//! A claim whose stored value no reader can read, read by a
//! cardinality-one read of its cell.
//!
//! Such a claim can only come from another writer: a typed write cannot
//! produce it. Only the value can be bad without moving the claim out of
//! the cell, since a cell is keyed by the entity's and the attribute's
//! bytes, so a bad value is the one kind that stands beside readable
//! claims. Three are pinned here: text that is not UTF-8, an entity value
//! that is not a URI, and an entity value that is a URI but not in its
//! canonical rendering, which reading would have to rewrite.
//!
//! A cell holding such a claim and a readable one answers what it would
//! answer if the bad claim could not be read at all: the readable claim
//! when it is the newer, and nothing when the bad claim is. It never
//! answers the readable claim the bad one stands over, never answers a
//! value other than the one stored, and never fails the read.

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

use anyhow::{Context as _, Result};
use dialog_corpus::history::Version;
use dialog_corpus::{
    AttributeKey, Cardinality, Datum, EntityKey, Fact, Instruction, Key, State, ValueKey,
};
use dialog_peer::helpers::{test_session_with_peer, unique_name};
use dialog_query::attribute::Relation;
use dialog_query::query::Output as _;
use dialog_query::types::Any;
use dialog_query::{AttributeQuery, Claim, Entity, Term, Value};
use futures_util::stream;

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
/// as long: the keys still say the value's type, and no reader can read
/// the value.
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

/// What a cardinality-one read of `the` of [`ITEM`] answers on `branch`.
pub(super) async fn read_one<Env>(branch: &Branch, the: &str, env: &Env) -> Result<Vec<Value>>
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
            Some(Cardinality::One),
        ))
        .perform(env)
        .try_vec()
        .await?;
    Ok(rows.into_iter().map(|row| row.is().clone()).collect())
}

/// Which of a cell's two claims a commit wrote last.
#[derive(Clone, Copy)]
enum Last {
    /// The readable claim, as a cardinality-one assertion, then the bad
    /// one: the bad claim is the newer.
    Bad,
    /// The bad claim, then the readable one, as a cardinality-many
    /// assertion, which reads no claim of the cell: the readable claim is
    /// the newer.
    Readable,
}

/// A cell of `the` holding the readable claim `readable` and a claim of
/// `bad` with `marker` replaced, written in the order `last` says. What a
/// cardinality-one read of the cell answers.
async fn cell_of(
    the: &str,
    readable: Value,
    bad: Value,
    marker: &[u8],
    replacement: &[u8],
    last: Last,
) -> Result<Vec<Value>> {
    let (operator, profile) = test_session_with_peer().await;
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
    if let Last::Bad = last {
        branch
            .commit(stream::iter(vec![readable(Cardinality::One)?]))
            .perform(&operator)
            .await?;
    }
    branch
        .commit(stream::iter(Vec::<Instruction>::new()))
        .with_entries(undecodable(
            &claim(the, bad)?,
            marker,
            replacement,
            &next_version(&branch)?,
        )?)
        .perform(&operator)
        .await?;
    if let Last::Readable = last {
        branch
            .commit(stream::iter(vec![readable(Cardinality::Many)?]))
            .perform(&operator)
            .await?;
    }
    read_one(&branch, the, &operator).await
}

fn entity(uri: &str) -> Result<Value> {
    Ok(Value::Entity(uri.parse()?))
}

/// An entity value that is not a URI, newer than the readable one: the
/// cell answers nothing, not the readable value the bad one stands over.
#[dialog_common::test]
async fn a_newer_entity_value_that_is_not_a_uri_leaves_its_cell_answering_nothing() -> Result<()> {
    let answer = cell_of(
        "item/owner",
        entity("user:alice")?,
        entity("ref:qq")?,
        b"ref:qq",
        b"r f:qq",
        Last::Bad,
    )
    .await?;
    assert_eq!(answer, Vec::<Value>::new());
    Ok(())
}

/// An entity value that is not a URI, older than the readable one: the
/// cell answers the readable value.
#[dialog_common::test]
async fn an_older_entity_value_that_is_not_a_uri_leaves_its_cell_answering_the_newer() -> Result<()>
{
    let answer = cell_of(
        "item/owner",
        entity("user:alice")?,
        entity("ref:qq")?,
        b"ref:qq",
        b"r f:qq",
        Last::Readable,
    )
    .await?;
    assert_eq!(answer, vec![entity("user:alice")?]);
    Ok(())
}

/// An entity value stored as `Ref:qq`, a URI whose canonical rendering is
/// `ref:qq`, newer than the readable one: the cell answers nothing. It
/// does not answer `ref:qq`, which no claim of the cell stores: a
/// retraction of `ref:qq` would not reach the stored claim, and a claim
/// of `ref:qq` written beside it would be a second claim reading as the
/// same value.
#[dialog_common::test]
async fn a_newer_entity_value_not_in_canonical_rendering_leaves_its_cell_answering_nothing()
-> Result<()> {
    let answer = cell_of(
        "item/owner",
        entity("user:alice")?,
        entity("ref:qq")?,
        b"ref:qq",
        b"Ref:qq",
        Last::Bad,
    )
    .await?;
    assert_eq!(answer, Vec::<Value>::new());
    Ok(())
}

/// Text that is not UTF-8, newer than the readable text: the cell answers
/// nothing, not the readable text the bad one stands over, and the read
/// does not fail.
#[dialog_common::test]
async fn a_newer_text_that_is_not_utf8_leaves_its_cell_answering_nothing() -> Result<()> {
    let answer = cell_of(
        "item/size",
        Value::String("large".into()),
        Value::String("QZ".into()),
        b"QZ",
        b"\xff\xfe",
        Last::Bad,
    )
    .await?;
    assert_eq!(answer, Vec::<Value>::new());
    Ok(())
}

/// Text that is not UTF-8, older than the readable text: the cell answers
/// the readable text.
#[dialog_common::test]
async fn an_older_text_that_is_not_utf8_leaves_its_cell_answering_the_newer() -> Result<()> {
    let answer = cell_of(
        "item/size",
        Value::String("large".into()),
        Value::String("QZ".into()),
        b"QZ",
        b"\xff\xfe",
        Last::Readable,
    )
    .await?;
    assert_eq!(answer, vec![Value::String("large".into())]);
    Ok(())
}
