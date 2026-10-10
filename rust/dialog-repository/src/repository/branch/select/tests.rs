//! Branch-level pins of select and write semantics: facts are committed
//! through a branch and read back through branch selects, the production
//! path, rather than through a standalone artifact store.

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

use dialog_effects::blob::{BlobError, Read as BlobRead};
use std::collections::BTreeSet;
use std::pin::pin;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use dialog_artifacts::selector::Constrained;
use dialog_artifacts::{
    Artifact, ArtifactSelector, Changes, DialogArtifactsError, Entity, Exporter, Instruction,
    LoadBlob, NameShape, Relation, Symbol, Update as _, Value, encode_value_owned,
};
use dialog_capability::{Fork, Provider};
use dialog_common::{Blake3Hash, Buffer, ConditionalSync};
use dialog_credentials::Credential;
use dialog_effects::archive::{Get, Put};
use dialog_effects::memory::Resolve;
use dialog_peer::helpers::{generate_data, test_session_with_peer};
use dialog_peer::{Peer, Session};
use dialog_search_tree::{DialogSearchTreeError, LoadBlock, Manifest};
use dialog_storage::provider::storage::VolatileSpace;
use futures_util::{StreamExt as _, TryStreamExt as _, stream};
use parking_lot::Mutex;

use crate::helpers::{Counting, test_repo};
use crate::repository::archive::local::read_all;
use crate::{Branch, HeldCaches, Hydrate, LocalIndex, RemoteSite, Repository, Revision};

/// The session every test commits and reads through.
type Operator = Peer<VolatileSpace, Session>;

/// A fact with no cause, as an application writes it.
fn fact(the: &str, of: Entity, is: Value) -> Result<Artifact> {
    Ok(Artifact {
        the: the.parse()?,
        of,
        is,
        cause: None,
    })
}

/// Commits `instructions` to `branch` as one batch.
async fn commit(
    branch: &Branch,
    instructions: Vec<Instruction>,
    operator: &Operator,
) -> Result<Revision> {
    Ok(branch
        .commit(stream::iter(instructions))
        .perform(operator)
        .await?)
}

/// Asserts every fact in `facts` on `branch` as one batch.
async fn assert_all(
    branch: &Branch,
    facts: impl IntoIterator<Item = Artifact>,
    operator: &Operator,
) -> Result<Revision> {
    commit(
        branch,
        facts
            .into_iter()
            .map(|artifact| Instruction::Assert(artifact, dialog_artifacts::Pick::All))
            .collect(),
        operator,
    )
    .await
}

/// Every row `selector` matches on `branch`, read through `env`.
async fn select<Env>(
    branch: &Branch,
    selector: ArtifactSelector<Constrained>,
    env: &Env,
) -> Result<Vec<Artifact>>
where
    Env: Provider<BlobRead>
        + Provider<Get>
        + Provider<Put>
        + Provider<Resolve>
        + Provider<Hydrate>
        + Provider<Fork<RemoteSite, Resolve>>
        + ConditionalSync
        + 'static,
{
    Ok(branch
        .claims()
        .select(selector)
        .to_owned()
        .perform(env)
        .await?
        .try_collect::<Vec<Artifact>>()
        .await?)
}

/// The string payloads of `rows`, sorted.
fn strings(rows: &[Artifact]) -> Vec<String> {
    let mut values: Vec<String> = rows
        .iter()
        .map(|row| match &row.is {
            Value::String(string) => string.clone(),
            other => panic!("unexpected non-string value {other:?}"),
        })
        .collect();
    values.sort();
    values
}

/// Writes an effect environment performed: archive puts and imports,
/// memory publishes and blob writes.
fn writes<P>(counting: &Counting<P>) -> u64 {
    [
        "archive::Put",
        "archive::Import",
        "memory::Publish",
        "blob::Write",
    ]
    .iter()
    .map(|effect| counting.count(effect))
    .sum()
}

/// Selects through a handle on `main` whose caches start empty (what the
/// environment held for the repository is released first), and reports
/// the rows with the archive block reads and the writes the select cost.
async fn cold_select(
    repo: &Repository<Credential>,
    selector: ArtifactSelector<Constrained>,
    operator: &Operator,
) -> Result<(Vec<Artifact>, u64, u64)> {
    HeldCaches::of(operator).release(&repo.did());
    let branch = repo.branch("main").load().perform(operator).await?;
    let counting = Counting::new(operator.clone());
    let rows = select(&branch, selector, &counting).await?;
    Ok((rows, counting.block_reads(), writes(&counting)))
}

/// The bytes the local archive holds under a spilled value's reference.
async fn spilled_block(
    branch: &Branch,
    value: &Value,
    operator: &Operator,
) -> Result<Option<Vec<u8>>> {
    let reference = Blake3Hash::from(value.to_reference());
    let block = LocalIndex::new(operator, branch.archive().index())
        .load(&reference)
        .await?;
    assert!(
        block.is_none(),
        "a spilled value is written to the blob store, never the block catalog"
    );
    match branch
        .archive()
        .index()
        .archive()
        .blob()
        .read(reference)
        .perform(operator)
        .await
    {
        Ok(reader) => Ok(Some(read_all(reader).await?)),
        Err(BlobError::NotFound(_)) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

/// The inline threshold of a fresh tree.
fn inline_n() -> usize {
    Manifest::default().inline_n as usize
}

/// Serves a line's tree nodes from the local archive but withholds every
/// spilled value, as an archive that lost a spilled block would.
struct WithoutSpills<'a>(LocalIndex<'a, Operator>);

impl Clone for WithoutSpills<'_> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<LoadBlock> for WithoutSpills<'_> {
    async fn execute(&self, load: LoadBlock) -> Result<Option<Buffer>, DialogSearchTreeError> {
        load.perform(&self.0).await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<LoadBlob> for WithoutSpills<'_> {
    async fn execute(&self, _: LoadBlob) -> Result<Option<Buffer>, DialogArtifactsError> {
        Ok(None)
    }
}

/// An [`Exporter`] that keeps every exported row in memory.
#[derive(Clone, Default)]
struct Collect(Arc<Mutex<Vec<Artifact>>>);

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Exporter for Collect {
    async fn write(&mut self, artifact: &Artifact) -> Result<(), DialogArtifactsError> {
        self.0.lock().push(artifact.clone());
        Ok(())
    }

    async fn close(&mut self) -> Result<(), DialogArtifactsError> {
        Ok(())
    }
}

/// A selector that constrains the entity, the attribute and the value
/// pins every component of the index key, so the scan range collapses
/// to a single exact key. Regression guard: the range must be treated
/// inclusively or the entry is unreachable (the old prolly tree papered
/// over this with a point-lookup special case for start == end ranges).
#[dialog_common::test]
async fn it_selects_fully_constrained_artifacts() -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let data = generate_data(4)?;
    let sample = data[0].clone();
    assert_all(&branch, data, &operator).await?;

    let results = select(
        &branch,
        ArtifactSelector::new()
            .of(sample.of.clone())
            .the(sample.the.clone())
            .is(sample.is.clone()),
        &operator,
    )
    .await?;

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].of, sample.of);
    assert_eq!(results[0].the, sample.the);
    assert_eq!(results[0].is, sample.is);
    Ok(())
}

/// Assert + retract of a fact in the same batch, when that fact had no
/// prior committed value, leaves nothing in any of the three fact
/// indexes: the transient-command shape (a concept asserted then
/// retracted in one commit). At branch level the batch still records the
/// retraction in the history region (see
/// `history::tests::it_collapses_a_same_batch_assert_and_retract`), so the
/// revision moves; what this pins is that the fact is reachable through
/// none of the entity, attribute or value orderings, and that the prior
/// fact is untouched.
#[dialog_common::test]
async fn it_leaves_no_key_when_assert_and_retract_a_novel_fact_in_one_batch() -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;

    let alice = fact("user/name", Entity::new()?, Value::String("Alice".into()))?;
    assert_all(&branch, [alice.clone()], &operator).await?;

    let transient = fact(
        "user/session",
        Entity::new()?,
        Value::String("ephemeral".into()),
    )?;
    commit(
        &branch,
        vec![
            Instruction::Assert(transient.clone(), dialog_artifacts::Pick::All),
            Instruction::Retract(transient.clone()),
        ],
        &operator,
    )
    .await?;

    for selector in [
        ArtifactSelector::new().the(transient.the.clone()),
        ArtifactSelector::new().of(transient.of.clone()),
        ArtifactSelector::new().is(transient.is.clone()),
    ] {
        let hits = select(&branch, selector, &operator).await?;
        assert!(hits.is_empty(), "the transient fact left a key: {hits:?}");
    }

    let names = select(&branch, ArtifactSelector::new().the(alice.the), &operator).await?;
    assert_eq!(names.len(), 1, "the prior fact survives the batch");
    assert_eq!(names[0].of, alice.of);
    Ok(())
}

/// An attribute-prefix selector ranges over the AEV index: attribute
/// names are stored raw in the key, so the range is exact.
#[dialog_common::test]
async fn it_selects_by_attribute_prefix() -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let alice = Entity::new()?;

    assert_all(
        &branch,
        [
            fact("person/name", alice.clone(), Value::String("Alice".into()))?,
            fact("person/age", alice.clone(), Value::UnsignedInt(40))?,
            fact("group/name", alice, Value::String("Admins".into()))?,
        ],
        &operator,
    )
    .await?;

    let selected = select(
        &branch,
        ArtifactSelector::new().the_starting_with("person/"),
        &operator,
    )
    .await?;
    assert_eq!(selected.len(), 2, "two person/* facts");
    assert!(
        selected
            .iter()
            .all(|fact| String::from(&fact.the).starts_with("person/")),
        "every selected fact carries the prefix"
    );
    Ok(())
}

/// A domain selector is the typed form of an attribute-prefix scan, and
/// a name filter narrows any selection to one entry per domain. Combined
/// they lower onto an exact attribute.
#[dialog_common::test]
async fn it_selects_by_domain_and_name() -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let alice = Entity::new()?;

    assert_all(
        &branch,
        [
            fact("person/name", alice.clone(), Value::String("Alice".into()))?,
            fact("person/age", alice.clone(), Value::UnsignedInt(40))?,
            fact("group/name", alice.clone(), Value::String("Admins".into()))?,
        ],
        &operator,
    )
    .await?;

    let person: Symbol = "person".parse()?;
    let selected = select(
        &branch,
        ArtifactSelector::new().with_domain(&person),
        &operator,
    )
    .await?;
    assert_eq!(selected.len(), 2, "two entries under the person domain");
    assert!(
        selected.iter().all(|fact| fact.the.domain() == "person"),
        "every selected fact is under the domain"
    );

    // A name filter alone applies across domains (here anchored by the
    // entity to keep the scan constrained).
    let name: Symbol = "name".parse()?;
    let named = select(
        &branch,
        ArtifactSelector::new().of(alice).with_name(name.clone()),
        &operator,
    )
    .await?;
    assert_eq!(named.len(), 2, "person/name and group/name both match");
    assert!(named.iter().all(|fact| fact.the.name() == "name"));

    // Domain plus name tightens to an exact attribute.
    let exact = select(
        &branch,
        ArtifactSelector::new().with_domain(&person).with_name(name),
        &operator,
    )
    .await?;
    assert_eq!(exact.len(), 1, "exactly person/name");
    assert_eq!(String::from(&exact[0].the), "person/name");
    Ok(())
}

/// A domain selection with a name shape yields exactly the matching half
/// of a mixed domain: the scan ranges over the shape's contiguous
/// sub-range (see `it_narrows_domain_ranges_by_name_shape` in
/// `dialog_artifacts::tree`), with attributes of adjacent domains and of
/// the other shape outside it.
#[dialog_common::test]
async fn it_selects_domain_halves_by_name_shape() -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let list = Entity::new()?;

    // A mixed domain (two entries, two ordered members) between two
    // adjacent domains that a sloppy range would sweep up.
    let data = [
        "todo.lisa/name",
        "todo.list/title",
        "todo.list/owner",
        "todo.list/N",
        "todo.list/N5",
        "todo.lists/name",
    ]
    .into_iter()
    .map(|attribute| fact(attribute, list.clone(), Value::String(attribute.into())))
    .collect::<Result<Vec<_>>>()?;
    assert_all(&branch, data, &operator).await?;

    let domain: Symbol = "todo.list".parse()?;
    let members = select(
        &branch,
        ArtifactSelector::new()
            .with_domain(&domain)
            .with_name_shape(NameShape::Position),
        &operator,
    )
    .await?;
    assert_eq!(
        members
            .iter()
            .map(|fact| String::from(&fact.the))
            .collect::<Vec<_>>(),
        vec!["todo.list/N", "todo.list/N5"],
        "the ordered half, in list order"
    );

    let entries = select(
        &branch,
        ArtifactSelector::new()
            .with_domain(&domain)
            .with_name_shape(NameShape::Symbol),
        &operator,
    )
    .await?;
    assert_eq!(
        entries
            .iter()
            .map(|fact| String::from(&fact.the))
            .collect::<Vec<_>>(),
        vec!["todo.list/owner", "todo.list/title"],
        "the dictionary half, in name order"
    );
    Ok(())
}

/// An entity-prefix selector ranges over the EAV index. The entity key
/// stores only the first 32 URI bytes raw, so a prefix longer than that
/// must be confirmed against the stored datum: the second half of this
/// test diverges two URIs past byte 32 to force that path.
#[dialog_common::test]
async fn it_selects_by_entity_prefix() -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;

    // Short prefixes (within the raw head) discriminate on key bytes
    // alone.
    let urn_alpha: Entity = "urn:alpha:1".parse()?;
    let urn_beta: Entity = "urn:beta:1".parse()?;
    // Long shared head: these two agree for well over 32 bytes and
    // diverge only in the hashed tail of the key.
    let shared = "urn:shared:0000000000000000000000000000:";
    let long_a: Entity = format!("{shared}aaaa").parse()?;
    let long_b: Entity = format!("{shared}bbbb").parse()?;

    assert_all(
        &branch,
        [
            fact(
                "person/name",
                urn_alpha.clone(),
                Value::String("Alpha".into()),
            )?,
            fact("person/name", urn_beta, Value::String("Beta".into()))?,
            fact("person/name", long_a.clone(), Value::String("LongA".into()))?,
            fact("person/name", long_b, Value::String("LongB".into()))?,
        ],
        &operator,
    )
    .await?;

    let selected = select(
        &branch,
        ArtifactSelector::new().of_starting_with("urn:alpha:"),
        &operator,
    )
    .await?;
    assert_eq!(selected.len(), 1, "short prefix selects on key bytes");
    assert_eq!(selected[0].of, urn_alpha);

    let long_prefix = format!("{shared}aaaa");
    assert!(long_prefix.len() > 32, "the prefix outruns the raw head");
    let selected = select(
        &branch,
        ArtifactSelector::new().of_starting_with(long_prefix),
        &operator,
    )
    .await?;
    assert_eq!(
        selected.len(),
        1,
        "the datum re-check discriminates beyond the raw head"
    );
    assert_eq!(selected[0].of, long_a);
    Ok(())
}

/// A value-prefix selector ranges over the VAE index. A string value's
/// bytes are stored inline and order-preservingly in the key, so a prefix
/// scan brackets the value dimension directly and returns exactly the
/// string values beginning with the prefix, across different attributes
/// and entities, and excluding non-string values that cannot carry it.
#[dialog_common::test]
async fn it_selects_by_value_prefix() -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let alice = Entity::new()?;
    let bob = Entity::new()?;

    assert_all(
        &branch,
        [
            fact("person/name", alice.clone(), Value::String("Alice".into()))?,
            fact(
                "person/city",
                alice.clone(),
                Value::String("Albuquerque".into()),
            )?,
            fact("person/name", bob, Value::String("Bob".into()))?,
            // A non-string value that must never match a string prefix.
            fact("person/age", alice, Value::UnsignedInt(40))?,
        ],
        &operator,
    )
    .await?;

    // "Al" spans two attributes (name and city) on the same entity.
    let selected = select(
        &branch,
        ArtifactSelector::new().is_starting_with("Al"),
        &operator,
    )
    .await?;
    assert_eq!(
        strings(&selected),
        vec!["Albuquerque".to_string(), "Alice".into()],
        "the two values beginning with Al"
    );

    // A narrower prefix isolates one value.
    let selected = select(
        &branch,
        ArtifactSelector::new().is_starting_with("Ali"),
        &operator,
    )
    .await?;
    assert_eq!(selected.len(), 1);
    assert_eq!(selected[0].is, Value::String("Alice".into()));

    // A prefix that matches nothing returns nothing.
    let selected = select(
        &branch,
        ArtifactSelector::new().is_starting_with("Zzz"),
        &operator,
    )
    .await?;
    assert!(selected.is_empty(), "no value begins with Zzz");
    Ok(())
}

/// A range predicate is a filter, never a type assertion: values of a
/// different type than the bound simply do not match (no error, no stream
/// abort) and the matching-type values still come back. Degenerate ranges
/// (mismatched bound types, inverted bounds) select nothing, silently.
#[dialog_common::test]
async fn it_treats_type_mismatched_values_as_non_matches_not_errors() -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;

    // One attribute holding a mix of types, all on separate entities.
    let data = [
        Value::UnsignedInt(10),
        Value::UnsignedInt(50),
        Value::String("forty".into()),
        Value::Float(30.0),
        Value::Boolean(true),
    ]
    .into_iter()
    .map(|is| fact("record/field", Entity::new()?, is))
    .collect::<Result<Vec<_>>>()?;
    assert_all(&branch, data, &operator).await?;

    // An unsigned range matches only the unsigned values; the string,
    // float, and boolean neighbors are non-matches, not errors.
    let selected = select(
        &branch,
        ArtifactSelector::new().is_at_least(Value::UnsignedInt(20)),
        &operator,
    )
    .await?;
    assert_eq!(
        selected.len(),
        1,
        "only UnsignedInt(50) is >= 20u: {selected:?}"
    );
    assert_eq!(selected[0].is, Value::UnsignedInt(50));

    // Same over the entity-pinned (EAV) route, where every type of the
    // entity's values passes through the filter.
    let mixed = Entity::new()?;
    assert_all(
        &branch,
        [
            fact("record/field", mixed.clone(), Value::UnsignedInt(40))?,
            fact(
                "record/label",
                mixed.clone(),
                Value::String("labelled".into()),
            )?,
        ],
        &operator,
    )
    .await?;
    let selected = select(
        &branch,
        ArtifactSelector::new()
            .of(mixed)
            .is_at_least(Value::UnsignedInt(20)),
        &operator,
    )
    .await?;
    assert_eq!(
        selected.len(),
        1,
        "the string label is a non-match: {selected:?}"
    );
    assert_eq!(selected[0].is, Value::UnsignedInt(40));

    // Degenerate ranges: mismatched bound types and inverted bounds are
    // empty, not errors.
    let selected = select(
        &branch,
        ArtifactSelector::new()
            .is_at_least(Value::UnsignedInt(20))
            .is_at_most(Value::Float(40.0)),
        &operator,
    )
    .await?;
    assert!(selected.is_empty(), "mixed-type bounds match nothing");

    let selected = select(
        &branch,
        ArtifactSelector::new()
            .is_at_least(Value::UnsignedInt(50))
            .is_at_most(Value::UnsignedInt(20)),
        &operator,
    )
    .await?;
    assert!(selected.is_empty(), "inverted bounds match nothing");
    Ok(())
}

/// Predicates whose answer lies beyond a spilled value's in-key prefix
/// load the block and post-filter: a probe longer than `spill_prefix`
/// distinguishes two large values that share their entire key prefix, in
/// both the matching and non-matching directions, and a long range bound
/// widens its scan edge to the prefix cluster so shared-prefix candidates
/// are not missed.
#[dialog_common::test]
async fn it_post_filters_predicates_beyond_the_spill_prefix() -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;

    let spill_prefix = Manifest::default().spill_prefix as usize;
    // Two large values identical through the whole in-key prefix (and
    // beyond), diverging only deep in the tail.
    let stem = "s".repeat(spill_prefix + 16);
    let apple = format!("{stem}-apple-{}", "x".repeat(inline_n()));
    let zebra = format!("{stem}-zebra-{}", "x".repeat(inline_n()));
    assert_all(
        &branch,
        [
            fact("doc/body", Entity::new()?, Value::String(apple.clone()))?,
            fact("doc/body", Entity::new()?, Value::String(zebra.clone()))?,
        ],
        &operator,
    )
    .await?;

    // A probe longer than the in-key prefix: undecidable from the key,
    // decided by loading the block. Only the matching value comes back.
    let probe = format!("{stem}-apple");
    let selected = select(
        &branch,
        ArtifactSelector::new().is_starting_with(&probe),
        &operator,
    )
    .await?;
    assert_eq!(selected.len(), 1, "the probe distinguishes the tails");
    assert_eq!(selected[0].is, Value::String(apple.clone()));

    // A long lower bound between the two: the scan edge widens to the
    // shared prefix cluster and the post-filter keeps exactly the value
    // above the bound.
    let bound = format!("{stem}-m");
    let selected = select(
        &branch,
        ArtifactSelector::new()
            .the("doc/body".parse()?)
            .is_at_least(Value::String(bound)),
        &operator,
    )
    .await?;
    assert_eq!(
        selected.len(),
        1,
        "the long bound decides beyond the prefix"
    );
    assert_eq!(selected[0].is, Value::String(zebra.clone()));

    // Cardinality-many: both same-prefix values coexist on one entity and
    // both read back (the trailing whole-value hash keeps their keys
    // distinct).
    let both = Entity::new()?;
    assert_all(
        &branch,
        [
            fact("doc/body", both.clone(), Value::String(apple.clone()))?,
            fact("doc/body", both.clone(), Value::String(zebra.clone()))?,
        ],
        &operator,
    )
    .await?;
    let selected = select(&branch, ArtifactSelector::new().of(both), &operator).await?;
    assert_eq!(strings(&selected), {
        let mut expected = vec![apple, zebra];
        expected.sort();
        expected
    });
    Ok(())
}

/// Combining `is_starting_with` with a value bound must intersect the two
/// constraints, not corrupt the scan range. Regression guard: the prefix
/// branch once installed an unterminated String payload in the range key,
/// and the bound branch's rebuild then failed to re-parse it and fell back
/// to `KeyParts::max`, silently discarding every previously set field: the
/// scan started above all real entries and returned nothing.
#[dialog_common::test]
async fn it_composes_value_prefix_with_value_bounds() -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;

    let data = ["a", "al", "am", "an", "z"]
        .into_iter()
        .map(|value| fact("user/name", Entity::new()?, Value::String(value.into())))
        .collect::<Result<Vec<_>>>()?;
    assert_all(&branch, data, &operator).await?;

    let selected = select(
        &branch,
        ArtifactSelector::new()
            .the("user/name".parse()?)
            .is_starting_with("a")
            .is_at_most(Value::String("am".into())),
        &operator,
    )
    .await?;
    assert_eq!(
        strings(&selected),
        vec!["a".to_string(), "al".into(), "am".into()],
        "prefix and bound intersect"
    );

    // The boundary value itself is included exactly when inclusive.
    let selected = select(
        &branch,
        ArtifactSelector::new()
            .the("user/name".parse()?)
            .is_starting_with("a")
            .is_less_than(Value::String("am".into())),
        &operator,
    )
    .await?;
    assert_eq!(selected.len(), 2, "exclusive bound drops the boundary");
    Ok(())
}

/// `-0.0` and `0.0` are numerically equal but encode differently
/// (order-preserving encodings place `-0.0` strictly below `+0.0`), so the
/// scanned range edges must widen to the zero cluster or a stored `-0.0`
/// silently falls below an `is_at_least(0.0)` range start that the
/// semantic filter would have admitted.
#[dialog_common::test]
async fn it_includes_negative_zero_in_zero_bounded_ranges() -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;

    let data = [-0.0f64, 0.0, 1.0, -1.0]
        .into_iter()
        .map(|float| fact("measure/value", Entity::new()?, Value::Float(float)))
        .collect::<Result<Vec<_>>>()?;
    assert_all(&branch, data, &operator).await?;

    let selected = select(
        &branch,
        ArtifactSelector::new().is_at_least(Value::Float(0.0)),
        &operator,
    )
    .await?;
    assert_eq!(
        selected.len(),
        3,
        "-0.0, +0.0 and 1.0 all satisfy >= 0.0: {selected:?}"
    );

    let selected = select(
        &branch,
        ArtifactSelector::new().is_at_most(Value::Float(-0.0)),
        &operator,
    )
    .await?;
    assert_eq!(
        selected.len(),
        3,
        "-1.0, -0.0 and +0.0 all satisfy <= -0.0: {selected:?}"
    );
    Ok(())
}

/// Value predicates treat a spilled value exactly as if it were inline:
/// its key carries the value's leading bytes, so a numeric range still
/// excludes it by type, a string prefix within the in-key prefix matches
/// it, and the entity-scoped (EAV) route agrees with the VAE route. A
/// prefix predicate is also a string predicate: a non-string value whose
/// raw payload bytes happen to begin with the prefix bytes must not match.
#[dialog_common::test]
async fn it_applies_value_predicates_to_spilled_values_as_if_inline() -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let alice = Entity::new()?;
    let bob = Entity::new()?;

    let body = Value::String("Zzz".repeat(inline_n()));
    assert_all(
        &branch,
        [
            fact("person/age", alice.clone(), Value::UnsignedInt(20))?,
            // Spills: well over the inline threshold, and begins with "Zzz".
            fact("doc/body", alice.clone(), body.clone())?,
            // Inline integer whose big-endian payload begins 0x41 0x6C
            // ("Al"), on its own entity so it cannot satisfy alice's
            // numeric range.
            fact(
                "stat/blob",
                bob.clone(),
                Value::UnsignedInt(0x416Cu128 << 112),
            )?,
        ],
        &operator,
    )
    .await?;

    // Entity-pinned numeric range: the spilled string is inside the
    // scanned EAV range but must not satisfy a numeric bound.
    let selected = select(
        &branch,
        ArtifactSelector::new()
            .of(alice.clone())
            .is_at_least(Value::UnsignedInt(30)),
        &operator,
    )
    .await?;
    assert!(
        selected.is_empty(),
        "a spilled string must not satisfy a numeric range: {selected:?}"
    );

    // Entity-pinned prefix: the spilled string begins with the prefix and
    // the answer lies within its in-key prefix, so it matches, decided
    // from the key, with the block fetched only for reconstruction.
    let selected = select(
        &branch,
        ArtifactSelector::new().of(alice).is_starting_with("Zzz"),
        &operator,
    )
    .await?;
    assert_eq!(
        selected.len(),
        1,
        "a spilled string matches a prefix within its in-key prefix"
    );
    assert_eq!(selected[0].is, body, "the spilled value reconstructs fully");

    // The VAE route agrees: the same prefix scan with no entity pin finds
    // the same spilled fact, because spilled strings sort inside the
    // String band by their leading bytes.
    let selected = select(
        &branch,
        ArtifactSelector::new().is_starting_with("Zzz"),
        &operator,
    )
    .await?;
    assert_eq!(
        selected.len(),
        1,
        "the VAE route sees spilled strings in-band"
    );

    // Entity-pinned prefix: an integer whose payload bytes spell the
    // prefix is not a string and must not match.
    let selected = select(
        &branch,
        ArtifactSelector::new().of(bob).is_starting_with("Al"),
        &operator,
    )
    .await?;
    assert!(
        selected.is_empty(),
        "a prefix predicate only matches string values: {selected:?}"
    );
    Ok(())
}

/// A float value's key must round-trip once it accumulates into a shared
/// leaf. Regression guard for a value-tail width bug: `encode_f64` writes
/// 8 bytes but the key parser once claimed 16 for `Float`, so a float key
/// over-read into the following components and split into fewer parts than
/// its schema; every commit that packed such a key into an index leaf then
/// failed. Commit enough float-valued facts to force a leaf, then read
/// them back.
#[dialog_common::test]
async fn it_round_trips_many_float_values() -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;

    // A mix of magnitudes and signs, including a large timestamp-like
    // integer stored as a float (the shape that first tripped this in real
    // data).
    let mut data = (0..400u64)
        .map(|index| {
            fact(
                "measure/value",
                Entity::new()?,
                Value::Float(index as f64 * 1.5 - 100.0),
            )
        })
        .collect::<Result<Vec<_>>>()?;
    data.push(fact(
        "measure/value",
        Entity::new()?,
        Value::Float(1783112056217.0),
    )?);
    let expected = data.len();
    assert_all(&branch, data, &operator).await?;

    let selected = select(
        &branch,
        ArtifactSelector::new().the("measure/value".parse()?),
        &operator,
    )
    .await?;
    assert_eq!(selected.len(), expected, "every float fact reads back");
    assert!(
        selected
            .iter()
            .all(|fact| matches!(fact.is, Value::Float(_))),
        "every value round-trips as a float"
    );
    Ok(())
}

/// Numeric value range scans over the VAE index. Numeric values sort
/// order-preservingly within their type band, so `is_at_least`,
/// `is_at_most`, `is_between` (and their exclusive variants) bracket the
/// value dimension; exclusivity and the type band are enforced by the
/// per-entry re-check.
#[dialog_common::test]
async fn it_selects_by_value_range() -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;

    let mut data = [10u128, 20, 30, 40, 50]
        .into_iter()
        .map(|value| fact("person/age", Entity::new()?, Value::UnsignedInt(value)))
        .collect::<Result<Vec<_>>>()?;
    // A string value on the same attribute must never match a numeric
    // range.
    data.push(fact(
        "person/age",
        Entity::new()?,
        Value::String("not a number".into()),
    )?);
    assert_all(&branch, data, &operator).await?;

    let numbers = |facts: &[Artifact]| -> Vec<u128> {
        let mut out: Vec<u128> = facts
            .iter()
            .map(|fact| match fact.is {
                Value::UnsignedInt(value) => value,
                ref other => panic!("unexpected non-numeric match: {other:?}"),
            })
            .collect();
        out.sort_unstable();
        out
    };

    // Inclusive lower bound.
    let selected = select(
        &branch,
        ArtifactSelector::new().is_at_least(Value::UnsignedInt(30)),
        &operator,
    )
    .await?;
    assert_eq!(numbers(&selected), vec![30, 40, 50], ">= 30");

    // Exclusive lower bound drops the boundary value.
    let selected = select(
        &branch,
        ArtifactSelector::new().is_greater_than(Value::UnsignedInt(30)),
        &operator,
    )
    .await?;
    assert_eq!(numbers(&selected), vec![40, 50], "> 30");

    // Inclusive upper bound.
    let selected = select(
        &branch,
        ArtifactSelector::new().is_at_most(Value::UnsignedInt(20)),
        &operator,
    )
    .await?;
    assert_eq!(numbers(&selected), vec![10, 20], "<= 20");

    // A closed interval.
    let selected = select(
        &branch,
        ArtifactSelector::new().is_between(Value::UnsignedInt(20), Value::UnsignedInt(40)),
        &operator,
    )
    .await?;
    assert_eq!(numbers(&selected), vec![20, 30, 40], "[20, 40]");

    // A range that spans nothing.
    let selected = select(
        &branch,
        ArtifactSelector::new().is_at_least(Value::UnsignedInt(1000)),
        &operator,
    )
    .await?;
    assert!(selected.is_empty(), ">= 1000 matches nothing");
    Ok(())
}

/// A select stream reads the tree of the revision the branch was at when
/// the select was performed: commits that land while the stream is being
/// drained do not leak into it, while a select performed afterwards sees
/// them.
#[dialog_common::test]
async fn it_pins_a_stream_at_the_version_where_iteration_begins() -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let data = generate_data(5)?;
    let entities = data
        .iter()
        .map(|artifact| artifact.of.clone())
        .collect::<BTreeSet<Entity>>();
    assert_all(&branch, data, &operator).await?;

    let stream = branch
        .claims()
        .select(ArtifactSelector::new().the("item/id".parse()?))
        .to_owned()
        .perform(&operator)
        .await?;
    let mut stream = pin!(stream);

    let mut count = 0u128;
    while let Some(artifact) = stream.try_next().await? {
        // Each commit adds a fresh item/id row the pinned stream must not
        // see.
        assert_all(
            &branch,
            [fact(
                "item/id",
                Entity::new()?,
                Value::UnsignedInt(1000 + count),
            )?],
            &operator,
        )
        .await?;
        assert!(entities.contains(&artifact.of));
        count += 1;
    }
    assert_eq!(count, 5, "the stream saw only the rows at its revision");

    let after = select(
        &branch,
        ArtifactSelector::new().the("item/id".parse()?),
        &operator,
    )
    .await?;
    assert_eq!(
        after.len(),
        10,
        "a later select sees the interleaved commits"
    );
    Ok(())
}

/// A lookup by entity and value descends to one leaf of the entity index.
/// The old store-level test pinned an exact block count; branch commits
/// also write history records and a revision record, which reshape the
/// tree, so this pins the property the count stood for: from a cold
/// handle the lookup reads fewer blocks than an attribute-prefix scan
/// spanning several leaves, and a read writes nothing.
#[dialog_common::test]
async fn it_can_query_efficiently_by_entity_and_value() -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let data = generate_data(512)?;
    let name = Value::String("name18".into());
    let entity = data
        .iter()
        .find(|element| element.is == name)
        .map(|element| element.of.clone())
        .expect("the fixture holds name18");
    branch
        .commit(stream::iter(
            data.into_iter()
                .map(|artifact| Instruction::Assert(artifact, dialog_artifacts::Pick::All))
                .collect::<Vec<_>>(),
        ))
        .canonicalize()
        .perform(&operator)
        .await?;

    let (rows, reads, writes) = cold_select(
        &repo,
        ArtifactSelector::new().of(entity.clone()).is(name.clone()),
        &operator,
    )
    .await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(String::from(&rows[0].the), "item/name");
    assert_eq!(rows[0].of, entity);
    assert_eq!(rows[0].is, name);
    assert_eq!(writes, 0, "a read writes nothing");

    // The entity's facts could straddle a leaf boundary, so the scan it is
    // compared against spans two attributes' worth of leaves.
    let (all, scan_reads, _) = cold_select(
        &repo,
        ArtifactSelector::new().the_starting_with("item/"),
        &operator,
    )
    .await?;
    assert_eq!(all.len(), 1024);
    assert!(
        reads < scan_reads,
        "the point lookup read {reads} blocks, the attribute scan {scan_reads}"
    );
    Ok(())
}

/// A lookup by attribute and value descends to one leaf of the value
/// index. Adapted from an exact block count for the same reason as
/// `it_can_query_efficiently_by_entity_and_value`: from a cold handle it
/// reads fewer blocks than an attribute scan, and writes nothing.
#[dialog_common::test]
async fn it_can_query_efficiently_by_attribute_and_value() -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let data = generate_data(512)?;
    let name = Value::String("name18".into());
    let entity = data
        .iter()
        .find(|element| element.is == name)
        .map(|element| element.of.clone())
        .expect("the fixture holds name18");
    branch
        .commit(stream::iter(
            data.into_iter()
                .map(|artifact| Instruction::Assert(artifact, dialog_artifacts::Pick::All))
                .collect::<Vec<_>>(),
        ))
        .canonicalize()
        .perform(&operator)
        .await?;

    let attribute: Relation = "item/name".parse()?;
    let (rows, reads, writes) = cold_select(
        &repo,
        ArtifactSelector::new()
            .the(attribute.clone())
            .is(name.clone()),
        &operator,
    )
    .await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].the, attribute);
    assert_eq!(rows[0].of, entity);
    assert_eq!(rows[0].is, name);
    assert_eq!(writes, 0, "a read writes nothing");

    let (all, scan_reads, _) =
        cold_select(&repo, ArtifactSelector::new().the(attribute), &operator).await?;
    assert_eq!(all.len(), 512);
    assert!(
        reads < scan_reads,
        "the point lookup read {reads} blocks, the attribute scan {scan_reads}"
    );
    Ok(())
}

/// A value-only lookup goes through the value index rather than scanning:
/// from a cold handle it reads fewer blocks than a broad attribute scan
/// over the same data, and neither read writes anything. The old test
/// pinned exact counts (1 and 3) on a store without history records;
/// this pins the ordering those counts demonstrated.
#[dialog_common::test]
async fn it_uses_indexes_to_optimize_reads() -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    branch
        .commit(stream::iter(
            generate_data(512)?
                .into_iter()
                .map(|artifact| Instruction::Assert(artifact, dialog_artifacts::Pick::All))
                .collect::<Vec<_>>(),
        ))
        .canonicalize()
        .perform(&operator)
        .await?;

    let (rows, point_reads, point_writes) = cold_select(
        &repo,
        ArtifactSelector::new().is(Value::String("name64".into())),
        &operator,
    )
    .await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(point_writes, 0);

    let (rows, scan_reads, scan_writes) = cold_select(
        &repo,
        ArtifactSelector::new().the("item/id".parse()?),
        &operator,
    )
    .await?;
    assert_eq!(rows.len(), 512);
    assert_eq!(scan_writes, 0);
    assert!(
        point_reads < scan_reads,
        "the value lookup read {point_reads} blocks, the attribute scan {scan_reads}"
    );
    Ok(())
}

/// A value lookup that matches nothing terminates with no rows rather than
/// hanging or erroring, even when neighboring values exist.
#[dialog_common::test]
async fn it_completes_a_query_when_no_data_matches() -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let data = (0..3)
        .map(|_| fact("test/attribute", Entity::new()?, Value::UnsignedInt(124)))
        .collect::<Result<Vec<_>>>()?;
    assert_all(&branch, data, &operator).await?;

    let results = select(
        &branch,
        ArtifactSelector::new().is(Value::UnsignedInt(123)),
        &operator,
    )
    .await?;
    assert!(results.is_empty());
    Ok(())
}

/// The same value on several entities is several facts: a value lookup
/// returns each entity, across commits and alongside unrelated data.
/// Regression guard for entities not being aggregated in the value index.
#[dialog_common::test]
async fn it_distinguishes_same_value_across_different_entities() -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let data = (0..3)
        .map(|_| fact("test/attribute", Entity::new()?, Value::UnsignedInt(123)))
        .collect::<Result<Vec<_>>>()?;
    assert_all(&branch, data, &operator).await?;
    assert_all(&branch, generate_data(32)?, &operator).await?;

    let entities = select(
        &branch,
        ArtifactSelector::new().is(Value::UnsignedInt(123)),
        &operator,
    )
    .await?
    .into_iter()
    .map(|artifact| artifact.of)
    .collect::<BTreeSet<Entity>>();
    assert_eq!(entities.len(), 3);
    Ok(())
}

/// The canonical form of a fact set does not depend on the order the facts
/// were written in. Commit roots are the buffered form, whose hash depends
/// on op order, so both orders are staged canonicalized from the same
/// unpublished head, where they mint the same version: two separate
/// branches would mint different versions and could not be compared.
#[dialog_common::test]
async fn it_produces_the_same_version_with_different_insertion_order() -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let data = generate_data(32)?;

    let mut forward = Changes::new();
    for artifact in data.iter().cloned() {
        forward.associate(
            artifact.the,
            artifact.of,
            artifact.is,
            dialog_artifacts::Pick::All,
        );
    }
    let mut backward = Changes::new();
    for artifact in data.into_iter().rev() {
        backward.associate(
            artifact.the,
            artifact.of,
            artifact.is,
            dialog_artifacts::Pick::All,
        );
    }

    let one = branch
        .transaction()
        .integrate(forward)
        .commit()
        .canonicalize()
        .perform(&operator)
        .await?;
    let two = branch
        .transaction()
        .integrate(backward)
        .commit()
        .canonicalize()
        .perform(&operator)
        .await?;

    assert_eq!(one.version(), two.version());
    assert_eq!(one.revision().tree, two.revision().tree);
    Ok(())
}

/// A replace supersedes the prior value at (entity, attribute) regardless
/// of the value, leaving exactly the new one.
#[dialog_common::test]
async fn it_can_upsert_facts() -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let entity = Entity::new()?;

    assert_all(
        &branch,
        [fact(
            "test/attribute",
            entity.clone(),
            Value::Boolean(false),
        )?],
        &operator,
    )
    .await?;
    commit(
        &branch,
        vec![Instruction::Assert(
            fact("test/attribute", entity.clone(), Value::Boolean(true))?,
            dialog_artifacts::Pick::Last,
        )],
        &operator,
    )
    .await?;

    let results = select(&branch, ArtifactSelector::new().of(entity), &operator).await?;
    assert_eq!(results.len(), 1);
    assert_eq!(String::from(&results[0].the), "test/attribute");
    assert_eq!(results[0].is, Value::Boolean(true));
    Ok(())
}

/// Re-opening a branch, refreshing it, and committing nothing (an empty
/// batch, or one that only retracts an absent fact) touch no storage:
/// no archive block is put or imported and no memory cell is published.
/// The old test pinned that reloading a store never rewrote its pointer;
/// at branch level the pointer is the revision cell. A real commit through
/// the same counter does write, so the zero is not a miscount.
#[dialog_common::test]
async fn it_avoids_unnecessary_storage_writes() -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let head = assert_all(
        &branch,
        [fact(
            "test/attribute",
            Entity::new()?,
            Value::String("test value".into()),
        )?],
        &operator,
    )
    .await?;

    let counting = Counting::new(operator.clone());
    let reopened = repo.branch("main").load().perform(&counting).await?;
    reopened.refresh(&counting).await?;
    assert_eq!(reopened.revision(), Some(head.clone()));

    let empty = reopened
        .commit(stream::iter(Vec::<Instruction>::new()))
        .perform(&counting)
        .await?;
    let absent = reopened
        .commit(stream::iter(vec![Instruction::Retract(fact(
            "test/attribute",
            Entity::new()?,
            Value::String("never asserted".into()),
        )?)]))
        .perform(&counting)
        .await?;
    assert_eq!(empty, head);
    assert_eq!(absent, head);
    assert_eq!(
        writes(&counting),
        0,
        "no-op work wrote: {:?}",
        counting.snapshot()
    );

    reopened
        .commit(stream::iter(vec![Instruction::Assert(
            fact(
                "test/attribute",
                Entity::new()?,
                Value::String("another value".into()),
            )?,
            dialog_artifacts::Pick::All,
        )]))
        .perform(&counting)
        .await?;
    assert!(counting.count("archive::Import") > 0);
    assert!(counting.count("memory::Publish") > 0);
    Ok(())
}

/// A value larger than the inline threshold spills: its key carries a
/// reference, its bytes land in the archive's blob store (keyed by that
/// reference), and a select reconstructs the exact
/// value by fetching the block. Inline values are unaffected.
#[dialog_common::test]
async fn it_round_trips_a_spilled_value() -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let value = Value::String("s".repeat(inline_n() + 1));

    assert_all(
        &branch,
        [fact("doc/body", Entity::new()?, value.clone())?],
        &operator,
    )
    .await?;

    assert_eq!(
        spilled_block(&branch, &value, &operator).await?,
        Some(value.to_bytes()),
        "spilled value bytes are stored as a blob under the value reference"
    );

    // A cold handle has no spill cache, so the read fetches the block.
    let cold = repo.branch("main").load().perform(&operator).await?;
    let results = select(
        &cold,
        ArtifactSelector::new().the("doc/body".parse()?),
        &operator,
    )
    .await?;
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].is, value, "spilled value reconstructs exactly");
    Ok(())
}

/// The inline threshold is inclusive: a value whose encoded form is
/// exactly `inline_n` bytes stays inline (no block written); one byte
/// larger spills.
#[dialog_common::test]
async fn it_spills_exactly_above_the_threshold() -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;

    // The encoding of a String is the 0x00-escaped bytes plus a
    // terminator; for an all-ASCII string with no NULs that is len + 1. So
    // a string of `inline_n - 1` ASCII bytes encodes to exactly `inline_n`.
    let at = Value::String("a".repeat(inline_n() - 1));
    let over = Value::String("a".repeat(inline_n()));
    assert_eq!(encode_value_owned(&at).len(), inline_n(), "at boundary");
    assert!(
        encode_value_owned(&over).len() > inline_n(),
        "over boundary"
    );

    for (value, should_spill) in [(at, false), (over, true)] {
        let entity = Entity::new()?;
        assert_all(
            &branch,
            [fact("doc/body", entity.clone(), value.clone())?],
            &operator,
        )
        .await?;
        assert_eq!(
            spilled_block(&branch, &value, &operator).await?.is_some(),
            should_spill,
            "spill decision at the exact boundary is inclusive"
        );
        // Either way the value reconstructs.
        let results = select(&branch, ArtifactSelector::new().of(entity), &operator).await?;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].is, value);
    }
    Ok(())
}

/// Every spillable value type round-trips through a spill: String, Bytes
/// (including a `0x00`-escape case) and Record reconstruct exactly. A
/// spilled record once hit an `unimplemented!()` on read.
#[dialog_common::test]
async fn it_spills_and_round_trips_every_value_type() -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let n = inline_n() + 64;
    let values = vec![
        Value::String("s".repeat(n)),
        Value::Bytes(vec![0xABu8; n]),
        // Exercises the 0x00-escape in the encoding.
        Value::Bytes(vec![0u8; n]),
        Value::Record(vec![7u8; n]),
    ];
    for value in values {
        assert!(
            encode_value_owned(&value).len() > inline_n(),
            "value must spill: {value:?}"
        );
        let entity = Entity::new()?;
        assert_all(
            &branch,
            [fact("doc/body", entity.clone(), value.clone())?],
            &operator,
        )
        .await?;
        let cold = repo.branch("main").load().perform(&operator).await?;
        let results = select(&cold, ArtifactSelector::new().of(entity), &operator).await?;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].is, value, "{value:?} round-trips through spill");
    }
    Ok(())
}

/// Replacing a fact whose prior value was spilled supersedes the prior
/// (reconstructed via the block) and leaves exactly the new value,
/// whether the new value is itself spilled or inline.
#[dialog_common::test]
async fn it_replaces_a_spilled_prior() -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let n = inline_n() + 8;
    let spilled_prior = Value::String("p".repeat(n));

    for new_value in [Value::String("r".repeat(n)), Value::String("small".into())] {
        let entity = Entity::new()?;
        for value in [spilled_prior.clone(), new_value.clone()] {
            commit(
                &branch,
                vec![Instruction::Assert(
                    fact("doc/body", entity.clone(), value)?,
                    dialog_artifacts::Pick::Last,
                )],
                &operator,
            )
            .await?;
        }

        let results = select(
            &branch,
            ArtifactSelector::new().of(entity).the("doc/body".parse()?),
            &operator,
        )
        .await?;
        assert_eq!(results.len(), 1, "cardinality-one keeps one value");
        assert_eq!(
            results[0].is, new_value,
            "the new value supersedes the spilled prior"
        );
    }
    Ok(())
}

/// Retracting a fact whose value spilled removes it from the scan, and no
/// fact is returned.
#[dialog_common::test]
async fn it_retracts_a_spilled_fact() -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let entity = Entity::new()?;
    let spilled = fact(
        "doc/body",
        entity.clone(),
        Value::String("t".repeat(inline_n() + 8)),
    )?;

    assert_all(&branch, [spilled.clone()], &operator).await?;
    commit(&branch, vec![Instruction::Retract(spilled)], &operator).await?;

    let results = select(&branch, ArtifactSelector::new().of(entity), &operator).await?;
    assert!(
        results.is_empty(),
        "a retracted spilled fact is not returned"
    );
    Ok(())
}

/// Two facts with the same large value share one content-addressed block:
/// the block is stored under the shared reference, and both facts
/// reconstruct it.
#[dialog_common::test]
async fn it_dedups_a_shared_spilled_block() -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let value = Value::String("d".repeat(inline_n() + 8));

    assert_all(
        &branch,
        [
            fact("doc/body", Entity::new()?, value.clone())?,
            fact("doc/body", Entity::new()?, value.clone())?,
        ],
        &operator,
    )
    .await?;

    assert_eq!(
        spilled_block(&branch, &value, &operator).await?,
        Some(value.to_bytes())
    );
    let results = select(
        &branch,
        ArtifactSelector::new().the("doc/body".parse()?),
        &operator,
    )
    .await?;
    assert_eq!(results.len(), 2, "two facts share one spilled block");
    assert!(results.iter().all(|row| row.is == value));
    Ok(())
}

/// A spilled block missing from the archive surfaces a clean
/// `InvalidValue` error on read, not a panic and not a silently dropped
/// row. The branch's tree nodes are all present; only the spilled value
/// is withheld. The same read through the real archive succeeds.
#[dialog_common::test]
async fn it_errors_when_a_spilled_block_is_missing() -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let value = Value::String("m".repeat(inline_n() + 8));
    assert_all(
        &branch,
        [fact("doc/body", Entity::new()?, value.clone())?],
        &operator,
    )
    .await?;

    // A cold handle: its spill cache does not already hold the value.
    let cold = repo.branch("main").load().perform(&operator).await?;
    let rows: Vec<Result<Artifact, DialogArtifactsError>> = cold
        .claims()
        .select(ArtifactSelector::new().the("doc/body".parse()?))
        .to_owned()
        .execute(WithoutSpills(LocalIndex::new(
            &operator,
            cold.archive().index(),
        )))
        .await?
        .collect()
        .await;
    assert!(
        matches!(
            rows.as_slice(),
            [Err(DialogArtifactsError::InvalidValue(_))]
        ),
        "a missing spilled block is a clean error, got {rows:?}"
    );

    let rows = select(
        &cold,
        ArtifactSelector::new().the("doc/body".parse()?),
        &operator,
    )
    .await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].is, value);
    Ok(())
}

/// A value-equality select on a spilled value returns exactly that fact:
/// the selector's value reference matches the spilled key's reference.
#[dialog_common::test]
async fn it_selects_by_a_spilled_value() -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let n = inline_n() + 8;
    let wanted = Value::String("w".repeat(n));
    let other = Value::String("o".repeat(n));
    for value in [wanted.clone(), other] {
        assert_all(
            &branch,
            [fact("doc/body", Entity::new()?, value)?],
            &operator,
        )
        .await?;
    }

    let results = select(
        &branch,
        ArtifactSelector::new().is(wanted.clone()),
        &operator,
    )
    .await?;
    assert_eq!(
        results.len(),
        1,
        "equality-by-spilled-value returns one fact"
    );
    assert_eq!(results[0].is, wanted);
    Ok(())
}

/// A branch handle that keeps its live spine across commits must mint
/// exactly the revisions a handle that re-opens the root from bytes before
/// every commit mints: same batches, same tree and version after every
/// commit, and identical reads at the end. The fresh handle stages its
/// commit without publishing, from the same head the live handle then
/// publishes onto, so both mint the same version. Each batch is one
/// instruction because a batch's buffered form depends on op order, which
/// a multi-fact transaction does not fix.
#[dialog_common::test]
async fn it_commits_identically_when_the_spine_is_reused() -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let live = repo.branch("main").open().perform(&operator).await?;

    for n in 0..90u32 {
        let mut changes = Changes::new();
        changes.associate(
            "test/value".parse()?,
            format!("entity:00000000-0000-0000-0000-{:012}", n % 40).parse()?,
            Value::String(format!("value {n}")),
            dialog_artifacts::Pick::All,
        );

        // The oracle opens a fresh handle (and therefore reads the root
        // frame from bytes) before every commit, so its spine slot never
        // carries anything.
        let fresh = repo.branch("main").open().perform(&operator).await?;
        let cold = fresh
            .transaction()
            .integrate(changes.clone())
            .commit()
            .perform(&operator)
            .await?
            .revision();
        let published = live
            .transaction()
            .integrate(changes)
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        assert_eq!(
            (published.tree.clone(), published.version()),
            (cold.tree.clone(), cold.version()),
            "spine reuse changed the minted revision at batch {n}"
        );
    }

    let selector = ArtifactSelector::new().the("test/value".parse()?);
    let reused = select(&live, selector.clone(), &operator).await?;
    let fresh = repo.branch("main").load().perform(&operator).await?;
    let reopened = select(&fresh, selector, &operator).await?;
    assert_eq!(strings(&reused), strings(&reopened));
    assert_eq!(reused.len(), 90);
    Ok(())
}

/// Exporting a branch and importing the export into another repository's
/// branch reproduces its application facts. The old test went through CSV
/// and compared revisions; the CSV format round trip is pinned in
/// `dialog-csv`, and two repositories mint different versions, so this
/// exports to memory and compares the facts. Machinery facts (the
/// revision record, under `dialog.`) ride the export but are refused by
/// an application import, so only application facts are re-imported.
#[dialog_common::test]
async fn it_can_export_to_and_import_from_a_branch() -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let source_repo = test_repo(&operator, &profile).await;
    let source = source_repo.branch("main").open().perform(&operator).await?;
    assert_all(&source, generate_data(8)?, &operator).await?;

    let exported = Collect::default();
    source.export(exported.clone()).perform(&operator).await?;
    let rows: Vec<Artifact> = exported
        .0
        .lock()
        .iter()
        .filter(|row| !row.the.as_str().starts_with("dialog."))
        .cloned()
        .collect();
    assert!(!rows.is_empty(), "the export carries the facts");

    let target_repo = test_repo(&operator, &profile).await;
    let target = target_repo.branch("main").open().perform(&operator).await?;
    target
        .import(stream::iter(
            rows.into_iter().map(Ok::<_, DialogArtifactsError>),
        ))
        .perform(&operator)
        .await?;

    let key = |row: &Artifact| (row.of.clone(), String::from(&row.the), row.is.to_utf8());
    let selector = ArtifactSelector::new().the("item/id".parse()?);
    let mut expected: Vec<_> = select(&source, selector.clone(), &operator)
        .await?
        .iter()
        .map(key)
        .collect();
    let mut actual: Vec<_> = select(&target, selector, &operator)
        .await?
        .iter()
        .map(key)
        .collect();
    expected.sort();
    actual.sort();
    assert_eq!(expected.len(), 8);
    assert_eq!(expected, actual);
    Ok(())
}
