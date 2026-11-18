//! The records measured: revision records a repository wrote, and tonk's
//! view bindings rebuilt from its library, each as dag-cbor (what is
//! stored today) and as pairs.

use super::Seeded;
use super::codec::{Field, Record};
use anyhow::{Context, Result, anyhow};
use dialog_corpus::history::RevisionRecord;
use dialog_corpus::{Entity, Pick, Relation as CorpusRelation, Value};
use dialog_peer::helpers::{test_repo, test_session_with_peer};
use dialog_query::attribute::Relation;
use dialog_query::query::Output as _;
use dialog_query::types::{Any, RecordBytes};
use dialog_query::{
    AttributeStatement, Cardinality, ConceptDescriptor, ConceptQuery, Parameters, Term,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

fn salary(of: &Entity, value: u64, pick: Pick) -> AttributeStatement {
    AttributeStatement {
        the: Relation::from("org/salary".parse::<CorpusRelation>().expect("relation")),
        of: of.clone(),
        is: Value::UnsignedInt(value.into()),
        cause: None,
        cardinality: Some(if pick == Pick::All {
            Cardinality::Many
        } else {
            Cardinality::One
        }),
        pick: Some(pick),
    }
}

/// Replays `steps` steps (seven in ten a commit of one to three writes on
/// one of three branches, the rest a pull of one from another), pulls until
/// the branches agree, and returns every revision record branch `a` holds,
/// as stored.
pub async fn revisions(steps: usize, seed: u64) -> Result<Vec<Vec<u8>>> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let mut branches = Vec::new();
    for name in ["a", "b", "c"] {
        branches.push(repo.branch(name).open().perform(&operator).await?);
    }
    let people: Vec<Entity> = (0..8)
        .map(|i| format!("id:person-{i}").parse())
        .collect::<Result<_, _>>()?;
    let mut random = Seeded(seed);
    for _ in 0..steps {
        let at = random.below(branches.len());
        if random.below(10) < 7 {
            let mut transaction = branches[at].transaction();
            for _ in 0..1 + random.below(3) {
                let person = &people[random.below(people.len())];
                let pick = if random.below(4) == 0 {
                    Pick::All
                } else {
                    Pick::Last
                };
                transaction = transaction.assert(salary(person, 1 + random.below(50) as u64, pick));
            }
            transaction.commit().publish().perform(&operator).await?;
        } else {
            let from = (at + 1 + random.below(branches.len() - 1)) % branches.len();
            if branches[from].revision().is_some() {
                branches[at]
                    .pull()
                    .from(&branches[from])
                    .perform(&operator)
                    .await?;
            }
        }
    }
    for _ in 0..8 {
        let mut changed = false;
        for at in 0..branches.len() {
            for from in 0..branches.len() {
                if at != from && branches[from].revision().is_some() {
                    changed |= branches[at]
                        .pull()
                        .from(&branches[from])
                        .perform(&operator)
                        .await?
                        .is_some();
                }
            }
        }
        if !changed {
            break;
        }
    }
    let predicate: ConceptDescriptor = serde_json::from_value(serde_json::json!({ "with": {
        "record": { "the": "dialog.db/revision", "as": "record:", "pick": "all" }
    }}))?;
    let mut terms = Parameters::new();
    terms.insert("this".into(), Term::<Any>::var("this"));
    terms.insert("record".into(), Term::<Any>::var("record"));
    let rows = branches[0]
        .select(ConceptQuery { predicate, terms })
        .perform(&operator)
        .try_vec()
        .await?;
    rows.iter()
        .map(|row| Ok(row.get::<RecordBytes>("record")?.0))
        .collect()
}

/// How a revision record's versions are written as pairs.
#[derive(Clone, Copy, Debug)]
pub enum Versions {
    /// A version as a nested record of its origin and its edition.
    Records,
    /// A version as its 40 key bytes (edition, then origin), as history
    /// keys hold it.
    Bytes,
}

/// A revision record as pairs, every field kept: parents and skips are
/// sets of versions (a skip list's order is its editions').
pub fn revision_pairs(bytes: &[u8], versions: Versions) -> Result<Record> {
    let record = RevisionRecord::try_from_bytes(bytes).map_err(|e| anyhow!("{e:?}"))?;
    let entity = |text: &str| -> Result<Field> {
        Ok(Field::Value(Value::Entity(
            text.parse().context("an entity")?,
        )))
    };
    let version = |v: &dialog_corpus::history::Version| -> Field {
        match versions {
            Versions::Bytes => Field::Value(Value::Bytes(v.key_bytes().to_vec())),
            Versions::Records => Field::Record(Record(vec![
                (
                    "dialog.version/origin".into(),
                    Field::Value(Value::Bytes(v.origin.key_bytes().to_vec())),
                ),
                (
                    "dialog.version/edition".into(),
                    Field::Value(Value::UnsignedInt(
                        u64::from_be_bytes(v.edition.key_bytes()).into(),
                    )),
                ),
            ])),
        }
    };
    let mut pairs = vec![
        (
            "dialog.revision/format".into(),
            Field::Value(Value::UnsignedInt(record.format.into())),
        ),
        (
            "dialog.revision/branch".into(),
            Field::Value(Value::Entity(record.branch.clone())),
        ),
        ("dialog.revision/issuer".into(), entity(&record.issuer)?),
        (
            "dialog.revision/authority".into(),
            entity(&record.authority)?,
        ),
        (
            "dialog.revision/signature".into(),
            Field::Value(Value::Bytes(record.signature.clone())),
        ),
    ];
    for parent in &record.parents {
        pairs.push(("dialog.revision/parent".into(), version(parent)));
    }
    for skip in &record.skips {
        pairs.push(("dialog.revision/skip".into(), version(skip)));
    }
    Ok(Record(pairs))
}

/// The 40 key bytes of every version a revision record names, as a
/// history key holds them: the file's version tokens.
pub fn version_keys(bytes: &[u8]) -> Result<Vec<Vec<u8>>> {
    let record = RevisionRecord::try_from_bytes(bytes).map_err(|e| anyhow!("{e:?}"))?;
    Ok(record
        .parents
        .iter()
        .chain(&record.skips)
        .map(|v| v.key_bytes().to_vec())
        .chain(std::iter::once(record.version().key_bytes().to_vec()))
        .collect())
}

/// tonk's `Source`, as its serde writes it.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Source {
    Field(String),
    Property(Vec<String>),
    Literal(String),
    Reference(String),
}

/// tonk's `EventDescriptor`, as its serde writes it.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct EventDescriptor {
    pub event_type: String,
    pub prevent_default: bool,
    pub stop_propagation: bool,
    pub sources: BTreeMap<String, Source>,
}

/// tonk's `Bindings`, as its serde writes it.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Bindings {
    pub kind: String,
    pub events: BTreeMap<String, EventDescriptor>,
}

#[derive(Deserialize)]
struct Library {
    views: Vec<Vec<String>>,
    events: BTreeMap<String, EventDescriptor>,
}

/// Each view's bindings, as tonk's analyzer would store them, from the
/// extract of its library.
pub fn bindings(path: &str) -> Result<Vec<Bindings>> {
    let library: Library = serde_json::from_slice(&std::fs::read(path)?)?;
    Ok(library
        .views
        .iter()
        .map(|names| Bindings {
            kind: "tonk/view-bindings@1".into(),
            events: names
                .iter()
                .map(|name| (name.clone(), library.events[name].clone()))
                .collect(),
        })
        .collect())
}

/// A view's bindings as pairs. A table keyed by names known only at run
/// time cannot be fields named by attributes, so each entry is a nested
/// record under a many-valued attribute, its key a field of its own. The
/// `kind` tag goes: the attributes' namespace says what this is.
pub fn binding_pairs(bindings: &Bindings) -> Record {
    let text = |s: &str| Field::Value(Value::String(s.into()));
    let flag = |b: bool| Field::Value(Value::Boolean(b));
    let events = bindings.events.iter().map(|(name, event)| {
        let mut pairs = vec![
            ("tonk.event/name".into(), text(name)),
            ("tonk.event/type".into(), text(&event.event_type)),
            (
                "tonk.event/prevent-default".into(),
                flag(event.prevent_default),
            ),
            (
                "tonk.event/stop-propagation".into(),
                flag(event.stop_propagation),
            ),
        ];
        for (field, source) in &event.sources {
            let mut source_pairs = vec![("tonk.source/field".into(), text(field))];
            match source {
                Source::Field(f) => source_pairs.push(("tonk.source/from-field".into(), text(f))),
                Source::Literal(l) => source_pairs.push(("tonk.source/literal".into(), text(l))),
                Source::Reference(r) => {
                    source_pairs.push(("tonk.source/reference".into(), text(r)))
                }
                // A path is ordered, so it is a string of its segments,
                // as written.
                Source::Property(path) => source_pairs.push((
                    "tonk.source/property".into(),
                    text(&format!(".{}", path.join("."))),
                )),
            }
            pairs.push((
                "tonk.event/source".into(),
                Field::Record(Record(source_pairs)),
            ));
        }
        ("tonk.view/event".to_string(), Field::Record(Record(pairs)))
    });
    Record(events.collect())
}
