//! Lower record fields and keyed collections to independent standing feeds.

use dialog_corpus::Value;
use dialog_query::{ConceptConclusion, ConceptQuery, EvaluationError};
use dialog_repository::{ProjectedNode, TreeId, TreeSource};

use super::{Collect, Defined, Rows, Selection, Source};
use crate::{Error, Query, Syntax, data};

/// The local content of one node in a notation result.
#[derive(Clone, Debug, PartialEq)]
pub enum RecordContent {
    /// A map of named fields.
    Record,
    /// A distinct collection of children.
    Set,
    /// One value.
    Scalar(Value),
}

/// A notation tree node's local content and optional field name.
#[derive(Clone, Debug, PartialEq)]
pub struct RecordNode {
    /// The field name when this node is a member of a record.
    pub field: Option<String>,
    /// The node's content.
    pub content: RecordContent,
}

impl RecordNode {
    /// Interpret a local payload from a standing notation selection.
    pub fn decode(value: &Value) -> Result<Self, Error> {
        let invalid = || Error::Refused("invalid notation tree payload".into());
        let items = data::items(value)?.ok_or_else(invalid)?;
        let [Value::String(kind), field, rest @ ..] = items.as_slice() else {
            return Err(invalid());
        };
        let field = match field {
            Value::Boolean(false) => None,
            Value::String(field) => Some(field.clone()),
            _ => return Err(invalid()),
        };
        let content = match (kind.as_str(), rest) {
            ("record", []) => RecordContent::Record,
            ("set", []) => RecordContent::Set,
            ("scalar", [value]) => RecordContent::Scalar(value.clone()),
            _ => return Err(invalid()),
        };
        Ok(Self { field, content })
    }
}

type Feed = (Query, TreeSource<ConceptQuery, Value>);
type Fields = std::collections::BTreeMap<String, Value>;

fn unsupported(reason: impl std::fmt::Display) -> Error {
    Error::Unsupported(format!("tree subscription: {reason}"))
}

fn fault(reason: impl std::fmt::Display) -> EvaluationError {
    EvaluationError::Planning {
        message: reason.to_string(),
    }
}

// A tree feed sends local values of the form [kind, field-or-false,
// value?]. The field labels a record member; set members need no label.
fn local(kind: &str, field: Option<&str>, value: Option<Value>) -> Result<Value, EvaluationError> {
    data::list(
        [
            data::text(kind),
            field.map_or(Value::Boolean(false), data::text),
        ]
        .into_iter()
        .chain(value),
    )
    .map_err(fault)
}

fn keyed(site: usize, value: &Value) -> TreeId {
    use base58::ToBase58 as _;
    format!("{site}/{}", value.identity_key().to_base58()).into()
}

fn field_id(parent: &TreeId, field: &str) -> TreeId {
    format!("{parent}/{}:{field}", field.len()).into()
}

#[derive(Clone)]
struct Read {
    query: Query,
    columns: Vec<String>,
}

impl Read {
    fn values(&self, row: &ConceptConclusion) -> Result<Fields, EvaluationError> {
        let report = self.query.values(row).map_err(fault)?;
        let record = report
            .get(super::ROW_FIELD)
            .map(data::entries)
            .transpose()
            .map_err(fault)?
            .flatten();
        Ok(self
            .columns
            .iter()
            .enumerate()
            .filter_map(|(index, name)| {
                let label = format!("c{index}");
                let value = record
                    .as_ref()
                    .and_then(|record| data::field(record, &label))
                    .or_else(|| report.get(&label))?;
                Some((name.clone(), value.clone()))
            })
            .collect())
    }

    fn feed(
        &self,
        project: impl Fn(&Fields, &mut Vec<ProjectedNode<Value>>) -> Result<(), EvaluationError>
        + Send
        + Sync
        + 'static,
    ) -> Feed {
        let read = self.clone();
        (
            self.query.clone(),
            TreeSource::new(self.query.select.clone(), move |row, out| {
                project(&read.values(row)?, out)
            }),
        )
    }
}

fn variable(term: &Syntax) -> Result<&str, Error> {
    match term {
        Syntax::Variable(name) => Ok(name),
        _ => Err(unsupported(
            "an entity or join key must be a named variable",
        )),
    }
}

fn binding<'a>(fields: &'a Fields, name: &str) -> Result<&'a Value, EvaluationError> {
    fields
        .get(name)
        .ok_or_else(|| fault(format!("tree row did not bind ?{name}")))
}

fn put(
    out: &mut Vec<ProjectedNode<Value>>,
    id: TreeId,
    parent: Option<TreeId>,
    order: Vec<u8>,
    value: Value,
) {
    out.push(ProjectedNode {
        id,
        parent,
        order,
        value,
    });
}

#[derive(Clone)]
struct Anchor {
    site: usize,
    key: Leaf,
    field: Option<String>,
}

impl Anchor {
    fn parent(&self, row: &Fields) -> Result<TreeId, EvaluationError> {
        let owner = keyed(self.site, &self.key.value(row)?);
        Ok(match &self.field {
            Some(field) => field_id(&owner, field),
            None => owner,
        })
    }
}

#[derive(Clone)]
enum Leaf {
    Column(String),
    Constant(Value),
}

impl Leaf {
    fn matches(&self, term: &Syntax) -> bool {
        match (self, term) {
            (Self::Column(column), Syntax::Variable(name)) => column == name,
            (Self::Constant(value), Syntax::Constant(term)) => value == term,
            _ => false,
        }
    }

    fn column(&self) -> Option<&String> {
        match self {
            Self::Column(column) => Some(column),
            Self::Constant(_) => None,
        }
    }

    fn parse(tree: &Syntax, read: &Read) -> Result<Self, Error> {
        match tree {
            Syntax::Variable(name) if read.columns.contains(name) => Ok(Self::Column(name.clone())),
            Syntax::Constant(value) => Ok(Self::Constant(value.clone())),
            _ => Err(unsupported(
                "a record field must be a scalar or a distinct collection",
            )),
        }
    }

    fn value(&self, row: &Fields) -> Result<Value, EvaluationError> {
        match self {
            Self::Column(name) => binding(row, name).cloned(),
            Self::Constant(value) => Ok(value.clone()),
        }
    }
}

struct Lower<'a> {
    selection: &'a Selection,
    feeds: Vec<Feed>,
    next: usize,
}

impl Selection {
    pub(crate) fn tree_feeds(&self) -> Result<Vec<Feed>, Error> {
        let call = self
            .call
            .operand()
            .ok_or_else(|| unsupported("expected a query call"))?;
        let pin = match call.items.as_slice() {
            [Syntax::Constant(Value::Entity(entity))] => Some(Value::Entity(entity.clone())),
            [Syntax::Variable(_) | Syntax::Blank] => None,
            _ => return Err(unsupported("the selected query must have an entity key")),
        };
        if call.terms.len() != 1 || !matches!(call.terms.get("is"), Some(Syntax::Variable(_))) {
            return Err(unsupported("select an entity and an unbound is: field"));
        }
        let name = self
            .call
            .applies()
            .ok_or_else(|| unsupported("expected a named query"))?;
        let mut lower = Lower {
            selection: self,
            feeds: Vec::new(),
            next: 0,
        };
        lower.query(name, None, pin, &mut Vec::new())?;
        Ok(lower.feeds)
    }
}

impl Lower<'_> {
    fn site(&mut self) -> usize {
        let site = self.next;
        self.next += 1;
        site
    }

    fn defined(&self, name: &str) -> Result<&Defined, Error> {
        self.selection
            .defined
            .get(name)
            .ok_or_else(|| unsupported(format!("unknown query {name}")))
    }

    fn read(&self, rows: &Rows) -> Result<Read, Error> {
        let run = self
            .selection
            .one(rows)
            .filter(|_| rows.runs.len() == 1)
            .ok_or_else(|| unsupported("a feed must lower to one concept query"))?;
        let query = self.selection.queries[run].clone();
        if query.asked.is_some() {
            return Err(unsupported("a feed cannot ask a supplied formula"));
        }
        Ok(Read {
            query,
            columns: rows.columns.clone(),
        })
    }

    fn query(
        &mut self,
        name: &str,
        parent: Option<Anchor>,
        pin: Option<Value>,
        trail: &mut Vec<String>,
    ) -> Result<(), Error> {
        if trail.iter().any(|seen| seen == name) {
            return Err(unsupported("recursive tree shape"));
        }
        trail.push(name.to_string());
        let defined = self.defined(name)?.clone();
        if !defined.calls.is_empty() {
            return Err(unsupported("uncollected query calls in a tree feed"));
        }
        let head = defined
            .head
            .operand()
            .ok_or_else(|| unsupported("expected a query head"))?;
        let [subject] = head.items.as_slice() else {
            return Err(unsupported("a feed needs one entity key"));
        };
        let target = variable(
            head.terms
                .get("is")
                .ok_or_else(|| unsupported("expected is:"))?,
        )?;
        let read = self.read(
            defined
                .required
                .as_ref()
                .ok_or_else(|| unsupported("a feed needs a required row"))?,
        )?;
        let subject = Leaf::parse(subject, &read)?;
        if parent
            .as_ref()
            .and_then(|edge| edge.key.column())
            .is_some_and(|column| !read.columns.contains(column))
        {
            return Err(unsupported("a feed must read its entity and parent keys"));
        }
        let site = self.site();
        let mut scalar = None;
        let mut fields = Vec::new();
        let mut collections = Vec::new();
        let mut kind = "scalar";
        if let Some(collect) = defined
            .collects
            .iter()
            .find(|collect| collect.target == target)
        {
            kind = "set";
            collections.push((None, collect));
        } else if let Some((_, shape)) = defined.shapes.iter().find(|(name, _)| name == target) {
            if let Syntax::Map(entries) = shape {
                kind = "record";
                if parent.is_some() && !entries.iter().any(|(_, value)| subject.matches(value)) {
                    return Err(unsupported(
                        "a collected record must include its own entity key",
                    ));
                }
                for (key, value) in entries {
                    let Syntax::Reference(field) = key else {
                        return Err(unsupported("record fields must have literal labels"));
                    };
                    if let Syntax::Variable(name) = value
                        && let Some(collect) = defined
                            .collects
                            .iter()
                            .find(|collect| &collect.target == name)
                    {
                        collections.push((Some(field.clone()), collect));
                    } else {
                        fields.push((field.clone(), Leaf::parse(value, &read)?));
                    }
                }
            } else {
                scalar = Some(Leaf::parse(shape, &read)?);
            }
        } else {
            scalar = Some(Leaf::parse(&Syntax::Variable(target.to_string()), &read)?);
        }
        if parent.is_some() && kind != "record" {
            return Err(unsupported("a collected query must answer a keyed record"));
        }
        let containers: Vec<_> = collections
            .iter()
            .filter_map(|(field, _)| field.clone())
            .collect();
        let own = subject.clone();
        self.feeds.push(read.feed(move |row, out| {
            let key = own.value(row)?;
            if pin.as_ref().is_some_and(|pin| pin != &key) {
                return Ok(());
            }
            let id = keyed(site, &key);
            let parent = parent
                .as_ref()
                .map(|anchor| anchor.parent(row))
                .transpose()?;
            let value = scalar.as_ref().map(|leaf| leaf.value(row)).transpose()?;
            put(
                out,
                id.clone(),
                parent,
                key.identity_key(),
                local(kind, None, value)?,
            );
            for (field, leaf) in &fields {
                put(
                    out,
                    field_id(&id, field),
                    Some(id.clone()),
                    field.as_bytes().to_vec(),
                    local("scalar", Some(field), Some(leaf.value(row)?))?,
                );
            }
            for field in &containers {
                put(
                    out,
                    field_id(&id, field),
                    Some(id.clone()),
                    field.as_bytes().to_vec(),
                    local("set", Some(field), None)?,
                );
            }
            Ok(())
        }));
        for (field, collect) in collections {
            self.collection(
                Anchor {
                    site,
                    key: subject.clone(),
                    field,
                },
                collect,
                trail,
            )?;
        }
        trail.pop();
        Ok(())
    }

    fn collection(
        &mut self,
        mut parent: Anchor,
        collect: &Collect,
        trail: &mut Vec<String>,
    ) -> Result<(), Error> {
        let member = variable(&collect.item)?.to_string();
        match &collect.source {
            Source::Rows(rows) => {
                let read = self.read(rows)?;
                if !read.columns.contains(&member)
                    || parent
                        .key
                        .column()
                        .is_some_and(|column| !read.columns.contains(column))
                    || read
                        .columns
                        .iter()
                        .any(|column| column != &member && parent.key.column() != Some(column))
                {
                    return Err(unsupported(
                        "a collected row must join by the parent entity and select one value",
                    ));
                }
                let site = self.site();
                self.feeds.push(read.feed(move |row, out| {
                    let value = binding(row, &member)?;
                    let owner = parent.parent(row)?;
                    let id = field_id(&owner, &keyed(site, value));
                    put(
                        out,
                        id,
                        Some(owner),
                        value.identity_key(),
                        local("scalar", None, Some(value.clone()))?,
                    );
                    Ok(())
                }));
            }
            Source::Call(call) => {
                let name = call
                    .applies()
                    .ok_or_else(|| unsupported("expected a query call"))?;
                let wanted = call
                    .operand()
                    .ok_or_else(|| unsupported("expected query operands"))?;
                let given = self
                    .defined(name)?
                    .head
                    .operand()
                    .ok_or_else(|| unsupported("expected query head"))?;
                let mut join = None;
                for (key, term) in &wanted.terms {
                    if key == "is" {
                        if !matches!(term, Syntax::Variable(name) if name == &member) {
                            return Err(unsupported("collect the called query's entire is: value"));
                        }
                    } else if parent.key.matches(term) && join.is_none() {
                        let term = given
                            .terms
                            .get(key)
                            .ok_or_else(|| unsupported("unknown parent field"))?;
                        join = Some(variable(term)?.to_string());
                    } else {
                        return Err(unsupported("a collected call may bind one parent field"));
                    }
                }
                if !matches!(wanted.items.as_slice(), [] | [Syntax::Blank]) {
                    return Err(unsupported("collect a query with a blank child entity"));
                }
                parent.key = Leaf::Column(
                    join.ok_or_else(|| unsupported("a collected call needs a parent field"))?,
                );
                self.query(name, Some(parent), None, trail)?;
            }
        }
        Ok(())
    }
}
