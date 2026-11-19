//! Definitions: what a concept, a query and a macro are kept as.
//!
//! A definition is kept compiled: every reference in it resolved and
//! written out where it stands, so what is kept holds no name and
//! nothing to follow. Its entity is made of what is kept, in canonical
//! form, so two spellings of one definition are one entity:
//!
//! | kind | what is kept |
//! |---|---|
//! | concept | its rule about syntax, `(rule (syntax READS) (expand (@ ?t label: ?x …)))` |
//! | macro | its rule about syntax, the template expanded where it names a definition |
//! | query | `(reached QUERY…)`: its analyzed tree and every query it reaches, each named by its entity |
//!
//! Where a definition's own name stands, `@` stands ([`SELF`]): a name
//! is given by a namespace, and a definition is bound under as many as
//! like it. The description is the comment above a form, and is no part
//! of it either.
//!
//! The canonical form is the engine's own labelling of a rule's
//! variables (colour refinement, then individualisation:
//! [`dialog_query::rule::canonical::labelling`]) over the parts of a
//! definition whose order says nothing, which are then sorted by their
//! encoding. Every variable is named `v0`, `v1` and on by it.
//!
//! Beside what is kept, a source map ([`Source`]) says what was written
//! and, for each reference, the name it was written under and the
//! entity it resolved to.

use std::collections::BTreeMap;
use std::fmt::{self, Display, Formatter};
use std::str::FromStr;

use base58::ToBase58;
use dialog_corpus::{Entity, Value};
use dialog_query::concept::descriptor::ConceptStatement;
use dialog_query::rule::canonical::{Atom, labelling};
use dialog_query::{AttributeDescriptor, Attribution};

use crate::Error;
use crate::data;
use crate::pattern::replaced;
use crate::syntax::{Parameters, Syntax};

/// What stands where a definition's own name would.
pub const SELF: &str = "@";

/// The scheme of a definition's entity.
pub const DEFINITION: &str = "definition";

/// The scheme of a name's namespace, apart from a person's `did:`.
pub const NAMESPACE: &str = "namespace";

/// The scheme of a source map's entity.
pub const SOURCE: &str = "source";

/// The scheme of a binding's entity.
pub const BINDING: &str = "binding";

/// The attributes a definition, its source map and a namespace's
/// bindings are kept under.
pub mod attribute {
    /// What a definition is, compiled: a record.
    pub const IS: &str = "syntax.definition/is";
    /// Which kind of definition it is: `concept`, `query` or `macro`.
    pub const KIND: &str = "syntax.definition/kind";
    /// The definition a source map is of.
    pub const SOURCE_DEFINITION: &str = "syntax.source/definition";
    /// The form as written: a record.
    pub const SOURCE_FORM: &str = "syntax.source/form";
    /// Each reference: a record, a list of `{:name … :entity …}`.
    pub const SOURCE_REFERENCES: &str = "syntax.source/references";
    /// The revision the names were resolved at, where one was given.
    pub const SOURCE_REVISION: &str = "syntax.source/revision";
    /// The namespace a binding is in.
    pub const BINDING_NAMESPACE: &str = "syntax.binding/namespace";
    /// The key a binding is under.
    pub const BINDING_KEY: &str = "syntax.binding/key";
    /// What a binding's key is bound to.
    pub const BINDING_REFERENT: &str = "syntax.binding/referent";
}

/// Which kind of thing a definition defines.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Kind {
    /// A concept or a command: a name applied with labels, standing for
    /// reads.
    Concept,
    /// A query: a head that says under `is:` what it answers, and the
    /// conditions it answers by.
    Query,
    /// Any other rule about syntax.
    Macro,
}

impl Kind {
    /// The word the kind is written as.
    pub fn word(self) -> &'static str {
        match self {
            Kind::Concept => "concept",
            Kind::Query => "query",
            Kind::Macro => "macro",
        }
    }
}

impl FromStr for Kind {
    type Err = Error;

    fn from_str(word: &str) -> Result<Self, Error> {
        match word {
            "concept" => Ok(Kind::Concept),
            "query" => Ok(Kind::Query),
            "macro" => Ok(Kind::Macro),
            other => Err(Error::Read(format!("{other} is no kind of definition"))),
        }
    }
}

impl Display for Kind {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.word())
    }
}

/// A definition as it is kept: its kind, what it compiled to, and the
/// entity that is made of the two in canonical form.
///
/// As the engine does with a rule, what is kept is the working
/// spelling, the variables named as written, and what is hashed is the
/// canonical spelling: two spellings of one definition are one entity,
/// and either, kept under it, answers as the other does.
#[derive(Clone, Debug, PartialEq)]
pub struct Definition {
    /// `definition:` and the hash of the canonical form.
    pub entity: Entity,
    /// Its kind.
    pub kind: Kind,
    /// What it compiled to, its own name [`SELF`], its variables as
    /// written.
    pub form: Syntax,
}

impl Definition {
    /// A definition of `kind` that compiled to `form`, whose canonical
    /// form is `canonical`.
    pub fn new(kind: Kind, form: Syntax, canonical: &Syntax) -> Result<Self, Error> {
        let entity = entity(kind, canonical)?;
        Ok(Definition { entity, kind, form })
    }

    /// The hash the entity names, as text: what `name#3fa9` is a prefix
    /// of.
    pub fn hash(&self) -> &str {
        hash_of(&self.entity)
    }
}

/// The hash a definition's entity names: what follows its scheme.
pub fn hash_of(entity: &Entity) -> &str {
    entity
        .as_str()
        .split_once(':')
        .map_or(entity.as_str(), |(_, hash)| hash)
}

/// One reference in a definition: the name it was written under and the
/// entity it resolved to, and inlined as.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Reference {
    /// The name as written: `todo`, `@cdata/todo`, `todo#3fa9`.
    pub name: String,
    /// The definition it resolved to.
    pub entity: Entity,
}

/// What a definition was written as, beside what it is kept as.
#[derive(Clone, Debug, PartialEq)]
pub struct Source {
    /// The form as written.
    pub written: Syntax,
    /// Each reference, in the order met, each once.
    pub references: Vec<Reference>,
    /// The revision its names were resolved at, where the caller gave
    /// one.
    pub revision: Option<String>,
}

/// A definition a document made, and where it is bound.
#[derive(Clone, Debug, PartialEq)]
pub struct Defined {
    /// What is kept.
    pub definition: Definition,
    /// The name it was written under.
    pub name: String,
    /// The key it is bound under in its name's namespace: its kind and
    /// its labels in canonical order, `concept (text: title:)`.
    pub signature: String,
    /// The namespace its name is bound in.
    pub namespace: Entity,
    /// What was written.
    pub source: Source,
}

impl Defined {
    /// The bindings that put it under its name: the name in its
    /// namespace bound to the name's own namespace, and in that the
    /// signature bound to the definition.
    pub fn bindings(&self) -> Result<[Binding; 2], Error> {
        let named = name_space(&self.namespace, &self.name)?;
        Ok([
            Binding {
                namespace: self.namespace.clone(),
                key: self.name.clone(),
                referent: named.clone(),
            },
            Binding {
                namespace: named,
                key: self.signature.clone(),
                referent: self.definition.entity.clone(),
            },
        ])
    }
}

/// A key in a namespace bound to one referent.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Binding {
    /// The namespace.
    pub namespace: Entity,
    /// The key.
    pub key: String,
    /// What it is bound to.
    pub referent: Entity,
}

impl Binding {
    /// The entity the binding is kept under: made of its namespace and
    /// its key, so a key is bound to one referent and binding it again
    /// replaces it.
    pub fn entity(&self) -> Result<Entity, Error> {
        binding_entity(&self.namespace, &self.key)
    }
}

/// The entity of the binding of `key` in `namespace`.
pub fn binding_entity(namespace: &Entity, key: &str) -> Result<Entity, Error> {
    let made = data::list([Value::Entity(namespace.clone()), data::text(key)]);
    schemed(BINDING, &made)
}

/// The namespace the name `key` in `namespace` is bound to: made of the
/// two, under a scheme of its own.
pub fn name_space(namespace: &Entity, key: &str) -> Result<Entity, Error> {
    let made = data::list([Value::Entity(namespace.clone()), data::text(key)]);
    schemed(NAMESPACE, &made)
}

/// Whether an entity is a name's namespace.
pub fn is_name_space(entity: &Entity) -> bool {
    entity.as_str().starts_with(&format!("{NAMESPACE}:"))
}

/// Whether an entity is a definition.
pub fn is_definition(entity: &Entity) -> bool {
    entity.as_str().starts_with(&format!("{DEFINITION}:"))
}

fn schemed(scheme: &str, value: &Value) -> Result<Entity, Error> {
    Entity::from_str(&format!(
        "{scheme}:{}",
        value.reference().as_bytes().to_base58()
    ))
    .map_err(|error| Error::Refused(format!("not an entity: {error}")))
}

/// The entity of a definition of `kind` that compiled to `form`.
pub fn entity(kind: Kind, form: &Syntax) -> Result<Entity, Error> {
    let made = data::list([data::text(kind.word()), form.value()?]);
    schemed(DEFINITION, &made)
}

/// The key a definition is bound under in its name's namespace: its
/// kind, then its labels in canonical order, each with its colon.
pub fn signature(kind: Kind, labels: impl IntoIterator<Item = String>) -> String {
    let mut labels: Vec<String> = labels.into_iter().collect();
    labels.sort();
    labels.dedup();
    let labels: Vec<String> = labels
        .into_iter()
        .map(|label| format!("{label}:"))
        .collect();
    format!("{} ({})", kind.word(), labels.join(" "))
}

/// The labels a signature key holds, and its kind.
pub fn labels_of(signature: &str) -> Option<(Kind, Vec<String>)> {
    let (kind, rest) = signature.split_once(' ')?;
    let inner = rest.strip_prefix('(')?.strip_suffix(')')?;
    let labels = inner
        .split_whitespace()
        .filter_map(|label| label.strip_suffix(':'))
        .map(str::to_string)
        .collect();
    Some((kind.parse().ok()?, labels))
}

/// What hole a variable leaves in a part's shape.
const HOLE: &str = "?";

/// The prefix of a canonical variable's name.
const CANONICAL: &str = "v";

/// Every variable of `tree`, each with where it stands, a path from the
/// part's root.
fn slots(tree: &Syntax, at: &mut String, into: &mut Vec<(String, String)>) {
    let mark = at.len();
    match tree {
        Syntax::Variable(name) => into.push((at.clone(), name.clone())),
        Syntax::Application(operator, operand) => {
            at.push_str(".o");
            slots(operator, at, into);
            at.truncate(mark);
            for (index, item) in operand.items.iter().enumerate() {
                at.push_str(&format!(".{index}"));
                slots(item, at, into);
                at.truncate(mark);
            }
            for (label, term) in &operand.terms {
                at.push_str(&format!(".{label}:"));
                slots(term, at, into);
                at.truncate(mark);
            }
        }
        // A map says the same in any order: an entry is placed by its
        // key's shape.
        Syntax::Map(entries) => {
            for (key, value) in entries {
                let shape = holed(key).to_string();
                at.push_str(&format!(".k[{shape}]"));
                slots(key, at, into);
                at.truncate(mark);
                at.push_str(&format!(".v[{shape}]"));
                slots(value, at, into);
                at.truncate(mark);
            }
        }
        Syntax::Tuple(items) => {
            for (index, item) in items.iter().enumerate() {
                at.push_str(&format!(".{index}"));
                slots(item, at, into);
                at.truncate(mark);
            }
        }
        Syntax::Blank | Syntax::Constant(_) | Syntax::Reference(_) => {}
    }
}

/// `tree` with every variable a hole.
fn holed(tree: &Syntax) -> Syntax {
    let mut names = Vec::new();
    tree.variables(&mut names);
    let holes: BTreeMap<String, Syntax> = names
        .into_iter()
        .map(|name| (name, Syntax::Variable(HOLE.into())))
        .collect();
    replaced(tree, &holes)
}

fn encoded(tree: &Syntax) -> Result<Vec<u8>, Error> {
    Ok(tree.value()?.to_bytes())
}

/// `tree` with its variables renamed.
pub(crate) fn renamed(tree: &Syntax, names: &BTreeMap<String, String>) -> Syntax {
    let given: BTreeMap<String, Syntax> = names
        .iter()
        .map(|(from, to)| (from.clone(), Syntax::Variable(to.clone())))
        .collect();
    replaced(tree, &given)
}

/// The canonical form of a definition's parts: `fixed`, whose order is
/// theirs, and `set`, whose order says nothing. Every variable is
/// renamed by the engine's labelling, and `set` is sorted by encoding.
pub fn canonical(fixed: &[Syntax], set: &[Syntax]) -> Result<(Vec<Syntax>, Vec<Syntax>), Error> {
    let fixed = &fixed
        .iter()
        .map(ordered)
        .collect::<Result<Vec<_>, Error>>()?;
    let set = &set.iter().map(ordered).collect::<Result<Vec<_>, Error>>()?;
    let mut locals = Vec::new();
    for part in fixed.iter().chain(set) {
        part.variables(&mut locals);
    }
    locals.sort();
    locals.dedup();
    let mut atoms = Vec::with_capacity(fixed.len() + set.len());
    for (index, part) in fixed.iter().enumerate() {
        let mut shape = format!("fixed {index} ").into_bytes();
        shape.extend(encoded(&holed(part))?);
        let mut found = Vec::new();
        slots(part, &mut String::new(), &mut found);
        found.sort();
        atoms.push(Atom {
            shape,
            slots: found,
        });
    }
    for part in set {
        let mut shape = b"set ".to_vec();
        shape.extend(encoded(&holed(part))?);
        let mut found = Vec::new();
        slots(part, &mut String::new(), &mut found);
        found.sort();
        atoms.push(Atom {
            shape,
            slots: found,
        });
    }
    let spell = |names: &BTreeMap<String, String>| -> Result<Vec<u8>, Error> {
        let mut bytes = Vec::new();
        for part in fixed {
            let part = encoded(&renamed(part, names))?;
            bytes.extend((part.len() as u64).to_be_bytes());
            bytes.extend(part);
        }
        let mut sorted = set
            .iter()
            .map(|part| encoded(&renamed(part, names)))
            .collect::<Result<Vec<_>, Error>>()?;
        sorted.sort();
        for part in sorted {
            bytes.extend((part.len() as u64).to_be_bytes());
            bytes.extend(part);
        }
        Ok(bytes)
    };
    let name = |classes: &[Vec<String>]| -> BTreeMap<String, String> {
        classes
            .iter()
            .flatten()
            .enumerate()
            .map(|(index, local)| (local.clone(), format!("{CANONICAL}{index}")))
            .collect()
    };
    let names = labelling(&locals, &atoms, name, spell)?;
    let fixed = fixed.iter().map(|part| renamed(part, &names)).collect();
    let mut keyed = set
        .iter()
        .map(|part| {
            let part = renamed(part, &names);
            Ok((encoded(&part)?, part))
        })
        .collect::<Result<Vec<_>, Error>>()?;
    keyed.sort_by(|a, b| a.0.cmp(&b.0));
    Ok((fixed, keyed.into_iter().map(|(_, part)| part).collect()))
}

/// `tree` with every map's entries in the order of their keys'
/// encoding: a map says the same in any order.
fn ordered(tree: &Syntax) -> Result<Syntax, Error> {
    Ok(match tree {
        Syntax::Application(operator, operand) => Syntax::Application(
            Box::new(ordered(operator)?),
            Parameters {
                items: operand
                    .items
                    .iter()
                    .map(ordered)
                    .collect::<Result<_, _>>()?,
                terms: operand
                    .terms
                    .iter()
                    .map(|(label, term)| Ok((label.clone(), ordered(term)?)))
                    .collect::<Result<_, Error>>()?,
            },
        ),
        Syntax::Map(entries) => {
            let mut keyed = entries
                .iter()
                .map(|(key, value)| Ok((encoded(&holed(key))?, ordered(key)?, ordered(value)?)))
                .collect::<Result<Vec<_>, Error>>()?;
            keyed.sort_by(|a, b| a.0.cmp(&b.0));
            Syntax::Map(
                keyed
                    .into_iter()
                    .map(|(_, key, value)| (key, value))
                    .collect(),
            )
        }
        Syntax::Tuple(items) => Syntax::Tuple(items.iter().map(ordered).collect::<Result<_, _>>()?),
        other => other.clone(),
    })
}

/// `tree` with the application of `from` as its operator, wherever it
/// stands, made an application of `to`.
pub(crate) fn named(tree: &Syntax, from: &str, to: &str) -> Syntax {
    match tree {
        Syntax::Reference(name) if name == from => Syntax::Reference(to.to_string()),
        Syntax::Application(operator, operand) => Syntax::Application(
            Box::new(named(operator, from, to)),
            Parameters {
                items: operand
                    .items
                    .iter()
                    .map(|item| named(item, from, to))
                    .collect(),
                terms: operand
                    .terms
                    .iter()
                    .map(|(label, term)| (label.clone(), named(term, from, to)))
                    .collect(),
            },
        ),
        Syntax::Map(entries) => Syntax::Map(
            entries
                .iter()
                .map(|(key, value)| (named(key, from, to), named(value, from, to)))
                .collect(),
        ),
        Syntax::Tuple(items) => {
            Syntax::Tuple(items.iter().map(|item| named(item, from, to)).collect())
        }
        other => other.clone(),
    }
}

/// `tree` with its operator, where it is a reference, `name`.
pub(crate) fn operated(tree: &Syntax, name: &str) -> Syntax {
    match tree {
        Syntax::Application(operator, operand) if matches!(**operator, Syntax::Reference(_)) => {
            Syntax::Application(
                Box::new(Syntax::Reference(name.to_string())),
                operand.clone(),
            )
        }
        other => other.clone(),
    }
}

fn attribute(name: &str, kind: &str) -> Result<AttributeDescriptor, Error> {
    serde_json::from_value(serde_json::json!({ "the": name, "as": kind }))
        .map_err(|error| Error::Refused(format!("{name}: {error}")))
}

fn attributed(name: &str, kind: &str, value: Value) -> Result<Attribution, Error> {
    attribute(name, kind)?
        .resolve(value)
        .map_err(|error| Error::Refused(format!("{name}: {error}")))
}

/// What a definition is kept as on a branch: its form and its kind under
/// its entity.
pub fn definition_statement(definition: &Definition) -> Result<ConceptStatement, Error> {
    Ok(ConceptStatement {
        this: definition.entity.clone(),
        with: vec![
            attributed(attribute::IS, "record:", definition.form.value()?)?,
            attributed(attribute::KIND, "text:", data::text(definition.kind.word()))?,
        ],
    })
}

/// What a binding is kept as on a branch: under the entity its namespace
/// and key make, the three of it, each of cardinality one.
pub fn binding_statement(binding: &Binding) -> Result<ConceptStatement, Error> {
    Ok(ConceptStatement {
        this: binding.entity()?,
        with: vec![
            attributed(
                attribute::BINDING_NAMESPACE,
                "entity:",
                Value::Entity(binding.namespace.clone()),
            )?,
            attributed(attribute::BINDING_KEY, "text:", data::text(&binding.key))?,
            attributed(
                attribute::BINDING_REFERENT,
                "entity:",
                Value::Entity(binding.referent.clone()),
            )?,
        ],
    })
}

/// What taking a binding away retracts: its referent. The definition it
/// was bound to stays.
pub fn unbinding_statement(binding: &Binding) -> Result<ConceptStatement, Error> {
    Ok(ConceptStatement {
        this: binding.entity()?,
        with: vec![attributed(
            attribute::BINDING_REFERENT,
            "entity:",
            Value::Entity(binding.referent.clone()),
        )?],
    })
}

/// What a source map is kept as on a branch.
pub fn source_statement(defined: &Defined) -> Result<ConceptStatement, Error> {
    let references = data::list(
        defined
            .source
            .references
            .iter()
            .map(|reference| {
                data::record([
                    ("name", data::text(&reference.name)),
                    ("entity", Value::Entity(reference.entity.clone())),
                ])
            })
            .collect::<Result<Vec<_>, Error>>()?,
    );
    let written = defined.source.written.value()?;
    let mut with = vec![
        attributed(
            attribute::SOURCE_DEFINITION,
            "entity:",
            Value::Entity(defined.definition.entity.clone()),
        )?,
        attributed(attribute::SOURCE_FORM, "record:", written.clone())?,
        attributed(attribute::SOURCE_REFERENCES, "record:", references.clone())?,
    ];
    if let Some(revision) = &defined.source.revision {
        with.push(attributed(
            attribute::SOURCE_REVISION,
            "text:",
            data::text(revision),
        )?);
    }
    let made = data::list([
        Value::Entity(defined.definition.entity.clone()),
        written,
        references,
    ]);
    Ok(ConceptStatement {
        this: schemed(SOURCE, &made)?,
        with,
    })
}

/// Everything a definition a document made writes: the definition, its
/// source map, and the two bindings that put it under its name.
pub fn statements(defined: &Defined) -> Result<Vec<ConceptStatement>, Error> {
    let mut statements = vec![
        definition_statement(&defined.definition)?,
        source_statement(defined)?,
    ];
    for binding in defined.bindings()? {
        statements.push(binding_statement(&binding)?);
    }
    Ok(statements)
}

/// The rule `Name` is derived by, in the notation: a name in any
/// namespace, bound to a name's namespace whose entries are signatures,
/// each bound to a definition. Its entity is the second binding, so a
/// row is one signature of one name in one namespace.
pub const NAME_RULE: &str = "(rule (syntax.name ?entry namespace: ?namespace is: ?name signature: ?signature definition: ?definition)
  (syntax.binding ?bound namespace: ?namespace key: ?name referent: ?named)
  (syntax.binding ?entry namespace: ?named key: ?signature referent: ?definition))";

/// The local namespace when none is given: an analyzer that is not
/// opened on a branch has no authority to stand for, and binds what it
/// defines here.
pub const MINE: &str = "namespace:mine";

/// One row of `Name`: a name in a namespace, one of its signatures, and
/// the definition bound under it.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Name {
    /// The namespace the name is bound in.
    pub namespace: Entity,
    /// The name.
    pub name: String,
    /// The signature, the key in the name's own namespace.
    pub signature: String,
    /// The definition bound under it.
    pub definition: Entity,
}

/// Every namespace's bindings, and which is mine: what a reference is
/// resolved by.
///
/// A namespace is a key bound to one referent, and nothing else. A name
/// is itself a namespace, whose keys are its signatures, so a name in a
/// namespace is two lookups and `Name` is the walk over every
/// namespace's bindings ([`rows`](Self::rows)), the namespace a field
/// of it. A name in mine bound to another namespace is a petname:
/// `@cdata/todo` walks through what `cdata` in mine is bound to.
#[derive(Clone, Debug, PartialEq)]
pub struct Namespaces {
    mine: Entity,
    bound: BTreeMap<Entity, BTreeMap<String, Entity>>,
}

impl Default for Namespaces {
    fn default() -> Self {
        Namespaces::new(Entity::from_str(MINE).expect("namespace:mine is an entity"))
    }
}

impl Namespaces {
    /// No binding, `mine` the local namespace.
    pub fn new(mine: Entity) -> Self {
        Namespaces {
            mine,
            bound: BTreeMap::new(),
        }
    }

    /// The local namespace: what stands for "you".
    pub fn mine(&self) -> &Entity {
        &self.mine
    }

    /// Bind `binding.key` in `binding.namespace` to its referent, in
    /// place of what it was bound to.
    pub fn bind(&mut self, binding: Binding) {
        self.bound
            .entry(binding.namespace)
            .or_default()
            .insert(binding.key, binding.referent);
    }

    /// Take the binding of `key` in `namespace` away. What it was bound
    /// to stays.
    pub fn unbind(&mut self, namespace: &Entity, key: &str) -> Option<Entity> {
        self.bound.get_mut(namespace)?.remove(key)
    }

    /// What `key` in `namespace` is bound to.
    pub fn get(&self, namespace: &Entity, key: &str) -> Option<&Entity> {
        self.bound.get(namespace)?.get(key)
    }

    /// Every binding.
    pub fn bindings(&self) -> impl Iterator<Item = Binding> + '_ {
        self.bound.iter().flat_map(|(namespace, keys)| {
            keys.iter().map(move |(key, referent)| Binding {
                namespace: namespace.clone(),
                key: key.clone(),
                referent: referent.clone(),
            })
        })
    }

    /// The rows of `Name` for `name` in `namespace`.
    fn named_in(&self, namespace: &Entity, name: &str) -> Vec<Name> {
        let Some(named) = self
            .get(namespace, name)
            .filter(|named| is_name_space(named))
        else {
            return Vec::new();
        };
        self.bound
            .get(named)
            .into_iter()
            .flatten()
            .filter(|(_, definition)| is_definition(definition))
            .map(|(signature, definition)| Name {
                namespace: namespace.clone(),
                name: name.to_string(),
                signature: signature.clone(),
                definition: definition.clone(),
            })
            .collect()
    }

    /// Every row of `Name`: each name in each namespace bound to a
    /// name's namespace, and each of its signatures.
    pub fn rows(&self) -> Vec<Name> {
        let mut rows = Vec::new();
        for (namespace, keys) in &self.bound {
            for name in keys.keys() {
                rows.extend(self.named_in(namespace, name));
            }
        }
        rows
    }

    /// What a reference as written may mean: the rows of `Name` it walks
    /// to. `todo` is every namespace's `todo`; `@cdata/todo` is the
    /// `todo` of what `cdata` in mine is bound to, and `@a/@b/todo`
    /// walks on from there; `todo#3fa9` keeps the rows whose
    /// definition's hash starts so. A petname bound to nothing is
    /// refused as unknown.
    pub fn resolve(&self, written: &str) -> Result<Vec<Name>, Error> {
        let (path, prefix) = match written.split_once('#') {
            Some((path, prefix)) => (path, Some(prefix)),
            None => (written, None),
        };
        let mut rows = match path.strip_prefix('@') {
            None => {
                let mut rows = Vec::new();
                for namespace in self.bound.keys() {
                    rows.extend(self.named_in(namespace, path));
                }
                rows
            }
            Some(qualified) => {
                let mut namespace = self.mine.clone();
                let mut rest = qualified;
                loop {
                    let Some((petname, after)) = rest.split_once('/') else {
                        return Err(Error::Unknown(written.to_string()));
                    };
                    namespace = self
                        .get(&namespace, petname)
                        .cloned()
                        .ok_or_else(|| Error::Unknown(written.to_string()))?;
                    match after.strip_prefix('@') {
                        Some(next) => rest = next,
                        None => break self.named_in(&namespace, after),
                    }
                }
            }
        };
        if let Some(prefix) = prefix {
            rows.retain(|row| hash_of(&row.definition).starts_with(prefix));
        }
        Ok(rows)
    }
}

/// The name a reference applies once it is resolved: `todo` of
/// `@cdata/todo`, `@a/@b/todo` and `todo#3fa9`.
pub fn bare(written: &str) -> &str {
    let path = written.split_once('#').map_or(written, |(path, _)| path);
    let mut rest = path;
    while let Some(qualified) = rest.strip_prefix('@') {
        match qualified.split_once('/') {
            Some((_, after)) => rest = after,
            None => return rest,
        }
    }
    rest
}

/// Whether a reference is qualified: it names a namespace or a hash.
pub fn qualified(written: &str) -> bool {
    written.contains('#') || (written.starts_with('@') && written.len() > 1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Analyzer, check, parse};

    /// The definitions a document makes, each by its name and its entity.
    fn defined(text: &str) -> Vec<(String, Entity)> {
        let mut analyzer = Analyzer::new().unwrap();
        let checked = check(&mut analyzer, text);
        assert!(
            checked.refusals().next().is_none(),
            "{:?}",
            checked.diagnostics
        );
        checked
            .defined
            .into_iter()
            .map(|made| (made.name, made.definition.entity))
            .collect()
    }

    fn entity_of(text: &str) -> Entity {
        let made = defined(text);
        made.last().expect("a definition").1.clone()
    }

    const CONCEPT: &str = "(concept (todo ?t title: ?x done: ?d)
        (xyz.todo/title of: ?t is: ?x as: :text)
        (xyz.todo/done of: ?t is: ?d as: :boolean))";

    /// Two spellings of one concept, its variables named otherwise and
    /// its lines in another order, are one entity; so is the same under
    /// another name, which is no part of it.
    #[test]
    fn it_makes_one_entity_of_two_spellings_of_a_concept() {
        let one = entity_of(CONCEPT);
        let other = entity_of(
            "(concept (todo ?e done: ?b title: ?y)
               (xyz.todo/done of: ?e is: ?b as: :boolean)
               (xyz.todo/title of: ?e is: ?y as: :text))",
        );
        assert_eq!(one, other);
        assert!(one.as_str().starts_with("definition:"));
        let renamed = entity_of(&CONCEPT.replace("(todo ", "(task "));
        assert_eq!(one, renamed, "the name is not hashed");
    }

    /// Other labels are another definition.
    #[test]
    fn it_makes_two_entities_of_two_concepts_of_other_labels() {
        let one = entity_of(CONCEPT);
        let other = entity_of(&CONCEPT.replace("title:", "name:"));
        assert_ne!(one, other);
    }

    /// The comment above a definition is its description, and no part of
    /// what is hashed.
    #[test]
    fn it_leaves_a_definitions_entity_as_it_is_whatever_its_description() {
        let bare = entity_of(CONCEPT);
        let described = entity_of(&format!("; What there is to do.\n{CONCEPT}"));
        assert_eq!(bare, described);
    }

    const QUERY: &str = "(query (todo/card ?t is: ?card)
        (todo ?t title: ?title)
        (xyz.todo ?t tag: ?tag)
        (is ?card {:title ?title :tag ?tag}))";

    /// A query with its conditions in another order and its variables
    /// named otherwise is one entity; other labels are another.
    #[test]
    fn it_makes_one_entity_of_two_spellings_of_a_query() {
        let one = entity_of(&format!("{CONCEPT}\n{QUERY}"));
        let other = entity_of(&format!(
            "{CONCEPT}
             (query (todo/card ?x is: ?made)
               (is ?made {{:tag ?g :title ?y}})
               (xyz.todo ?x tag: ?g)
               (todo ?x title: ?y))"
        ));
        assert_eq!(one, other);
        let labelled = entity_of(&format!(
            "{CONCEPT}\n{}",
            QUERY.replace(
                "(todo/card ?t is: ?card)",
                "(todo/card ?t title: ?title is: ?card)"
            )
        ));
        assert_ne!(one, labelled);
    }

    fn one(text: &str) -> Syntax {
        parse(text).unwrap().remove(0)
    }

    #[test]
    fn it_names_variables_apart_from_their_spelling() -> Result<(), Error> {
        let (fixed, set) = canonical(
            &[one("(q ?t is: ?x)")],
            &[one("(xyz.a ?t b: ?x)"), one("(xyz.c ?t d: ?y)")],
        )?;
        let (other_fixed, other_set) = canonical(
            &[one("(q ?e is: ?w)")],
            &[one("(xyz.c ?e d: ?z)"), one("(xyz.a ?e b: ?w)")],
        )?;
        assert_eq!(fixed, other_fixed);
        assert_eq!(set, other_set);
        Ok(())
    }

    #[test]
    fn it_keys_a_signature_by_its_kind_and_labels_in_order() {
        let key = signature(Kind::Concept, ["title".to_string(), "done".to_string()]);
        assert_eq!(key, "concept (done: title:)");
        assert_eq!(
            labels_of(&key),
            Some((Kind::Concept, vec!["done".into(), "title".into()]))
        );
        assert_eq!(signature(Kind::Query, []), "query ()");
    }
}
