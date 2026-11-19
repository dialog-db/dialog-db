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

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{self, Display, Formatter};
use std::str::FromStr;
use std::sync::{Arc, Mutex};

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
    /// A binding's key: its name, a space, its signature. What a name's
    /// bindings are read by, as one range of the value index.
    pub const BINDING_KEY: &str = "syntax.binding/key";
    /// A binding's name.
    pub const BINDING_NAME: &str = "syntax.binding/name";
    /// A binding's signature.
    pub const BINDING_SIGNATURE: &str = "syntax.binding/signature";
    /// The definition a binding is bound to.
    pub const BINDING_DEFINITION: &str = "syntax.binding/definition";
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

/// A definition a document made, and the name it is bound under.
#[derive(Clone, Debug, PartialEq)]
pub struct Defined {
    /// What is kept.
    pub definition: Definition,
    /// The name it was written under: a path, `todo` or `cdata/todo`.
    pub name: String,
    /// Its signature: its kind and its labels in canonical order,
    /// `concept (text: title:)`.
    pub signature: String,
    /// What was written.
    pub source: Source,
}

impl Defined {
    /// The binding that puts it under its name and signature.
    pub fn binding(&self) -> Binding {
        Binding {
            name: self.name.clone(),
            signature: self.signature.clone(),
            definition: self.definition.entity.clone(),
        }
    }
}

/// What separates a name from its signature in a binding's key. A name
/// is a token and never holds whitespace, so a name followed by it is a
/// prefix of no other name's key.
pub const SEPARATOR: char = ' ';

/// A name and signature of the space bound to one definition.
///
/// It is kept as one fact keyed by the whole of name and signature: two
/// signatures of one name are two keys, which is overloading, and one key
/// has one definition.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Binding {
    /// The name, a path.
    pub name: String,
    /// The signature.
    pub signature: String,
    /// The definition it is bound to.
    pub definition: Entity,
}

impl Binding {
    /// Its key: the name, [`SEPARATOR`], the signature.
    pub fn key(&self) -> String {
        key(&self.name, &self.signature)
    }

    /// The entity the binding is kept under: made of its key, so binding
    /// a key again replaces what it was bound to.
    pub fn entity(&self) -> Result<Entity, Error> {
        binding_entity(&self.key())
    }
}

/// The key of `name` under `signature`.
pub fn key(name: &str, signature: &str) -> String {
    format!("{name}{SEPARATOR}{signature}")
}

/// The range every key of `name` begins with, and no key of any other
/// name does.
pub fn range(name: &str) -> String {
    format!("{name}{SEPARATOR}")
}

/// The entity of the binding of `key`.
pub fn binding_entity(key: &str) -> Result<Entity, Error> {
    schemed(BINDING, &data::text(key))
}

/// Whether an entity is a definition.
pub fn is_definition(entity: &Entity) -> bool {
    entity.as_str().starts_with(&format!("{DEFINITION}:"))
}

/// The prefix of the entities of the definitions whose hash starts with
/// `hash`.
pub fn definitions_starting(hash: &str) -> String {
    format!("{DEFINITION}:{hash}")
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

/// What a binding is kept as on a branch: under the entity its key
/// makes, its key, name and signature, and its definition, each of
/// cardinality one.
pub fn binding_statement(binding: &Binding) -> Result<ConceptStatement, Error> {
    Ok(ConceptStatement {
        this: binding.entity()?,
        with: vec![
            attributed(attribute::BINDING_KEY, "text:", data::text(binding.key()))?,
            attributed(attribute::BINDING_NAME, "text:", data::text(&binding.name))?,
            attributed(
                attribute::BINDING_SIGNATURE,
                "text:",
                data::text(&binding.signature),
            )?,
            attributed(
                attribute::BINDING_DEFINITION,
                "entity:",
                Value::Entity(binding.definition.clone()),
            )?,
        ],
    })
}

/// What taking a binding away retracts: its definition. The definition
/// itself stays.
pub fn unbinding_statement(binding: &Binding) -> Result<ConceptStatement, Error> {
    Ok(ConceptStatement {
        this: binding.entity()?,
        with: vec![attributed(
            attribute::BINDING_DEFINITION,
            "entity:",
            Value::Entity(binding.definition.clone()),
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
/// source map, and the binding that puts it under its name.
pub fn statements(defined: &Defined) -> Result<Vec<ConceptStatement>, Error> {
    Ok(vec![
        definition_statement(&defined.definition)?,
        source_statement(defined)?,
        binding_statement(&defined.binding())?,
    ])
}

/// The rule `Name` is derived by, in the notation: each binding of the
/// space, by its name and signature.
pub const NAME_RULE: &str =
    "(rule (syntax.name ?binding is: ?name signature: ?signature definition: ?definition)
  (syntax.binding ?binding name: ?name signature: ?signature definition: ?definition))";

/// One row of `Name`: a name, one of its signatures, and the definition
/// bound under it.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Name {
    /// The name.
    pub name: String,
    /// The signature.
    pub signature: String,
    /// The definition bound under it.
    pub definition: Entity,
}

/// What resolving a name asks of where the bindings and definitions are
/// kept, which a driver reads and answers ([`Names::answer`]).
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Want {
    /// Every binding of a name: one range, [`range`].
    Name(String),
    /// Every binding of every name that begins with a prefix, a path's
    /// for instance: one range too.
    Prefix(String),
    /// Every binding of the space: what an error reads, to say which
    /// other names end as the one not found.
    All,
    /// The definitions whose hash begins so.
    Hash(String),
    /// A definition, by its entity.
    Definition(Entity),
}

/// The space's bindings this walker has read, and what it has asked for:
/// what a reference is resolved by.
///
/// A name is a path, `todo` or `cdata/todo`; the slash is a convention of
/// the name and nothing walks it. Each binding is keyed by the whole of
/// its name and signature, so a name's signatures are one range of keys.
#[derive(Clone, Debug, Default)]
pub struct Names {
    bound: BTreeMap<String, Binding>,
    /// What has been read, found or not: a lookup of these is answered.
    answered: BTreeSet<Want>,
    /// What a lookup asked for that was not answered: what a driver reads
    /// next. Shared by every copy, so what a trial that was refused asked
    /// for is still asked.
    wanted: Arc<Mutex<BTreeSet<Want>>>,
}

impl PartialEq for Names {
    /// Two are equal when they bind alike: what was asked and answered is
    /// how they came to.
    fn eq(&self, other: &Self) -> bool {
        self.bound == other.bound
    }
}

impl Names {
    fn asked(&self) -> std::sync::MutexGuard<'_, BTreeSet<Want>> {
        self.wanted
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Whether `want` has been answered: by itself, or by a read that
    /// covers it.
    pub fn is_answered(&self, want: &Want) -> bool {
        if self.answered.contains(want) {
            return true;
        }
        let every = self.answered.contains(&Want::All);
        let under = |covered: &str| {
            self.answered.iter().any(|answered| {
                matches!(answered, Want::Prefix(prefix) if covered.starts_with(prefix.as_str()))
            })
        };
        match want {
            Want::Name(name) => every || under(&range(name)),
            Want::Prefix(prefix) => every || under(prefix),
            _ => false,
        }
    }

    /// Ask for `want` unless it is answered.
    pub(crate) fn want(&self, want: Want) {
        if !self.is_answered(&want) {
            self.asked().insert(want);
        }
    }

    /// What lookups asked for that has not been answered.
    pub fn wanted(&self) -> Vec<Want> {
        self.asked()
            .iter()
            .filter(|want| !self.is_answered(want))
            .cloned()
            .collect()
    }

    /// Note `wants` answered, whatever was found for them.
    pub fn answer(&mut self, wants: impl IntoIterator<Item = Want>) {
        self.answered.extend(wants);
    }

    /// Forget every binding and every answer: what is bound is read
    /// again, so what moved since is seen.
    pub fn forget(&mut self) {
        self.bound.clear();
        self.answered.clear();
        self.wanted = Arc::new(Mutex::new(BTreeSet::new()));
    }

    /// Bind `binding`'s key to its definition, in place of what it was
    /// bound to.
    pub fn bind(&mut self, binding: Binding) {
        self.bound.insert(binding.key(), binding);
    }

    /// Bind as the analysis itself does: what it binds needs no reading.
    pub(crate) fn bind_here(&mut self, binding: Binding) {
        self.answered.insert(Want::Name(binding.name.clone()));
        self.bind(binding);
    }

    /// Take every binding of `name` away, giving what each was bound to.
    pub fn unbind(&mut self, name: &str) -> Vec<Binding> {
        let keys: Vec<String> = self
            .bound
            .range(range(name)..)
            .take_while(|(key, _)| key.starts_with(&range(name)))
            .map(|(key, _)| key.clone())
            .collect();
        keys.into_iter()
            .filter_map(|key| self.bound.remove(&key))
            .collect()
    }

    /// Every binding known.
    pub fn bindings(&self) -> impl Iterator<Item = Binding> + '_ {
        self.bound.values().cloned()
    }

    fn row(binding: &Binding) -> Name {
        Name {
            name: binding.name.clone(),
            signature: binding.signature.clone(),
            definition: binding.definition.clone(),
        }
    }

    /// The rows of `name`: one range of keys, asked for unless read.
    pub fn named(&self, name: &str) -> Vec<Name> {
        self.want(Want::Name(name.to_string()));
        self.bound
            .range(range(name)..)
            .take_while(|(key, _)| key.starts_with(&range(name)))
            .map(|(_, binding)| Self::row(binding))
            .collect()
    }

    /// Every row known, asking for nothing.
    pub fn rows(&self) -> Vec<Name> {
        self.bound.values().map(Self::row).collect()
    }

    /// What a reference as written means: the rows of its name, and of
    /// `todo#3fa9` those whose definition's hash starts so. A name is the
    /// space's; a path is a longer name.
    pub fn resolve(&self, written: &str) -> Vec<Name> {
        let (name, prefix) = match written.split_once('#') {
            Some((name, prefix)) => (name, Some(prefix)),
            None => (written, None),
        };
        let mut rows = self.named(name);
        if let Some(prefix) = prefix {
            rows.retain(|row| hash_of(&row.definition).starts_with(prefix));
        }
        rows
    }

    /// What may be meant by what is being typed: every name of the space
    /// that starts with `prefix`, each signature of it a suggestion.
    pub fn suggest(&self, prefix: &str) -> Vec<Suggestion> {
        self.want(Want::Prefix(prefix.to_string()));
        self.bound
            .range(prefix.to_string()..)
            .take_while(|(key, _)| key.starts_with(prefix))
            .map(|(_, binding)| Suggestion::of(binding))
            .collect()
    }

    /// The other names of the space whose last segment is `name`'s:
    /// `cdata/todo` for `todo`. What a name not found may have meant. It
    /// reads every binding, which only an error does.
    pub fn ending_as(&self, name: &str) -> Vec<Suggestion> {
        self.want(Want::All);
        let last = name.rsplit('/').next().unwrap_or(name);
        self.bound
            .values()
            .filter(|binding| binding.name != name && binding.name.rsplit('/').next() == Some(last))
            .map(Suggestion::of)
            .collect()
    }
}

/// A name that may be meant: under which signature, and the definition
/// it is bound to.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Suggestion {
    /// The name.
    pub name: String,
    /// Its signature.
    pub signature: String,
    /// The definition it is bound to.
    pub definition: Entity,
}

impl Suggestion {
    fn of(binding: &Binding) -> Self {
        Suggestion {
            name: binding.name.clone(),
            signature: binding.signature.clone(),
            definition: binding.definition.clone(),
        }
    }
}

impl Display for Suggestion {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{} ({})", self.name, self.signature)
    }
}

/// The name a reference applies once it is resolved: `todo` of
/// `todo#3fa9`.
pub fn bare(written: &str) -> &str {
    written.split_once('#').map_or(written, |(name, _)| name)
}

/// Whether a reference is qualified by a hash.
pub fn qualified(written: &str) -> bool {
    written.contains('#')
}

/// The signature a definition is bound under, read off what it was kept
/// as: a rule about syntax's pattern, or a query's head.
pub fn signature_of(definition: &Definition) -> Option<String> {
    let labels = |tree: &Syntax| -> Vec<String> {
        tree.operand()
            .map(|operand| operand.terms.keys().cloned().collect())
            .unwrap_or_default()
    };
    let form = &definition.form;
    let operand = form.operand()?;
    match definition.kind {
        Kind::Query => {
            let own = operand.items.last()?;
            let head = own.operand()?.items.first()?;
            Some(signature(Kind::Query, labels(head)))
        }
        kind => {
            let expand = operand
                .items
                .iter()
                .find(|part| part.applies() == Some("expand"))?;
            let pattern = expand.operand()?.items.first()?;
            Some(signature(kind, labels(pattern)))
        }
    }
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

    /// A name's range holds every key of that name and no key of any
    /// other: not of a longer name it is a prefix of, not of a path under
    /// it, not of a name it is under. A path's prefix is a range too.
    #[test]
    fn it_keys_a_name_so_that_its_range_is_its_own() {
        let names = [
            "todo",
            "todo-list",
            "todo/model",
            "todo.x",
            "todox",
            "tod",
            "cdata/todo",
            "cdata/todo/model",
            "cdata",
            "cdatax/todo",
            "t",
            "todo!",
        ];
        let signatures = ["concept (title:)", "concept ()", "query (is:)", "macro ()"];
        let mut keys: Vec<(String, &str)> = Vec::new();
        for name in names {
            for signature in signatures {
                keys.push((key(name, signature), name));
            }
        }
        keys.sort();
        for name in names {
            let inside: Vec<&str> = keys
                .iter()
                .filter(|(key, _)| key.starts_with(&range(name)))
                .map(|(_, of)| *of)
                .collect();
            assert_eq!(inside, vec![name; signatures.len()], "{name}");
            // The range is contiguous in key order.
            let at: Vec<usize> = keys
                .iter()
                .enumerate()
                .filter(|(_, (key, _))| key.starts_with(&range(name)))
                .map(|(at, _)| at)
                .collect();
            assert!(at.windows(2).all(|pair| pair[1] == pair[0] + 1), "{name}");
        }
        let under: Vec<&str> = keys
            .iter()
            .filter(|(key, _)| key.starts_with("cdata/"))
            .map(|(_, of)| *of)
            .collect();
        assert!(under.iter().all(|name| name.starts_with("cdata/")));
        assert_eq!(under.len(), 2 * signatures.len());
        assert_eq!(
            [
                key("todo", "concept (done: title:)"),
                key("cdata/todo", "query (is:)"),
                key("todo-list", "macro ()"),
            ],
            [
                "todo concept (done: title:)".to_string(),
                "cdata/todo query (is:)".to_string(),
                "todo-list macro ()".to_string(),
            ]
        );
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
