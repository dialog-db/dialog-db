use crate::Parameters;
use crate::artifact::{ArtifactsRelation, Entity, Value};
use crate::attribute::Relation;
use crate::attribute::query::AttributeQuery;
use crate::error::{FieldTypeError, TypeError};
use crate::schema::Cardinality;
use crate::term::Term;
use crate::type_system::Type as Kind;
use crate::types::Any;
use crate::types::Type;
use dialog_artifacts::{NameShape, Pick, Symbol};

use base58::ToBase58;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter, Result as FmtResult};
use std::iter;
use std::str::FromStr;

/// A validated attribute–value pair with its cardinality, produced by
/// [`AttributeDescriptor::resolve`]. Used inside [`ConceptStatement`](crate::concept::descriptor::ConceptStatement)
/// to represent the set of facts that make up a concept instance.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Attribution {
    /// The fully-qualified attribute selector.
    pub the: ArtifactsRelation,
    /// The resolved value for this attribute.
    pub is: Value,
    /// Whether this attribute allows one or many values per entity.
    pub cardinality: Cardinality,
}

/// What an attribute's `the` holds: one relation, or every entry of a
/// keyed collection.
///
/// Dialog stores a collection as facts sharing a domain whose *name*
/// half is the entry's key — `todo.list/title` for a dictionary
/// entry, `todo.list/N5` for a sequence member. The two key kinds are
/// disjoint by their first byte (symbols lowercase, positions
/// uppercase), so one domain can carry both and a scan can take
/// either half as a contiguous key range.
///
/// The key kind is the variant rather than a field, so a collection
/// cannot be described with a key kind that disagrees with it: there
/// is no state to validate.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum The {
    /// One relation, named in full: `todo.list/title`.
    Relation(Relation),
    /// Every entry of one domain, keyed by name.
    Collection {
        /// The domain the entries share, without a trailing separator.
        domain: Symbol,
        /// Which half of the domain: symbol-named entries
        /// (a dictionary) or position-named members (a sequence).
        keyed: Keyed,
    },
}

/// Which half of a domain a [`The::Collection`] selects.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Keyed {
    /// Symbol-named entries: a dictionary.
    Dictionary,
    /// Position-named members, in list order: a sequence.
    Sequence,
}

impl From<Keyed> for NameShape {
    fn from(keyed: Keyed) -> Self {
        match keyed {
            Keyed::Dictionary => NameShape::Symbol,
            Keyed::Sequence => NameShape::Position,
        }
    }
}

impl The {
    /// A collection of `domain`, keyed as `keyed`.
    pub fn collection(domain: Symbol, keyed: Keyed) -> Self {
        The::Collection { domain, keyed }
    }

    /// The domain these facts live under.
    pub fn domain(&self) -> &str {
        match self {
            The::Relation(the) => the.domain(),
            The::Collection { domain, .. } => domain.as_str(),
        }
    }

    /// The attribute's name half, or `None` for a collection — whose
    /// name half is a key that varies per entry rather than a fixed
    /// part of the selector.
    pub fn name(&self) -> Option<&str> {
        match self {
            The::Relation(the) => Some(the.name()),
            The::Collection { .. } => None,
        }
    }

    /// How this relation is selected when lowered for the field
    /// `field`: a constant for one attribute, a variable refined by
    /// the domain and key kind for a collection. The refined form is
    /// what narrows a domain scan to the demanded half — see
    /// `dialog_artifacts::NameShape`.
    ///
    /// The variable is named after the field (`<field>/the`) so two
    /// collection fields on one concept scan independently rather
    /// than unifying on a shared name.
    pub fn term(&self, field: &str) -> Term<Relation> {
        match self {
            The::Relation(the) => Term::Constant(Value::from(the.clone())),
            The::Collection { .. } => Term::<Relation>::var(Self::attribute_variable(field))
                .with_kind(
                    self.kind()
                        .expect("a collection is refined by construction"),
                ),
        }
    }

    /// The kind a collection's attribute slot is refined to: a symbol
    /// under the domain prefix, narrowed to the key kind's name shape.
    /// `None` for a plain attribute, which is selected by constant.
    pub fn kind(&self) -> Option<Kind> {
        match self {
            The::Relation(_) => None,
            The::Collection { domain, keyed } => Some(
                Kind::from(Type::Symbol)
                    .with_prefix(format!("{}/", domain.as_str()))
                    .expect("symbol is textual")
                    .with_name_shape(NameShape::from(*keyed))
                    .expect("a name shape composes with a domain prefix"),
            ),
        }
    }

    /// The body variable a collection field's scan binds the matched
    /// attribute to. Internal to the concept's own rule: the key an
    /// author sees is the attribute's name half, bound to the
    /// [`key_operand`](Self::key_operand) by `dialog/attribute-parts`.
    pub fn attribute_variable(field: &str) -> String {
        format!("{field}/the")
    }

    /// The operand carrying a collection field's key: the name half
    /// of each matched attribute. `{?key: ?value}` under the field in
    /// notation is this operand and the field's own, and the two
    /// serialize back into that form.
    pub fn key_operand(field: &str) -> String {
        format!("{field}/key")
    }
}

impl The {
    /// The concrete attribute to write a fact under, when there is
    /// one. A collection has none: its facts are keyed per entry, so
    /// the key has to come from the writer rather than the schema.
    pub fn attribute(&self) -> Option<ArtifactsRelation> {
        match self {
            The::Relation(the) => Some(ArtifactsRelation::from(the)),
            The::Collection { .. } => None,
        }
    }

    /// The attribute one entry of a collection is written under:
    /// `domain/key`, where the key must have the collection's name
    /// shape — a position for a sequence, a symbol for a dictionary.
    /// A plain attribute ignores the key and is its own entry.
    pub fn entry(&self, key: &str) -> Result<ArtifactsRelation, FieldTypeError> {
        match self {
            The::Relation(the) => Ok(ArtifactsRelation::from(the)),
            The::Collection { domain, keyed } => {
                let mismatch = || FieldTypeError::KeyShape {
                    domain: domain.as_str().to_owned(),
                    key: key.to_owned(),
                    keyed: *keyed,
                };
                let attribute = ArtifactsRelation::try_from(format!("{}/{key}", domain.as_str()))
                    .map_err(|_| mismatch())?;
                let (_, name) = attribute.split().map_err(|_| mismatch())?;
                if name.shape() != NameShape::from(*keyed) {
                    return Err(mismatch());
                }
                Ok(attribute)
            }
        }
    }
}

impl From<Relation> for The {
    fn from(the: Relation) -> Self {
        The::Relation(the)
    }
}

/// A relation spells as an attribute does, `domain/name`, with a
/// collection's name half naming the key kind in brackets:
/// `todo.list/[position]` for a sequence, `todo.list/[symbol]` for a
/// dictionary — the same bracket the declaration form uses. A
/// bracketed name can never be a stored attribute's name, so the two
/// forms are disjoint and the spelling round-trips.
impl Display for The {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            The::Relation(the) => write!(f, "{the}"),
            The::Collection { domain, keyed } => {
                write!(f, "{}/{}", domain.as_str(), keyed.bracketed())
            }
        }
    }
}

impl Keyed {
    /// The bracketed key kind a collection's name slot spells:
    /// `[position]` or `[symbol]`.
    pub fn bracketed(self) -> &'static str {
        match self {
            Keyed::Sequence => "[position]",
            Keyed::Dictionary => "[symbol]",
        }
    }

    fn from_bracketed(name: &str) -> Option<Keyed> {
        match name {
            "[position]" => Some(Keyed::Sequence),
            "[symbol]" => Some(Keyed::Dictionary),
            _ => None,
        }
    }
}

impl FromStr for The {
    type Err = <Relation as FromStr>::Err;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        if let Some((domain, name)) = text.split_once('/')
            && let Some(keyed) = Keyed::from_bracketed(name)
            && let Ok(domain) = Symbol::from_str(domain)
        {
            return Ok(The::Collection { domain, keyed });
        }
        text.parse::<Relation>().map(The::Relation)
    }
}

/// Static metadata for an attribute: the relation it reads, or several
/// ranked, its human-readable description, value type or listed value
/// domain, and the pick it reads under.
///
/// An attribute is a relation read under a type and a pick. A list
/// is a ranked choice: `the: [a, b]` reads the first listed relation
/// holding a candidate, `as: [v1, v2]` the first listed value a
/// candidate holds, and either implies `pick: top`. Rules derive
/// into relations and are found by them, so two attributes over one
/// relation under different types or picks are two attributes.
///
/// `AttributeDescriptor` is used in two contexts:
/// 1. Inside a [`ConceptDescriptor`](crate::concept::descriptor::ConceptDescriptor)
///    to describe each attribute that makes up the concept.
/// 2. During query construction, where [`resolve`](AttributeDescriptor::resolve)
///    validates a runtime value against the descriptor's type and produces
///    an [`Attribution`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "Wire", into = "Wire")]
pub struct AttributeDescriptor {
    /// The relation read first.
    the: The,
    /// The relations read after it, in rank order: a candidate from an
    /// earlier relation outranks one from a later.
    then: Vec<The>,
    description: String,
    content_type: Option<Type>,
    /// The values `as` lists, best first: the domain a value is one
    /// of, which a `top` pick ranks by.
    among: Vec<Value>,
    /// Which of the relation's candidates the attribute picks; a `top`
    /// carries [`among`](Self::among). The cardinality is the pick's
    /// arity.
    pick: Pick,
}

/// The wire form of an [`AttributeDescriptor`]: `the` is one relation
/// or a ranked list, `as` a type or a ranked value list, `pick` the
/// pick where it says more than the lists imply, and `cardinality`
/// the older spelling of `last` and `all`, read but never written.
#[derive(Serialize, Deserialize)]
struct Wire {
    the: OneOrMany<The>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    description: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    cardinality: Option<Cardinality>,
    #[serde(rename = "as", default, skip_serializing_if = "Option::is_none")]
    content: Option<As>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pick: Option<PickName>,
    /// The preview spelling of `pick`, read and never written.
    #[serde(default, skip_serializing)]
    select: Option<PickName>,
}

/// A pick as the wire spells it: its entity alone (`all:`), the values
/// a `top` ranks being the `as` list. The plain name a 0.2 release
/// wrote (`all`) is still read.
#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum PickName {
    #[serde(rename = "last:", alias = "last")]
    Last,
    #[serde(rename = "all:", alias = "all")]
    All,
    #[serde(rename = "top:", alias = "top")]
    Top,
    #[serde(rename = "max:", alias = "max")]
    Max,
    #[serde(rename = "min:", alias = "min")]
    Min,
}

impl PickName {
    fn of(pick: &Pick) -> Self {
        match pick {
            Pick::Last => PickName::Last,
            Pick::All => PickName::All,
            Pick::Top(_) => PickName::Top,
            Pick::Max => PickName::Max,
            Pick::Min => PickName::Min,
        }
    }

    fn with(self, ranked: Vec<Value>) -> Pick {
        match self {
            PickName::Last => Pick::Last,
            PickName::All => Pick::All,
            PickName::Top => Pick::Top(ranked),
            PickName::Max => Pick::Max,
            PickName::Min => Pick::Min,
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(untagged)]
enum OneOrMany<T> {
    One(T),
    Many(Vec<T>),
}

#[derive(Serialize, Deserialize)]
#[serde(untagged)]
enum As {
    Kind(Type),
    Domain(Vec<Value>),
}

impl TryFrom<Wire> for AttributeDescriptor {
    type Error = String;

    fn try_from(wire: Wire) -> Result<Self, Self::Error> {
        let (the, then) = match wire.the {
            OneOrMany::One(the) => (the, Vec::new()),
            OneOrMany::Many(mut list) => {
                if list.is_empty() {
                    return Err("`the` lists no relation".to_string());
                }
                let the = list.remove(0);
                (the, list)
            }
        };
        let (content_type, among) = match wire.content {
            None => (None, Vec::new()),
            Some(As::Kind(kind)) => (Some(kind), Vec::new()),
            Some(As::Domain(mut values)) => {
                if values.is_empty() {
                    return Err("`as` lists no value".to_string());
                }
                // A bare string reads as an entity when it parses as a
                // URI, so a list of text written bare reads as text and
                // entities mixed; it is text.
                if values.iter().any(|value| matches!(value, Value::String(_)))
                    && values
                        .iter()
                        .all(|value| matches!(value, Value::String(_) | Value::Entity(_)))
                {
                    values = values
                        .into_iter()
                        .map(|value| value.conform(Type::String))
                        .collect();
                }
                let kind = values[0].data_type();
                if values.iter().any(|value| value.data_type() != kind) {
                    return Err("`as` lists values of more than one type".to_string());
                }
                (Some(kind), values)
            }
        };
        // A listed domain or relation chain is a ranked choice, `top`
        // unless the pick says otherwise; `cardinality` is the older
        // spelling of `last` and `all`.
        let pick = match wire.pick.or(wire.select) {
            Some(name) => name.with(among.clone()),
            None if !then.is_empty() || !among.is_empty() => Pick::Top(among.clone()),
            None => wire.cardinality.unwrap_or_default().pick(),
        };
        let descriptor = AttributeDescriptor {
            the,
            then,
            description: wire.description,
            content_type,
            among,
            pick,
        };
        match descriptor.pick_error() {
            Some(reason) => Err(reason),
            None => Ok(descriptor),
        }
    }
}

impl From<AttributeDescriptor> for Wire {
    fn from(descriptor: AttributeDescriptor) -> Self {
        let the = if descriptor.then.is_empty() {
            OneOrMany::One(descriptor.the)
        } else {
            OneOrMany::Many(iter::once(descriptor.the).chain(descriptor.then).collect())
        };
        let name = PickName::of(&descriptor.pick);
        let content = if descriptor.among.is_empty() {
            descriptor.content_type.map(As::Kind)
        } else {
            Some(As::Domain(descriptor.among))
        };
        // The pick is written where it says more than the lists imply:
        // a ranked choice is `top` by itself, and a plain attribute is
        // `last` by itself.
        let implied = if matches!(the, OneOrMany::Many(_)) || matches!(content, Some(As::Domain(_)))
        {
            PickName::Top
        } else {
            PickName::Last
        };
        Wire {
            the,
            description: descriptor.description,
            cardinality: None,
            content,
            pick: Some(name).filter(|name| *name != implied),
            select: None,
        }
    }
}

impl AttributeDescriptor {
    /// Creates a new descriptor from a validated [`Relation`] selector.
    pub fn new(
        the: Relation,
        description: impl Into<String>,
        cardinality: Cardinality,
        content_type: Option<Type>,
    ) -> Self {
        Self::over(The::Relation(the), description, cardinality, content_type)
    }

    /// Creates a descriptor over any [`The`] — one attribute, or
    /// every entry of a keyed collection.
    pub fn over(
        the: The,
        description: impl Into<String>,
        cardinality: Cardinality,
        content_type: Option<Type>,
    ) -> Self {
        Self {
            the,
            then: Vec::new(),
            description: description.into(),
            content_type,
            among: Vec::new(),
            pick: cardinality.pick(),
        }
    }

    /// This descriptor reading `then` after its relation, in rank
    /// order: a ranked choice by relation, read as `top`.
    pub fn with_then(mut self, then: Vec<The>) -> Self {
        self.then = then;
        self.pick = Pick::Top(self.among.clone());
        self
    }

    /// This descriptor over the listed value domain, best first: a
    /// ranked choice by value, read as `top` unless it is picked
    /// otherwise.
    pub fn with_domain(mut self, ranked: Vec<Value>) -> Self {
        if matches!(self.pick, Pick::Last | Pick::Top(_)) {
            self.pick = Pick::Top(ranked.clone());
        }
        self.among = ranked;
        self
    }

    /// The values `as` lists, best first, if it lists any.
    pub fn among(&self) -> &[Value] {
        &self.among
    }

    /// Every relation this attribute reads, the first ranked highest.
    pub fn relations(&self) -> impl Iterator<Item = &The> {
        iter::once(&self.the).chain(self.then.iter())
    }

    /// Whether this attribute reads several relations, ranked.
    pub fn is_chain(&self) -> bool {
        !self.then.is_empty()
    }

    /// This descriptor under `pick`. The pick is checked against the
    /// descriptor by [`pick_error`](Self::pick_error) wherever a concept
    /// is built. A `top` with no values of its own ranks by the listed
    /// domain; one with values lists them as the domain.
    pub fn with_pick(mut self, pick: Pick) -> Self {
        self.pick = match pick {
            Pick::Top(ranked) if ranked.is_empty() => Pick::Top(self.among.clone()),
            Pick::Top(ranked) => {
                self.among = ranked.clone();
                Pick::Top(ranked)
            }
            pick => pick,
        };
        self
    }

    /// Which of its relation's candidates this attribute picks. A write
    /// through the attribute carries it: any pick but `all` succeeds
    /// the claim a read returns, retracted beside the written value;
    /// `all` appends.
    pub fn pick(&self) -> &Pick {
        &self.pick
    }

    /// Whether a field over this attribute must read through the
    /// attribute concept to be read as declared: its pick is not the
    /// plain stored read (`last` or `all`), or it reads several
    /// relations, so the candidates have to be gathered and elected
    /// even where nothing derives them.
    pub fn reads_elected(&self) -> bool {
        !matches!(self.pick, Pick::Last | Pick::All) || self.is_chain()
    }

    /// This descriptor's first relation read plainly, under its pick's
    /// arity alone: the relation itself, which is what rules derive
    /// into.
    pub fn without_pick(mut self) -> Self {
        self.pick = Cardinality::of(&self.pick).pick();
        self.among = Vec::new();
        self.then = Vec::new();
        self
    }

    /// Why the pick does not fit this attribute, if it does not: `top`
    /// ranks listed values or relations, and `max` and `min` order a
    /// comparable value type.
    pub fn pick_error(&self) -> Option<String> {
        if self.is_chain() && !matches!(self.pick, Pick::Top(_)) {
            return Some(
                "a list of relations is a ranked choice: it reads as `top` only".to_string(),
            );
        }
        let content = self.content_type;
        match &self.pick {
            Pick::Top(ranked) if ranked.is_empty() && !self.is_chain() => {
                Some("`top` ranks listed values or relations, and none are listed".to_string())
            }
            Pick::Top(_) => None,
            pick @ (Pick::Max | Pick::Min) => match content {
                Some(Type::Bytes) | Some(Type::Boolean) => Some(format!(
                    "`{pick}` orders a comparable value type, not {:?}",
                    content.expect("checked")
                )),
                _ => None,
            },
            Pick::Last | Pick::All => None,
        }
    }

    /// Returns a relation identifier comprised of the attribute's domain and name.
    pub fn the(&self) -> &The {
        &self.the
    }

    /// Returns the attribute domain.
    pub fn domain(&self) -> &str {
        self.the.domain()
    }

    /// Returns the attribute name, or `None` for a collection — whose
    /// name half is a per-entry key rather than part of the selector.
    pub fn name(&self) -> Option<&str> {
        self.the.name()
    }

    /// Returns the human-readable description.
    pub fn description(&self) -> &str {
        &self.description
    }

    /// Returns the cardinality: the pick's arity, `all` being many
    /// and every other pick one. `cardinality` is the older spelling
    /// of `last` and `all`, so the two never disagree.
    pub fn cardinality(&self) -> Cardinality {
        Cardinality::of(&self.pick)
    }

    /// The arity the stored scan of this attribute reads under. A
    /// `last` read is the stored scan itself: one claim per entity, the
    /// newest. Every other pick elects among the claims, so the scan
    /// hands over every claim and the election chooses; a scan that
    /// kept one would choose by `last` before the pick saw the rest.
    pub fn scan_cardinality(&self) -> Cardinality {
        match self.pick {
            Pick::Last => Cardinality::One,
            _ => Cardinality::Many,
        }
    }

    /// Returns the expected value type, or `None` if any type is accepted.
    pub fn content_type(&self) -> Option<Type> {
        self.content_type
    }

    /// Checks that the given parameter's type is compatible with this
    /// attribute's content type.
    pub fn check(&self, parameter: &Term<Any>) -> Result<(), FieldTypeError> {
        match (self.content_type(), parameter.content_type()) {
            (None, _) => Ok(()),
            (_, None) => Ok(()),
            (Some(_expected), _actual) => Ok(()),
        }
    }

    /// Type-checks an optional parameter against this attribute.
    pub fn conform(&self, parameter: Option<&Term<Any>>) -> Result<(), FieldTypeError> {
        if let Some(param) = parameter {
            self.check(param)?;
        }
        Ok(())
    }

    /// Validates a concrete [`Value`] against this attribute's content type and
    /// produces an [`Attribution`] — a validated (attribute, value, cardinality)
    /// triple ready for storage.
    pub fn resolve(&self, value: Value) -> Result<Attribution, FieldTypeError> {
        let type_matches = match self.content_type() {
            Some(expected) => value.data_type() == expected,
            None => true,
        };

        if type_matches {
            // A collection field describes many facts, one per key,
            // so there is no single attribute to write this value
            // under: the key belongs to the entry, not the schema.
            let the = self
                .the
                .attribute()
                .ok_or_else(|| FieldTypeError::UnkeyedCollection {
                    domain: self.the.domain().to_owned(),
                })?;
            Ok(Attribution {
                the,
                is: value.clone(),
                cardinality: self.cardinality(),
            })
        } else {
            Err(FieldTypeError::TypeMismatch {
                expected: self.content_type().unwrap(), // Safe because we checked Some above
                actual: Box::new(Term::Constant(value.clone())),
            })
        }
    }

    /// Estimates the cost of a fact query on this attribute given what's known.
    ///
    /// # Parameters
    /// - `the`: Is the attribute known? (usually true for Attribute)
    /// - `of`: Is the entity known?
    /// - `is`: Is the value known?
    pub fn estimate(&self, of: bool, is: bool) -> usize {
        self.cardinality()
            .estimate(true, of, is)
            .expect("Should succeed if we know attribute")
    }

    /// Builds an [`AttributeQuery`] from named parameters, type-checking each
    /// binding against this attribute's schema.
    pub fn apply(&self, parameters: Parameters) -> Result<AttributeQuery, TypeError> {
        // Check that type of the `is` parameter matches the attribute's data type
        self.conform(parameters.get("is"))
            .map_err(|e| e.at("is".to_string()))?;

        // Check that if `this` parameter is provided, it has entity type.
        if let Some(this) = parameters.get("this")
            && let Some(actual) = this.content_type()
            && actual != Type::Entity
        {
            return Err(TypeError::TypeMismatch {
                binding: "this".to_string(),
                expected: Type::Entity,
                actual: Box::new(this.clone()),
            });
        }

        // Get the entity term (this), converting from Parameter to Term<Entity>
        let of = match parameters.get("this").cloned() {
            Some(Term::Variable {
                name: Some(name), ..
            }) => Term::var(name.clone()),
            Some(Term::Variable { name: None, .. }) => Term::blank(),
            Some(Term::Constant(value)) => Term::Constant(value),
            None => Term::blank(),
        };

        // Get the value parameter (is) -- passed directly as Parameter
        let is = parameters
            .get("is")
            .cloned()
            .unwrap_or_else(Term::<Any>::blank);

        // Get the cause term
        let cause = match parameters.get("cause").cloned() {
            Some(Term::Variable {
                name: Some(name), ..
            }) => Term::var(name.clone()),
            Some(Term::Variable { name: None, .. }) => Term::blank(),
            Some(Term::Constant(value)) => Term::Constant(value),
            None => Term::blank(),
        };

        Ok(AttributeQuery::new(
            self.the().term("the"),
            of,
            is,
            cause,
            Some(self.cardinality()),
        ))
    }

    /// Encode this attribute descriptor as CBOR for hashing
    ///
    /// Creates a CBOR-encoded representation with fields:
    /// - domain: domain
    /// - name: name
    /// - cardinality: cardinality
    /// - type: content_type
    ///
    /// Description is excluded from the encoding.
    pub fn to_cbor_bytes(&self) -> Vec<u8> {
        use serde::Serialize;

        // `name` carries the attribute's name half for a plain
        // attribute and the key kind for a collection — the two are
        // the same slot because they are the same thing: what the
        // name half of these facts holds. A plain attribute therefore
        // encodes exactly as it did before collections existed, so
        // every existing identity is preserved.
        // An attribute is a relation, or a ranked chain of them, read
        // under a type or a listed value domain and a pick: all of
        // it is the identity, and two reads of one relation under
        // different picks are two attributes.
        #[derive(Serialize)]
        struct CborAttributeDescriptor<'a> {
            domain: &'a str,
            name: &'a str,
            #[serde(skip_serializing_if = "Vec::is_empty")]
            then: Vec<String>,
            #[serde(rename = "type")]
            content_type: Option<Type>,
            pick: &'static str,
            #[serde(skip_serializing_if = "<[Value]>::is_empty")]
            among: &'a [Value],
        }

        let name = match &self.the {
            The::Relation(the) => the.name(),
            The::Collection { keyed, .. } => match keyed {
                Keyed::Dictionary => "<dictionary>",
                Keyed::Sequence => "<sequence>",
            },
        };
        let schema = CborAttributeDescriptor {
            domain: self.domain(),
            name,
            then: self
                .then
                .iter()
                .map(|relation| relation.to_string())
                .collect(),
            content_type: self.content_type(),
            pick: self.pick.uri(),
            among: &self.among,
        };

        serde_ipld_dagcbor::to_vec(&schema).expect("CBOR encoding should not fail")
    }

    /// Compute blake3 hash of this attribute descriptor
    ///
    /// Returns a 32-byte blake3 hash of the CBOR-encoded descriptor
    pub fn hash(&self) -> blake3::Hash {
        let cbor_bytes = self.to_cbor_bytes();
        blake3::hash(&cbor_bytes)
    }

    /// Format this attribute's hash as a URI
    ///
    /// Returns a string in the format: `the:{base58(blake3)}`
    pub fn to_uri(&self) -> String {
        let encoded = self.hash().as_bytes().as_ref().to_base58();
        format!("the:{encoded}")
    }

    /// Parse an attribute URI and extract the hash
    ///
    /// Expects format: `the:{base58(blake3)}`
    /// Returns None if the format is invalid
    pub fn parse_uri(uri: &str) -> Option<blake3::Hash> {
        let encoded = uri.strip_prefix("the:")?;
        let bytes = base58::FromBase58::from_base58(encoded).ok()?;
        if bytes.len() != 32 {
            return None;
        }
        let mut arr = [0u8; 32];
        arr.copy_from_slice(&bytes);
        Some(blake3::Hash::from(arr))
    }
}

impl From<AttributeDescriptor> for Entity {
    fn from(descriptor: AttributeDescriptor) -> Self {
        descriptor.to_uri().parse().expect("valid entity URI")
    }
}

/// A descriptor's concrete attribute. Panics for a keyed collection,
/// which has no single attribute — use
/// [`The::attribute`](Relation::attribute) where that is
/// possible.
impl From<&AttributeDescriptor> for ArtifactsRelation {
    fn from(descriptor: &AttributeDescriptor) -> Self {
        descriptor
            .the
            .attribute()
            .expect("a keyed collection has no single attribute")
    }
}

impl From<AttributeDescriptor> for ArtifactsRelation {
    fn from(descriptor: AttributeDescriptor) -> Self {
        ArtifactsRelation::from(&descriptor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ConceptFieldDescriptor;
    use crate::the;

    fn ranked(values: Vec<Value>, content_type: Type) -> AttributeDescriptor {
        AttributeDescriptor::new(
            "job/status".parse().expect("a relation"),
            "",
            Cardinality::One,
            Some(content_type),
        )
        .with_domain(values)
    }

    /// A ranked list keeps the type of each value it lists through dag-cbor,
    /// alone and as a concept's field, where the field's options are read
    /// beside it in one map.
    #[dialog_common::test]
    fn it_keeps_the_type_of_each_listed_value_through_dag_cbor() {
        let lists = [
            (
                vec![Value::UnsignedInt(1), Value::UnsignedInt(2)],
                Type::UnsignedInt,
            ),
            (
                vec![Value::SignedInt(1), Value::SignedInt(-2)],
                Type::SignedInt,
            ),
            (
                vec![
                    Value::String("https://example.com".into()),
                    Value::String("foo:".into()),
                ],
                Type::String,
            ),
        ];
        for (values, content_type) in lists {
            let descriptor = ranked(values.clone(), content_type);
            let bytes = serde_ipld_dagcbor::to_vec(&descriptor).expect("encodes");
            let decoded: AttributeDescriptor =
                serde_ipld_dagcbor::from_slice(&bytes).expect("decodes");
            assert_eq!(decoded.among(), values.as_slice());

            let field = ConceptFieldDescriptor::optional(descriptor);
            let bytes = serde_ipld_dagcbor::to_vec(&field).expect("encodes");
            let decoded: ConceptFieldDescriptor =
                serde_ipld_dagcbor::from_slice(&bytes).expect("decodes");
            assert_eq!(decoded.descriptor().among(), values.as_slice());
        }
    }

    /// A list of text written bare reads as text, though some of its
    /// entries parse as URIs.
    #[dialog_common::test]
    fn it_reads_a_bare_list_of_text_as_text() {
        let descriptor: AttributeDescriptor = serde_json::from_value(serde_json::json!({
            "the": "page/link",
            "as": ["https://example.com", "home"]
        }))
        .expect("descriptor parses");
        assert_eq!(descriptor.content_type(), Some(Type::String));
        assert_eq!(
            descriptor.among(),
            &[
                Value::String("https://example.com/".into()),
                Value::String("home".into())
            ]
        );
    }

    /// Two attributes that list the same spelling under different types
    /// are two attributes.
    #[dialog_common::test]
    fn it_tells_attributes_apart_by_the_type_of_their_listed_values() {
        let text = ranked(vec![Value::String("case:active".into())], Type::String);
        let entity = ranked(
            vec![Value::Entity("case:active".parse().expect("an entity"))],
            Type::Entity,
        );
        assert_ne!(text.to_uri(), entity.to_uri());
        let natural = ranked(vec![Value::UnsignedInt(1)], Type::UnsignedInt);
        let integer = ranked(vec![Value::SignedInt(1)], Type::SignedInt);
        assert_ne!(natural.to_uri(), integer.to_uri());
    }

    /// A list is a ranked choice: `as: [..]` ranks values, `the: [..]`
    /// ranks relations, and either reads as `top` without saying so.
    /// `cardinality` is read as the older spelling of `last` and `all`
    /// and never written; a pick is written where it says more than
    /// the lists imply.
    #[dialog_common::test]
    fn it_reads_a_list_as_a_ranked_choice() {
        let ranked: AttributeDescriptor = serde_json::from_value(serde_json::json!({
            "the": "job/status",
            "as": ["case:suspended", "case:active"]
        }))
        .expect("descriptor parses");
        assert!(matches!(ranked.pick(), Pick::Top(_)));
        assert_eq!(
            ranked.content_type(),
            Some(Type::Entity),
            "the domain's type"
        );
        assert_eq!(ranked.among().len(), 2);
        assert!(ranked.reads_elected());
        assert_eq!(
            serde_json::to_value(&ranked).expect("serializes"),
            serde_json::json!({ "the": "job/status", "as": ["case:suspended", "case:active"] })
        );

        let chain: AttributeDescriptor = serde_json::from_value(serde_json::json!({
            "the": ["user/email", "user/phone"],
            "as": "text:"
        }))
        .expect("descriptor parses");
        assert!(matches!(chain.pick(), Pick::Top(_)));
        assert!(chain.is_chain());
        assert_eq!(chain.relations().count(), 2);
        assert_eq!(
            serde_json::to_value(&chain).expect("serializes"),
            serde_json::json!({ "the": ["user/email", "user/phone"], "as": "text:" })
        );
        assert!(
            serde_json::from_value::<AttributeDescriptor>(serde_json::json!({
                "the": ["user/email", "user/phone"],
                "as": "text:",
                "pick": "all:"
            }))
            .is_err(),
            "a relation chain reads as top only"
        );

        let many: AttributeDescriptor = serde_json::from_value(serde_json::json!({
            "the": "job/tag",
            "as": "text:",
            "cardinality": "many"
        }))
        .expect("descriptor parses");
        assert_eq!(many.pick(), &Pick::All, "the older spelling");
        assert_eq!(
            serde_json::to_value(&many).expect("serializes"),
            serde_json::json!({ "the": "job/tag", "as": "text:", "pick": "all:" })
        );
        let named: AttributeDescriptor = serde_json::from_value(serde_json::json!({
            "the": "job/tag",
            "as": "text:",
            "pick": "all"
        }))
        .expect("descriptor parses");
        assert_eq!(named.pick(), &Pick::All, "the spelling 0.2 wrote");
        assert_eq!(named.to_uri(), many.to_uri());
        let one: AttributeDescriptor = serde_json::from_value(serde_json::json!({
            "the": "job/tag",
            "as": "text:"
        }))
        .expect("descriptor parses");
        assert_eq!(one.pick(), &Pick::Last);
        assert!(
            serde_json::to_value(&one)
                .expect("serializes")
                .get("pick")
                .is_none()
        );
    }

    /// A pick is part of the attribute: two reads of one relation
    /// under different picks are two attributes. `last` and `all`
    /// are what `cardinality: one` and `many` always said, so they
    /// are the same attributes, whichever way they are spelled.
    #[dialog_common::test]
    fn it_tells_attributes_apart_by_policy() {
        let one =
            AttributeDescriptor::new(the!("job/status"), "", Cardinality::One, Some(Type::Entity));
        let many = AttributeDescriptor::new(
            the!("job/status"),
            "",
            Cardinality::Many,
            Some(Type::Entity),
        );
        let ranked = one.clone().with_domain(vec![Value::Boolean(true)]);
        let chained = one
            .clone()
            .with_then(vec!["job/fallback".parse().expect("a relation")]);
        let newest = many.clone().with_pick(Pick::Last);
        let every = one.clone().with_pick(Pick::All);
        assert_ne!(one.to_uri(), ranked.to_uri());
        assert_ne!(one.to_uri(), chained.to_uri());
        assert_ne!(one.to_uri(), many.to_uri());
        assert_eq!(newest.to_uri(), one.to_uri(), "`last` is cardinality one");
        assert_eq!(every.to_uri(), many.to_uri(), "`all` is cardinality many");
        assert_eq!(newest.cardinality(), Cardinality::One);
        assert_eq!(every.cardinality(), Cardinality::Many);
        assert_eq!(
            ranked.cardinality(),
            Cardinality::One,
            "a ranked read is one value"
        );
        assert!(
            matches!(ranked.pick(), Pick::Top(_)),
            "a listed domain reads as top"
        );
        assert!(
            matches!(chained.pick(), Pick::Top(_)),
            "a relation chain reads as top"
        );
    }

    /// A pick that does not fit its attribute is named: `top` without
    /// listed values or relations, `max` over an unordered carrier.
    #[dialog_common::test]
    fn it_names_a_policy_its_attribute_cannot_read_under() {
        let status = |pick: Pick, among: Vec<Value>, kind: Type| {
            AttributeDescriptor::new(the!("job/status"), "", Cardinality::One, Some(kind))
                .with_domain(among)
                .with_pick(pick)
                .pick_error()
        };
        assert!(status(Pick::Top(Vec::new()), Vec::new(), Type::Entity).is_some());
        assert!(
            status(
                Pick::Top(Vec::new()),
                vec![Value::Boolean(true)],
                Type::Entity
            )
            .is_none()
        );
        assert!(
            status(Pick::All, vec![Value::Boolean(true)], Type::Entity).is_none(),
            "a listed domain read as a set is an enum-typed set"
        );
        assert!(status(Pick::Max, Vec::new(), Type::Boolean).is_some());
        assert!(status(Pick::Max, Vec::new(), Type::String).is_none());
    }

    #[dialog_common::test]
    fn it_serializes_all_fields() {
        let attr = AttributeDescriptor::new(
            the!("io.gozala.person/name"),
            "Name of the person",
            Cardinality::One,
            Some(Type::String),
        );
        let json: serde_json::Value = serde_json::to_value(&attr).unwrap();
        assert_eq!(json["the"], "io.gozala.person/name");
        assert_eq!(json["description"], "Name of the person");
        assert_eq!(json["as"], "text:");
        assert!(
            json.get("cardinality").is_none() && json.get("pick").is_none(),
            "`last` is the pick a plain attribute implies: {json}"
        );
    }

    #[dialog_common::test]
    fn it_serializes_many_cardinality() {
        let attr = AttributeDescriptor::new(
            the!("person/email"),
            "Email addresses",
            Cardinality::Many,
            Some(Type::String),
        );
        let json: serde_json::Value = serde_json::to_value(&attr).unwrap();
        assert_eq!(json["pick"], "all:", "many is spelled as its pick");
        assert!(json.get("cardinality").is_none(), "{json}");
    }

    #[dialog_common::test]
    fn it_omits_as_when_type_is_none() {
        let attr = AttributeDescriptor::new(
            the!("person/data"),
            "Arbitrary data",
            Cardinality::One,
            None,
        );
        let json: serde_json::Value = serde_json::to_value(&attr).unwrap();
        assert!(json.get("as").is_none() || json["as"].is_null());
    }

    #[dialog_common::test]
    fn it_serializes_all_value_types() {
        let cases: Vec<(Type, &str)> = vec![
            (Type::Bytes, "bytes:"),
            (Type::Entity, "entity:"),
            (Type::Boolean, "boolean:"),
            (Type::String, "text:"),
            (Type::UnsignedInt, "natural:"),
            (Type::SignedInt, "integer:"),
            (Type::Float, "float:"),
            (Type::Record, "record:"),
            (Type::Symbol, "symbol:"),
        ];
        for (ty, expected_name) in cases {
            let attr =
                AttributeDescriptor::new(the!("test/field"), "test", Cardinality::One, Some(ty));
            let json: serde_json::Value = serde_json::to_value(&attr).unwrap();
            assert_eq!(
                json["as"], expected_name,
                "Type {:?} should serialize as {expected_name}",
                ty
            );
        }
    }

    #[dialog_common::test]
    fn it_deserializes_all_fields() {
        let json = r#"{
            "the": "io.gozala.person/name",
            "description": "Name of the person",
            "cardinality": "one",
            "as": "text:"
        }"#;
        let attr: AttributeDescriptor = serde_json::from_str(json).unwrap();
        assert_eq!(attr.domain(), "io.gozala.person");
        assert_eq!(attr.name(), Some("name"));
        assert_eq!(attr.description(), "Name of the person");
        assert_eq!(attr.cardinality(), Cardinality::One);
        assert_eq!(attr.content_type(), Some(Type::String));
    }

    #[dialog_common::test]
    fn it_defaults_optional_fields() {
        let json = r#"{ "the": "person/name" }"#;
        let attr: AttributeDescriptor = serde_json::from_str(json).unwrap();
        assert_eq!(attr.domain(), "person");
        assert_eq!(attr.name(), Some("name"));
        assert_eq!(attr.description(), "");
        assert_eq!(attr.cardinality(), Cardinality::One);
        assert_eq!(attr.content_type(), None);
    }

    #[dialog_common::test]
    fn it_deserializes_many_cardinality() {
        let json = r#"{
            "the": "person/email",
            "cardinality": "many",
            "as": "text:"
        }"#;
        let attr: AttributeDescriptor = serde_json::from_str(json).unwrap();
        assert_eq!(attr.cardinality(), Cardinality::Many);
    }

    #[dialog_common::test]
    fn it_round_trips() {
        let original = AttributeDescriptor::new(
            the!("diy.cook/quantity"),
            "Amount needed",
            Cardinality::Many,
            Some(Type::UnsignedInt),
        );
        let json = serde_json::to_string(&original).unwrap();
        let restored: AttributeDescriptor = serde_json::from_str(&json).unwrap();
        assert_eq!(original, restored);
    }

    #[dialog_common::test]
    fn it_rejects_missing_the() {
        let json = r#"{ "description": "oops", "as": "text:" }"#;
        let result = serde_json::from_str::<AttributeDescriptor>(json);
        assert!(result.is_err(), "should reject attribute without 'the'");
    }

    #[dialog_common::test]
    fn it_rejects_the_without_slash() {
        let json = r#"{ "the": "no-slash-here" }"#;
        let result = serde_json::from_str::<AttributeDescriptor>(json);
        assert!(result.is_err(), "should reject 'the' without '/' separator");
    }

    #[dialog_common::test]
    fn it_rejects_empty_the() {
        let json = r#"{ "the": "" }"#;
        let result = serde_json::from_str::<AttributeDescriptor>(json);
        assert!(result.is_err(), "should reject empty 'the'");
    }

    #[dialog_common::test]
    fn it_ignores_type_field() {
        let json = r#"{ "the": "person/name", "type": "Text" }"#;
        let attr: AttributeDescriptor = serde_json::from_str(json).unwrap();
        assert_eq!(
            attr.content_type(),
            None,
            "'type' field should be ignored — must use 'as'"
        );
    }

    #[dialog_common::test]
    fn it_rejects_unknown_type() {
        let json = r#"{ "the": "person/name", "as": "Blob" }"#;
        let result = serde_json::from_str::<AttributeDescriptor>(json);
        assert!(result.is_err(), "should reject unknown type name 'Blob'");
    }

    #[dialog_common::test]
    fn it_rejects_invalid_cardinality() {
        let json = r#"{ "the": "person/name", "cardinality": "few" }"#;
        let result = serde_json::from_str::<AttributeDescriptor>(json);
        assert!(result.is_err(), "should reject invalid cardinality 'few'");
    }

    #[dialog_common::test]
    fn it_rejects_the_exceeding_max_length() {
        let long = format!("{}/{}", "a".repeat(50), "b".repeat(50));
        let json = format!(r#"{{ "the": "{long}" }}"#);
        let result = serde_json::from_str::<AttributeDescriptor>(&json);
        assert!(
            result.is_err(),
            "should reject 'the' exceeding max selector length"
        );
    }

    #[dialog_common::test]
    fn it_rejects_old_domain_name_format() {
        let json = r#"{
            "domain": "person",
            "name": "email",
            "description": "Email",
            "type": "String"
        }"#;
        let result = serde_json::from_str::<AttributeDescriptor>(json);
        assert!(
            result.is_err(),
            "should reject old format using domain/name/type fields"
        );
    }

    #[dialog_common::test]
    fn it_rejects_non_string_type() {
        let json = r#"{ "the": "person/name", "as": 42 }"#;
        let result = serde_json::from_str::<AttributeDescriptor>(json);
        assert!(result.is_err(), "should reject non-string type value");
    }

    #[dialog_common::test]
    fn it_rejects_non_string_cardinality() {
        let json = r#"{ "the": "person/name", "cardinality": 1 }"#;
        let result = serde_json::from_str::<AttributeDescriptor>(json);
        assert!(
            result.is_err(),
            "should reject non-string cardinality value"
        );
    }
}

#[cfg(test)]
mod collection_tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use crate::the;
    use dialog_artifacts::NameShape;
    use std::str::FromStr;

    fn domain() -> Symbol {
        Symbol::from_str("todo.list").expect("a valid domain")
    }

    fn collection(keyed: Keyed) -> AttributeDescriptor {
        AttributeDescriptor::over(
            The::collection(domain(), keyed),
            "the list's members",
            Cardinality::Many,
            Some(Type::Entity),
        )
    }

    /// A relation spells as `domain/name`, a collection with its key
    /// kind bracketed, and either form parses back.
    #[dialog_common::test]
    fn it_spells_and_parses_a_relation() {
        let sequence = The::collection(
            Symbol::from_str("todo.list").expect("a valid domain"),
            Keyed::Sequence,
        );
        assert_eq!(sequence.to_string(), "todo.list/[position]");
        let parse = |text: &str| text.parse::<The>().expect("a relation parses");
        assert_eq!(parse("todo.list/[position]"), sequence);
        assert_eq!(
            parse("todo.list/[symbol]"),
            The::collection(
                Symbol::from_str("todo.list").expect("a valid domain"),
                Keyed::Dictionary
            )
        );
        assert_eq!(
            parse("todo.list/title"),
            The::Relation(the!("todo.list/title"))
        );
        assert!("todo.list/[list]".parse::<The>().is_err());
    }

    /// A plain attribute selects itself: the query pins `the` to a
    /// constant, exactly as before collections existed.
    #[dialog_common::test]
    fn it_selects_one_attribute_by_constant() {
        let descriptor = AttributeDescriptor::new(
            the!("todo.list/title"),
            "the list's title",
            Cardinality::One,
            Some(Type::String),
        );
        assert!(
            matches!(descriptor.the().term("title"), Term::Constant(_)),
            "an attribute pins `the`"
        );
        assert_eq!(descriptor.domain(), "todo.list");
        assert_eq!(descriptor.name(), Some("title"));
    }

    /// A collection selects a whole domain: the query leaves `the` a
    /// variable, refined by the domain and the key kind, so the scan
    /// covers exactly the demanded half.
    #[dialog_common::test]
    fn it_selects_a_collection_by_refined_variable() {
        for (keyed, shape) in [
            (Keyed::Sequence, NameShape::Position),
            (Keyed::Dictionary, NameShape::Symbol),
        ] {
            let descriptor = collection(keyed);
            let term = descriptor.the().term("member");
            assert!(
                matches!(term, Term::Variable { .. }),
                "a collection leaves `the` open"
            );
            let refinement = term
                .kind()
                .as_ref()
                .and_then(Kind::refinement)
                .cloned()
                .expect("the term carries a refinement");

            assert_eq!(
                refinement.prefix.as_deref(),
                Some("todo.list/"),
                "the domain becomes the scan's prefix, separator included"
            );
            assert_eq!(
                refinement.name_shape,
                Some(shape),
                "the key kind becomes the name shape"
            );
        }
    }

    /// A collection has no single name: its name half is a per-entry
    /// key, not part of the selector.
    #[dialog_common::test]
    fn it_has_no_name_for_a_collection() {
        let descriptor = collection(Keyed::Sequence);
        assert_eq!(descriptor.domain(), "todo.list");
        assert_eq!(descriptor.name(), None);
        assert_eq!(descriptor.the().attribute(), None);
    }

    /// Writing one value to a collection is refused rather than
    /// guessed at: every entry needs its own key, which the schema
    /// does not carry.
    #[dialog_common::test]
    fn it_refuses_to_write_a_collection_without_a_key() {
        let descriptor = collection(Keyed::Sequence);
        let entity = Entity::new().expect("an entity");
        assert_eq!(
            descriptor.resolve(Value::Entity(entity)),
            Err(FieldTypeError::UnkeyedCollection {
                domain: "todo.list".to_owned()
            })
        );
    }

    /// Adding the collection variant must not move any existing
    /// attribute's identity: a plain attribute hashes exactly what it
    /// hashed before, and the two key kinds are distinct from it and
    /// from each other.
    #[dialog_common::test]
    fn it_keeps_identities_distinct_and_stable() {
        let attribute = AttributeDescriptor::new(
            the!("todo.list/title"),
            "",
            Cardinality::Many,
            Some(Type::Entity),
        );
        let sequence = collection(Keyed::Sequence);
        let dictionary = collection(Keyed::Dictionary);

        assert_ne!(sequence.hash(), dictionary.hash(), "key kinds differ");
        assert_ne!(
            sequence.hash(),
            attribute.hash(),
            "a collection is not an attribute"
        );

        // The encoding a plain attribute produces is the one it
        // produced before collections existed: domain, name,
        // cardinality, type — nothing added, nothing reordered.
        let cbor = attribute.to_cbor_bytes();
        let decoded: serde_json::Value =
            serde_ipld_dagcbor::from_slice(&cbor).expect("valid dag-cbor");
        assert_eq!(decoded["domain"], "todo.list");
        assert_eq!(decoded["name"], "title");
    }

    /// The wire form round-trips, and a stored attribute still reads
    /// as one: `"the": "todo.list/title"` is untagged, so documents
    /// written before this variant existed load unchanged.
    #[dialog_common::test]
    fn it_round_trips_through_serde() {
        let stored = r#"{"the":"todo.list/title","as":"Entity","cardinality":"one"}"#;
        let attribute: AttributeDescriptor =
            serde_json::from_str(stored).expect("a stored attribute still loads");
        assert_eq!(attribute.name(), Some("title"));

        let sequence = collection(Keyed::Sequence);
        let json = serde_json::to_string(&sequence).expect("serializes");
        let back: AttributeDescriptor = serde_json::from_str(&json).expect("deserializes");
        assert_eq!(back, sequence, "a collection survives the round trip");
    }
}
