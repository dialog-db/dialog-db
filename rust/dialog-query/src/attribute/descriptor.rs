use crate::Parameters;
use crate::attribute::Relation;
use crate::attribute::query::AttributeQuery;
use crate::corpus::{CorpusRelation, Entity, Value};
use crate::error::{FieldTypeError, TypeError};
use crate::schema::{Cardinality, estimate_cost};
use crate::term::Term;
use crate::type_system::Type as Kind;
use crate::types::Any;
use crate::types::Type;
use dialog_corpus::{NameShape, Symbol};

use crate::identity::{self, ATTRIBUTE, field};
use base58::ToBase58;
use dialog_corpus::identity::{ENTITY, SYMBOL};
use merkle_reference::{Reference, Type as ReferenceType};
use serde::de::IgnoredAny;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter, Result as FmtResult};
use std::str::FromStr;

/// A validated attribute–value pair with the cardinality it is written
/// under,
/// produced by [`AttributeDescriptor::resolve`]. Used inside
/// [`ConceptStatement`](crate::concept::descriptor::ConceptStatement) to
/// represent the set of facts that make up a concept instance.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Attribution {
    /// The fully-qualified attribute selector.
    pub the: CorpusRelation,
    /// The resolved value for this attribute.
    pub is: Value,
    /// The cardinality the attribute is written under: a cardinality
    /// one field's write succeeds the newest claim the cell holds, a
    /// cardinality many field's lands beside the others.
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
    /// `dialog_corpus::NameShape`.
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
    pub fn attribute(&self) -> Option<CorpusRelation> {
        match self {
            The::Relation(the) => Some(CorpusRelation::from(the)),
            The::Collection { .. } => None,
        }
    }

    /// The attribute one entry of a collection is written under:
    /// `domain/key`, where the key must have the collection's name
    /// shape — a position for a sequence, a symbol for a dictionary.
    /// A plain attribute ignores the key and is its own entry.
    pub fn entry(&self, key: &str) -> Result<CorpusRelation, FieldTypeError> {
        match self {
            The::Relation(the) => Ok(CorpusRelation::from(the)),
            The::Collection { domain, keyed } => {
                let mismatch = || FieldTypeError::KeyShape {
                    domain: domain.as_str().to_owned(),
                    key: key.to_owned(),
                    keyed: *keyed,
                };
                let attribute = CorpusRelation::try_from(format!("{}/{key}", domain.as_str()))
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

/// Static metadata for an attribute: the relation it reads, its
/// human-readable description, its value type or listed value domain,
/// and its cardinality.
///
/// An attribute is a relation, a type and a cardinality. Under
/// cardinality one a read returns the cell's newest claim and a write
/// succeeds it; under cardinality many a read returns every claim and a
/// write adds one. `as: [v1, v2]` lists the values the attribute holds,
/// an enumerated type with no ranking meaning. A read that ranks
/// candidates (the greatest, the least, the best listed) is a rule whose
/// `reduce` clause folds them with `max`, `min` or `top`. Rules derive
/// into relations and are found by them, so two attributes over one
/// relation under different types or cardinalities are two attributes.
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
    /// The relation read.
    the: The,
    description: String,
    content_type: Option<Type>,
    /// The values `as` lists: the domain a value is one of.
    among: Vec<Value>,
    /// How many claims a cell holds: what a read returns and what a
    /// write does.
    cardinality: Cardinality,
}

/// The wire form of an [`AttributeDescriptor`]: `the` one relation, `as`
/// a type or a value list, and `cardinality` one (the default, and not
/// written) or many.
///
/// An earlier release spelled a `pick` (or its preview spelling,
/// `select`) and could list relations under `the`, a ranked chain. Both
/// are refused by name, so a definition in that format is never read
/// with another meaning than it was written with; the migration in
/// `dialog_query::migration` rewrites the ones that can be rewritten.
#[derive(Serialize, Deserialize)]
struct Wire {
    the: OneOrMany<The>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    description: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    cardinality: Option<CardinalityName>,
    #[serde(rename = "as", default, skip_serializing_if = "Option::is_none")]
    content: Option<As>,
    /// The pick an earlier release spelled: refused.
    #[serde(default, skip_serializing)]
    pick: Option<IgnoredAny>,
    /// The preview spelling of `pick`: refused.
    #[serde(default, skip_serializing)]
    select: Option<IgnoredAny>,
}

/// A cardinality as an attribute's wire form spells it: `one` or
/// `many`, and nothing else.
#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
enum CardinalityName {
    One,
    Many,
}

impl From<CardinalityName> for Cardinality {
    fn from(name: CardinalityName) -> Self {
        match name {
            CardinalityName::One => Cardinality::One,
            CardinalityName::Many => Cardinality::Many,
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

/// Why an attribute an earlier release spelled with a `pick` is not
/// read.
pub const PICK_REFUSED: &str = "an attribute no longer has a `pick`: it is written with \
     `cardinality: one` (what `last` read) or `cardinality: many` (what `all` read), and a \
     read under `max`, `min` or `top` is a rule whose `reduce` folds with that aggregator";

/// Why an attribute listing relations under `the`, a ranked chain an
/// earlier release read, is not read.
pub const CHAIN_REFUSED: &str = "`the` names one relation: a list of relations, ranked, \
     is no longer read; a rule reading each relation and folding with `top` replaces it";

impl TryFrom<Wire> for AttributeDescriptor {
    type Error = String;

    fn try_from(wire: Wire) -> Result<Self, Self::Error> {
        if wire.pick.is_some() || wire.select.is_some() {
            return Err(PICK_REFUSED.to_string());
        }
        let the = match wire.the {
            OneOrMany::One(the) => the,
            OneOrMany::Many(_) => return Err(CHAIN_REFUSED.to_string()),
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
        Ok(AttributeDescriptor {
            the,
            description: wire.description,
            content_type,
            among,
            cardinality: wire.cardinality.map(Cardinality::from).unwrap_or_default(),
        })
    }
}

impl From<AttributeDescriptor> for Wire {
    fn from(descriptor: AttributeDescriptor) -> Self {
        let content = if descriptor.among.is_empty() {
            descriptor.content_type.map(As::Kind)
        } else {
            Some(As::Domain(descriptor.among))
        };
        Wire {
            the: OneOrMany::One(descriptor.the),
            description: descriptor.description,
            cardinality: match descriptor.cardinality {
                Cardinality::One => None,
                Cardinality::Many => Some(CardinalityName::Many),
            },
            content,
            pick: None,
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
            description: description.into(),
            content_type,
            among: Vec::new(),
            cardinality,
        }
    }

    /// This descriptor over the listed value domain: the values the
    /// attribute holds. The list carries no ranking.
    pub fn with_domain(mut self, values: Vec<Value>) -> Self {
        self.among = values;
        self
    }

    /// This descriptor under `cardinality`.
    pub fn with_cardinality(mut self, cardinality: Cardinality) -> Self {
        self.cardinality = cardinality;
        self
    }

    /// The values `as` lists, if it lists any.
    pub fn among(&self) -> &[Value] {
        &self.among
    }

    /// This descriptor's relation read plainly, under its cardinality
    /// alone and with no listed domain: the relation itself, which is
    /// what rules derive into.
    pub fn plain(mut self) -> Self {
        self.among = Vec::new();
        self
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

    /// This descriptor with no description: what identity sees.
    pub(crate) fn without_description(&self) -> Self {
        Self {
            description: String::new(),
            ..self.clone()
        }
    }

    /// Returns the human-readable description.
    pub fn description(&self) -> &str {
        &self.description
    }

    /// Returns the cardinality: under one a read returns the cell's
    /// newest claim and a write succeeds it, under many a read returns
    /// every claim and a write adds one.
    pub fn cardinality(&self) -> Cardinality {
        self.cardinality
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
            Some(expected) => expected.holds(&value),
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
                cardinality: self.cardinality,
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
        estimate_cost(self.cardinality(), true, of, is)
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

    /// The attribute's identity: its merkle reference under
    /// [`ATTRIBUTE`](crate::identity::ATTRIBUTE).
    ///
    /// It maps `the` to the symbol of the relation the attribute reads,
    /// `cardinality` to the symbol naming its cardinality (`one` or
    /// `many`), `as` to the entity naming its type, when it declares one,
    /// and `among` to the values it lists, when it lists any. A
    /// collection reads as the symbol its relation slot spells,
    /// `todo.list/[symbol]`, which no relation can spell. The description
    /// is not part of it. Two attributes over one relation under
    /// different types or cardinalities are two attributes.
    ///
    /// The release before this one hashed a `pick` where this hashes the
    /// cardinality, and the relation as a list of one.
    pub fn reference(&self) -> Reference {
        let mut entries = vec![
            (
                field::THE,
                SYMBOL.reference_of(self.the.to_string().as_bytes()),
            ),
            (
                field::CARDINALITY,
                SYMBOL.reference_of(self.cardinality.name().as_bytes()),
            ),
        ];
        if let Some(content_type) = self.content_type {
            entries.push((
                field::AS,
                ENTITY.reference_of(content_type.uri().as_bytes()),
            ));
        }
        if !self.among.is_empty() {
            entries.push((
                field::AMONG,
                ReferenceType::LIST.list(self.among.iter().map(Value::reference)),
            ));
        }
        ATTRIBUTE.map(entries)
    }

    /// The attribute's entity: `the:` followed by its
    /// [reference](Self::reference) in base58.
    pub fn to_uri(&self) -> String {
        format!("the:{}", self.reference().as_bytes().to_base58())
    }

    /// The reference an attribute entity, `the:{base58}`, names, or
    /// `None` if `uri` is not one.
    pub fn parse_uri(uri: &str) -> Option<Reference> {
        identity::parse("the:", uri)
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
impl From<&AttributeDescriptor> for CorpusRelation {
    fn from(descriptor: &AttributeDescriptor) -> Self {
        descriptor
            .the
            .attribute()
            .expect("a keyed collection has no single attribute")
    }
}

impl From<AttributeDescriptor> for CorpusRelation {
    fn from(descriptor: AttributeDescriptor) -> Self {
        CorpusRelation::from(&descriptor)
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

    /// A listed `as` is an enumerated type: the values the attribute
    /// holds, in no ranking order, read under the cardinality the
    /// attribute declares. It is written back as listed.
    #[dialog_common::test]
    fn it_reads_a_listed_domain_as_an_enumerated_type() {
        let listed: AttributeDescriptor = serde_json::from_value(serde_json::json!({
            "the": "job/status",
            "as": ["case:suspended", "case:active"]
        }))
        .expect("descriptor parses");
        assert_eq!(listed.cardinality(), Cardinality::One);
        assert_eq!(
            listed.content_type(),
            Some(Type::Entity),
            "the domain's type"
        );
        assert_eq!(listed.among().len(), 2);
        assert_eq!(
            serde_json::to_value(&listed).expect("serializes"),
            serde_json::json!({ "the": "job/status", "as": ["case:suspended", "case:active"] })
        );
        let many: AttributeDescriptor = serde_json::from_value(serde_json::json!({
            "the": "job/status",
            "as": ["case:suspended", "case:active"],
            "cardinality": "many"
        }))
        .expect("descriptor parses");
        assert_eq!(many.cardinality(), Cardinality::Many);
        assert_ne!(many.to_uri(), listed.to_uri());
    }

    /// An attribute spelled with a `pick`, in its preview spelling
    /// `select`, or over a list of relations, as the release before this
    /// one read it, is refused by name, whatever the pick: it is never
    /// read with another meaning than it was written with.
    #[dialog_common::test]
    fn it_refuses_an_attribute_spelled_with_a_pick_or_a_chain() {
        for pick in ["last:", "all:", "max:", "min:", "top:", "last", "all"] {
            for key in ["pick", "select"] {
                let mut spelled = serde_json::json!({ "the": "job/tag", "as": "text:" });
                spelled[key] = pick.into();
                let error = serde_json::from_value::<AttributeDescriptor>(spelled)
                    .expect_err("a pick is refused");
                assert!(
                    error.to_string().contains("no longer has a `pick`"),
                    "{error}"
                );
            }
        }
        let error = serde_json::from_value::<AttributeDescriptor>(serde_json::json!({
            "the": ["user/email", "user/phone"],
            "as": "text:"
        }))
        .expect_err("a chain is refused");
        assert!(error.to_string().contains("names one relation"), "{error}");
    }

    /// Cardinality is part of the attribute: two attributes over one
    /// relation under different cardinalities, or listing different
    /// values, are two attributes.
    #[dialog_common::test]
    fn it_tells_attributes_apart_by_cardinality() {
        let one =
            AttributeDescriptor::new(the!("job/status"), "", Cardinality::One, Some(Type::Entity));
        let many = AttributeDescriptor::new(
            the!("job/status"),
            "",
            Cardinality::Many,
            Some(Type::Entity),
        );
        let listed = one.clone().with_domain(vec![Value::Boolean(true)]);
        assert_ne!(one.to_uri(), many.to_uri());
        assert_ne!(one.to_uri(), listed.to_uri());
        assert_eq!(one.clone().with_cardinality(Cardinality::Many), many);
        assert_eq!(listed.cardinality(), Cardinality::One);
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
            json.get("cardinality").is_none(),
            "one is the cardinality a plain attribute implies: {json}"
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
        assert_eq!(json["cardinality"], "many");
        assert!(json.get("pick").is_none(), "{json}");
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
    use dialog_corpus::NameShape;
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

    /// A collection's identity is distinct from a plain attribute's, and
    /// the two key kinds are distinct from each other.
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

        assert_ne!(
            sequence.reference(),
            dictionary.reference(),
            "key kinds differ"
        );
        assert_ne!(
            sequence.reference(),
            attribute.reference(),
            "a collection is not an attribute"
        );

        // A collection reads as the symbol its relation slot spells.
        let symbol = |text: &str| SYMBOL.reference_of(text.as_bytes());
        let mut entries = vec![
            (field::THE, symbol("todo.list/[position]")),
            (field::CARDINALITY, symbol("many")),
        ];
        if let Some(content_type) = sequence.content_type() {
            entries.push((
                field::AS,
                ENTITY.reference_of(content_type.uri().as_bytes()),
            ));
        }
        assert_eq!(sequence.reference(), ATTRIBUTE.map(entries));
    }

    /// An attribute is referenced as a map from field names under
    /// `the:`: the relation it reads, its cardinality, its type, and the
    /// values it lists. Nothing else, the description included.
    #[dialog_common::test]
    fn it_references_an_attribute_by_its_fields() {
        let attribute = AttributeDescriptor::new(
            the!("todo.list/title"),
            "The list's title",
            Cardinality::One,
            Some(Type::String),
        );
        let expected = ATTRIBUTE.map([
            (field::THE, SYMBOL.reference_of(b"todo.list/title")),
            (field::CARDINALITY, SYMBOL.reference_of(b"one")),
            (field::AS, ENTITY.reference_of(b"text:")),
        ]);
        assert_eq!(attribute.reference(), expected);
        assert_eq!(
            attribute.to_uri(),
            format!("the:{}", expected.as_bytes().to_base58())
        );

        // Listed values enter by their own references, in order.
        let listed = attribute.clone().with_domain(vec![
            Value::String("todo".into()),
            Value::String("done".into()),
        ]);
        let reordered = attribute.with_domain(vec![
            Value::String("done".into()),
            Value::String("todo".into()),
        ]);
        assert_ne!(listed.reference(), reordered.reference());
    }

    /// Listed numbers are hashed by value alone: a natural, an integer and
    /// a float of the same number are one value. The value index still
    /// keeps them apart, so these attributes share an identity while
    /// reading different claims until it moves to one band.
    #[dialog_common::test]
    fn it_hashes_listed_numbers_by_value_alone() {
        let status = || AttributeDescriptor::new(the!("task/priority"), "", Cardinality::One, None);
        let natural = status().with_domain(vec![Value::UnsignedInt(1), Value::UnsignedInt(2)]);
        let integer = status().with_domain(vec![Value::SignedInt(1), Value::SignedInt(2)]);
        let float = status().with_domain(vec![Value::Float(1.0), Value::Float(2.0)]);
        let halves = status().with_domain(vec![Value::Float(1.5), Value::Float(2.0)]);
        assert_eq!(natural.reference(), integer.reference());
        assert_eq!(integer.reference(), float.reference());
        assert_ne!(float.reference(), halves.reference());
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
