//! A rule's identity: the merkle reference of its canonical spelling.
//!
//! A rule is referenced as a map under [`RULE`] from its parts:
//!
//! - its head, under `deduce` (or `assert!` / `retract!` for an inductive
//!   rule): the head concept by its reference, and each head field bound to
//!   the canonical variable it names;
//! - `when`: the set of its positive premises;
//! - `unless`: the set of its negated premises, when there are any;
//! - `reduce`: each reduced head field to its aggregation, when there are any.
//!
//! A concept premise is the concept by its reference and its `where`
//! bindings, each keyed by the field it binds (the field's attribute and
//! options), not by a field name, since field names are notation. Every other
//! premise is referenced by its wire form, read as a merkle structure: maps,
//! lists, text, numbers and booleans as dialog's own types, and a constant,
//! which the wire form writes as `{"<type entity>": payload}`, as the value it
//! is ([`Value::reference`](crate::Value::reference)).
//!
//! Concepts enter by reference, so a rule is the same whether its concepts
//! are embedded or linked, and nothing a concept's identity leaves out, its
//! description or its attributes' descriptions, can change a rule's.

use std::collections::BTreeSet;
use std::{error, fmt};

use dialog_artifacts::identity::{BOOLEAN, ENTITY, NUMBER, RECORD, SYMBOL, TEXT};
use merkle_reference::{Reference, Type};
use serde::Serialize;
use serde::ser::{self, SerializeMap as _};

use crate::attribute::The;
use crate::concept::descriptor::{ConceptDescriptor, ConceptFieldDescriptor};
use crate::concept::query::ConceptQuery;
use crate::identity::{NULL, RULE, field};
use crate::negation::Negation;
use crate::premise::Premise;
use crate::proposition::Proposition;
use crate::reduce::ReduceSpec;
use crate::term::Term;
use crate::types::Any;

/// The reference of a rule concluding `conclusion` under `head` (`deduce`,
/// `assert!` or `retract!`) from `premises` and `reduce`, or `None` if a part
/// has no wire form.
pub(crate) fn rule(
    head: &str,
    conclusion: &ConceptDescriptor,
    premises: &[Premise],
    reduce: &[(String, ReduceSpec)],
) -> Option<Reference> {
    let mut when = Vec::new();
    let mut unless = Vec::new();
    for premise in premises {
        match premise {
            Premise::Assert(proposition) => when.push(proposition),
            Premise::Unless(Negation(proposition)) => unless.push(proposition),
        }
    }
    described(
        head,
        conclusion,
        when,
        unless,
        reduce.iter().map(|(name, spec)| (name.as_str(), spec)),
    )
}

/// The reference of a rule from its parts as a descriptor holds them: the
/// positive and the negated propositions, and the reduced head fields.
pub(crate) fn described<'a>(
    head: &str,
    conclusion: &ConceptDescriptor,
    when: impl IntoIterator<Item = &'a Proposition>,
    unless: impl IntoIterator<Item = &'a Proposition>,
    reduce: impl IntoIterator<Item = (&'a str, &'a ReduceSpec)>,
) -> Option<Reference> {
    let when = when
        .into_iter()
        .map(proposition_reference)
        .collect::<Option<Vec<_>>>()?;
    let unless = unless
        .into_iter()
        .map(proposition_reference)
        .collect::<Option<Vec<_>>>()?;
    let mut entries = vec![
        (text(head), head_reference(conclusion)),
        (text("when"), Type::SET.set(when)),
    ];
    if !unless.is_empty() {
        entries.push((text("unless"), Type::SET.set(unless)));
    }
    let mut aggregations = Vec::new();
    for (name, spec) in reduce {
        aggregations.push((text(name), wire_reference(spec)?));
    }
    if !aggregations.is_empty() {
        entries.push((text("reduce"), Type::MAP.map(aggregations)));
    }
    Some(RULE.map(entries))
}

/// A head: the concept, and each field bound to the variable it names.
fn head_reference(conclusion: &ConceptDescriptor) -> Reference {
    let bindings = conclusion
        .with()
        .iter()
        .map(|(name, field)| (field_reference(field), text(name)));
    Type::MAP.map([
        (text("concept"), conclusion.reference()),
        (text("bind"), Type::MAP.map(bindings)),
    ])
}

/// A premise's proposition: a concept application by its concept and
/// bindings, anything else by its wire form.
fn proposition_reference(proposition: &Proposition) -> Option<Reference> {
    match proposition {
        Proposition::Concept(query) => concept_premise(query),
        other => wire_reference(other),
    }
}

/// A concept application: the concept by reference, and its bindings keyed
/// by the field each binds. A collection field binds its key and its value,
/// under `the` and `is`; a binding that names no field (the entity, `this`)
/// is keyed by its name.
fn concept_premise(query: &ConceptQuery) -> Option<Reference> {
    let collections: BTreeSet<&str> = query
        .predicate
        .collections()
        .map(|(name, _)| name)
        .collect();
    let mut named: BTreeSet<String> = BTreeSet::new();
    let mut bindings = Vec::new();
    for (name, field) in query.predicate.with().iter() {
        named.insert(name.to_string());
        let key_operand = The::key_operand(name);
        let binding = if collections.contains(name) {
            named.insert(key_operand.clone());
            let mut entry = Vec::new();
            if let Some(term) = query.terms.get(&key_operand) {
                entry.push((text("the"), term_reference(term)?));
            }
            if let Some(term) = query.terms.get(name) {
                entry.push((text("is"), term_reference(term)?));
            }
            Type::MAP.map(entry)
        } else {
            match query.terms.get(name) {
                Some(term) => term_reference(term)?,
                None => continue,
            }
        };
        bindings.push((field_reference(field), binding));
    }
    for (name, term) in query.terms.iter() {
        if !named.contains(name.as_str()) {
            bindings.push((text(name), term_reference(term)?));
        }
    }
    Some(Type::MAP.map([
        (text("assert"), query.predicate.reference()),
        (text("where"), Type::MAP.map(bindings)),
    ]))
}

/// A concept field, as a binding's key: its attribute's reference and its
/// options, which together say what the field reads.
fn field_reference(field: &ConceptFieldDescriptor) -> Reference {
    let mut options = Vec::new();
    if field.is_optional() {
        options.push((field::OPTIONAL, field::TRUE));
    }
    if let Some(target) = field.conforms() {
        options.push((field::CONFORMS, target.reference()));
    }
    Type::LIST.list([field.descriptor().reference(), Type::MAP.map(options)])
}

fn term_reference(term: &Term<Any>) -> Option<Reference> {
    wire_reference(term)
}

fn text(value: &str) -> Reference {
    TEXT.reference_of(value.as_bytes())
}

/// The reference of `value`'s wire form, read as a merkle structure, or
/// `None` if it has no wire form.
pub(crate) fn wire_reference<T: Serialize + ?Sized>(value: &T) -> Option<Reference> {
    value.serialize(Wire).ok().map(|node| node.reference())
}

/// A wire form, as a tree.
enum Node {
    Unit,
    Bool(bool),
    /// A number's `dialog-number` encoding.
    Number(Vec<u8>),
    Text(String),
    Bytes(Vec<u8>),
    List(Vec<Node>),
    Map(Vec<(Node, Node)>),
}

impl Node {
    fn reference(&self) -> Reference {
        match self {
            Node::Map(entries) => {
                if let [(Node::Text(tag), payload)] = entries.as_slice()
                    && let Some(constant) = constant(tag, payload)
                {
                    return constant;
                }
                Type::MAP.map(
                    entries
                        .iter()
                        .map(|(key, value)| (key.reference(), value.reference())),
                )
            }
            Node::List(items) => Type::LIST.list(items.iter().map(Node::reference)),
            Node::Text(value) => text(value),
            Node::Bool(value) => BOOLEAN.reference_of(&[u8::from(*value)]),
            Node::Number(content) => NUMBER.reference_of(content),
            Node::Bytes(bytes) => Reference::hash(bytes),
            Node::Unit => NULL.reference_of(&[]),
        }
    }
}

/// A constant in its wire form, `{"<type entity>": payload}`, referenced as
/// the value it is, so it agrees with
/// [`Value::reference`](crate::Value::reference).
fn constant(tag: &str, payload: &Node) -> Option<Reference> {
    Some(match (tag, payload) {
        ("text:", Node::Text(value)) => TEXT.reference_of(value.as_bytes()),
        ("entity:", Node::Text(value)) => ENTITY.reference_of(value.as_bytes()),
        ("symbol:", Node::Text(value)) => SYMBOL.reference_of(value.as_bytes()),
        ("boolean:", Node::Bool(value)) => BOOLEAN.reference_of(&[u8::from(*value)]),
        ("natural:" | "integer:" | "float:", Node::Number(content)) => NUMBER.reference_of(content),
        ("bytes:", payload) => Reference::hash(&bytes_of(payload)?),
        ("record:", payload) => RECORD.reference_of(&bytes_of(payload)?),
        _ => return None,
    })
}

/// A byte string, written either as bytes or, as `Vec<u8>` serializes, as a
/// sequence of numbers from 0 to 255.
fn bytes_of(node: &Node) -> Option<Vec<u8>> {
    match node {
        Node::Bytes(bytes) => Some(bytes.clone()),
        Node::List(items) => items
            .iter()
            .map(|item| match item {
                Node::Number(content) => dialog_number::decode_i128(content)
                    .ok()
                    .flatten()
                    .and_then(|value| u8::try_from(value).ok()),
                _ => None,
            })
            .collect(),
        _ => None,
    }
}

/// Why a value has no wire form.
#[derive(Debug)]
struct Unwritable(String);

impl fmt::Display for Unwritable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl error::Error for Unwritable {}

impl ser::Error for Unwritable {
    fn custom<T: fmt::Display>(message: T) -> Self {
        Unwritable(message.to_string())
    }
}

/// A serializer that builds a value's wire form as a [`Node`] tree. It is a
/// binary format, so a [`Value`](crate::Value) writes itself tagged by its
/// type.
struct Wire;

fn number_of(encode: impl FnOnce(&mut Vec<u8>)) -> Node {
    let mut content = Vec::with_capacity(12);
    encode(&mut content);
    Node::Number(content)
}

impl ser::Serializer for Wire {
    type Ok = Node;
    type Error = Unwritable;
    type SerializeSeq = Sequence;
    type SerializeTuple = Sequence;
    type SerializeTupleStruct = Sequence;
    type SerializeTupleVariant = Variant<Sequence>;
    type SerializeMap = Entries;
    type SerializeStruct = Entries;
    type SerializeStructVariant = Variant<Entries>;

    fn is_human_readable(&self) -> bool {
        false
    }

    fn serialize_bool(self, value: bool) -> Result<Node, Unwritable> {
        Ok(Node::Bool(value))
    }
    fn serialize_i8(self, value: i8) -> Result<Node, Unwritable> {
        self.serialize_i128(i128::from(value))
    }
    fn serialize_i16(self, value: i16) -> Result<Node, Unwritable> {
        self.serialize_i128(i128::from(value))
    }
    fn serialize_i32(self, value: i32) -> Result<Node, Unwritable> {
        self.serialize_i128(i128::from(value))
    }
    fn serialize_i64(self, value: i64) -> Result<Node, Unwritable> {
        self.serialize_i128(i128::from(value))
    }
    fn serialize_i128(self, value: i128) -> Result<Node, Unwritable> {
        Ok(number_of(|out| dialog_number::encode_i128(value, out)))
    }
    fn serialize_u8(self, value: u8) -> Result<Node, Unwritable> {
        self.serialize_u128(u128::from(value))
    }
    fn serialize_u16(self, value: u16) -> Result<Node, Unwritable> {
        self.serialize_u128(u128::from(value))
    }
    fn serialize_u32(self, value: u32) -> Result<Node, Unwritable> {
        self.serialize_u128(u128::from(value))
    }
    fn serialize_u64(self, value: u64) -> Result<Node, Unwritable> {
        self.serialize_u128(u128::from(value))
    }
    fn serialize_u128(self, value: u128) -> Result<Node, Unwritable> {
        Ok(number_of(|out| dialog_number::encode_u128(value, out)))
    }
    fn serialize_f32(self, value: f32) -> Result<Node, Unwritable> {
        self.serialize_f64(f64::from(value))
    }
    fn serialize_f64(self, value: f64) -> Result<Node, Unwritable> {
        Ok(number_of(|out| dialog_number::encode_f64(value, out)))
    }
    fn serialize_char(self, value: char) -> Result<Node, Unwritable> {
        Ok(Node::Text(value.to_string()))
    }
    fn serialize_str(self, value: &str) -> Result<Node, Unwritable> {
        Ok(Node::Text(value.to_string()))
    }
    fn serialize_bytes(self, value: &[u8]) -> Result<Node, Unwritable> {
        Ok(Node::Bytes(value.to_vec()))
    }
    fn serialize_none(self) -> Result<Node, Unwritable> {
        Ok(Node::Unit)
    }
    fn serialize_some<T: Serialize + ?Sized>(self, value: &T) -> Result<Node, Unwritable> {
        value.serialize(self)
    }
    fn serialize_unit(self) -> Result<Node, Unwritable> {
        Ok(Node::Unit)
    }
    fn serialize_unit_struct(self, _name: &'static str) -> Result<Node, Unwritable> {
        Ok(Node::Unit)
    }
    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _index: u32,
        variant: &'static str,
    ) -> Result<Node, Unwritable> {
        Ok(Node::Text(variant.to_string()))
    }
    fn serialize_newtype_struct<T: Serialize + ?Sized>(
        self,
        _name: &'static str,
        value: &T,
    ) -> Result<Node, Unwritable> {
        value.serialize(self)
    }
    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        _name: &'static str,
        _index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<Node, Unwritable> {
        Ok(Node::Map(vec![(
            Node::Text(variant.to_string()),
            value.serialize(Wire)?,
        )]))
    }
    fn serialize_seq(self, length: Option<usize>) -> Result<Sequence, Unwritable> {
        Ok(Sequence(Vec::with_capacity(length.unwrap_or(0))))
    }
    fn serialize_tuple(self, length: usize) -> Result<Sequence, Unwritable> {
        Ok(Sequence(Vec::with_capacity(length)))
    }
    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        length: usize,
    ) -> Result<Sequence, Unwritable> {
        Ok(Sequence(Vec::with_capacity(length)))
    }
    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _index: u32,
        variant: &'static str,
        length: usize,
    ) -> Result<Variant<Sequence>, Unwritable> {
        Ok(Variant(variant, Sequence(Vec::with_capacity(length))))
    }
    fn serialize_map(self, length: Option<usize>) -> Result<Entries, Unwritable> {
        Ok(Entries::with_capacity(length.unwrap_or(0)))
    }
    fn serialize_struct(self, _name: &'static str, length: usize) -> Result<Entries, Unwritable> {
        Ok(Entries::with_capacity(length))
    }
    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _index: u32,
        variant: &'static str,
        length: usize,
    ) -> Result<Variant<Entries>, Unwritable> {
        Ok(Variant(variant, Entries::with_capacity(length)))
    }
}

/// A sequence being written.
struct Sequence(Vec<Node>);

impl ser::SerializeSeq for Sequence {
    type Ok = Node;
    type Error = Unwritable;
    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Unwritable> {
        self.0.push(value.serialize(Wire)?);
        Ok(())
    }
    fn end(self) -> Result<Node, Unwritable> {
        Ok(Node::List(self.0))
    }
}

impl ser::SerializeTuple for Sequence {
    type Ok = Node;
    type Error = Unwritable;
    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Unwritable> {
        ser::SerializeSeq::serialize_element(self, value)
    }
    fn end(self) -> Result<Node, Unwritable> {
        ser::SerializeSeq::end(self)
    }
}

impl ser::SerializeTupleStruct for Sequence {
    type Ok = Node;
    type Error = Unwritable;
    fn serialize_field<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Unwritable> {
        ser::SerializeSeq::serialize_element(self, value)
    }
    fn end(self) -> Result<Node, Unwritable> {
        ser::SerializeSeq::end(self)
    }
}

/// A map or struct being written.
struct Entries {
    entries: Vec<(Node, Node)>,
    key: Option<Node>,
}

impl Entries {
    fn with_capacity(capacity: usize) -> Self {
        Entries {
            entries: Vec::with_capacity(capacity),
            key: None,
        }
    }
}

impl ser::SerializeMap for Entries {
    type Ok = Node;
    type Error = Unwritable;
    fn serialize_key<T: Serialize + ?Sized>(&mut self, key: &T) -> Result<(), Unwritable> {
        self.key = Some(key.serialize(Wire)?);
        Ok(())
    }
    fn serialize_value<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Unwritable> {
        let key = self
            .key
            .take()
            .ok_or_else(|| Unwritable("a map value without a key".into()))?;
        self.entries.push((key, value.serialize(Wire)?));
        Ok(())
    }
    fn end(self) -> Result<Node, Unwritable> {
        Ok(Node::Map(self.entries))
    }
}

impl ser::SerializeStruct for Entries {
    type Ok = Node;
    type Error = Unwritable;
    fn serialize_field<T: Serialize + ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), Unwritable> {
        self.serialize_entry(key, value)
    }
    fn end(self) -> Result<Node, Unwritable> {
        ser::SerializeMap::end(self)
    }
}

/// An enum variant with fields, written `{variant: fields}`.
struct Variant<T>(&'static str, T);

impl ser::SerializeTupleVariant for Variant<Sequence> {
    type Ok = Node;
    type Error = Unwritable;
    fn serialize_field<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Unwritable> {
        ser::SerializeSeq::serialize_element(&mut self.1, value)
    }
    fn end(self) -> Result<Node, Unwritable> {
        Ok(Node::Map(vec![(
            Node::Text(self.0.to_string()),
            ser::SerializeSeq::end(self.1)?,
        )]))
    }
}

impl ser::SerializeStructVariant for Variant<Entries> {
    type Ok = Node;
    type Error = Unwritable;
    fn serialize_field<T: Serialize + ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), Unwritable> {
        self.1.serialize_entry(key, value)
    }
    fn end(self) -> Result<Node, Unwritable> {
        Ok(Node::Map(vec![(
            Node::Text(self.0.to_string()),
            ser::SerializeMap::end(self.1)?,
        )]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Value;
    use crate::artifact::Entity;
    use crate::attribute::Relation;
    use crate::rule::{DeductiveRule, DeductiveRuleDescriptor};

    /// A constant inside a premise's wire form references as the value it
    /// is, so a constant has one identity wherever it appears.
    #[dialog_common::test]
    fn it_references_a_wire_constant_as_its_value() {
        let entity: Entity = "foo:".parse().unwrap();
        let symbol: Relation = "person/name".parse().unwrap();
        for value in [
            Value::String("foo:".into()),
            Value::Entity(entity),
            Value::Symbol(symbol.into()),
            Value::Boolean(true),
            Value::UnsignedInt(5),
            Value::SignedInt(-5),
            Value::Float(5.5),
            Value::Bytes(vec![1, 2, 3]),
            Value::Record(vec![4, 5]),
        ] {
            assert_eq!(wire_reference(&value), Some(value.reference()), "{value:?}");
        }
        // A natural, an integer and a float of one number are one constant.
        assert_eq!(
            wire_reference(&Value::UnsignedInt(5)),
            wire_reference(&Value::Float(5.0))
        );
    }

    fn compile(json: serde_json::Value) -> DeductiveRule {
        let descriptor: DeductiveRuleDescriptor =
            serde_json::from_value(json).expect("descriptor parses");
        descriptor.compile().expect("rule compiles")
    }

    fn employee(person_description: &str, constant: serde_json::Value) -> serde_json::Value {
        serde_json::json!({
            "deduce": { "with": {
                "name": { "the": "org/employee-name", "as": "text:" }
            } },
            "when": [{
                "assert": { "with": {
                    "name": { "the": "org/person-name", "as": "text:", "description": person_description },
                    "level": { "the": "org/person-level" }
                } },
                "where": {
                    "this": { "?": { "name": "this" } },
                    "name": { "?": { "name": "name" } },
                    "level": constant
                }
            }]
        })
    }

    /// An attribute's description is not part of any identity, so it
    /// cannot change a rule's: the rule's concepts enter by reference.
    #[dialog_common::test]
    fn it_leaves_descriptions_out_of_a_rules_identity() {
        let a = compile(employee(
            "A person's name",
            serde_json::json!({ "integer:": 5 }),
        ));
        let b = compile(employee(
            "What they are called",
            serde_json::json!({ "integer:": 5 }),
        ));
        assert_eq!(a.this(), b.this());
    }

    /// A rule's constants are identified by value: a natural, an integer
    /// and a float of one number make one rule, and another number makes
    /// another.
    #[dialog_common::test]
    fn it_identifies_a_rules_constants_by_value() {
        let integer = compile(employee("", serde_json::json!({ "integer:": 5 })));
        let natural = compile(employee("", serde_json::json!({ "natural:": 5 })));
        let float = compile(employee("", serde_json::json!({ "float:": 5.0 })));
        let other = compile(employee("", serde_json::json!({ "integer:": 6 })));
        assert_eq!(integer.this(), natural.this());
        assert_eq!(integer.this(), float.this());
        assert_ne!(integer.this(), other.this());
    }

    /// A premise's field names are notation: a premise reading the same
    /// attributes under other names, binding the same terms, is the same
    /// premise, and the rule the same rule.
    #[dialog_common::test]
    fn it_keys_a_premises_bindings_by_what_they_read() {
        let renamed = serde_json::json!({
            "deduce": { "with": {
                "name": { "the": "org/employee-name", "as": "text:" }
            } },
            "when": [{
                "assert": { "with": {
                    "called": { "the": "org/person-name", "as": "text:" },
                    "rank": { "the": "org/person-level" }
                } },
                "where": {
                    "this": { "?": { "name": "this" } },
                    "called": { "?": { "name": "name" } },
                    "rank": { "integer:": 5 }
                }
            }]
        });
        assert_eq!(
            compile(renamed).this(),
            compile(employee("", serde_json::json!({ "integer:": 5 }))).this()
        );
    }

    /// What the head derives is part of the identity: the same body
    /// concluding another attribute is another rule.
    #[dialog_common::test]
    fn it_tells_rules_apart_by_their_head() {
        let mut other = employee("", serde_json::json!({ "integer:": 5 }));
        other["deduce"]["with"]["name"]["the"] = serde_json::json!("org/manager-name");
        assert_ne!(
            compile(other).this(),
            compile(employee("", serde_json::json!({ "integer:": 5 }))).this()
        );
    }

    /// With several variables, the canonical labeling orders them by the
    /// shapes of the premises they occur in; a description must not
    /// reorder them.
    #[dialog_common::test]
    fn it_leaves_descriptions_out_of_the_labeling() {
        let rule = |first: &str, second: &str| {
            compile(serde_json::json!({
                "deduce": { "with": {
                    "manager": { "the": "org/manager-of", "as": "entity:" }
                } },
                "when": [
                    {
                        "assert": { "with": {
                            "team": { "the": "org/member-of", "as": "entity:", "description": first }
                        } },
                        "where": {
                            "this": { "?": { "name": "this" } },
                            "team": { "?": { "name": "team" } }
                        }
                    },
                    {
                        "assert": { "with": {
                            "lead": { "the": "org/team-lead", "as": "entity:", "description": second }
                        } },
                        "where": {
                            "this": { "?": { "name": "team" } },
                            "lead": { "?": { "name": "manager" } }
                        }
                    }
                ]
            }))
        };
        let plain = rule("", "");
        for (first, second) in [
            ("a", "b"),
            ("zzz", "aaa"),
            ("", "something long"),
            ("x", ""),
        ] {
            assert_eq!(
                rule(first, second).this(),
                plain.this(),
                "{first:?} {second:?}"
            );
        }
    }
}
