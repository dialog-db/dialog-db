//! The wire form of a [`Type`]: what a `/query` request body carries.
//!
//! [`Type`] is a bitset (`Primitive`) optionally narrowed by a
//! [`Refinement`], and its derived serde form published both — a
//! `{"bits": 128}` field whose meaning depends on enum discriminant
//! order, and a positional `[set, refinement]` tuple. This module is
//! the stable form instead: named variants, named constraints, and
//! nothing whose spelling changes when a `ValueType` is added.
//!
//! ```json
//! {"type": {"symbol": {}},
//!  "domain": {"is": "todo.list"},
//!  "name": {"case": "position"}}
//! ```
//!
//! Two container conventions run through it, and they are not
//! interchangeable:
//!
//! - An **object is a union** where entries are alternatives of one
//!   kind. `type` is the case: a value matching any present key is
//!   admitted, and intersecting two sets keeps the shared keys —
//!   which is what `Primitive::intersect` does to the bits.
//! - An **array is an intersection** where entries are independent
//!   obligations. `as` (conformance) is the case: every listed
//!   concept must hold.
//! - A **record of fixed slots** is neither. The constraint object
//!   itself is one: each slot appears at most once, and two
//!   constraints on one slot merge rather than accumulate.
//!
//! Reading is deliberately looser than writing: the legacy
//! `{"primitive": …}` / `{"refined": …}` forms still parse, so a rule
//! stored before this module loads unchanged. Only the form above is
//! ever written.

use serde::de::Error as DeError;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value as Json};
use std::error::Error as StdError;
use std::fmt::{Display, Formatter, Result as FmtResult};

use crate::artifact::{Type as ValueType, Value, decode_value};

use super::{ConceptRef, NameShape, Primitive, Refinement, Type};

/// The wire name for each admissible type, and the `Primitive` bit it
/// denotes. `option` has no `ValueType`: it is the synthetic absence
/// atom, and a set containing it alongside others marks the variable
/// optional.
const VARIANTS: [(&str, Option<ValueType>); 10] = [
    ("bytes", Some(ValueType::Bytes)),
    ("entity", Some(ValueType::Entity)),
    ("boolean", Some(ValueType::Boolean)),
    ("text", Some(ValueType::String)),
    ("uint", Some(ValueType::UnsignedInt)),
    ("int", Some(ValueType::SignedInt)),
    ("float", Some(ValueType::Float)),
    ("record", Some(ValueType::Record)),
    ("symbol", Some(ValueType::Symbol)),
    ("option", None),
];

/// The wire name for a `ValueType`.
fn variant_name(value_type: ValueType) -> &'static str {
    VARIANTS
        .iter()
        .find_map(|(name, vt)| (*vt == Some(value_type)).then_some(*name))
        .expect("every ValueType has a wire name")
}

/// Why a constraint object could not be read.
#[derive(Debug, PartialEq, Eq)]
pub enum WireError {
    /// A `type` key that names no known variant.
    UnknownVariant(String),
    /// A constraint slot that is not one of the known ones.
    UnknownSlot(String),
    /// A slot's value had the wrong JSON shape.
    Malformed(&'static str),
    /// The constraint narrowed the admissible set to nothing — e.g. a
    /// prefix on a boolean, or conformance on a non-entity.
    Uninhabited(&'static str),
}

impl Display for WireError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Self::UnknownVariant(name) => {
                write!(f, "unknown type variant {name:?}")
            }
            Self::UnknownSlot(name) => write!(f, "unknown constraint {name:?}"),
            Self::Malformed(what) => write!(f, "malformed {what}"),
            Self::Uninhabited(what) => {
                write!(f, "{what} leaves no admissible type")
            }
        }
    }
}

impl StdError for WireError {}

/// A [`Type`] in its wire form: `serialize` writes the named shape,
/// `deserialize` accepts it and the legacy derived shape alike.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Wire(pub Type);

impl Serialize for Wire {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        encode(&self.0).serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Wire {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let json = Json::deserialize(deserializer)?;
        decode(&json).map(Wire).map_err(DeError::custom)
    }
}

/// Write a [`Type`] as the named wire form.
pub fn encode(kind: &Type) -> Json {
    let mut out = Map::new();
    out.insert("type".into(), encode_set(kind.primitive_part()));
    if let Some(refinement) = kind.refinement() {
        encode_refinement(refinement, &mut out);
    }
    Json::Object(out)
}

/// The `type` set: one key per admitted variant, each valued by an
/// (empty, for now) parameter record.
fn encode_set(primitive: Primitive) -> Json {
    let mut set = Map::new();
    for value_type in primitive.iter() {
        set.insert(variant_name(value_type).into(), Json::Object(Map::new()));
    }
    if primitive.contains_nothing() {
        set.insert("option".into(), Json::Object(Map::new()));
    }
    Json::Object(set)
}

/// The constraint slots, written beside `type`.
fn encode_refinement(refinement: &Refinement, out: &mut Map<String, Json>) {
    if let Some(prefix) = &refinement.prefix {
        // A prefix that ends at the name boundary IS a domain, and
        // saying so structurally is what keeps the trailing separator
        // from being something a writer has to remember: omit it and
        // the scan silently degrades from a range narrowing to a
        // per-row filter.
        match prefix.strip_suffix('/') {
            Some(domain) if !domain.is_empty() && !domain.contains('/') => {
                let mut slot = Map::new();
                slot.insert("is".into(), Json::String(domain.into()));
                out.insert("domain".into(), Json::Object(slot));
            }
            _ => {
                out.insert("starts-with".into(), Json::String(prefix.clone()));
            }
        }
    }
    if let Some(shape) = refinement.name_shape {
        let case = match shape {
            NameShape::Position => "position",
            NameShape::Symbol => "symbol",
        };
        let mut slot = Map::new();
        slot.insert("case".into(), Json::String(case.into()));
        out.insert("name".into(), Json::Object(slot));
    }
    if !refinement.conforms.is_empty() {
        let targets = refinement
            .conforms
            .iter()
            .map(|concept| {
                let mut entry = Map::new();
                entry.insert(concept.0.clone(), Json::Object(Map::new()));
                Json::Object(entry)
            })
            .collect();
        out.insert("as".into(), Json::Array(targets));
    }
    if let Some(interval) = &refinement.interval {
        // Bounds are stored order-preservingly encoded under the
        // literal's own type; decode back to the value that was
        // written so the wire carries `>=: 5`, not a byte array.
        let mut bound = |side: &Option<super::IntervalBound>, inclusive: &str, strict: &str| {
            if let Some(bound) = side
                && let Some((value, _)) = decode_value(interval.value_type, &bound.encoded)
            {
                let key = if bound.inclusive { inclusive } else { strict };
                out.insert(key.into(), value_to_json(&value));
            }
        };
        bound(&interval.lower, ">=", ">");
        bound(&interval.upper, "<=", "<");
    }
}

/// A bound literal as JSON. Only the comparable types can appear.
fn value_to_json(value: &Value) -> Json {
    match value {
        Value::UnsignedInt(n) => Json::Number((*n as u64).into()),
        Value::SignedInt(n) => Json::Number((*n as i64).into()),
        Value::Float(n) => serde_json::Number::from_f64(*n)
            .map(Json::Number)
            .unwrap_or(Json::Null),
        Value::String(text) => Json::String(text.clone()),
        Value::Symbol(symbol) => Json::String(String::from(symbol)),
        Value::Entity(entity) => Json::String(entity.as_str().to_string()),
        Value::Boolean(flag) => Json::Bool(*flag),
        other => Json::String(format!("{other:?}")),
    }
}

/// Read a [`Type`] from either the named wire form or the legacy
/// derived one.
pub fn decode(json: &Json) -> Result<Type, WireError> {
    let Json::Object(map) = json else {
        return Err(WireError::Malformed("type: expected an object"));
    };
    // Legacy: the derived `Type` shape, kept readable so a rule
    // written before this module still loads.
    if map.contains_key("primitive") || map.contains_key("refined") {
        return serde_json::from_value::<Type>(json.clone())
            .map_err(|_| WireError::Malformed("legacy type"));
    }

    let mut kind = match map.get("type") {
        Some(set) => Type::from(decode_set(set)?),
        // No `type` slot: unconstrained, then narrowed by whatever
        // constraints follow.
        None => Type::from(Primitive::ANY),
    };

    for (slot, value) in map {
        kind = match slot.as_str() {
            "type" => continue,
            "domain" => {
                let domain = field(value, "is", "domain")?;
                kind.with_prefix(format!("{domain}/"))
                    .ok_or(WireError::Uninhabited("domain"))?
            }
            "name" => {
                let case = field(value, "case", "name")?;
                let shape = match case {
                    "position" => NameShape::Position,
                    "symbol" => NameShape::Symbol,
                    _ => return Err(WireError::Malformed("name: case")),
                };
                kind.with_name_shape(shape)
                    .ok_or(WireError::Uninhabited("name"))?
            }
            "starts-with" => {
                let Json::String(prefix) = value else {
                    return Err(WireError::Malformed("starts-with"));
                };
                kind.with_prefix(prefix.clone())
                    .ok_or(WireError::Uninhabited("starts-with"))?
            }
            "as" => {
                let Json::Array(targets) = value else {
                    return Err(WireError::Malformed("as: expected an array"));
                };
                for target in targets {
                    let Json::Object(entry) = target else {
                        return Err(WireError::Malformed("as: entry"));
                    };
                    for concept in entry.keys() {
                        kind = kind
                            .with_conformance(ConceptRef(concept.clone()))
                            .ok_or(WireError::Uninhabited("as"))?;
                    }
                }
                kind
            }
            ">=" | ">" | "<=" | "<" => {
                let bound = json_to_value(value)?;
                let inclusive = slot == ">=" || slot == "<=";
                let lower = slot.starts_with('>');
                kind.with_interval(&bound, inclusive, lower)
                    .ok_or(WireError::Uninhabited("bound"))?
            }
            other => return Err(WireError::UnknownSlot(other.into())),
        };
    }
    Ok(kind)
}

/// Read one string field out of a constraint slot.
fn field<'a>(value: &'a Json, key: &str, slot: &'static str) -> Result<&'a str, WireError> {
    value
        .get(key)
        .and_then(Json::as_str)
        .ok_or(WireError::Malformed(slot))
}

/// Read the `type` set: an object whose keys are variant names.
fn decode_set(json: &Json) -> Result<Primitive, WireError> {
    let Json::Object(entries) = json else {
        return Err(WireError::Malformed("type: expected an object"));
    };
    let mut primitive = Primitive::EMPTY;
    for name in entries.keys() {
        let (_, value_type) = VARIANTS
            .iter()
            .find(|(variant, _)| variant == name)
            .ok_or_else(|| WireError::UnknownVariant(name.clone()))?;
        primitive = primitive.union(match value_type {
            Some(value_type) => Primitive::from(*value_type),
            None => Primitive::NOTHING,
        });
    }
    Ok(primitive)
}

/// A bound literal from JSON.
fn json_to_value(json: &Json) -> Result<Value, WireError> {
    Ok(match json {
        Json::Number(number) => {
            if let Some(unsigned) = number.as_u64() {
                Value::UnsignedInt(unsigned as u128)
            } else if let Some(signed) = number.as_i64() {
                Value::SignedInt(signed as i128)
            } else if let Some(float) = number.as_f64() {
                Value::Float(float)
            } else {
                return Err(WireError::Malformed("bound: number"));
            }
        }
        Json::String(text) => Value::String(text.clone()),
        Json::Bool(flag) => Value::Boolean(*flag),
        _ => return Err(WireError::Malformed("bound")),
    })
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;

    fn roundtrip(kind: &Type) -> Type {
        let json = encode(kind);
        decode(&json).unwrap_or_else(|error| panic!("{json} failed to decode: {error}"))
    }

    /// EVERY set of admissible types survives the round trip. The
    /// bitset has ten members, so this walks all 1024 subsets: a
    /// variant whose wire name were missing or duplicated would show
    /// up here rather than the first time someone queried for it.
    #[dialog_common::test]
    async fn it_round_trips_every_type_set() {
        for bits in 0u16..1024 {
            let mut primitive = Primitive::EMPTY;
            for (index, (_, value_type)) in VARIANTS.iter().enumerate() {
                if bits & (1 << index) == 0 {
                    continue;
                }
                primitive = primitive.union(match value_type {
                    Some(value_type) => Primitive::from(*value_type),
                    None => Primitive::NOTHING,
                });
            }
            let kind = Type::from(primitive);
            assert_eq!(
                roundtrip(&kind),
                kind,
                "type set {bits:#012b} did not survive the round trip"
            );
        }
    }

    /// A whole-domain prefix is written as a `domain`, not as a raw
    /// prefix — the trailing separator is an encoding detail, and a
    /// writer who forgets it silently loses the range narrowing.
    #[dialog_common::test]
    async fn it_writes_a_whole_domain_as_a_domain() {
        let kind = Type::from(ValueType::Symbol)
            .with_prefix("todo.list/")
            .expect("symbol is textual");
        let json = encode(&kind);

        assert_eq!(
            json.get("domain").and_then(|slot| slot.get("is")),
            Some(&Json::String("todo.list".into())),
            "the domain is written without its separator"
        );
        assert!(
            json.get("starts-with").is_none(),
            "a domain is not also written as a prefix"
        );
        assert_eq!(roundtrip(&kind), kind);
    }

    /// A prefix that does NOT end at the name boundary has no domain
    /// to name, so it stays a lexical prefix. Both forms round-trip.
    #[dialog_common::test]
    async fn it_keeps_a_partial_prefix_lexical() {
        let kind = Type::from(ValueType::Symbol)
            .with_prefix("todo.list/N")
            .expect("symbol is textual");
        let json = encode(&kind);

        assert_eq!(
            json.get("starts-with"),
            Some(&Json::String("todo.list/N".into())),
            "a prefix past the separator is not a domain"
        );
        assert!(json.get("domain").is_none());
        assert_eq!(roundtrip(&kind), kind);
    }

    /// The collection case: a domain plus a name shape. Both halves
    /// survive, and `name.case` says which half of a mixed domain the
    /// scan wants.
    #[dialog_common::test]
    async fn it_round_trips_a_keyed_collection() {
        for (shape, case) in [
            (NameShape::Position, "position"),
            (NameShape::Symbol, "symbol"),
        ] {
            let kind = Type::from(ValueType::Symbol)
                .with_prefix("xyz.tonk.notebook/")
                .expect("symbol is textual")
                .with_name_shape(shape)
                .expect("shapes compose with prefixes");
            let json = encode(&kind);

            assert_eq!(
                json.get("name").and_then(|slot| slot.get("case")),
                Some(&Json::String(case.into())),
            );
            assert_eq!(
                json.get("domain").and_then(|slot| slot.get("is")),
                Some(&Json::String("xyz.tonk.notebook".into())),
            );
            assert_eq!(roundtrip(&kind), kind);
        }
    }

    /// An optional refined type — the shape EVERY `maybe:` field with
    /// a constraint produces. The refinement belongs to the set, not
    /// to a member of it, so `option` rides alongside without
    /// disturbing it.
    #[dialog_common::test]
    async fn it_round_trips_an_optional_refined_type() {
        let kind = Type::from(ValueType::String)
            .optional()
            .with_prefix("alice")
            .expect("text is textual");

        let json = encode(&kind);
        let set = json.get("type").expect("a type set");
        assert!(set.get("text").is_some(), "the present type survives");
        assert!(set.get("option").is_some(), "so does admissible absence");
        assert_eq!(roundtrip(&kind), kind);
    }

    /// Conformance is an ARRAY because it is an intersection: every
    /// listed concept must hold. Two targets survive as two entries.
    #[dialog_common::test]
    async fn it_round_trips_conformance_as_an_intersection() {
        let kind = Type::from(ValueType::Entity)
            .with_conformance(ConceptRef("concept:abc".into()))
            .expect("entity conforms")
            .with_conformance(ConceptRef("concept:def".into()))
            .expect("entity conforms");

        let json = encode(&kind);
        let targets = json.get("as").and_then(Json::as_array).expect("an array");
        assert_eq!(targets.len(), 2, "both obligations are carried");
        assert_eq!(roundtrip(&kind), kind);
    }

    /// Order bounds are written as the literals they came from, not
    /// as the order-preserving bytes they are stored in.
    #[dialog_common::test]
    async fn it_round_trips_order_bounds() {
        let kind = Type::from(ValueType::UnsignedInt)
            .with_interval(&Value::UnsignedInt(5), true, true)
            .expect("uint is comparable")
            .with_interval(&Value::UnsignedInt(100), false, false)
            .expect("uint is comparable");

        let json = encode(&kind);
        assert_eq!(json.get(">="), Some(&Json::Number(5u64.into())));
        assert_eq!(json.get("<"), Some(&Json::Number(100u64.into())));
        assert_eq!(roundtrip(&kind), kind);
    }

    /// Reading is looser than writing: the derived shape a rule was
    /// stored under before this module still loads.
    #[dialog_common::test]
    async fn it_reads_the_legacy_derived_form() {
        let kind = Type::from(ValueType::Symbol)
            .with_prefix("todo.list/")
            .expect("symbol is textual")
            .with_name_shape(NameShape::Position)
            .expect("shapes compose");
        let legacy = serde_json::to_value(&kind).expect("the derived form");

        assert_eq!(decode(&legacy), Ok(kind), "a stored type still loads");
    }

    /// A constraint that cannot narrow anything is refused rather
    /// than silently yielding a variable that matches nothing.
    #[dialog_common::test]
    async fn it_rejects_constraints_that_admit_nothing() {
        let prefixed_boolean = serde_json::json!({
            "type": {"boolean": {}},
            "starts-with": "alice"
        });
        assert_eq!(
            decode(&prefixed_boolean),
            Err(WireError::Uninhabited("starts-with")),
            "a boolean has no lexical form to prefix"
        );

        let conforming_text = serde_json::json!({
            "type": {"text": {}},
            "as": [{"concept:abc": {}}]
        });
        assert_eq!(
            decode(&conforming_text),
            Err(WireError::Uninhabited("as")),
            "conformance is a property of entities"
        );
    }

    /// A misspelling is an error, not a silently dropped constraint.
    #[dialog_common::test]
    async fn it_rejects_names_it_does_not_know() {
        assert_eq!(
            decode(&serde_json::json!({"type": {"integer": {}}})),
            Err(WireError::UnknownVariant("integer".into())),
        );
        assert_eq!(
            decode(&serde_json::json!({"type": {"text": {}}, "named-by": "position"})),
            Err(WireError::UnknownSlot("named-by".into())),
        );
        assert_eq!(
            decode(&serde_json::json!({"type": {"symbol": {}}, "name": {"case": "sideways"}})),
            Err(WireError::Malformed("name: case")),
        );
    }

    /// The published shape, pinned. This is the contract a client
    /// writes against, so a change here is a change to the wire.
    #[dialog_common::test]
    async fn it_writes_the_documented_shape() {
        let kind = Type::from(ValueType::Symbol)
            .with_prefix("xyz.tonk.notebook/")
            .expect("symbol is textual")
            .with_name_shape(NameShape::Position)
            .expect("shapes compose");

        assert_eq!(
            encode(&kind),
            serde_json::json!({
                "type": {"symbol": {}},
                "domain": {"is": "xyz.tonk.notebook"},
                "name": {"case": "position"}
            })
        );
    }
}
