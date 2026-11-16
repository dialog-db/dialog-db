use crate::Cardinality;
use crate::attribute::AttributeDescriptor;
use crate::attribute::CardinalityName;
use crate::attribute::The;
use crate::concept::descriptor::ConceptDescriptor;
use crate::corpus::Type;
use crate::error::TypeError;
use crate::memo::Memo;
use crate::selective::{Ranking, Selective};
use crate::type_system::Primitive;
use serde::de::{Error as _, IgnoredAny};
use serde::{Deserialize, Deserializer, Serialize};
use std::collections::BTreeMap;

/// `skip_serializing_if` helper: a required field omits the
/// `optional` key on the wire, so a required entry serializes
/// exactly as a bare [`AttributeDescriptor`] did before optionality
/// existed.
fn is_not_optional(optional: &bool) -> bool {
    !*optional
}

/// A single attribute as it participates in a concept: the
/// [`AttributeDescriptor`] plus the concept-membership facts about
/// it (currently just whether it is `optional`).
///
/// Attributes themselves are always required: an attribute is an
/// attribute. Optionality is a property of *how a concept uses* the
/// attribute (`Option<T>` field in a `#[derive(Concept)]` struct),
/// so it lives here, at the concept-field layer, not on
/// [`AttributeDescriptor`].
///
/// On the wire the descriptor is flattened, so a field serializes as
/// a single object carrying the attribute fields plus an optional
/// `"optional": true`:
///
/// ```json
/// { "the": "person/nickname", "as": "text:", "optional": true }
/// ```
///
/// A required field omits `optional`, so it is byte-identical to the
/// pre-optionality encoding.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "FieldWire", into = "FieldWire")]
pub struct ConceptFieldDescriptor {
    descriptor: AttributeDescriptor,
    /// Whether this field is optional (set-widened) within the
    /// concept: a missing fact yields an `Absent` row instead of
    /// dropping the row. Required fields (the default) omit this on
    /// the wire.
    optional: bool,
    /// The concept the field's target entity must conform to — a
    /// *concept-typed* field. Only entity-valued attributes can
    /// conform (validated at every construction path), and a
    /// conforming field cannot be optional: the left-join over
    /// "edge exists AND target conforms" is absence-over-IDB, which
    /// stratification (M4) has to own first.
    ///
    /// Carried as the full target descriptor so rule lowering can
    /// conjoin the target's premises without a registry; identity
    /// hashing uses only the target's `concept:{hash}` URI.
    conforms: Option<Box<ConceptDescriptor>>,
    /// The pick the field reads one of its relation's claims with, when
    /// it declares one: `None` reads the newest claim under cardinality
    /// one and every claim under many. Like optionality, it is how the
    /// concept reads the attribute, not a property of the attribute.
    aggregate: Option<Selective>,
    /// The attribute concept of this field (see
    /// [`ConceptDescriptor::of_attribute`]), built on first use. Every
    /// resolution of a concept asks for it, and for its identity, per
    /// field; clones share the memo, so the identity is hashed once.
    attribute: Memo<ConceptDescriptor>,
}

/// Why a field's `pick` in a spelling this release does not know, such
/// as the entity an earlier release wrote (`top:`), is not read.
pub const FIELD_PICK_REFUSED: &str = "a field's `pick` is `last`, `all`, `max`, `min` or \
     `top`: `last` reads the newest claim of a cardinality-one attribute, `all` every claim \
     of a cardinality-many one, and `max`, `min` and `top` one claim of a cardinality-many \
     one, `top` ranking the values `as` lists best first";

/// A field's `pick` as its wire form spells it.
#[derive(Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
enum PickName {
    Last,
    All,
    Max,
    Min,
    Top,
}

impl<'de> Deserialize<'de> for PickName {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Spelled {
            Name(String),
            Other(IgnoredAny),
        }
        match Spelled::deserialize(deserializer)? {
            Spelled::Name(name) => match name.as_str() {
                "last" => Ok(PickName::Last),
                "all" => Ok(PickName::All),
                "max" => Ok(PickName::Max),
                "min" => Ok(PickName::Min),
                "top" => Ok(PickName::Top),
                _ => Err(D::Error::custom(FIELD_PICK_REFUSED)),
            },
            Spelled::Other(_) => Err(D::Error::custom(FIELD_PICK_REFUSED)),
        }
    }
}

impl PickName {
    /// The cardinality a pick stands for where none is written: `last`
    /// reads a cell that keeps its newest claim, every other pick one
    /// that keeps them all.
    fn cardinality(pick: Option<PickName>) -> Cardinality {
        match pick {
            None | Some(PickName::Last) => Cardinality::One,
            Some(_) => Cardinality::Many,
        }
    }
}

/// The wire form of a [`ConceptFieldDescriptor`]: the attribute's own
/// keys, flattened, beside `optional`, `conforms` and `pick`.
///
/// `pick` is `last`, `all`, `max`, `min` or `top`, and `cardinality` is
/// derived from it unless it is written: `last` reads cardinality one
/// and the others many. Written beside a pick, `cardinality` has to be
/// one the pick can read: `many` beside `last` keeps every claim and
/// reads the newest, and `one` beside any other pick is refused. On the
/// way out a cardinality its pick implies is not written.
#[derive(Serialize, Deserialize)]
struct FieldWire {
    #[serde(flatten)]
    descriptor: AttributeDescriptor,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    cardinality: Option<CardinalityName>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pick: Option<PickName>,
    #[serde(default, skip_serializing_if = "is_not_optional")]
    optional: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    conforms: Option<Box<ConceptDescriptor>>,
}

impl TryFrom<FieldWire> for ConceptFieldDescriptor {
    type Error = String;

    fn try_from(wire: FieldWire) -> Result<Self, Self::Error> {
        let cardinality = wire
            .cardinality
            .map(Cardinality::from)
            .unwrap_or_else(|| PickName::cardinality(wire.pick));
        let aggregate = match wire.pick {
            None => None,
            Some(PickName::All) if cardinality == Cardinality::One => {
                return Err(format!(
                    "{} cannot be read with `pick: all` under `cardinality: one`: \
                     its cell keeps one claim",
                    wire.descriptor.the()
                ));
            }
            Some(PickName::All) => None,
            Some(PickName::Last) => Some(Selective::Last),
            Some(PickName::Max) => Some(Selective::Max),
            Some(PickName::Min) => Some(Selective::Min),
            Some(PickName::Top) => Some(Selective::Top),
        };
        let field = Self {
            descriptor: wire.descriptor.with_cardinality(cardinality),
            optional: wire.optional,
            conforms: wire.conforms,
            aggregate: None,
            attribute: Memo::default(),
        };
        match aggregate {
            Some(pick) => field.picking(pick).map_err(|error| error.to_string()),
            None => Ok(field),
        }
    }
}

impl From<ConceptFieldDescriptor> for FieldWire {
    fn from(field: ConceptFieldDescriptor) -> Self {
        let pick = field.aggregate.map(|pick| match pick {
            Selective::Last => PickName::Last,
            Selective::Max => PickName::Max,
            Selective::Min => PickName::Min,
            Selective::Top => PickName::Top,
        });
        let cardinality = field.descriptor.cardinality();
        FieldWire {
            cardinality: (cardinality != PickName::cardinality(pick)).then(|| cardinality.into()),
            // The attribute writes its own cardinality when it is many,
            // and the field has just said what there is to say of it.
            descriptor: field.descriptor.with_cardinality(Cardinality::One),
            pick,
            optional: field.optional,
            conforms: field.conforms,
        }
    }
}

impl ConceptFieldDescriptor {
    /// This field with no descriptions, its attribute's or a conformance
    /// target's: what identity sees.
    pub(crate) fn without_descriptions(&self) -> Self {
        Self {
            descriptor: self.descriptor.without_description(),
            optional: self.optional,
            conforms: self
                .conforms
                .as_ref()
                .map(|target| Box::new(target.without_descriptions())),
            aggregate: self.aggregate,
            attribute: Memo::default(),
        }
    }

    /// A required concept field wrapping the given attribute descriptor.
    pub fn required(descriptor: AttributeDescriptor) -> Self {
        Self {
            descriptor,
            optional: false,
            conforms: None,
            aggregate: None,
            attribute: Memo::default(),
        }
    }

    /// An optional (set-widened) concept field wrapping the given
    /// attribute descriptor.
    pub fn optional(descriptor: AttributeDescriptor) -> Self {
        Self {
            descriptor,
            optional: true,
            conforms: None,
            aggregate: None,
            attribute: Memo::default(),
        }
    }

    /// The attribute concept of this field: the single-field concept
    /// that is the relation of its attribute.
    pub fn attribute_concept(&self) -> &ConceptDescriptor {
        self.attribute
            .get_or_init(|| ConceptDescriptor::attribute_concept(self))
    }

    /// A *concept-typed* field: a required, entity-valued attribute
    /// whose target entity must conform to `target`. Errors when the
    /// attribute's value type is not `Entity` — only entities can
    /// conform to a concept.
    pub fn conforming(
        descriptor: AttributeDescriptor,
        target: ConceptDescriptor,
    ) -> Result<Self, TypeError> {
        if descriptor.content_type() != Some(Type::Entity) {
            return Err(TypeError::NonEntityConformance {
                the: descriptor.the().to_string(),
                concept: target.this().to_string(),
                actual: descriptor.content_type(),
            });
        }
        Ok(Self {
            descriptor,
            optional: false,
            conforms: Some(Box::new(target)),
            aggregate: None,
            attribute: Memo::default(),
        })
    }

    /// This field reading its claim with `pick`. A ranking pick (`max`,
    /// `min`, `top`) reads a cardinality-many attribute, whose cell
    /// keeps every claim to rank; `top` ranks by the values the
    /// attribute lists, best first; `max` and `min` need a comparable
    /// type; and a keyed collection, whose entries are each their own
    /// claim, picks nothing. `last` over a cardinality-one attribute is
    /// what the field reads anyway, so it declares nothing.
    pub fn picking(mut self, pick: Selective) -> Result<Self, TypeError> {
        self.aggregate = Some(pick);
        self.check_aggregate()?;
        if pick == Selective::Last && self.cardinality() == Cardinality::One {
            self.aggregate = None;
        }
        Ok(self)
    }

    /// The pick the field reads its claim with, if it declares one.
    pub fn pick(&self) -> Option<Selective> {
        self.aggregate
    }

    /// The aggregator and listed values a read of this field chooses its
    /// claim by, when it declares an aggregator.
    pub fn ranking(&self) -> Option<Ranking> {
        self.aggregate
            .map(|aggregate| Ranking::new(aggregate, self.descriptor.among().to_vec()))
    }

    /// Refuse an aggregator the field's attribute cannot be read under.
    pub(crate) fn check_aggregate(&self) -> Result<(), TypeError> {
        let Some(aggregate) = self.aggregate else {
            return Ok(());
        };
        let refuse = |reason: &str| {
            Err(TypeError::SelectiveAggregate {
                the: self.the().to_string(),
                aggregate,
                reason: reason.to_string(),
            })
        };
        if self.the().attribute().is_none() {
            return refuse("a keyed collection's entries are each a claim of their own");
        }
        if aggregate != Selective::Last && self.cardinality() != Cardinality::Many {
            return refuse(
                "a ranked field reads a cardinality-many attribute: under one a write \
                 succeeds the newest claim, so the cell keeps no other claim to rank",
            );
        }
        if aggregate == Selective::Top && self.descriptor.among().is_empty() {
            return refuse(
                "`top` ranks by the values the attribute's `as` lists, and it lists none",
            );
        }
        if aggregate.compares()
            && self
                .content_type()
                .is_some_and(|kind| !Primitive::COMPARABLE.contains(kind))
        {
            return refuse("`max` and `min` compare values, and the attribute's type has no order");
        }
        Ok(())
    }

    /// The underlying attribute descriptor.
    pub fn descriptor(&self) -> &AttributeDescriptor {
        &self.descriptor
    }

    /// Returns `true` iff this field is optional (set-widened).
    pub fn is_optional(&self) -> bool {
        self.optional
    }

    /// The concept this field's target entity must conform to, if
    /// the field is concept-typed.
    pub fn conforms(&self) -> Option<&ConceptDescriptor> {
        self.conforms.as_deref()
    }

    /// Convenience: what this field selects — one attribute, or every
    /// entry of a keyed collection.
    pub fn the(&self) -> &The {
        self.descriptor.the()
    }

    /// Convenience: the attribute's domain.
    pub fn domain(&self) -> &str {
        self.descriptor.domain()
    }

    /// Convenience: the attribute's name, or `None` for a keyed
    /// collection, whose name half varies per entry.
    pub fn name(&self) -> Option<&str> {
        self.descriptor.name()
    }

    /// Convenience: the attribute's content type, if known.
    pub fn content_type(&self) -> Option<Type> {
        self.descriptor.content_type()
    }

    /// Convenience: the attribute's cardinality.
    pub fn cardinality(&self) -> Cardinality {
        self.descriptor.cardinality()
    }

    /// Convenience: the attribute's description.
    pub fn description(&self) -> &str {
        self.descriptor.description()
    }

    /// Convenience: the attribute's content-addressed URI.
    pub fn to_uri(&self) -> String {
        self.descriptor.to_uri()
    }

    /// Convenience: cost estimate for scanning this attribute. See
    /// [`AttributeDescriptor::estimate`].
    pub fn estimate(&self, of: bool, is: bool) -> usize {
        self.descriptor.estimate(of, is)
    }
}

impl From<AttributeDescriptor> for ConceptFieldDescriptor {
    /// A bare attribute descriptor becomes a *required* concept field.
    fn from(descriptor: AttributeDescriptor) -> Self {
        Self::required(descriptor)
    }
}

/// A non-empty collection of named concept fields.
///
/// `NamedAttributes` maps a field name to a [`ConceptFieldDescriptor`]
/// (an attribute plus its concept-membership facts), stored in a
/// [`BTreeMap`] so iteration and serialization are deterministic
/// (sorted by field name) and field names are de-duplicated.
///
/// It is valid by construction: it cannot hold zero entries, and at
/// least one entry must be required. Every public construction path
/// is fallible ([`TryFrom`] for arrays / `Vec` / `HashMap`, and
/// [`Deserialize`]) and errors otherwise: a concept with no required
/// attribute would constrain nothing and match every entity.
///
/// Serializes as a JSON map: `{ "field-name": { "the": "domain/name", ... } }`.
#[repr(transparent)]
#[derive(Debug, Clone, PartialEq)]
pub struct NamedAttributes(BTreeMap<String, ConceptFieldDescriptor>);

impl NamedAttributes {
    /// Returns an iterator over all fields as (name, field) pairs,
    /// sorted by field name.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = (&str, &ConceptFieldDescriptor)> {
        self.0.iter().map(|(k, v)| (k.as_str(), v))
    }

    /// Returns an iterator over field names, sorted.
    pub fn keys(&self) -> impl Iterator<Item = &str> + '_ {
        self.0.keys().map(String::as_str)
    }

    /// Fallible builder shared by the [`TryFrom`] impls: rejects an
    /// empty set, or a set with no required field, with
    /// [`TypeError::EmptyConcept`]; rejects malformed concept-typed
    /// fields (non-entity value type, or optional conformance) so
    /// the wire path enforces the same invariants
    /// [`ConceptFieldDescriptor::conforming`] does.
    fn try_new(map: BTreeMap<String, ConceptFieldDescriptor>) -> Result<Self, TypeError> {
        if map.is_empty() || map.values().all(|field| field.is_optional()) {
            return Err(TypeError::EmptyConcept);
        }
        for field in map.values() {
            field.check_aggregate()?;
            if field.is_optional() && field.the().attribute().is_none() {
                return Err(TypeError::OptionalCollection {
                    domain: field.domain().to_string(),
                });
            }
            if let Some(target) = field.conforms() {
                if field.content_type() != Some(Type::Entity) {
                    return Err(TypeError::NonEntityConformance {
                        the: field.the().to_string(),
                        concept: target.this().to_string(),
                        actual: field.content_type(),
                    });
                }
                if field.is_optional() {
                    return Err(TypeError::OptionalConformance {
                        the: field.the().to_string(),
                    });
                }
            }
        }
        Ok(NamedAttributes(map))
    }
}

impl Serialize for NamedAttributes {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.0.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for NamedAttributes {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let map = BTreeMap::<String, ConceptFieldDescriptor>::deserialize(deserializer)?;
        // Reuse the same validity check as every other construction
        // path rather than re-implementing it: at least one entry,
        // at least one of them required.
        NamedAttributes::try_from(map).map_err(D::Error::custom)
    }
}

impl<const N: usize> TryFrom<[(&str, ConceptFieldDescriptor); N]> for NamedAttributes {
    type Error = TypeError;

    fn try_from(arr: [(&str, ConceptFieldDescriptor); N]) -> Result<Self, Self::Error> {
        Self::try_new(
            arr.into_iter()
                .map(|(name, field)| (name.to_string(), field))
                .collect(),
        )
    }
}

impl<const N: usize> TryFrom<[(String, ConceptFieldDescriptor); N]> for NamedAttributes {
    type Error = TypeError;

    fn try_from(arr: [(String, ConceptFieldDescriptor); N]) -> Result<Self, Self::Error> {
        Self::try_new(arr.into_iter().collect())
    }
}

impl TryFrom<Vec<(&str, ConceptFieldDescriptor)>> for NamedAttributes {
    type Error = TypeError;

    fn try_from(vec: Vec<(&str, ConceptFieldDescriptor)>) -> Result<Self, Self::Error> {
        Self::try_new(
            vec.into_iter()
                .map(|(name, field)| (name.to_string(), field))
                .collect(),
        )
    }
}

impl TryFrom<Vec<(String, ConceptFieldDescriptor)>> for NamedAttributes {
    type Error = TypeError;

    fn try_from(vec: Vec<(String, ConceptFieldDescriptor)>) -> Result<Self, Self::Error> {
        Self::try_new(vec.into_iter().collect())
    }
}

impl TryFrom<BTreeMap<String, ConceptFieldDescriptor>> for NamedAttributes {
    type Error = TypeError;

    fn try_from(map: BTreeMap<String, ConceptFieldDescriptor>) -> Result<Self, Self::Error> {
        Self::try_new(map)
    }
}
