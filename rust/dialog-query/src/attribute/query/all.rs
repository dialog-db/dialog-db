use crate::Cardinality;
use crate::Claim;
use crate::artifact::{ArtifactSelector, ArtifactsAttribute, Constrained, decode_value};
use crate::attribute::The;
use crate::environment::Environment;
use crate::formula::number::Numeric;
use crate::query::Application;
use crate::query::Output;
use crate::selection::{Match, Selection};
use crate::source::SelectRules;
use crate::type_system::{Primitive, Type as Kind};
use crate::types::{Any, Record};
use crate::{
    Binding, Entity, EvaluationError, Field, Parameters, Requirement, Schema, Term, Type, Value,
    try_stream,
};
use dialog_artifacts::{Artifact, Cause, Select};
use dialog_capability::Provider;
use dialog_common::ConditionalSync;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::fmt::{Formatter, Result as FmtResult};

/// Base EAV scan query that yields all matching artifacts.
///
/// Represents a query against the fact store in the form
/// `(the, of, is, cause)` where each position is a [`Term`]: either a
/// constant that constrains the lookup or a variable that will be bound
/// by the results. All matches are yielded without cardinality filtering.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AttributeQueryAll {
    /// The relation identifier (e.g., "person/name")
    the: Term<The>,
    /// The entity
    of: Term<Entity>,
    /// The value
    is: Term<Any>,
    /// The cause/provenance
    cause: Term<Cause>,
    /// Internal handle for claim storage.
    source: Term<Record>,
}

impl AttributeQueryAll {
    /// Create a new attribute query.
    ///
    /// The associative layer is scalar: a fact either exists or the
    /// row is filtered. Set-widening (`Absent` on miss) is a
    /// semantic-layer construct realized by
    /// [`OptionalAttributeQuery`](crate::optional::OptionalAttributeQuery), so a `Nothing` bit
    /// on the `is` term's kind is meaningless here and is stripped.
    pub fn new(the: Term<The>, of: Term<Entity>, is: Term<Any>, cause: Term<Cause>) -> Self {
        let is = match (is.name(), is.kind()) {
            (Some(name), Some(kind)) if kind.is_optional() => {
                Term::<Any>::typed_var(name.to_string(), kind.required())
            }
            _ => is,
        };
        Self {
            the,
            of,
            is,
            cause,
            source: Term::<Record>::unique(),
        }
    }

    /// Get the 'the' (attribute) term.
    pub fn the(&self) -> &Term<The> {
        &self.the
    }

    /// Get the 'of' (entity) term.
    pub fn of(&self) -> &Term<Entity> {
        &self.of
    }

    /// Get the 'is' (value) parameter.
    pub fn is(&self) -> &Term<Any> {
        &self.is
    }

    /// Return a copy of this query with the `is` term's type
    /// narrowed to `kind`. The planner uses this to stamp the
    /// rule-inferred kind onto the value variable before evaluation;
    /// `the`/`of`/`cause` are fixed. A non-variable `is` (a constant)
    /// is left unchanged.
    pub(crate) fn with_type(self, kind: Kind) -> Self {
        let is = match self.is.name() {
            Some(name) => Term::<Any>::typed_var(name.to_string(), kind),
            None => self.is,
        };
        Self { is, ..self }
    }

    /// Return a copy with the `the`/`of` variable terms carrying
    /// the given kinds. The planner stamps what rule-level
    /// inference proved about the attribute and entity variables —
    /// in particular prefix refinements, which the selector
    /// conversion turns into index-range bounds. Constants and
    /// `None` kinds are left unchanged.
    pub(crate) fn with_subject_kinds(self, the: Option<Kind>, of: Option<Kind>) -> Self {
        let the = match the {
            Some(kind) => self.the.with_kind(kind),
            None => self.the,
        };
        let of = match of {
            Some(kind) => self.of.with_kind(kind),
            None => self.of,
        };
        Self { the, of, ..self }
    }

    /// Get the 'cause' term.
    pub fn cause(&self) -> &Term<Cause> {
        &self.cause
    }

    /// Get the source term (internal claim handle).
    pub fn source(&self) -> &Term<Record> {
        &self.source
    }

    /// Map `Term<The>` to `Term<ArtifactsAttribute>`.
    pub fn attribute(&self) -> Term<ArtifactsAttribute> {
        match &self.the {
            Term::Constant(value) => Term::Constant(value.clone()),
            Term::Variable {
                name: Some(name), ..
            } => Term::var(name.clone()),
            Term::Variable { name: None, .. } => Term::blank(),
        }
    }

    /// Merge a matched artifact into a match: store the claim and bind
    /// the/of/is/cause values to the corresponding terms.
    pub(crate) fn merge(
        &self,
        candidate: &mut Match,
        artifact: &Artifact,
    ) -> Result<(), EvaluationError> {
        let claim = Claim::from(artifact);
        candidate.cite(&self.source, &claim)?;
        candidate.bind(&Term::<Any>::from(&self.the), Value::from(claim.the()))?;
        candidate.bind(
            &Term::<Any>::from(&self.of),
            Value::Entity(claim.of().clone()),
        )?;
        candidate.bind(&self.is, claim.is().clone())?;
        candidate.bind(
            &Term::<Any>::from(&self.cause),
            Value::Bytes(claim.cause().clone().0.into()),
        )?;
        Ok(())
    }

    /// True when the row pins one of this scan's named parameters to
    /// [`Binding::Absent`](crate::Binding::Absent). A scalar lookup
    /// demands present values in every slot, so such a row can match
    /// nothing: a positive premise filters it, and a negated premise
    /// passes it (the inner query has no rows). By the time a row
    /// reaches the associative layer, an Absent binding means "known
    /// to have no value", produced upstream by a
    /// [`OptionalAttributeQuery`](crate::optional::OptionalAttributeQuery) left-join.
    pub(crate) fn absent_blocked(&self, base: &Match) -> bool {
        let absent = |term: &Term<Any>| matches!(base.lookup(term), Ok(Binding::Absent));
        absent(&Term::<Any>::from(&self.the))
            || absent(&Term::<Any>::from(&self.of))
            || absent(&self.is)
            || absent(&Term::<Any>::from(&self.cause))
    }

    /// True when a fact's value inhabits the `is` term's kind. A
    /// typed value slot is a constraint: attribute values are
    /// dynamically typed in the store (one attribute may hold values
    /// of several types across facts), so a fact whose value falls
    /// outside the term's kind is a non-match to be filtered, never
    /// an error.
    pub(crate) fn admits(&self, value: &Value) -> bool {
        match self.is.kind() {
            Some(kind) => kind.admits(value),
            None => true,
        }
    }

    /// Resolves variables from the given match. `Absent` bindings
    /// leave the term unchanged (same as unbound); only Present
    /// bindings substitute.
    pub fn resolve(&self, source: &Match) -> Self {
        let the = self.the.resolve(source);
        let of = self.of.resolve(source);
        let is = match source.lookup(&self.is).and_then(|b| b.content()) {
            Ok(value) => Term::Constant(value),
            Err(_) => self.is.clone(),
        };
        let cause = self.cause.resolve(source);

        Self {
            the,
            of,
            is,
            cause,
            source: self.source.clone(),
        }
    }

    /// Returns the schema describing this application's parameters.
    pub fn schema(&self) -> Schema {
        let requirement = Requirement::new_group();
        let mut schema = Schema::new();

        schema.insert(
            "the".to_string(),
            Field {
                description: "The relation identifier".to_string(),
                content_type: Some(Kind::from(Type::Symbol)),
                requirement: requirement.required(),
                cardinality: Cardinality::One,
            },
        );

        schema.insert(
            "of".to_string(),
            Field {
                description: "Entity of the relation".to_string(),
                content_type: Some(Kind::from(Type::Entity)),
                requirement: requirement.required(),
                cardinality: Cardinality::One,
            },
        );

        // `None` means "no static info"; the unifier resolves at
        // rule-compile time.
        schema.insert(
            "is".to_string(),
            Field {
                description: "Value of the relation".to_string(),
                content_type: self.is.kind(),
                requirement: requirement.required(),
                cardinality: Cardinality::One,
            },
        );

        schema.insert(
            "cause".to_string(),
            Field {
                description: "Causal stamp of the relation".to_string(),
                content_type: Some(Kind::from(Type::Bytes)),
                requirement: requirement.required(),
                cardinality: Cardinality::One,
            },
        );

        schema
    }

    /// Estimate cost for Cardinality::Many semantics.
    pub fn estimate(&self, env: &Environment) -> Option<usize> {
        let the = self.the.is_bound(env);
        let of = self.of.is_bound(env);
        let is = self.is.is_bound(env);

        Cardinality::Many.estimate(the, of, is)
    }

    /// Returns the parameters for this query.
    pub fn parameters(&self) -> Parameters {
        let mut params = Parameters::new();

        params.insert("the".to_string(), Term::<Any>::from(&self.the));
        params.insert("of".to_string(), Term::<Any>::from(&self.of));
        params.insert("is".to_string(), self.is.clone());
        params.insert("cause".to_string(), Term::<Any>::from(&self.cause));
        params
    }

    /// Evaluate yielding all matching artifacts.
    ///
    /// Standard EAV semantics: zero rows are yielded for an input
    /// when no fact matches the lookup: the premise filters the
    /// row. Set-widening (`Absent` on miss) lives at the semantic
    /// layer in [`OptionalAttributeQuery`](crate::optional::OptionalAttributeQuery).
    pub fn evaluate<'a, Env, M: Selection + 'a>(
        self,
        env: &'a Env,
        selection: M,
    ) -> impl Selection + 'a
    where
        Env: Provider<Select<'a>> + Provider<SelectRules> + ConditionalSync,
    {
        let selector = self;
        try_stream! {
            for await candidate in selection {
                let base = candidate?;

                // An Absent-bound parameter matches nothing at the
                // scalar layer: filter the row without scanning.
                if selector.absent_blocked(&base) {
                    continue;
                }

                let selection = selector.resolve(&base);

                let stream = Provider::<Select<'_>>::execute(env, (&selection).try_into()?).await?;
                for await artifact in stream {
                    let artifact = artifact?;
                    // A typed `is` slot filters facts whose value
                    // falls outside the kind.
                    if !selector.admits(&artifact.is) {
                        continue;
                    }
                    let mut extension = base.clone();
                    selector.merge(&mut extension, &artifact)?;
                    yield extension;
                }
            }
        }
    }

    /// Execute this query, returning a stream of claims.
    pub fn perform<'a, Env>(self, env: &'a Env) -> impl Output<Claim> + 'a
    where
        Env: Provider<Select<'a>> + Provider<SelectRules> + ConditionalSync,
        Self: Sized,
    {
        Application::perform(self, env)
    }
}

impl Application for AttributeQueryAll {
    type Conclusion = Claim;

    fn evaluate<'a, Env, M: Selection + 'a>(self, selection: M, env: &'a Env) -> impl Selection + 'a
    where
        Env: Provider<Select<'a>> + Provider<SelectRules> + ConditionalSync,
    {
        self.evaluate(env, selection)
    }

    fn realize(&self, input: Match) -> Result<Claim, EvaluationError> {
        input.prove(&self.source)
    }
}

impl TryFrom<&AttributeQueryAll> for ArtifactSelector<Constrained> {
    type Error = EvaluationError;

    fn try_from(from: &AttributeQueryAll) -> Result<Self, Self::Error> {
        let mut selector: Option<ArtifactSelector<Constrained>> = None;

        match &from.the {
            Term::Constant(the) => {
                let relation = ArtifactsAttribute::try_from(the.clone()).map_err(|_| {
                    EvaluationError::Store("Could not convert value to Attribute".to_string())
                })?;
                selector = Some(match selector {
                    None => ArtifactSelector::new().the(relation),
                    Some(s) => s.the(relation),
                });
            }
            Term::Variable { .. } => {
                // A prefix refinement the planner stamped onto the
                // attribute variable becomes an AEV range bound.
                // Conformance-only refinements carry no prefix and
                // produce no range bound.
                if let Some(prefix) = from
                    .the
                    .kind()
                    .as_ref()
                    .and_then(Kind::refinement)
                    .and_then(|refinement| refinement.prefix.clone())
                {
                    selector = Some(match selector {
                        None => ArtifactSelector::new().the_starting_with(prefix),
                        Some(s) => s.the_starting_with(prefix),
                    });
                }
            }
        }

        match &from.of {
            Term::Constant(of) => {
                let entity = Entity::try_from(of.clone()).map_err(|_| {
                    EvaluationError::Store("Could not convert value to Entity".to_string())
                })?;
                selector = Some(match selector {
                    None => ArtifactSelector::new().of(entity.clone()),
                    Some(s) => s.of(entity),
                });
            }
            Term::Variable { .. } => {
                // A prefix refinement on the entity variable becomes
                // an EAV range bound over the URI's raw head.
                // Conformance-only refinements carry no prefix and
                // produce no range bound.
                if let Some(prefix) = from
                    .of
                    .kind()
                    .as_ref()
                    .and_then(Kind::refinement)
                    .and_then(|refinement| refinement.prefix.clone())
                {
                    selector = Some(match selector {
                        None => ArtifactSelector::new().of_starting_with(prefix),
                        Some(s) => s.of_starting_with(prefix),
                    });
                }
            }
        }

        match &from.is {
            Term::Constant(value) => {
                selector = Some(match selector {
                    None => ArtifactSelector::new().is(value.clone()),
                    Some(s) => s.is(value.clone()),
                });
            }
            Term::Variable { .. } => {
                // A prefix refinement the planner stamped onto the value
                // variable (e.g. from a `text/starts-with` constraint on it)
                // becomes a VAE range bound over the value. The M3
                // value-in-key format sorts values order-preservingly — and
                // spilled strings sort into the same band by their in-key
                // prefix, so the pushed range covers them too (the scan
                // loads and post-filters the rare probe that outruns the
                // stored prefix). Pushed ONLY when the kind admits nothing
                // but strings: the pushed range brackets the String band,
                // and the scan is the row generator, so a kind that still
                // admits Symbol or Entity lexical forms (which the residual
                // refinement would match) must keep the un-narrowed scan or
                // those rows would be silently dropped.
                // Conformance-only refinements carry no prefix and add no bound.
                if let Some(kind) = from.is.kind().as_ref()
                    && kind.primitive_part().required() == Primitive::from(Type::String)
                    && let Some(prefix) = kind
                        .refinement()
                        .and_then(|refinement| refinement.prefix.clone())
                {
                    selector = Some(match selector {
                        None => ArtifactSelector::new().is_starting_with(prefix),
                        Some(s) => s.is_starting_with(prefix),
                    });
                }
                // An interval refinement (from a comparison predicate on the
                // value variable) becomes VAE range bounds. Pushed ONLY when
                // the kind admits exactly one comparable type: a numeric
                // comparison adapts its literal to each ROW's type, so a
                // NUMERIC-wide kind admits rows of other numeric types that a
                // single-band range would silently drop (and a STRING_LIKE-
                // wide kind admits symbol rows a String band would drop). The
                // bound literal adapts to the scanned type exactly as the row
                // filter would — numeric literals losslessly, non-numeric
                // literals only to their own type; a bound that cannot adapt
                // pushes nothing (the residual comparison filters every row
                // of that type anyway).
                if let Some(kind) = from.is.kind().as_ref()
                    && let Some(interval) = kind
                        .refinement()
                        .and_then(|refinement| refinement.interval.as_ref())
                    && let Some(data_type) =
                        single_comparable_type(kind.primitive_part().required())
                {
                    if let Some(bound) = &interval.lower
                        && let Some(value) =
                            adapt_bound(interval.value_type, &bound.encoded, data_type)
                    {
                        selector = Some(push_bound(selector.take(), value, bound.inclusive, true));
                    }
                    if let Some(bound) = &interval.upper
                        && let Some(value) =
                            adapt_bound(interval.value_type, &bound.encoded, data_type)
                    {
                        selector = Some(push_bound(selector.take(), value, bound.inclusive, false));
                    }
                }
            }
        }

        selector.ok_or_else(|| EvaluationError::EmptySelector {
            message: "At least one field must be constrained".to_string(),
        })
    }
}

/// The single comparable type a primitive set admits, or `None` when it
/// admits none, several, or any non-comparable member — the gate for
/// pushing an interval bound into a scan range (see the pushdown comment
/// above).
fn single_comparable_type(primitive: Primitive) -> Option<Type> {
    [
        Type::UnsignedInt,
        Type::SignedInt,
        Type::Float,
        Type::String,
        Type::Symbol,
        Type::Entity,
        Type::Bytes,
    ]
    .into_iter()
    .find(|comparable| primitive == Primitive::from(*comparable))
}

/// Decodes an interval bound's literal and adapts it to the scanned
/// column's type, exactly as the comparison predicate adapts its literal
/// per row: a same-type bound passes through, a numeric literal adapts
/// LOSSLESSLY across the numeric types, and nothing else adapts. `None`
/// means no push: an unadaptable literal (`1.5` against integer data, a
/// string against a symbol column) admits no row of that type, and the
/// residual comparison already filters them all.
fn adapt_bound(literal_type: Type, encoded: &[u8], data_type: Type) -> Option<Value> {
    let (value, rest) = decode_value(literal_type, encoded)?;
    if !rest.is_empty() {
        return None;
    }
    if literal_type == data_type {
        return Some(value);
    }
    Numeric::try_from(value)
        .ok()?
        .instantiate(data_type)
        .map(Value::from)
}

/// Extends `selector` with one interval bound, choosing the matching
/// range constructor.
fn push_bound(
    selector: Option<ArtifactSelector<Constrained>>,
    value: Value,
    inclusive: bool,
    lower: bool,
) -> ArtifactSelector<Constrained> {
    match selector {
        Some(constrained) => match (lower, inclusive) {
            (true, true) => constrained.is_at_least(value),
            (true, false) => constrained.is_greater_than(value),
            (false, true) => constrained.is_at_most(value),
            (false, false) => constrained.is_less_than(value),
        },
        None => match (lower, inclusive) {
            (true, true) => ArtifactSelector::new().is_at_least(value),
            (true, false) => ArtifactSelector::new().is_greater_than(value),
            (false, true) => ArtifactSelector::new().is_at_most(value),
            (false, false) => ArtifactSelector::new().is_less_than(value),
        },
    }
}

impl Display for AttributeQueryAll {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "Claim {{")?;
        write!(f, "the: {},", self.the)?;
        write!(f, "of: {},", self.of)?;
        write!(f, "is: {},", self.is)?;
        write!(f, "cause: {},", self.cause)?;
        write!(f, "}}")
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use crate::artifact::encode_value_owned;
    use crate::query::Output;
    use crate::session::RuleRegistry;
    use crate::source::test::TestEnv;
    use crate::the;
    use crate::type_system::{Interval, IntervalBound, Refinement};
    use dialog_repository::helpers::{test_operator_with_profile, test_repo};
    use std::collections::BTreeSet;

    /// A prefix refinement stamped onto a variable term becomes a
    /// range bound on the selector — the end of the
    /// predicate → inference → planner → scan pipeline.
    #[dialog_common::test]
    fn it_pushes_prefix_refinements_into_the_selector() -> anyhow::Result<()> {
        let entity_kind = Kind::from(Type::Entity)
            .with_prefix("did:key:")
            .expect("entity is textual");
        let query = AttributeQueryAll::new(
            Term::from(the!("person/name")),
            Term::<Entity>::var("e").with_kind(entity_kind),
            Term::var("v"),
            Term::var("cause"),
        );
        let selector = ArtifactSelector::<Constrained>::try_from(&query)?;
        assert_eq!(selector.entity_prefix(), Some("did:key:"));

        let attribute_kind = Kind::from(Type::Symbol)
            .with_prefix("person/")
            .expect("symbol is textual");
        let query = AttributeQueryAll::new(
            Term::<The>::var("a").with_kind(attribute_kind),
            Term::<Entity>::var("e"),
            Term::var("v"),
            Term::var("cause"),
        );
        let selector = ArtifactSelector::<Constrained>::try_from(&query)?;
        assert_eq!(selector.attribute_prefix(), Some("person/"));

        // A prefix refinement on the value variable (e.g. from a
        // `text/starts-with` constraint) becomes a VAE value-range bound.
        let value_kind = Kind::from(Type::String)
            .with_prefix("alice")
            .expect("string is textual");
        let query = AttributeQueryAll::new(
            Term::from(the!("person/name")),
            Term::<Entity>::var("e"),
            Term::var("v").with_kind(value_kind),
            Term::var("cause"),
        );
        let selector = ArtifactSelector::<Constrained>::try_from(&query)?;
        assert_eq!(selector.value_prefix(), Some("alice"));

        // An unrefined variable contributes nothing, as before.
        let query = AttributeQueryAll::new(
            Term::from(the!("person/name")),
            Term::<Entity>::var("e"),
            Term::var("v"),
            Term::var("cause"),
        );
        let selector = ArtifactSelector::<Constrained>::try_from(&query)?;
        assert_eq!(selector.entity_prefix(), None);
        assert_eq!(selector.attribute_prefix(), None);
        assert_eq!(selector.value_prefix(), None);
        Ok(())
    }

    /// An interval refinement stamped onto a single-numeric-typed value
    /// variable becomes VAE range bounds on the selector; the bound
    /// literal adapts to the scanned type exactly as the residual
    /// comparison adapts per row.
    #[dialog_common::test]
    fn it_pushes_interval_refinements_into_the_selector() -> anyhow::Result<()> {
        // A same-type bound is pushed verbatim.
        let kind = Kind::from(Type::UnsignedInt)
            .with_interval(&Value::UnsignedInt(30), true, true)
            .expect("numeric admits an interval");
        let query = AttributeQueryAll::new(
            Term::from(the!("person/age")),
            Term::<Entity>::var("e"),
            Term::var("v").with_kind(kind),
            Term::var("cause"),
        );
        let selector = ArtifactSelector::<Constrained>::try_from(&query)?;
        let lower = selector.value_lower().expect("the bound is pushed");
        assert_eq!(lower.value, Value::UnsignedInt(30));
        assert!(lower.inclusive);
        assert!(selector.value_upper().is_none());

        // An integer literal bounding float data adapts to `30.0`.
        let kind = Kind::from(Type::Float)
            .with_interval(&Value::UnsignedInt(30), false, false)
            .expect("numeric admits an interval");
        let query = AttributeQueryAll::new(
            Term::from(the!("person/age")),
            Term::<Entity>::var("e"),
            Term::var("v").with_kind(kind),
            Term::var("cause"),
        );
        let selector = ArtifactSelector::<Constrained>::try_from(&query)?;
        let upper = selector.value_upper().expect("the bound is pushed");
        assert_eq!(upper.value, Value::Float(30.0));
        assert!(!upper.inclusive);
        assert!(selector.value_lower().is_none());

        // Both sides of a two-sided interval push together.
        let kind = Kind::from(Type::UnsignedInt)
            .with_interval(&Value::UnsignedInt(10), true, true)
            .expect("numeric admits an interval")
            .with_interval(&Value::UnsignedInt(20), false, false)
            .expect("the meet keeps both bounds");
        let query = AttributeQueryAll::new(
            Term::from(the!("person/age")),
            Term::<Entity>::var("e"),
            Term::var("v").with_kind(kind),
            Term::var("cause"),
        );
        let selector = ArtifactSelector::<Constrained>::try_from(&query)?;
        assert_eq!(
            selector.value_lower().expect("lower pushed").value,
            Value::UnsignedInt(10)
        );
        assert_eq!(
            selector.value_upper().expect("upper pushed").value,
            Value::UnsignedInt(20)
        );
        Ok(())
    }

    /// A string (or other non-numeric comparable) bound pushes when
    /// the kind admits exactly that one type, Datomic's
    /// `[(<= "Q" ?name)]` shape.
    #[dialog_common::test]
    fn it_pushes_string_interval_refinements_into_the_selector() -> anyhow::Result<()> {
        let kind = Kind::from(Type::String)
            .with_interval(&Value::String("Q".into()), true, true)
            .expect("string is comparable");
        let query = AttributeQueryAll::new(
            Term::from(the!("person/name")),
            Term::<Entity>::var("e"),
            Term::var("v").with_kind(kind),
            Term::var("cause"),
        );
        let selector = ArtifactSelector::<Constrained>::try_from(&query)?;
        let lower = selector.value_lower().expect("the bound is pushed");
        assert_eq!(lower.value, Value::String("Q".into()));
        assert!(lower.inclusive);
        Ok(())
    }

    /// The single-comparable-type gate: a kind admitting several
    /// types pushes no bound (rows of the other types would be
    /// silently dropped from the scan), and an unadaptable literal
    /// pushes no bound (no row of the scanned type satisfies it; the
    /// residual comparison filters every one).
    #[dialog_common::test]
    fn it_gates_interval_pushdown_to_single_comparable_kinds() -> anyhow::Result<()> {
        let wide = Kind::from(Primitive::NUMERIC)
            .with_interval(&Value::UnsignedInt(30), true, true)
            .expect("numeric admits an interval");
        let query = AttributeQueryAll::new(
            Term::from(the!("person/age")),
            Term::<Entity>::var("e"),
            Term::var("v").with_kind(wide),
            Term::var("cause"),
        );
        let selector = ArtifactSelector::<Constrained>::try_from(&query)?;
        assert!(
            selector.value_lower().is_none() && selector.value_upper().is_none(),
            "a kind admitting several numeric types pushes nothing"
        );

        // A String bound on a STRING_LIKE kind narrows the membership
        // to String (a non-numeric literal never adapts, so symbol
        // rows could never match), which makes it pushable.
        let narrowed = Kind::from(Primitive::STRING_LIKE)
            .with_interval(&Value::String("Q".into()), true, true)
            .expect("strings are comparable");
        let query = AttributeQueryAll::new(
            Term::from(the!("person/name")),
            Term::<Entity>::var("e"),
            Term::var("v").with_kind(narrowed),
            Term::var("cause"),
        );
        let selector = ArtifactSelector::<Constrained>::try_from(&query)?;
        assert!(
            selector.value_lower().is_some(),
            "the narrowed membership admits only String, so the bound pushes"
        );

        // A hand-built (e.g. deserialized) kind that stayed wide must
        // still gate: a String band would drop the symbol rows it
        // admits.
        let string_like = Kind::Refined(
            Primitive::STRING_LIKE,
            Refinement {
                prefix: None,
                conforms: BTreeSet::default(),
                interval: Some(Box::new(Interval {
                    value_type: Type::String,
                    lower: Some(IntervalBound {
                        encoded: encode_value_owned(&Value::String("Q".into())),
                        inclusive: true,
                    }),
                    upper: None,
                })),
            },
        );
        let query = AttributeQueryAll::new(
            Term::from(the!("person/name")),
            Term::<Entity>::var("e"),
            Term::var("v").with_kind(string_like),
            Term::var("cause"),
        );
        let selector = ArtifactSelector::<Constrained>::try_from(&query)?;
        assert!(
            selector.value_lower().is_none() && selector.value_upper().is_none(),
            "a kind still admitting Symbol pushes no String bound"
        );

        let unadaptable = Kind::from(Type::UnsignedInt)
            .with_interval(&Value::Float(1.5), true, true)
            .expect("numeric admits an interval");
        let query = AttributeQueryAll::new(
            Term::from(the!("person/age")),
            Term::<Entity>::var("e"),
            Term::var("v").with_kind(unadaptable),
            Term::var("cause"),
        );
        let selector = ArtifactSelector::<Constrained>::try_from(&query)?;
        assert!(
            selector.value_lower().is_none() && selector.value_upper().is_none(),
            "a literal that cannot adapt losslessly pushes nothing"
        );

        // Defense in depth: `with_interval` can no longer construct a
        // kind whose interval type disagrees with its membership (the
        // meet is empty), but a hand-built or deserialized kind still
        // can — the adaptation gate must hold on its own.
        let cross_type = Kind::Refined(
            Primitive::from(Type::Symbol),
            Refinement {
                prefix: None,
                conforms: BTreeSet::default(),
                interval: Some(Box::new(Interval {
                    value_type: Type::String,
                    lower: Some(IntervalBound {
                        encoded: encode_value_owned(&Value::String("Q".into())),
                        inclusive: true,
                    }),
                    upper: None,
                })),
            },
        );
        let query = AttributeQueryAll::new(
            Term::from(the!("tag/kind")),
            Term::<Entity>::var("e"),
            Term::var("v").with_kind(cross_type),
            Term::var("cause"),
        );
        let selector = ArtifactSelector::<Constrained>::try_from(&query)?;
        assert!(
            selector.value_lower().is_none() && selector.value_upper().is_none(),
            "a string literal never adapts to a symbol column"
        );
        Ok(())
    }

    /// A pushed interval bound scans the right rows from a real tree,
    /// bound-equal rows included.
    #[dialog_common::test]
    async fn it_scans_with_pushed_interval_bounds() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        branch
            .transaction()
            .assert(the!("person/age").of(Entity::new()?).is(10u64))
            .assert(the!("person/age").of(Entity::new()?).is(30u64))
            .assert(the!("person/age").of(Entity::new()?).is(50u64))
            .commit()
            .perform(&operator)
            .await?;

        let kind = Kind::from(Type::UnsignedInt)
            .with_interval(&Value::UnsignedInt(30), true, true)
            .expect("numeric admits an interval");
        let query = AttributeQueryAll::new(
            Term::from(the!("person/age")),
            Term::<Entity>::var("e"),
            Term::var("v").with_kind(kind),
            Term::var("cause"),
        );

        let source = TestEnv::new(&branch, &operator, RuleRegistry::new());
        let results = query.perform(&source).try_vec().await?;
        let values: Vec<&Value> = results.iter().map(|artifact| artifact.is()).collect();
        assert_eq!(values.len(), 2, "10 is below the bound");
        assert!(
            values.contains(&&Value::UnsignedInt(30)),
            "the range starts at the inclusive bound"
        );
        assert!(values.contains(&&Value::UnsignedInt(50)));
        Ok(())
    }

    /// A pushed string bound scans the right rows from a real tree —
    /// the Datomic `[(<= "Q" ?name)]` example, index-accelerated.
    #[dialog_common::test]
    async fn it_scans_with_pushed_string_bounds() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        branch
            .transaction()
            .assert(
                the!("person/name")
                    .of(Entity::new()?)
                    .is("Alice".to_string()),
            )
            .assert(
                the!("person/name")
                    .of(Entity::new()?)
                    .is("Quinn".to_string()),
            )
            .assert(the!("person/name").of(Entity::new()?).is("Zed".to_string()))
            .commit()
            .perform(&operator)
            .await?;

        let kind = Kind::from(Type::String)
            .with_interval(&Value::String("Q".into()), true, true)
            .expect("string is comparable");
        let query = AttributeQueryAll::new(
            Term::from(the!("person/name")),
            Term::<Entity>::var("e"),
            Term::var("v").with_kind(kind),
            Term::var("cause"),
        );

        let source = TestEnv::new(&branch, &operator, RuleRegistry::new());
        let results = query.perform(&source).try_vec().await?;
        let values: Vec<&Value> = results.iter().map(|artifact| artifact.is()).collect();
        assert_eq!(values.len(), 2, "Alice sorts below the bound");
        assert!(values.contains(&&Value::String("Quinn".into())));
        assert!(values.contains(&&Value::String("Zed".into())));
        Ok(())
    }

    /// Mixed numeric rows under a NUMERIC-wide kind: nothing is
    /// pushed, so every numeric row reaches the residual comparison —
    /// the scan produces no false negatives.
    #[dialog_common::test]
    async fn it_keeps_mixed_numeric_rows_without_single_type_kind() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        branch
            .transaction()
            .assert(the!("score/value").of(Entity::new()?).is(5u64))
            .assert(the!("score/value").of(Entity::new()?).is(2.5f64))
            .assert(the!("score/value").of(Entity::new()?).is(-3i64))
            .commit()
            .perform(&operator)
            .await?;

        let wide = Kind::from(Primitive::NUMERIC)
            .with_interval(&Value::UnsignedInt(1), true, true)
            .expect("numeric admits an interval");
        let query = AttributeQueryAll::new(
            Term::from(the!("score/value")),
            Term::<Entity>::var("e"),
            Term::var("v").with_kind(wide),
            Term::var("cause"),
        );

        let source = TestEnv::new(&branch, &operator, RuleRegistry::new());
        let results = query.perform(&source).try_vec().await?;
        assert_eq!(
            results.len(),
            3,
            "the un-narrowed scan yields every numeric row"
        );
        Ok(())
    }

    /// A prefix refinement whose kind still admits Symbol (or Entity)
    /// lexical forms must NOT be pushed into the scan: the pushed range
    /// brackets only the inline String band of the VAE index, and the scan
    /// is the row generator, so a symbol row the refinement admits would be
    /// silently dropped with nothing downstream able to restore it.
    #[dialog_common::test]
    async fn it_keeps_symbol_matches_under_prefix_pushdown() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let e = Entity::new()?;
        let symbol = ArtifactsAttribute::try_from("user/name".to_string())?;
        branch
            .transaction()
            .assert(the!("tag/kind").of(e.clone()).is(symbol))
            .commit()
            .perform(&operator)
            .await?;

        // The kind a `starts-with` constraint stamps onto a symbol-valued
        // variable: Symbol membership refined by the prefix.
        let value_kind = Kind::from(Type::Symbol)
            .with_prefix("user/")
            .expect("symbol is textual");
        let query = AttributeQueryAll::new(
            Term::from(the!("tag/kind")),
            Term::<Entity>::var("e"),
            Term::var("v").with_kind(value_kind),
            Term::var("cause"),
        );

        let source = TestEnv::new(&branch, &operator, RuleRegistry::new());
        let results = query.perform(&source).try_vec().await?;
        assert_eq!(
            results.len(),
            1,
            "a symbol admitted by the refinement must be scanned"
        );
        assert_eq!(
            results[0].is(),
            &Value::Symbol("user/name".to_string().try_into()?)
        );
        Ok(())
    }

    #[dialog_common::test]
    async fn it_scans_with_all_variables() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let alice = Entity::new()?;

        branch
            .transaction()
            .assert(
                the!("person/name")
                    .of(alice.clone())
                    .is("Alice".to_string()),
            )
            .commit()
            .perform(&operator)
            .await?;

        let query = AttributeQueryAll::new(
            Term::from(the!("person/name")),
            Term::var("person"),
            Term::var("name"),
            Term::var("cause"),
        );

        let source = TestEnv::new(&branch, &operator, RuleRegistry::new());
        let results = query.perform(&source).try_vec().await?;

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].of(), &alice);
        assert_eq!(results[0].is(), &Value::String("Alice".to_string()));

        Ok(())
    }

    /// An *optional* `is` whose variable was pre-bound to a value the
    /// entity does not have must not emit an Absent fallback. The
    /// value-keyed scan finds nothing equal to the pinned value, but
    /// that is a value *mismatch*, not absence: the attribute exists
    /// with a different value. Before the fix, the fallback fired and
    /// tried to bind the already-Present `is` variable to Absent,
    /// which errors and aborts the stream.
    #[dialog_common::test]
    async fn it_does_not_emit_absent_on_optional_value_mismatch() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let bob = Entity::new()?;

        // Bob HAS a nickname, but it is "Bobby".
        branch
            .transaction()
            .assert(
                the!("person/nickname")
                    .of(bob.clone())
                    .is("Bobby".to_string()),
            )
            .commit()
            .perform(&operator)
            .await?;

        let optional_is: Term<Any> = Term::<Option<String>>::var("nickname").into();
        let query = AttributeQueryAll::new(
            Term::from(the!("person/nickname")),
            Term::from(bob.clone()),
            optional_is.clone(),
            Term::var("cause"),
        );

        // An earlier premise pinned ?nickname to "Ali", not Bob's.
        let mut seed = Match::new();
        seed.bind(&optional_is, Value::from("Ali".to_string()))?;

        let source = TestEnv::new(&branch, &operator, RuleRegistry::new());
        let results =
            Selection::try_vec(Application::evaluate(query, seed.seed(), &source)).await?;

        assert_eq!(
            results.len(),
            0,
            "a value mismatch on an optional field is not absence: no Absent fallback"
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn it_scans_with_constant_entity() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let alice = Entity::new()?;
        let bob = Entity::new()?;

        branch
            .transaction()
            .assert(
                the!("person/name")
                    .of(alice.clone())
                    .is("Alice".to_string()),
            )
            .assert(the!("person/name").of(bob.clone()).is("Bob".to_string()))
            .commit()
            .perform(&operator)
            .await?;

        let query = AttributeQueryAll::new(
            Term::from(the!("person/name")),
            Term::from(alice.clone()),
            Term::var("name"),
            Term::var("cause"),
        );

        let source = TestEnv::new(&branch, &operator, RuleRegistry::new());
        let results = query.perform(&source).try_vec().await?;

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].of(), &alice);
        assert_eq!(results[0].is(), &Value::String("Alice".to_string()));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_returns_multiple_values() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let alice = Entity::new()?;

        branch
            .transaction()
            .assert(
                the!("person/name")
                    .of(alice.clone())
                    .is("Alice".to_string()),
            )
            .commit()
            .perform(&operator)
            .await?;

        branch
            .transaction()
            .assert(
                the!("person/name")
                    .of(alice.clone())
                    .is("Alicia".to_string()),
            )
            .commit()
            .perform(&operator)
            .await?;

        let query = AttributeQueryAll::new(
            Term::from(the!("person/name")),
            Term::from(alice.clone()),
            Term::var("name"),
            Term::var("cause"),
        );

        let source = TestEnv::new(&branch, &operator, RuleRegistry::new());
        let results = query.perform(&source).try_vec().await?;

        assert_eq!(
            results.len(),
            2,
            "AttributeQueryAll should return all values, not just the winner"
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn it_scans_with_constant_value() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let alice = Entity::new()?;

        branch
            .transaction()
            .assert(
                the!("person/name")
                    .of(alice.clone())
                    .is("Alice".to_string()),
            )
            .assert(
                the!("person/name")
                    .of(alice.clone())
                    .is("Alicia".to_string()),
            )
            .commit()
            .perform(&operator)
            .await?;

        let query = AttributeQueryAll::new(
            Term::from(the!("person/name")),
            Term::var("person"),
            Term::constant("Alice".to_string()),
            Term::var("cause"),
        );

        let source = TestEnv::new(&branch, &operator, RuleRegistry::new());
        let results = query.perform(&source).try_vec().await?;

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].is(), &Value::String("Alice".to_string()));

        Ok(())
    }

    /// `AttributeQueryAll::schema()` declares the `the` slot as
    /// a singleton primitive over `Symbol`.
    #[dialog_common::test]
    fn schema_the_slot_is_primitive_symbol() {
        let query = AttributeQueryAll::new(
            Term::var("the"),
            Term::var("of"),
            Term::var("is"),
            Term::var("cause"),
        );
        let schema = query.schema();
        let the = schema.get("the").expect("the field present");
        let content = the.content_type().expect("symbol kind present");
        assert!(!content.is_optional());
        assert_eq!(content.as_value_type(), Some(Type::Symbol));
        assert!(matches!(content, Kind::Primitive(_)));
    }

    /// `AttributeQueryAll::schema()` declares the `of` slot as
    /// a singleton primitive over `Entity`.
    #[dialog_common::test]
    fn schema_of_slot_is_primitive_entity() {
        let query = AttributeQueryAll::new(
            Term::var("the"),
            Term::var("of"),
            Term::var("is"),
            Term::var("cause"),
        );
        let schema = query.schema();
        let of = schema.get("of").expect("of field present");
        let content = of.content_type().expect("entity kind present");
        assert_eq!(content.as_value_type(), Some(Type::Entity));
    }

    /// `AttributeQueryAll::schema()` declares the `is` slot as
    /// `None` (unknown) when the term carries no static info;
    /// the unifier narrows at rule-compile time.
    #[dialog_common::test]
    fn schema_is_slot_is_unknown_when_untyped() {
        let query = AttributeQueryAll::new(
            Term::var("the"),
            Term::var("of"),
            Term::var("is"),
            Term::var("cause"),
        );
        let schema = query.schema();
        let is = schema.get("is").expect("is field present");
        assert!(
            is.content_type().is_none(),
            "untyped `is` term should yield unknown content_type"
        );
    }
}
