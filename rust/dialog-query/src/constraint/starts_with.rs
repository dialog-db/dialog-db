//! Prefix predicate over the textual kinds.
//!
//! `StartsWith` asserts that the lexical form of a term's value
//! begins with a string prefix. One predicate ranges over every
//! kind with a lexical form — strings, symbols, entities — via the
//! TEXTUAL bound, rather than a variant per kind. Like
//! [`TypeOf`](super::TypeOf), one declaration has two effects:
//!
//! - **Inference**: the subject slot's content type is the set of
//!   TEXTUAL members the prefix could actually begin. Each member
//!   has a lexical grammar (symbols are length-bounded
//!   `namespace/predicate` names, entities are serialized URLs), so
//!   a constant prefix that no value of some member could start
//!   with drops that member from the subject's inferred kind: a
//!   space drops Entity, a 65-byte prefix drops Symbol. Only
//!   *certainly impossible* members are dropped — String admits any
//!   prefix, so the contributed kind is never empty.
//! - **Evaluation**: a row whose value's lexical form does not begin
//!   with the prefix is a non-match — filtered, never an error —
//!   and so is a value with no lexical form at all.
//!
//! The refined kind plus the prefix is exactly the per-member index
//! range bound the planned scan pushdown consumes.

use std::fmt;
use std::fmt::Display;
use std::ops::Not;

use crate::artifact::Type as ValueType;
use crate::artifact::Value;
use crate::error::EvaluationError;
use crate::selection::Selection;
use crate::type_system::Primitive;
use crate::type_system::Type as Kind;
use crate::type_system::lexical_form;
use crate::types::Any;
use crate::{Binding, Cardinality, Environment, Field, Parameters, Requirement, Schema, Term};
use crate::{Constraint, Negation, Premise, Proposition, try_stream};

/// Cost for evaluating a prefix predicate (single row lookup + string
/// comparison).
const STARTS_WITH_COST: usize = 1;

/// The longest lexical form a symbol can have, in bytes. Mirrors
/// `dialog_artifacts::ATTRIBUTE_LENGTH`, which is crate-private;
/// pinned against the real validator by test.
const SYMBOL_LEXICAL_LIMIT: usize = 64;

/// Prefix predicate: the lexical form of `of`'s value begins with
/// `prefix`.
///
/// Constructed via the [`Term::starts_with`] sugar. The subject
/// ranges over the TEXTUAL kinds; the prefix is a string.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct StartsWith {
    /// The subject term, whose value's lexical form is compared.
    pub of: Term<Any>,
    /// The prefix the lexical form must begin with.
    pub prefix: Term<String>,
}

/// The TEXTUAL members whose lexical grammar admits a value
/// beginning with `prefix`. Conservative: a member is dropped only
/// when *no* value of that kind can start with the prefix.
///
/// - `String` admits anything; never dropped.
/// - `Symbol` lexical forms are at most [`SYMBOL_LEXICAL_LIMIT`]
///   bytes (the only constraint the validator enforces besides the
///   mandatory `/`, which a prefix cannot rule out).
/// - `Entity` lexical forms are serialized URLs; see
///   [`could_begin_entity`].
fn admissible_members(prefix: &str) -> Primitive {
    let mut members = Primitive::singleton(ValueType::String);
    if prefix.len() <= SYMBOL_LEXICAL_LIMIT {
        members = members.union(Primitive::singleton(ValueType::Symbol));
    }
    if could_begin_entity(prefix) {
        members = members.union(Primitive::singleton(ValueType::Entity));
    }
    members
}

/// Whether `prefix` could begin the serialized form of an entity
/// URI.
///
/// Entities serialize through the WHATWG URL algorithm, whose
/// output always begins `scheme:` — an ASCII-alphabetic head
/// followed by alphanumerics or `+`/`-`/`.` — and never contains
/// raw whitespace or control characters (those are percent-encoded
/// away). Anything not certainly impossible stays admissible; URI
/// length is unbounded (index keys hash the tail), so length never
/// excludes.
fn could_begin_entity(prefix: &str) -> bool {
    if prefix.chars().any(|c| c.is_whitespace() || c.is_control()) {
        return false;
    }
    let scheme = prefix.split(':').next().unwrap_or("");
    let mut chars = scheme.chars();
    match chars.next() {
        // The empty prefix begins everything; a leading `:` means an
        // empty scheme, which no URL has.
        None => prefix.is_empty(),
        Some(head) => {
            head.is_ascii_alphabetic()
                && chars.all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '-' | '.'))
        }
    }
}

impl StartsWith {
    /// Create a prefix predicate over the given subject and prefix.
    pub fn new(of: Term<Any>, prefix: Term<String>) -> Self {
        Self { of, prefix }
    }

    /// Schema: both slots are *hard* requirements (the predicate
    /// consumes bound values; the planner orders it after the
    /// premises binding them). The subject's content type carries
    /// both halves of what a constant prefix proves: the admissible
    /// TEXTUAL member set (the lexical-grammar refinement) and the
    /// prefix itself as a [`Refinement`](crate::type_system::Refinement)
    /// — which is how the prefix travels through inference to the
    /// scan-range pushdown. The prefix slot is a `String`.
    pub fn schema(&self) -> Schema {
        let content = match self.prefix.as_constant() {
            Some(Value::String(prefix)) => {
                let members = Kind::from(admissible_members(prefix));
                members
                    .clone()
                    .with_prefix(prefix.clone())
                    .unwrap_or(members)
            }
            _ => Kind::from(Primitive::TEXTUAL),
        };
        let mut schema = Schema::new();
        schema.insert(
            "of".to_string(),
            Field {
                description: "Term whose value's lexical form is compared".to_string(),
                content_type: Some(content),
                requirement: Requirement::required(),
                cardinality: Cardinality::One,
            },
        );
        schema.insert(
            "prefix".to_string(),
            Field {
                description: "Prefix the lexical form must begin with".to_string(),
                content_type: Some(Kind::from(ValueType::String)),
                requirement: Requirement::required(),
                cardinality: Cardinality::One,
            },
        );
        schema
    }

    /// Estimate cost. Constant — a row-local string comparison.
    pub fn estimate(&self, _env: &Environment) -> Option<usize> {
        Some(STARTS_WITH_COST)
    }

    /// Returns the named parameters for this constraint.
    pub fn parameters(&self) -> Parameters {
        let mut params = Parameters::new();
        params.insert("of".to_string(), self.of.clone());
        params.insert("prefix".to_string(), Term::from(&self.prefix));
        params
    }

    /// Evaluate: filter rows whose value's lexical form does not
    /// begin with the prefix.
    ///
    /// - Subject and prefix both `Present`: yield iff the subject
    ///   has a lexical form beginning with the (string) prefix. A
    ///   non-textual subject or a non-string prefix is a non-match.
    /// - Either `Absent`: a non-match (a scalar slot matches nothing
    ///   against a claimed absence).
    /// - Either unbound: a planner-contract violation — the schema
    ///   hard-requires both — surfaced as an error.
    pub fn evaluate<M: Selection>(self, selection: M) -> impl Selection {
        let of = self.of;
        let prefix: Term<Any> = Term::from(&self.prefix);
        try_stream! {
            for await candidate in selection {
                let base = candidate?;
                match (base.lookup(&of), base.lookup(&prefix)) {
                    (Ok(Binding::Present(subject)), Ok(Binding::Present(Value::String(needle)))) => {
                        if let Some(form) = lexical_form(&subject)
                            && form.starts_with(needle.as_str())
                        {
                            yield base;
                        }
                    }
                    (Ok(_), Ok(_)) => {}
                    (Err(_), _) => {
                        Err(EvaluationError::UnboundVariable {
                            variable_name: of.name().unwrap_or("of").to_string(),
                        })?;
                    }
                    (_, Err(_)) => {
                        Err(EvaluationError::UnboundVariable {
                            variable_name: prefix.name().unwrap_or("prefix").to_string(),
                        })?;
                    }
                }
            }
        }
    }
}

impl Display for StartsWith {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "starts-with({}, {})", self.of, self.prefix)
    }
}

impl Term<Any> {
    /// Prefix predicate: this term's value has a lexical form
    /// beginning with `prefix`. Ranges over the TEXTUAL kinds; a
    /// constant prefix narrows the subject to the members it could
    /// begin (see [`TypeOf`](super::TypeOf) for the narrowing
    /// mechanics).
    pub fn starts_with(self, prefix: impl Into<Term<String>>) -> Premise {
        Premise::Assert(Proposition::Constraint(Constraint::StartsWith(
            StartsWith::new(self, prefix.into()),
        )))
    }
}

impl From<StartsWith> for Constraint {
    fn from(predicate: StartsWith) -> Self {
        Constraint::StartsWith(predicate)
    }
}

impl Not for StartsWith {
    type Output = Premise;

    fn not(self) -> Self::Output {
        Premise::Unless(Negation::not(Proposition::Constraint(
            Constraint::StartsWith(self),
        )))
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use crate::artifact::{ArtifactsAttribute, Entity};
    use crate::rule::TypeEnv;
    use crate::selection::Match;
    use futures_util::TryStreamExt;

    fn symbol(name: &str) -> Value {
        Value::Symbol(ArtifactsAttribute::try_from(name.to_string()).expect("valid attribute"))
    }

    async fn matches(predicate: StartsWith, value: Value) -> Result<usize, EvaluationError> {
        let mut row = Match::new();
        row.bind(&Term::var("x"), value)?;
        let results: Vec<Match> = predicate.evaluate(row.seed()).try_collect().await?;
        Ok(results.len())
    }

    fn prefix(text: &str) -> StartsWith {
        StartsWith::new(Term::var("x"), Term::from(text))
    }

    /// Every textual kind compares its lexical form.
    #[dialog_common::test]
    async fn it_compares_lexical_forms() -> Result<(), EvaluationError> {
        assert_eq!(
            matches(prefix("he"), Value::String("hello".into())).await?,
            1,
            "string prefix matches"
        );
        assert_eq!(
            matches(prefix("lo"), Value::String("hello".into())).await?,
            0,
            "non-prefix is a non-match"
        );
        assert_eq!(
            matches(prefix("user/"), symbol("user/name")).await?,
            1,
            "symbol compares its namespace/predicate form"
        );
        assert_eq!(
            matches(prefix("group/"), symbol("user/name")).await?,
            0,
            "symbol outside the prefix is a non-match"
        );

        let entity = Entity::new().expect("fresh entity");
        let did = Value::Entity(entity);
        assert_eq!(
            matches(prefix("did:key:"), did.clone()).await?,
            1,
            "entity compares its URI form"
        );
        assert_eq!(
            matches(prefix("http:"), did).await?,
            0,
            "entity outside the prefix is a non-match"
        );
        Ok(())
    }

    /// A value with no lexical form is a non-match, not an error.
    #[dialog_common::test]
    async fn it_filters_non_textual_values() -> Result<(), EvaluationError> {
        assert_eq!(matches(prefix(""), Value::UnsignedInt(7)).await?, 0);
        assert_eq!(matches(prefix(""), Value::Boolean(true)).await?, 0);
        Ok(())
    }

    /// An Absent subject matches nothing, both slots being scalar.
    #[dialog_common::test]
    async fn it_filters_absent_subjects() -> Result<(), EvaluationError> {
        let predicate = prefix("he");
        let mut row = Match::new();
        row.bind_absent(&Term::var("x"))?;
        let results: Vec<Match> = predicate.evaluate(row.seed()).try_collect().await?;
        assert_eq!(results.len(), 0, "Absent matches nothing scalar");
        Ok(())
    }

    /// A prefix bound to a non-string value is a non-match.
    #[dialog_common::test]
    async fn it_filters_non_string_prefixes() -> Result<(), EvaluationError> {
        let predicate = StartsWith::new(Term::var("x"), Term::var("p"));
        let mut row = Match::new();
        row.bind(&Term::var("x"), Value::String("hello".into()))?;
        row.bind(&Term::<Any>::var("p"), Value::UnsignedInt(7))?;
        let results: Vec<Match> = predicate.evaluate(row.seed()).try_collect().await?;
        assert_eq!(results.len(), 0, "a numeric prefix matches nothing");
        Ok(())
    }

    /// An unbound slot is a planner-contract violation.
    #[dialog_common::test]
    async fn it_errors_on_unbound_slots() {
        let predicate = prefix("he");
        let results: Result<Vec<Match>, _> =
            predicate.evaluate(Match::new().seed()).try_collect().await;
        assert!(results.is_err(), "unbound subject must error");

        let predicate = StartsWith::new(Term::var("x"), Term::var("p"));
        let mut row = Match::new();
        row.bind(&Term::var("x"), Value::String("hello".into()))
            .expect("binds");
        let results: Result<Vec<Match>, _> = predicate.evaluate(row.seed()).try_collect().await;
        assert!(results.is_err(), "unbound prefix must error");
    }

    /// The lexical-grammar refinement: a constant prefix drops the
    /// TEXTUAL members no value of which could begin with it.
    #[dialog_common::test]
    fn it_refines_members_by_lexical_grammar() {
        let string_only = Primitive::singleton(ValueType::String);
        let string_and_symbol = string_only.union(Primitive::singleton(ValueType::Symbol));
        let string_and_entity = string_only.union(Primitive::singleton(ValueType::Entity));

        assert_eq!(
            admissible_members(""),
            Primitive::TEXTUAL,
            "the empty prefix begins everything"
        );
        assert_eq!(
            admissible_members("did:key:z6Mk"),
            Primitive::TEXTUAL,
            "a URI-shaped prefix begins symbols and strings too"
        );
        assert_eq!(
            admissible_members("has space"),
            string_and_symbol,
            "whitespace never appears in a serialized URL"
        );
        assert_eq!(
            admissible_members("1user/"),
            string_and_symbol,
            "a URL scheme cannot begin with a digit"
        );
        assert_eq!(
            admissible_members(":oops"),
            string_and_symbol,
            "no URL has an empty scheme"
        );
        assert_eq!(
            admissible_members(&"a".repeat(SYMBOL_LEXICAL_LIMIT + 1)),
            string_and_entity,
            "a prefix longer than any symbol drops Symbol"
        );
        assert_eq!(
            admissible_members(&" ".repeat(SYMBOL_LEXICAL_LIMIT + 1)),
            string_only,
            "both grammars can drop at once; String always remains"
        );
    }

    /// Pin [`SYMBOL_LEXICAL_LIMIT`] against the real validator.
    #[dialog_common::test]
    fn it_mirrors_the_attribute_length_limit() {
        let longest = format!("ns/{}", "a".repeat(SYMBOL_LEXICAL_LIMIT - 3));
        assert!(
            ArtifactsAttribute::try_from(longest.clone()).is_ok(),
            "a symbol of exactly the limit is valid"
        );
        assert!(
            ArtifactsAttribute::try_from(format!("{longest}a")).is_err(),
            "one byte past the limit is rejected"
        );
    }

    /// Inference consumes the refinement: a constant prefix narrows
    /// the subject's kind rule-wide — both the member set and the
    /// prefix itself, which the scan boundary turns into range
    /// bounds.
    #[dialog_common::test]
    fn it_narrows_subjects_via_prefix_refinement() -> anyhow::Result<()> {
        let premises = vec![Term::<Any>::var("x").starts_with("has space")];
        let env = TypeEnv::infer(&premises)?;
        let kind = env.get("x").expect("inferred");
        assert_eq!(
            kind.primitive_part(),
            Primitive::singleton(ValueType::String).union(Primitive::singleton(ValueType::Symbol)),
            "a prefix with a space excludes entities"
        );
        assert_eq!(
            kind.refinement().expect("the prefix travels").prefix,
            "has space",
            "the inferred kind carries the prefix refinement"
        );
        Ok(())
    }

    /// A variable prefix contributes the full TEXTUAL bound to the
    /// subject and String to itself.
    #[dialog_common::test]
    fn it_bounds_with_full_textual_for_variable_prefixes() -> anyhow::Result<()> {
        let predicate = StartsWith::new(Term::var("x"), Term::var("p"));
        let premises = vec![Premise::Assert(Proposition::Constraint(
            Constraint::StartsWith(predicate),
        ))];
        let env = TypeEnv::infer(&premises)?;
        assert_eq!(
            env.get("x").expect("inferred").primitive_part(),
            Primitive::TEXTUAL,
            "the subject stays bounded TEXTUAL"
        );
        assert_eq!(
            env.get("p").expect("inferred").primitive_part(),
            Primitive::singleton(ValueType::String),
            "the prefix is a string"
        );
        Ok(())
    }

    /// Known misalignment is a compile-time conflict: a subject
    /// already narrowed to Entity cannot start with a prefix no URL
    /// begins with.
    #[dialog_common::test]
    fn it_rejects_entity_subject_with_impossible_prefix() {
        let premises = vec![
            Term::<Any>::var("x").entity(),
            Term::<Any>::var("x").starts_with("has space"),
        ];
        assert!(
            TypeEnv::infer(&premises).is_err(),
            "Entity and String|Symbol have an empty meet"
        );
    }
}
