//! Coalesce constraint — set-widening unwrap with a fallback.
//!
//! `Coalesce` is the v2 expression of "if `source` is `Present`,
//! bind `is` to it; otherwise bind `is` to `fallback`." It is
//! the operator a `Term::<Option<U>>::unwrap_or` builder produces:
//!
//! ```text
//! nickname.unwrap_or("Anon").is(display_name)
//! ```
//!
//! Evaluation is row-local: one row in, one row out. The
//! `source` term is looked up against the input row's bindings;
//! if [`Binding::Present`](crate::Binding::Present) the value
//! flows into `is`; if [`Binding::Absent`](crate::Binding::Absent)
//! (or if `source` is unbound) the fallback value flows into `is`
//! instead. `fallback` may itself be a term — variable or
//! constant — and is resolved in the same row.

use std::fmt;
use std::fmt::Display;

use crate::type_system::unifier::{Context, Type as UnifierType, UnifyError, lift};
use crate::types::Any;
use crate::{
    Binding, Cardinality, Environment, Field, Parameters, Requirement, Schema, Selection, Term,
    try_stream,
};

/// Cost for evaluating a coalesce constraint (single row lookup +
/// branch + bind).
const COALESCE_COST: usize = 1;

/// Set-widening unwrap: produce a non-optional value from an
/// optional `source`, falling back to `fallback` when the source
/// is `Absent`.
///
/// Builder shape:
/// ```no_run
/// # use dialog_query::Term;
/// let nickname: Term<Option<String>> = Term::var("nickname");
/// let display_name: Term<String> = Term::var("display_name");
/// let premise = nickname.unwrap_or("Anon").is(display_name);
/// ```
///
/// Type contract (checked at rule-compile time by
/// [`Coalesce::validate`]):
/// - `source` has kind `Optional<α>` for some `α`.
/// - `fallback` has kind `α`.
/// - `is` has kind `α`.
///
/// At runtime, evaluation is `.map`-style — one row in, one row
/// out:
/// - If `source` looks up to `Present(v)`: bind `is` to `v`.
/// - If `source` looks up to `Absent` or is unbound: bind `is` to
///   `fallback`'s resolved value.
/// - If `fallback` itself is an unbound variable, the row is
///   filtered out (we can't bind without a concrete value).
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Coalesce {
    /// The optional input term — typically a variable bound by an
    /// upstream optional attribute query.
    pub source: Term<Any>,
    /// The default value used when `source` is `Absent` or unbound.
    pub fallback: Term<Any>,
    /// The output term that receives either `source`'s value or
    /// `fallback`'s value.
    pub is: Term<Any>,
}

impl Coalesce {
    /// Create a new coalesce constraint.
    ///
    /// The typed builder
    /// [`Term::<Option<U>>::unwrap_or`](crate::Term::unwrap_or)
    /// enforces the type contract at Rust's type level — call
    /// sites with type mismatches fail to compile. For dynamic
    /// construction (wire-format deserialization, raw
    /// `Coalesce::new` calls), [`Self::validate`] checks the
    /// contract at runtime; it is invoked automatically by
    /// [`DeductiveRule::new`](crate::DeductiveRule::new) so any
    /// Coalesce reaching the rule compiler is checked, regardless
    /// of how it was constructed.
    pub fn new(source: Term<Any>, fallback: Term<Any>, is: Term<Any>) -> Self {
        Self {
            source,
            fallback,
            is,
        }
    }

    /// Validate the type contract of this coalesce against a
    /// unification context: pick a fresh `α`, then unify `α` with
    /// `source`'s underlying (non-Nothing) shape, `fallback`'s
    /// kind, and `is`'s kind.
    ///
    /// **Source typing is enforced.** If `source` carries a static
    /// kind, it must be set-widened (admit `Nothing`); otherwise
    /// the coalesce can never trigger the fallback path and is
    /// rejected with [`UnifyError::SourceNotOptional`]. If `source`
    /// has no static kind (`kind() == None`), it is treated as
    /// fully unconstrained — the unifier may not narrow it. The
    /// caller is then responsible for ensuring the source is
    /// actually set-widened at runtime; the typed
    /// [`unwrap_or`](crate::Term::unwrap_or) builder enforces this
    /// at the Rust type level.
    ///
    /// **`fallback` and `is` with static kinds** must unify with
    /// `α` — so they agree with each other and with the source's
    /// underlying shape (when known).
    pub fn validate(&self, ctx: &mut Context) -> Result<(), UnifyError> {
        let alpha = ctx.fresh_var();

        match self.source.kind() {
            Some(source) if source.is_optional() => {
                // Strip the Nothing bit to get α's underlying shape.
                let underlying = source.without_nothing();
                ctx.unify(&UnifierType::Static(underlying), &alpha)?;
            }
            Some(_) => return Err(UnifyError::SourceNotOptional),
            None => {
                // No static kind on source — caller takes responsibility.
                // α stays open; fallback/is still link to it below.
            }
        }
        if let Some(k) = self.fallback.kind() {
            ctx.unify(&lift(&k), &alpha)?;
        }
        if let Some(k) = self.is.kind() {
            ctx.unify(&lift(&k), &alpha)?;
        }
        Ok(())
    }

    /// Schema describing the three slots. All three are required;
    /// `source` is set-widened (Optional), `fallback` and `is`
    /// share the unwrapped shape.
    pub fn schema(&self) -> Schema {
        let mut schema = Schema::new();
        let requirement = Requirement::new_group();
        schema.insert(
            "source".into(),
            Field {
                description:
                    "Optional input term — value flows to `is` when Present, else `fallback` does."
                        .into(),
                content_type: self.source.kind(),
                requirement: requirement.required(),
                cardinality: Cardinality::One,
            },
        );
        schema.insert(
            "fallback".into(),
            Field {
                description: "Default value used when `source` is `Absent` or unbound.".into(),
                content_type: self.fallback.kind(),
                requirement: requirement.required(),
                cardinality: Cardinality::One,
            },
        );
        schema.insert(
            "is".into(),
            Field {
                description: "Output term — receives `source`'s value or `fallback`'s.".into(),
                content_type: self.is.kind(),
                requirement: requirement.required(),
                cardinality: Cardinality::One,
            },
        );
        schema
    }

    /// Estimate cost. Constant — coalesce is a row-local rewrite.
    pub fn estimate(&self, _env: &Environment) -> Option<usize> {
        Some(COALESCE_COST)
    }

    /// Returns the named parameters for this constraint.
    pub fn parameters(&self) -> Parameters {
        let mut params = Parameters::new();
        params.insert("source".to_string(), self.source.clone());
        params.insert("fallback".to_string(), self.fallback.clone());
        params.insert("is".to_string(), self.is.clone());
        params
    }

    /// Evaluate: row-local `.map` — for each input row, decide
    /// whether to bind `is` from `source` or from `fallback`.
    /// Never consumes the input stream into a buffer; passes rows
    /// through one at a time.
    pub fn evaluate<M: Selection>(self, selection: M) -> impl Selection {
        let source = self.source;
        let fallback = self.fallback;
        let is = self.is;
        try_stream! {
            for await candidate in selection {
                let base = candidate?;

                // Resolve the source binding: Present, Absent, or unbound.
                let source_binding = base.lookup(&source);

                // Resolve the fallback value: must be concrete to bind output.
                let fallback_value = base.lookup(&fallback);

                match source_binding {
                    Ok(Binding::Present(value)) => {
                        let mut extension = base.clone();
                        extension.bind(&is, value)?;
                        yield extension;
                    }
                    Ok(Binding::Absent) | Err(_) => {
                        // Fallback must resolve to a Present value to bind output.
                        // Absent or unbound fallback filters the row.
                        if let Ok(Binding::Present(value)) = fallback_value {
                            let mut extension = base.clone();
                            extension.bind(&is, value)?;
                            yield extension;
                        }
                    }
                }
            }
        }
    }
}

impl Display for Coalesce {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "coalesce({}, {}) -> {}",
            self.source, self.fallback, self.is
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::EvaluationError;
    use crate::Value;
    use crate::selection::Match;
    use futures_util::TryStreamExt;
    use futures_util::stream::iter as stream_iter;

    /// Source is Present — output binds to source's value.
    #[dialog_common::test]
    async fn it_binds_output_from_source_when_present() -> Result<(), EvaluationError> {
        let coalesce = Coalesce::new(
            Term::var("source"),
            Term::Constant(Value::from("default".to_string())),
            Term::var("out"),
        );

        let mut candidate = Match::new();
        candidate.bind(&Term::var("source"), Value::from("hello".to_string()))?;

        let results: Vec<Match> = coalesce.evaluate(candidate.seed()).try_collect().await?;

        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0].lookup(&Term::var("out"))?.content()?,
            Value::from("hello".to_string()),
            "output should bind to source's value"
        );
        Ok(())
    }

    /// Source is Absent — output binds to fallback's value.
    #[dialog_common::test]
    async fn it_binds_output_from_fallback_when_absent() -> Result<(), EvaluationError> {
        let coalesce = Coalesce::new(
            Term::var("source"),
            Term::Constant(Value::from("default".to_string())),
            Term::var("out"),
        );

        let mut candidate = Match::new();
        candidate.bind_absent(&Term::var("source"))?;

        let results: Vec<Match> = coalesce.evaluate(candidate.seed()).try_collect().await?;

        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0].lookup(&Term::var("out"))?.content()?,
            Value::from("default".to_string()),
            "output should bind to fallback's value"
        );
        Ok(())
    }

    /// Source unbound (no binding at all) is also handled like Absent —
    /// output takes from fallback.
    #[dialog_common::test]
    async fn it_binds_output_from_fallback_when_source_unbound() -> Result<(), EvaluationError> {
        let coalesce = Coalesce::new(
            Term::var("source"),
            Term::Constant(Value::from("default".to_string())),
            Term::var("out"),
        );

        let candidate = Match::new();

        let results: Vec<Match> = coalesce.evaluate(candidate.seed()).try_collect().await?;

        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0].lookup(&Term::var("out"))?.content()?,
            Value::from("default".to_string())
        );
        Ok(())
    }

    /// Fallback may itself be a variable resolved against the row.
    #[dialog_common::test]
    async fn it_resolves_fallback_when_variable() -> Result<(), EvaluationError> {
        let coalesce = Coalesce::new(Term::var("source"), Term::var("fallback"), Term::var("out"));

        let mut candidate = Match::new();
        candidate.bind_absent(&Term::var("source"))?;
        candidate.bind(&Term::var("fallback"), Value::from(42u32))?;

        let results: Vec<Match> = coalesce.evaluate(candidate.seed()).try_collect().await?;

        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0].lookup(&Term::var("out"))?.content()?,
            Value::from(42u32)
        );
        Ok(())
    }

    /// Fallback also unbound — can't produce a value, row is filtered.
    #[dialog_common::test]
    async fn it_filters_when_source_absent_and_fallback_unbound() -> Result<(), EvaluationError> {
        let coalesce = Coalesce::new(Term::var("source"), Term::var("fallback"), Term::var("out"));

        let mut candidate = Match::new();
        candidate.bind_absent(&Term::var("source"))?;

        let results: Vec<Match> = coalesce.evaluate(candidate.seed()).try_collect().await?;

        assert_eq!(
            results.len(),
            0,
            "row should be filtered when no value can be produced"
        );
        Ok(())
    }

    /// Multiple rows in → multiple rows out, each independently decided.
    /// Confirms streaming `.map` behavior, not collect-then-process.
    #[dialog_common::test]
    async fn it_processes_each_row_independently() -> Result<(), EvaluationError> {
        let coalesce = Coalesce::new(
            Term::var("source"),
            Term::Constant(Value::from("default".to_string())),
            Term::var("out"),
        );

        // Two rows: one with Present source, one with Absent source.
        let mut present_row = Match::new();
        present_row.bind(&Term::var("source"), Value::from("present".to_string()))?;

        let mut absent_row = Match::new();
        absent_row.bind_absent(&Term::var("source"))?;

        // Build a seed selection containing both rows.
        let selection = stream_iter(vec![Ok(present_row), Ok(absent_row)]);

        let results: Vec<Match> = coalesce.evaluate(selection).try_collect().await?;

        assert_eq!(results.len(), 2);
        assert_eq!(
            results[0].lookup(&Term::var("out"))?.content()?,
            Value::from("present".to_string())
        );
        assert_eq!(
            results[1].lookup(&Term::var("out"))?.content()?,
            Value::from("default".to_string())
        );
        Ok(())
    }

    /// Schema reports all three slots.
    #[dialog_common::test]
    fn schema_describes_three_slots() {
        let coalesce = Coalesce::new(Term::var("source"), Term::var("fallback"), Term::var("out"));
        let schema = coalesce.schema();
        assert!(schema.get("source").is_some());
        assert!(schema.get("fallback").is_some());
        assert!(schema.get("is").is_some());
    }

    /// `validate` succeeds when source is `Optional<String>` and
    /// fallback / is are both `String`.
    #[dialog_common::test]
    fn validate_accepts_matching_types() {
        use crate::artifact::Type as ValueType;
        use crate::type_system;
        use crate::type_system::unifier::Context;

        let source = Term::<Any>::typed_var(
            "source",
            type_system::Type::primitive(ValueType::String).optional(),
        );
        let fallback = Term::<Any>::constant("Anon".to_string());
        let is = Term::<Any>::typed_var("is", type_system::Type::primitive(ValueType::String));

        let coalesce = Coalesce::new(source, fallback, is);
        let mut ctx = Context::new();
        coalesce
            .validate(&mut ctx)
            .expect("matching types should validate");
    }

    /// `validate` rejects a source whose kind isn't set-widened.
    #[dialog_common::test]
    fn validate_rejects_non_optional_source() {
        use crate::artifact::Type as ValueType;
        use crate::type_system;
        use crate::type_system::unifier::{Context, UnifyError};

        // Source kind is `String`, not `Optional<String>` — bug.
        let source =
            Term::<Any>::typed_var("source", type_system::Type::primitive(ValueType::String));
        let fallback = Term::<Any>::constant("Anon".to_string());
        let is = Term::<Any>::var("is");

        let coalesce = Coalesce::new(source, fallback, is);
        let mut ctx = Context::new();
        match coalesce.validate(&mut ctx) {
            Err(UnifyError::SourceNotOptional) => {}
            other => panic!("expected SourceNotOptional, got {:?}", other),
        }
    }

    /// `validate` rejects mismatched fallback type — source's
    /// underlying is `String`, fallback is `u32`.
    #[dialog_common::test]
    fn validate_rejects_fallback_type_mismatch() {
        use crate::artifact::Type as ValueType;
        use crate::type_system;
        use crate::type_system::unifier::{Context, UnifyError};

        let source = Term::<Any>::typed_var(
            "source",
            type_system::Type::primitive(ValueType::String).optional(),
        );
        let fallback = Term::<Any>::constant(42u32);
        let is = Term::<Any>::var("is");

        let coalesce = Coalesce::new(source, fallback, is);
        let mut ctx = Context::new();
        match coalesce.validate(&mut ctx) {
            Err(UnifyError::ConstraintConflict { .. }) => {}
            other => panic!("expected ConstraintConflict, got {:?}", other),
        }
    }
}
