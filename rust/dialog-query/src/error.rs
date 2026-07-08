//! Error types for the query engine

use std::collections::BTreeSet;
use std::convert::Infallible;
use std::error;
use std::fmt;

use crate::artifact::{ArtifactTypeError, DialogArtifactsError, Type, Value};
pub use crate::environment::Environment;
pub use crate::proposition::Proposition;
pub use crate::rule::Rule;
pub use crate::rule::deductive::DeductiveRule;
use crate::term::Term;
use crate::type_system::Type as Kind;
use crate::types::Any;
pub use thiserror::Error;

/// Errors that occur before query execution: during rule compilation, schema
/// validation, planning, and syntax parsing.
#[derive(Error, Debug, Clone, PartialEq)]
pub enum TypeError {
    /// A binding has the wrong type.
    #[error("Expected binding \"{binding}\" with {expected} type, instead got {actual}")]
    TypeMismatch {
        /// Name of the binding.
        binding: String,
        /// Expected type.
        expected: Type,
        /// Actual term provided.
        actual: Box<Term<Any>>,
    },

    /// A binding's term cannot inhabit the slot's kind (lattice type).
    #[error("Expected binding \"{binding}\" whose kind meets {expected}, instead got {actual}")]
    KindMismatch {
        /// Name of the binding.
        binding: String,
        /// The slot's kind.
        expected: Kind,
        /// Actual term provided.
        actual: Box<Term<Any>>,
    },

    /// A required binding was not provided.
    #[error("Required binding \"{binding}\" was omitted")]
    OmittedRequirement {
        /// Name of the omitted binding.
        binding: String,
    },

    /// A required binding was given the blank wildcard.
    #[error("Required binding \"{binding}\" can not be blank")]
    BlankRequirement {
        /// Name of the blank binding.
        binding: String,
    },

    /// A fact selector has no constrained terms.
    #[error("Unconstrained fact selector")]
    UnconstrainedSelector,

    /// A concept declares no required (`with`) attributes. Such a
    /// concept constrains nothing about an entity, so every entity
    /// trivially matches it: the same degenerate shape as a concept
    /// with no attributes at all. Optional (`maybe`) attributes do
    /// not count: they widen results rather than constrain them. A
    /// concept must have at least one required attribute.
    #[error("Concept declares no required attributes; at least one `with` attribute is required")]
    EmptyConcept,

    /// A concept-typed field's attribute is not entity-valued. Only
    /// an entity can conform to a concept, so a field that declares
    /// `conforms` must have `Entity` as its value type.
    #[error(
        "Attribute \"{the}\" declares conformance to {concept} but its value type is \
         {actual:?}; a concept-typed field must be entity-valued"
    )]
    NonEntityConformance {
        /// The offending attribute selector.
        the: String,
        /// The target concept's URI.
        concept: String,
        /// The attribute's declared value type.
        actual: Option<Type>,
    },

    /// A concept-typed field is marked optional. An optional
    /// concept-typed field is a left-join over "edge exists AND the
    /// target conforms" — absence over a rule-derived (IDB)
    /// predicate — which requires stratification to evaluate
    /// soundly. Not supported yet; make the field required or drop
    /// the conformance.
    #[error(
        "Attribute \"{the}\" is both optional and concept-typed; optional conformance \
         (absence over a derived predicate) is not supported yet"
    )]
    OptionalConformance {
        /// The offending attribute selector.
        the: String,
    },

    /// A rule declares a parameter that none of its premises use.
    #[error("Rule {rule} does not use parameter \"{parameter}\"")]
    UnusedParameter {
        /// The rule containing the unused parameter.
        rule: Box<Rule>,
        /// Name of the unused parameter.
        parameter: String,
    },

    /// A rule's conclusion references a variable that no premise binds.
    #[error("Rule {rule} does not bind variable \"{variable}\"")]
    UnboundVariable {
        /// The rule with the unbound variable.
        rule: Box<Rule>,
        /// Name of the unbound variable.
        variable: String,
    },

    /// A rule's conclusion references a required variable that is
    /// bound only by Optional (set-widened) premises: the rule
    /// could produce a row with the conclusion variable in Absent
    /// state, which a required head cannot accept. Fix by adding
    /// a required-binding premise for the variable, or by coalescing
    /// the optional source with a fallback before reaching the head.
    #[error(
        "Rule {rule}: field \"{variable}\" is optional but the conclusion requires a value. \
         Use coalesce(...) to provide a fallback, or bind \"{variable}\" from a required premise."
    )]
    RequiredHeadFromOptional {
        /// The offending rule.
        rule: Box<Rule>,
        /// Name of the optionally-bound head variable.
        variable: String,
    },

    /// A `Coalesce` constraint in this rule violates its type
    /// contract: its source must be set-widened (`Optional<α>`)
    /// and its `fallback` and `is` must each unify with `α`.
    #[error("Rule {rule} has an invalid Coalesce constraint: {reason}")]
    CoalesceTypeMismatch {
        /// The offending rule.
        rule: Box<Rule>,
        /// Human-readable reason for the mismatch.
        reason: String,
    },

    /// A set-widening (`maybe`) premise appears under `unless`. A
    /// left-join always yields a row for a bound entity (Present or
    /// the Absent fallback), so negating it filters every row and
    /// the rule is vacuously false. Negate the scalar lookup ("the
    /// entity has no such fact") or the concept instead.
    #[error(
        "Rule {rule} negates an optional (maybe) premise, which is always false. \
         Negate the scalar attribute lookup or the concept instead."
    )]
    NegatedOptional {
        /// The offending rule.
        rule: Box<Rule>,
    },

    /// A rule negates its own conclusion concept: a negative
    /// self-loop no stratification can order (the rule would derive
    /// a row exactly when it doesn't). Cycles through negation that
    /// span multiple rules are the global stratification pass's
    /// job; this is the local, always detectable case.
    #[error("Rule {rule} negates its own conclusion {concept}")]
    SelfNegation {
        /// The offending rule.
        rule: Box<Rule>,
        /// The conclusion concept's URI.
        concept: String,
    },

    /// Type inference over a rule's premises produced a
    /// contradiction (a variable appears in slots with conflicting
    /// kinds). The planner cannot proceed because the rule has no
    /// valid interpretation.
    #[error("Type inference failed: {reason}")]
    TypeInference {
        /// Human-readable description of the conflict, including
        /// the offending variable.
        reason: String,
    },

    /// A rule application omits a required parameter.
    #[error("Rule {rule} application omits required parameter \"{parameter}\"")]
    OmittedParameter {
        /// The rule missing the parameter.
        rule: Box<Rule>,
        /// Name of the omitted parameter.
        parameter: String,
    },

    /// A rule uses a local variable that no premise can provide.
    #[error("Rule {rule} uses local {variable} that no premise can provide")]
    RequiredLocalVariable {
        /// The rule with the unprovided local variable.
        rule: Box<Rule>,
        /// Name of the local variable.
        variable: String,
    },

    /// A rule passes an unbound term into a required parameter.
    #[error("Rule {rule} passes unbound {term} into required parameter \"{parameter}\"")]
    UnboundRuleParameter {
        /// The rule with the unbound parameter.
        rule: Box<Rule>,
        /// Name of the required parameter.
        parameter: String,
        /// The unbound term.
        term: Box<Term<Any>>,
    },

    /// A premise passes an unbound variable in a required parameter.
    #[error("Premise {application} passes unbound variable in required parameter \"{parameter}\"")]
    UnboundParameter {
        /// The premise application.
        application: Box<Proposition>,
        /// Name of the required parameter.
        parameter: String,
        /// The unbound term.
        term: Box<Term<Any>>,
    },

    /// A formula application omits a required cell.
    #[error("Formula {formula} application omits required cell \"{cell}\"")]
    OmittedCell {
        /// Name of the formula.
        formula: &'static str,
        /// Name of the omitted cell.
        cell: String,
    },

    /// A formula application passes blank into a required cell.
    #[error("Formula {formula} can not pass blank '_' in required cell \"{cell}\"")]
    BlankCell {
        /// Name of the formula.
        formula: &'static str,
        /// Name of the cell.
        cell: String,
    },

    /// A formula application passes an unbound variable into a required cell.
    #[error("Formula {formula} passes unbound variable '{variable}' into required cell \"{cell}\"")]
    UnboundCell {
        /// Name of the formula.
        formula: &'static str,
        /// Name of the cell.
        cell: String,
        /// Name of the unbound variable.
        variable: String,
    },

    /// A formula passes an unbound parameter into a required cell.
    #[error("Formula {formula} passes unbound {parameter} into required cell \"{cell}\"")]
    UnboundFormulaParameter {
        /// Name of the formula.
        formula: &'static str,
        /// Name of the cell.
        cell: String,
        /// The unbound parameter term.
        parameter: Box<Term<Any>>,
    },

    /// An application has no non-blank parameters.
    #[error("Application requires at least one non-blank parameter")]
    UnparameterizedApplication,

    /// Required bindings are not bound in the rule environment.
    #[error("Required bindings {required} are not bound in the rule environment")]
    RequiredBindings {
        /// The set of required but unbound bindings.
        required: Environment,
    },

    /// An attribute identifier has invalid syntax.
    #[error("Attribute format is \"domain/predicate\", but got \"{actual}\"")]
    InvalidAttributeSyntax {
        /// The malformed attribute string.
        actual: String,
    },
}

impl From<AnalyzerError> for TypeError {
    fn from(error: AnalyzerError) -> Self {
        match error {
            AnalyzerError::UnusedParameter { rule, parameter } => {
                TypeError::UnusedParameter { rule, parameter }
            }
            AnalyzerError::UnboundVariable { rule, variable } => {
                TypeError::UnboundVariable { rule, variable }
            }
            AnalyzerError::RequiredParameter { rule, parameter } => {
                TypeError::OmittedParameter { rule, parameter }
            }
            AnalyzerError::OmitsRequiredCell { formula, cell } => {
                TypeError::OmittedCell { formula, cell }
            }
            AnalyzerError::RequiredLocalVariable { rule, variable } => {
                TypeError::RequiredLocalVariable { rule, variable }
            }
        }
    }
}

impl<'a> From<EstimateError<'a>> for TypeError {
    fn from(error: EstimateError<'a>) -> Self {
        match error {
            EstimateError::RequiredParameters { required } => TypeError::RequiredBindings {
                required: required.clone(),
            },
        }
    }
}

/// Per-field type validation error used by `Cell` and `AttributeDescriptor`.
/// Use `.at(binding)` to convert into a [`TypeError`] with context.
#[derive(Error, Debug, Clone, PartialEq)]
pub enum FieldTypeError {
    /// The term has the wrong type.
    #[error("Expected a term with type {expected}, instead got {actual}")]
    TypeMismatch {
        /// Expected type.
        expected: Type,
        /// Actual term provided.
        actual: Box<Term<Any>>,
    },
    /// The term cannot inhabit the slot's kind (lattice type).
    #[error("Expected a term whose kind meets {expected}, instead got {actual}")]
    KindMismatch {
        /// The slot's kind.
        expected: Kind,
        /// Actual term provided.
        actual: Box<Term<Any>>,
    },
    /// A required term is missing.
    #[error("Required term is missing")]
    OmittedRequirement,
    /// A required term was given the blank wildcard.
    #[error("Required term can not be blank")]
    BlankRequirement,
}

impl FieldTypeError {
    /// Converts this error into a [`TypeError`] by attaching a binding name.
    pub fn at(self, binding: String) -> TypeError {
        match self {
            FieldTypeError::TypeMismatch { expected, actual } => TypeError::TypeMismatch {
                binding,
                expected,
                actual,
            },
            FieldTypeError::KindMismatch { expected, actual } => TypeError::KindMismatch {
                binding,
                expected,
                actual,
            },
            FieldTypeError::OmittedRequirement => TypeError::OmittedRequirement { binding },
            FieldTypeError::BlankRequirement => TypeError::BlankRequirement { binding },
        }
    }
}

/// Errors that occur during query execution at runtime.
#[derive(Error, Debug, Clone, PartialEq)]
pub enum EvaluationError {
    /// A named variable has no binding in the current match.
    #[error("Unbound variable {variable_name:?}")]
    UnboundVariable {
        /// Name of the unbound variable.
        variable_name: String,
    },

    /// A binding for the variable exists, but it is
    /// [`Binding::Absent`](crate::Binding::Absent), i.e., an
    /// optional resolver looked up the entity's attribute and
    /// found no fact. Distinct from
    /// [`Self::UnboundVariable`] (no binding at all): `Absent`
    /// means "we looked, and the answer is no value." Consumers
    /// that require a `Value` can call
    /// [`Binding::content`](crate::Binding::content) to convert
    /// this case into an error; consumers that handle optionality
    /// (Coalesce, the macro's `realize`) pattern-match on
    /// [`Binding`](crate::Binding) directly.
    #[error("Variable {variable_name:?} is bound to Absent")]
    Absent {
        /// Name of the variable bound to Absent.
        variable_name: String,
    },

    /// A formula parameter references a variable that is not bound.
    #[error("Variable for '{parameter}' is not bound: {term}")]
    UnboundFormulaVariable {
        /// Name of the formula parameter.
        parameter: String,
        /// The unbound term.
        term: Box<Term<Any>>,
    },

    /// A variable is already bound to a different value than expected.
    #[error("Inconsistency on '{parameter}': bound to {actual}, expected {expected}")]
    Conflict {
        /// Name of the conflicting parameter.
        parameter: String,
        /// The value the variable is currently bound to.
        actual: Box<Term<Any>>,
        /// The value that was expected.
        expected: Box<Term<Any>>,
    },

    /// A value has the wrong artifact type.
    #[error("Type mismatch: expected {expected}, got {actual}")]
    TypeMismatch {
        /// Expected artifact type.
        expected: Type,
        /// Actual artifact type.
        actual: Type,
    },

    /// Two concrete values do not match.
    #[error("Value mismatch: expected {expected:?}, got {actual:?}")]
    ValueMismatch {
        /// The expected value.
        expected: Box<Value>,
        /// The actual value.
        actual: Box<Value>,
    },

    /// A variable assignment failed.
    #[error("Cannot assign variable: {reason}")]
    Assignment {
        /// Description of why the assignment failed.
        reason: String,
    },

    /// A value's type fell outside a variable's rule-inferred kind
    /// at bind time. Scans filter mismatched facts before binding,
    /// so reaching this is a planner-contract violation (an untyped
    /// construction path fed a value the rule's types exclude), not
    /// a data-dependent non-match.
    #[error("Variable ?{variable} of kind {kind} cannot bind {value} ({value_type})")]
    KindMismatch {
        /// Name of the variable whose kind rejected the value.
        variable: String,
        /// The variable's rule-inferred kind (display form).
        kind: String,
        /// The rejected value (debug form).
        value: String,
        /// The rejected value's type (debug form).
        value_type: String,
    },

    /// A constraint was violated during evaluation.
    #[error("Constraint violation: {constraint}")]
    ConstraintViolation {
        /// Description of the violated constraint.
        constraint: String,
    },

    /// A required parameter was not provided.
    #[error("Missing required parameter \"{parameter}\"")]
    MissingParameter {
        /// Name of the missing parameter.
        parameter: String,
    },

    /// A selector matched no facts.
    #[error("Empty selector: {message}")]
    EmptySelector {
        /// Description of the empty selector.
        message: String,
    },

    /// An error from the underlying fact store.
    #[error("Fact store: {0}")]
    Store(String),

    /// A serialization or deserialization error.
    #[error("Serialization error: {message}")]
    Serialization {
        /// Description of the serialization error.
        message: String,
    },

    /// An error that occurred during query planning.
    #[error("Planning error: {message}")]
    Planning {
        /// Description of the planning error.
        message: String,
    },

    /// The queried concept's dependency closure contains a cycle
    /// through negation: some rule concluding `concept` negates
    /// `negated` inside the same dependency cycle, so the negation
    /// reads a set the cycle itself is still deriving. No
    /// stratified semantics exists for such a program. Rules are
    /// installed unconditionally (replicas must converge on the
    /// merged rule set), so this surfaces at query time, on exactly
    /// the queries whose closure is ill-stratified.
    #[error(
        "Negation through recursion: rules for {concept} negate {negated} \
         inside the same dependency cycle; no stratified semantics exists"
    )]
    NegationThroughRecursion {
        /// The concluding concept whose rule negates into its cycle.
        concept: String,
        /// The negated concept inside the same cycle.
        negated: String,
    },

    /// A recursive concept's semi-naive fixpoint did not converge
    /// within the round cap. A round derives at least one new row,
    /// so purely fact-driven recursion terminates well under the
    /// cap; hitting it means the rule set generates fresh values
    /// every round (e.g. through a formula) and would spin forever.
    #[error("Fixpoint for {concept} did not converge after {rounds} rounds")]
    FixpointDivergence {
        /// The queried recursive concept.
        concept: String,
        /// Rounds evaluated before giving up.
        rounds: usize,
    },
}

/// Result type for query operations
pub type QueryResult<T> = Result<T, EvaluationError>;

impl From<serde_json::Error> for EvaluationError {
    fn from(err: serde_json::Error) -> Self {
        Self::Serialization {
            message: err.to_string(),
        }
    }
}

impl From<DialogArtifactsError> for EvaluationError {
    fn from(value: DialogArtifactsError) -> Self {
        EvaluationError::Store(format!("{value}"))
    }
}

impl From<Infallible> for EvaluationError {
    fn from(_: Infallible) -> Self {
        unreachable!("Infallible can not occur")
    }
}

impl From<ArtifactTypeError> for EvaluationError {
    fn from(error: ArtifactTypeError) -> Self {
        let ArtifactTypeError::TypeMismatch(expected, actual) = error;
        EvaluationError::TypeMismatch { expected, actual }
    }
}

/// Errors that can occur during rule or formula analysis.
/// These errors indicate structural problems with rules that would prevent execution.
#[derive(Error, Debug, Clone, PartialEq)]
pub enum AnalyzerError {
    /// A rule declares a parameter that none of its premises use.
    #[error("Rule {rule} does not makes use of the \"{parameter}\" parameter")]
    UnusedParameter {
        /// The rule containing the unused parameter.
        rule: Box<Rule>,
        /// Name of the unused parameter.
        parameter: String,
    },
    /// A rule application omits a required parameter.
    #[error("Rule {rule} application omits required parameter \"{parameter}\"")]
    RequiredParameter {
        /// The rule missing the parameter.
        rule: Box<Rule>,
        /// Name of the required parameter.
        parameter: String,
    },
    /// A formula application omits a required cell.
    #[error("Formula {formula} application omits required cell \"{cell}\"")]
    OmitsRequiredCell {
        /// Name of the formula.
        formula: &'static str,
        /// Name of the omitted cell.
        cell: String,
    },
    /// A rule uses a local variable that no premise can provide.
    #[error("Rule {rule} makes use of local {variable} that no premise can provide")]
    RequiredLocalVariable {
        /// The rule with the unprovided local variable.
        rule: Box<Rule>,
        /// Name of the local variable.
        variable: String,
    },
    /// A rule's conclusion references a variable that no premise binds.
    #[error("Rule {rule} does not bind a variable \"{variable}\"")]
    UnboundVariable {
        /// The rule with the unbound variable.
        rule: Box<Rule>,
        /// Name of the unbound variable.
        variable: String,
    },
}

/// Errors that can occur when estimating the cost of a premise.
#[derive(Error, Debug, Clone, PartialEq)]
pub enum EstimateError<'a> {
    /// Required parameters are not bound in the environment.
    #[error("Required parameters {required} are not bound in the environment ")]
    RequiredParameters {
        /// The set of required but unbound parameters.
        required: &'a Environment,
    },
}

/// Error from validating a relation identifier (`The`).
///
/// Carries the raw input bytes so it can be produced in `const` context.
/// The human-readable input is rendered on display.
#[derive(Debug, Clone, PartialEq)]
pub struct InvalidIdentifier<'a> {
    /// The raw input that failed validation.
    pub input: &'a [u8],
    /// Why the input is invalid.
    pub reason: &'static str,
}

impl fmt::Display for InvalidIdentifier<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let input = String::from_utf8_lossy(self.input);
        write!(f, "invalid relation \"{input}\": {}", self.reason)
    }
}

impl error::Error for InvalidIdentifier<'_> {}

/// Owned version of [`InvalidIdentifier`] for use in contexts that cannot
/// carry the input lifetime (e.g. [`FromStr`](std::str::FromStr)).
#[derive(Debug, Clone, PartialEq)]
pub struct OwnedInvalidIdentifier {
    /// The input that failed validation.
    pub input: String,
    /// Why the input is invalid.
    pub reason: &'static str,
}

impl fmt::Display for OwnedInvalidIdentifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid relation \"{}\": {}", self.input, self.reason)
    }
}

impl error::Error for OwnedInvalidIdentifier {}

impl From<OwnedInvalidIdentifier> for DialogArtifactsError {
    fn from(e: OwnedInvalidIdentifier) -> Self {
        DialogArtifactsError::InvalidAttribute(e.to_string())
    }
}

/// Error types that can occur during transaction operations
#[derive(Debug, Error)]
pub enum TransactionError {
    /// The operation is invalid for the current transaction state.
    #[error("Invalid operation: {reason}")]
    InvalidOperation {
        /// Description of why the operation is invalid.
        reason: String,
    },
    /// An error from the underlying storage layer.
    #[error("Storage error: {0}")]
    Storage(#[from] DialogArtifactsError),
}

/// Why a premise cannot run yet under the current bindings: the
/// `Err` case of the SIPS binding function
/// [`Premise::feasible`](crate::Premise::feasible). Names which
/// variables the premise is still waiting on, so the planner (and
/// later demand reification) knows what would unblock it.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum Infeasible {
    /// All of these still-unbound variables must be bound before the
    /// premise can run. A choice group already satisfied (by a
    /// constant or a bound variable) contributes nothing here.
    #[error("premise needs these variables bound first: {0:?}")]
    NeedsAll(BTreeSet<String>),
}

/// Errors raised by type inference over a rule's premises
/// ([`TypeEnv::infer`](crate::rule::types::TypeEnv::infer)).
#[derive(Debug, Clone, PartialEq, Error)]
pub enum InferenceError {
    /// A variable appears in slots whose declared kinds have no
    /// common type: unification produced a contradiction.
    #[error("variable {variable} has conflicting kinds across premises: {reason}")]
    Conflict {
        /// Name of the offending variable.
        variable: String,
        /// Underlying unifier error message.
        reason: String,
    },
}

/// Errors raised by rule analysis
/// ([`analyze`](crate::rule::analyzer::analyze)). Each variant
/// describes a structural or type problem that prevents a rule from
/// being plannable.
#[derive(Debug, Clone, PartialEq, Error)]
pub enum AnalysisError {
    /// Type inference produced a contradiction; see
    /// [`InferenceError`].
    #[error("type inference failed: {reason}")]
    Inference {
        /// Description of the conflict.
        reason: String,
    },
    /// A conclusion variable's inferred type admits `Nothing`:
    /// the rule could produce `Absent` in a required slot.
    #[error("conclusion variable {variable} is optional")]
    RequiredHeadFromOptional {
        /// Name of the offending head variable.
        variable: String,
    },
    /// A `Coalesce` constraint's type contract is violated.
    #[error("Coalesce type mismatch: {reason}")]
    CoalesceTypeMismatch {
        /// Human-readable reason from the unifier.
        reason: String,
    },
    /// A set-widening (`maybe`) premise appears under `unless`. A
    /// left-join always yields a row for a bound entity (Present or
    /// Absent), so negating it filters every row; the rule would
    /// be vacuously false.
    #[error("negation over an optional (maybe) premise is always false")]
    NegatedOptional,
    /// A rule negates its own conclusion concept: a negative
    /// self-loop no stratification can order. The local, always
    /// detectable case of recursion through negation; cycles
    /// spanning multiple rules are the global stratification
    /// pass's job.
    #[error("rule negates its own conclusion {concept}")]
    SelfNegation {
        /// The conclusion concept's URI.
        concept: String,
    },
}
