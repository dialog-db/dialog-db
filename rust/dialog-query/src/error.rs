//! Error types for the query engine

use crate::artifact::{DialogArtifactsError, Type, TypeError as ArtifactTypeError, Value};
pub use crate::environment::Environment;
pub use crate::proposition::Proposition;
pub use crate::rule::deductive::DeductiveRule;
use crate::term::Term;
use crate::types::Any;
pub use thiserror::Error;

/// Errors that occur before query execution — during rule compilation, schema
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

    /// A rule declares a parameter that none of its premises use.
    #[error("Rule {rule} does not use parameter \"{parameter}\"")]
    UnusedParameter {
        /// The rule containing the unused parameter.
        rule: Box<DeductiveRule>,
        /// Name of the unused parameter.
        parameter: String,
    },

    /// A rule's conclusion references a variable that no premise binds.
    #[error("Rule {rule} does not bind variable \"{variable}\"")]
    UnboundVariable {
        /// The rule with the unbound variable.
        rule: Box<DeductiveRule>,
        /// Name of the unbound variable.
        variable: String,
    },

    /// A rule application omits a required parameter.
    #[error("Rule {rule} application omits required parameter \"{parameter}\"")]
    OmittedParameter {
        /// The rule missing the parameter.
        rule: Box<DeductiveRule>,
        /// Name of the omitted parameter.
        parameter: String,
    },

    /// A rule uses a local variable that no premise can provide.
    #[error("Rule {rule} uses local {variable} that no premise can provide")]
    RequiredLocalVariable {
        /// The rule with the unprovided local variable.
        rule: Box<DeductiveRule>,
        /// Name of the local variable.
        variable: String,
    },

    /// A rule passes an unbound term into a required parameter.
    #[error("Rule {rule} passes unbound {term} into required parameter \"{parameter}\"")]
    UnboundRuleParameter {
        /// The rule with the unbound parameter.
        rule: Box<DeductiveRule>,
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
            AnalyzerError::UnusedParameter { rule, parameter } => TypeError::UnusedParameter {
                rule: Box::new(rule),
                parameter,
            },
            AnalyzerError::UnboundVariable { rule, variable } => TypeError::UnboundVariable {
                rule: Box::new(rule),
                variable,
            },
            AnalyzerError::RequiredParameter { rule, parameter } => TypeError::OmittedParameter {
                rule: Box::new(rule),
                parameter,
            },
            AnalyzerError::OmitsRequiredCell { formula, cell } => {
                TypeError::OmittedCell { formula, cell }
            }
            AnalyzerError::RequiredLocalVariable { rule, variable } => {
                TypeError::RequiredLocalVariable {
                    rule: Box::new(rule),
                    variable,
                }
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
            FieldTypeError::OmittedRequirement => TypeError::OmittedRequirement { binding },
            FieldTypeError::BlankRequirement => TypeError::BlankRequirement { binding },
        }
    }
}

/// Errors that occur during query execution at runtime.
#[derive(Error, Debug, Clone, PartialEq)]
pub enum EvaluationError {
    /// A named variable has no binding in the current answer.
    #[error("Unbound variable {variable_name:?}")]
    UnboundVariable {
        /// Name of the unbound variable.
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

impl From<std::convert::Infallible> for EvaluationError {
    fn from(_: std::convert::Infallible) -> Self {
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
        rule: DeductiveRule,
        /// Name of the unused parameter.
        parameter: String,
    },
    /// A rule application omits a required parameter.
    #[error("Rule {rule} application omits required parameter \"{parameter}\"")]
    RequiredParameter {
        /// The rule missing the parameter.
        rule: DeductiveRule,
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
        rule: DeductiveRule,
        /// Name of the local variable.
        variable: String,
    },
    /// A rule's conclusion references a variable that no premise binds.
    #[error("Rule {rule} does not bind a variable \"{variable}\"")]
    UnboundVariable {
        /// The rule with the unbound variable.
        rule: DeductiveRule,
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

impl std::fmt::Display for InvalidIdentifier<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let input = String::from_utf8_lossy(self.input);
        write!(f, "invalid relation \"{input}\": {}", self.reason)
    }
}

impl std::error::Error for InvalidIdentifier<'_> {}

/// Owned version of [`InvalidIdentifier`] for use in contexts that cannot
/// carry the input lifetime (e.g. [`FromStr`](std::str::FromStr)).
#[derive(Debug, Clone, PartialEq)]
pub struct OwnedInvalidIdentifier {
    /// The input that failed validation.
    pub input: String,
    /// Why the input is invalid.
    pub reason: &'static str,
}

impl std::fmt::Display for OwnedInvalidIdentifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "invalid relation \"{}\": {}", self.input, self.reason)
    }
}

impl std::error::Error for OwnedInvalidIdentifier {}

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
