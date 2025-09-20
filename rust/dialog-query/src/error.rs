//! Error types for the query engine

pub use crate::analyzer::AnalyzerError;
pub use crate::application::Application;
use crate::artifact::{DialogArtifactsError, Value, ValueDataType};
pub use crate::predicate::DeductiveRule;
use crate::term::Term;
pub use crate::FactSelector;
pub use thiserror::Error;

/// Errors that can occur during query planning and execution
#[derive(Error, Debug, Clone, PartialEq)]
pub enum QueryError {
    /// A variable was referenced but not bound in the current scope
    #[error("Unbound variable {variable_name:?} referenced")]
    UnboundVariable { variable_name: String },

    /// A rule application is missing required parameters
    #[error("Rule application omits required parameter \"{parameter}\"")]
    MissingRuleParameter { parameter: String },

    /// A formula evaluation error
    #[error("Formula application omits required parameter: \"{parameter}\"")]
    RequiredFormulaParamater { parameter: String },

    /// A variable was used inconsistently in a formula
    #[error("Variable inconsistency: {parameter:?} has actual value {actual:?} but expected {expected:?}")]
    VariableInconsistency {
        parameter: String,
        actual: Term<Value>,
        expected: Term<Value>,
    },

    /// A variable appears in both input and output of a formula
    #[error("Variable {variable_name:?} cannot appear in both input and output")]
    VariableInputOutputConflict { variable_name: String },

    /// Planning failed due to circular dependencies
    #[error("Cannot plan query due to circular dependencies")]
    CircularDependency,

    /// Invalid rule structure
    #[error("Invalid rule: {reason}")]
    InvalidRule { reason: String },

    /// Serialization/deserialization errors
    #[error("Serialization error: {message}")]
    Serialization { message: String },

    /// Variable not supported in this context
    #[error("Variable not supported: {message}")]
    VariableNotSupported { message: String },

    /// Invalid attribute format
    #[error("Invalid attribute: {attribute}")]
    InvalidAttribute { attribute: String },

    /// Invalid term type
    #[error("Invalid term: {message}")]
    InvalidTerm { message: String },

    /// Empty selector error
    #[error("Empty selector: {message}")]
    EmptySelector { message: String },

    #[error("Fact store: {0}")]
    FactStore(String),

    /// Query planning errors
    #[error("Planning error: {message}")]
    PlanningError { message: String },
}

/// Result type for query operations
pub type QueryResult<T> = Result<T, QueryError>;

impl From<serde_json::Error> for QueryError {
    fn from(err: serde_json::Error) -> Self {
        Self::Serialization {
            message: err.to_string(),
        }
    }
}

impl From<DialogArtifactsError> for QueryError {
    fn from(value: DialogArtifactsError) -> Self {
        QueryError::FactStore(format!("{value}"))
    }
}

impl From<InconsistencyError> for QueryError {
    fn from(err: InconsistencyError) -> Self {
        match err {
            InconsistencyError::UnboundVariableError(var) => {
                QueryError::UnboundVariable { variable_name: var }
            }
            _ => QueryError::FactStore(err.to_string()),
        }
    }
}

impl From<PlanError> for QueryError {
    fn from(error: PlanError) -> Self {
        QueryError::PlanningError {
            message: error.to_string(),
        }
    }
}

#[derive(Error, Debug)]
pub enum InconsistencyError {
    #[error("Variable type is inconsistent with value: {0}")]
    TypeError(String),
    #[error("Different variable cannot be assigned: {0}")]
    AssignmentError(String),

    #[error("Type mismatch: expected {expected:?}, got {actual:?}")]
    TypeMismatch { expected: Value, actual: Value },

    #[error("Unbound variable: {0}")]
    UnboundVariableError(String),

    #[error("Type mismatch: expected value of type {expected}, got {actual}")]
    UnexpectedType {
        expected: ValueDataType,
        actual: ValueDataType,
    },

    #[error("Invalid fact selector")]
    UnconstrainedSelector,

    #[error("Type conversion error: {0}")]
    TypeConversion(#[from] crate::artifact::TypeError),
}

/// Errors that can occur during formula evaluation
///
/// These errors cover all failure modes in the formula system, from missing
/// parameters to type mismatches. Each error provides detailed context to
/// help diagnose issues during development and debugging.
#[derive(Error, Debug, Clone, PartialEq)]
pub enum FormulaEvaluationError {
    /// A required parameter is not present in the term mapping
    ///
    /// This occurs when a formula tries to read a parameter that wasn't
    /// provided in the Terms mapping when the formula was applied.
    ///
    /// # Example
    /// ```should_panic
    /// # use dialog_query::math::{Sum};
    /// # use dialog_query::{Term, Match, Value, Parameters, Formula};
    /// let mut parameters = Parameters::new();
    /// // Missing "with" parameter!
    /// parameters.insert("of".to_string(), Term::var("x"));
    ///
    /// let sum = Sum::apply(parameters);
    /// let input = Match::new().set(Term::var("x"), 5u32).unwrap();
    /// let result = sum.derive(input).unwrap(); // Will panic with RequiredParameter
    /// ```
    #[error("Formula application omits required parameter \"{parameter}\"")]
    RequiredParameter { parameter: String },

    /// A variable required by the formula is not bound in the input match
    ///
    /// This occurs when the formula's parameter is mapped to a variable,
    /// but that variable has no value in the current match frame.
    ///
    /// # Example
    /// ```should_panic
    /// # use dialog_query::math::{Sum};
    /// # use dialog_query::{Term, Match, Value, Parameters, Formula};
    /// # let mut parameters = Parameters::new();
    /// # parameters.insert("of".to_string(), Term::var("x"));
    /// # parameters.insert("with".to_string(), Term::var("y"));
    /// # parameters.insert("is".to_string(), Term::var("result"));
    /// # let sum = Sum::apply(parameters);
    /// let input = Match::new();
    /// // Variable ?x is not bound!
    /// let result = sum.derive(input).unwrap(); // Will panic with UnboundVariable
    /// ```
    #[error("Variable {term} for '{parameter}' required parameter is not bound")]
    UnboundVariable {
        parameter: String,
        term: Term<Value>,
    },

    /// Attempt to write a value that conflicts with an existing binding
    ///
    /// This occurs when a formula tries to write a value to a variable
    /// that already has a different value bound to it. This maintains
    /// logical consistency in the query evaluation.
    ///
    /// # Example
    /// ```ignore
    /// # use dialog_query::math::Sum;
    /// # use dialog_query::formula::{Formula};
    /// # use dialog_query::{Term, Match, Value, Parameters};
    /// # let mut terms = Terms::new();
    /// # terms.insert("of".to_string(), Term::var("x"));
    /// # terms.insert("with".to_string(), Term::var("y"));
    /// # terms.insert("is".to_string(), Term::var("result"));
    /// # let app = Sum::apply(terms);
    /// let input = Match::new()
    ///     .set(Term::var("x"), 5u32).unwrap()
    ///     .set(Term::var("y"), 3u32).unwrap()
    ///     .set(Term::var("result"), 10u32).unwrap(); // Already bound to 10
    ///
    /// // Behavior when trying to write to already bound variable is TBD
    /// let result = app.expand(input);
    /// // Implementation details for handling inconsistencies are still being refined
    /// ```
    #[error(
        "Variable for the '{parameter}' is bound to {actual} which is inconsistent with value being set: {expected}"
    )]
    VariableInconsistency {
        parameter: String,
        actual: Term<Value>,
        expected: Term<Value>,
    },

    /// Type conversion failed when casting a Value to the requested type
    ///
    /// This occurs when using `TryFrom<Value>` to convert a Value to a
    /// specific Rust type, but the Value's actual type is incompatible.
    ///
    /// # Example
    /// ```ignore
    /// let value = Value::String("hello".to_string());
    /// let number: u32 = u32::try_cast(&value)?;
    /// // Fails with TypeMismatch { expected: "u32", actual: "String" }
    /// ```
    #[error("Type mismatch: expected {expected}, got {actual}")]
    TypeMismatch {
        expected: ValueDataType,
        actual: ValueDataType,
    },
}

/// Errors that can occur during query planning.
/// These errors indicate problems that prevent creating a valid execution plan.
#[derive(Error, Debug, Clone, PartialEq)]
pub enum PlanError {
    #[error("Rule {rule} does not makes use of the \"{parameter}\" parameter")]
    UnusedParameter {
        rule: DeductiveRule,
        parameter: String,
    },
    #[error("Rule {rule} application omits required parameter \"{parameter}\"")]
    OmitsRequiredParameter {
        rule: DeductiveRule,
        parameter: String,
    },
    #[error("Rule {rule} makes use of local {variable} that no premise can provide")]
    RequiredLocalVariable {
        rule: DeductiveRule,
        variable: String,
    },
    #[error(
        "Rule {rule} application passes unbound {term} into a required parameter \"{parameter}\""
    )]
    UnboundRuleParameter {
        rule: DeductiveRule,
        parameter: String,
        term: Term<Value>,
    },

    #[error(
        "Premise {application} passes unbound variable in a required parameter \"{parameter}\""
    )]
    UnboundParameter {
        application: Application,
        parameter: String,
        term: Term<Value>,
    },

    #[error("Formula {formula} application omits required cell \"{cell}\"")]
    OmitsRequiredCell { formula: &'static str, cell: String },
    #[error(
        "Formula {formula} application can not pass blank '_' variable in required cell \"{cell}\""
    )]
    BlankRequiredCell { formula: &'static str, cell: String },

    #[error(
        "Formula {formula} application passes '{variable}' unbound variable into a required cell \"{cell}\""
    )]
    UnboundRequiredCell {
        formula: &'static str,
        cell: String,
        variable: String,
    },

    #[error(
        "Formula {formula} application passes unbound {parameter} into a required cell \"{cell}\""
    )]
    UnboundFormulaParameter {
        formula: &'static str,
        cell: String,
        parameter: Term<Value>,
    },

    #[error("Fact application {selector} requires at least one bound parameter")]
    UnconstrainedSelector { selector: FactSelector },

    #[error("Application requires at least one non-blank parameter")]
    UnparameterizedApplication,

    #[error("Unexpected error occured while planning a rule")]
    UnexpectedError,
}

impl From<AnalyzerError> for PlanError {
    fn from(error: AnalyzerError) -> Self {
        match error {
            AnalyzerError::UnusedParameter { rule, parameter } => {
                PlanError::UnusedParameter { rule, parameter }
            }
            AnalyzerError::RequiredParameter { rule, parameter } => {
                PlanError::OmitsRequiredParameter { rule, parameter }
            }
            AnalyzerError::OmitsRequiredCell { formula, cell } => {
                PlanError::OmitsRequiredCell { formula, cell }
            }
            AnalyzerError::RequiredLocalVariable { rule, variable } => {
                PlanError::RequiredLocalVariable { rule, variable }
            }
        }
    }
}
