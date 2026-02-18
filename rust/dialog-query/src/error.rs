//! Error types for the query engine

pub use crate::analyzer::AnalyzerError;
pub use crate::analyzer::Required;
pub use crate::application::Application;
use crate::artifact::{DialogArtifactsError, Type, Value};
pub use crate::predicate::DeductiveRule;
use crate::term::Term;
pub use thiserror::Error;

/// Errors that can occur during query planning and execution
///
/// TODO: Large enum variant - VariableInconsistency (344 bytes) contains two Term<Value> fields
/// which are large. Consider boxing these fields or the entire variant to reduce memory usage.
/// Most error variants are small (24 bytes), so this wastes significant memory.
#[allow(clippy::large_enum_variant)]
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

    /// Constraint requirements have not been met (e.g., neither term is bound)
    #[error("Constraint requirements have not been met: {constraint}")]
    ConstraintViolation { constraint: String },

    /// A variable was used inconsistently in a formula
    #[error(
        "Variable inconsistency: {parameter:?} has actual value {actual:?} but expected {expected:?}"
    )]
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
            InconsistencyError::TypeMismatch { expected, actual } => {
                QueryError::VariableInconsistency {
                    parameter: "value".to_string(),
                    expected: Term::Constant(expected),
                    actual: Term::Constant(actual),
                }
            }
            _ => QueryError::InvalidTerm {
                message: err.to_string(),
            },
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

/// TODO: Large enum variant - TypeMismatch (320 bytes) contains two Value fields which are large
/// (160 bytes each). Consider boxing these fields to reduce the enum size from 320 bytes to ~24 bytes,
/// matching the other error variants.
#[allow(clippy::large_enum_variant)]
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
    UnexpectedType { expected: Type, actual: Type },

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
    /// ```no_run
    /// # use dialog_query::formula::math::Sum;
    /// # use dialog_query::{Term, selection::Answer, Value, Parameters, Formula};
    /// # use dialog_query::error::SchemaError;
    /// let mut parameters = Parameters::new();
    /// // Missing required "with" parameter!
    /// parameters.insert("of".to_string(), Term::var("x"));
    /// parameters.insert("is".to_string(), Term::var("result"));
    ///
    /// // This will fail because "with" parameter is required
    /// let result = Sum::apply(parameters);
    /// assert!(matches!(result, Err(SchemaError::OmittedRequirement { .. })));
    /// ```
    #[error("Formula application omits required parameter \"{parameter}\"")]
    RequiredParameter { parameter: String },

    /// A variable required by the formula is not bound in the input match
    ///
    /// This occurs when the formula's parameter is mapped to a variable,
    /// but that variable has no value in the current match frame.
    ///
    /// # Example
    /// ```no_run
    /// # use dialog_query::formula::math::Sum;
    /// # use dialog_query::{Term, selection::Answer, Value, Parameters, Formula};
    /// # use dialog_query::error::FormulaEvaluationError;
    /// # let mut parameters = Parameters::new();
    /// # parameters.insert("of".to_string(), Term::var("x"));
    /// # parameters.insert("with".to_string(), Term::var("y"));
    /// # parameters.insert("is".to_string(), Term::var("result"));
    /// # let sum = Sum::apply(parameters).unwrap();
    /// let input = Answer::new();
    /// // Variable ?x is not bound!
    /// let result = sum.derive(input);
    /// assert!(matches!(result, Err(FormulaEvaluationError::UnboundVariable { .. })));
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
    /// # use dialog_query::formula::math::Sum;
    /// # use dialog_query::formula::{Formula};
    /// # use dialog_query::{Term, selection::Answer, Value, Parameters};
    /// # let mut terms = Terms::new();
    /// # terms.insert("of".to_string(), Term::var("x"));
    /// # terms.insert("with".to_string(), Term::var("y"));
    /// # terms.insert("is".to_string(), Term::var("result"));
    /// # let app = Sum::apply(terms);
    /// let input = Answer::new()
    ///     .set(Term::var("x"), 5u32).unwrap()
    ///     .set(Term::var("y"), 3u32).unwrap()
    ///     .set(Term::var("result"), 10u32).unwrap(); // Already bound to 10
    ///
    /// // Behavior when trying to write to already bound variable is TBD
    /// let result = app.derive(input);
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
    TypeMismatch { expected: Type, actual: Type },
}

impl From<InconsistencyError> for FormulaEvaluationError {
    fn from(e: InconsistencyError) -> Self {
        match e {
            InconsistencyError::UnboundVariableError(var) => {
                FormulaEvaluationError::UnboundVariable {
                    parameter: var.clone(),
                    term: crate::Term::var(&var),
                }
            }
            InconsistencyError::AssignmentError(msg) => {
                FormulaEvaluationError::VariableInconsistency {
                    parameter: msg,
                    actual: crate::Term::var("_"),
                    expected: crate::Term::var("_"),
                }
            }
            InconsistencyError::UnexpectedType { expected, actual } => {
                FormulaEvaluationError::TypeMismatch { expected, actual }
            }
            InconsistencyError::TypeConversion(type_error) => {
                let crate::artifact::TypeError::TypeMismatch(expected, actual) = type_error;
                FormulaEvaluationError::TypeMismatch { expected, actual }
            }
            InconsistencyError::TypeError(msg) => FormulaEvaluationError::VariableInconsistency {
                parameter: msg,
                actual: crate::Term::var("_"),
                expected: crate::Term::var("_"),
            },
            InconsistencyError::TypeMismatch { expected, actual } => {
                FormulaEvaluationError::VariableInconsistency {
                    parameter: "value".to_string(),
                    actual: crate::Term::Constant(actual),
                    expected: crate::Term::Constant(expected),
                }
            }
            InconsistencyError::UnconstrainedSelector => {
                FormulaEvaluationError::RequiredParameter {
                    parameter: "unconstrained selector".to_string(),
                }
            }
        }
    }
}

// Implement conversion from Infallible to FormulaEvaluationError because in our
// Formula macro we generate code like shown below
//
// ```rs
// impl TryFrom<Cursor> for MyFormulaInput {
//     type Error = FormulaEvaluationError;
//     fn try_from(cursor: Cursor) -> Result<Self, Self::Error> {
//         cursor.resolve("field")?.try_into()?
//     }
// }
// ```
//
// However if `field` of my `MyFormulaInput` has `Value` type doing `try_into()`
// produces `Result<Value, Infallible>` that need to be converted to be
// converted `FormulaEvaluationError`. Since `Infallible` can never occur, we
// can simply mark this conversion as unreachable because in case of `Value` we
// will always get `Ok(Value)`. Other types return `TypeError` that has
// own `From<TypeError> for FormulaEvaluationError` implementation.
impl From<std::convert::Infallible> for FormulaEvaluationError {
    fn from(_: std::convert::Infallible) -> Self {
        unreachable!("Infallible can not occur")
    }
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
    #[error("Rule {rule} does not bind a variable \"{variable}\"")]
    UnboundVariable {
        rule: DeductiveRule,
        variable: String,
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
        application: Box<Application>,
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
            AnalyzerError::UnboundVariable { rule, variable } => {
                PlanError::UnboundVariable { rule, variable }
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

/// Errors that can occur during compilation of rules or a predicate
/// application
#[derive(Error, Debug, Clone, PartialEq)]
pub enum TypeError {
    #[error("Expected a term with type {expected}, instead got {actual}")]
    TypeMismatch { expected: Type, actual: Term<Value> },
    #[error("Required term is missing")]
    OmittedRequirement,
    #[error("Required term can not be blank")]
    BlankRequirement,
}
impl TypeError {
    pub fn at(self, binding: String) -> SchemaError {
        match self {
            TypeError::TypeMismatch { expected, actual } => SchemaError::TypeError {
                binding,
                expected,
                actual,
            },
            TypeError::OmittedRequirement => SchemaError::OmittedRequirement { binding },
            TypeError::BlankRequirement => SchemaError::BlankRequirement { binding },
        }
    }
}

/// Errors that can occur during compilation of rules or a predicate
/// application
#[derive(Error, Debug, Clone, PartialEq)]
pub enum SchemaError {
    #[error("Expected binding \"{binding}\" with {expected} type, instead got {actual}")]
    TypeError {
        binding: String,
        expected: Type,
        actual: Term<Value>,
    },
    #[error("Required binding \"{binding}\" was omitted")]
    OmittedRequirement { binding: String },

    #[error("Required binding \"{binding}\" can not be blank")]
    BlankRequirement { binding: String },

    #[error("Unconstrained fact selector")]
    UnconstrainedSelector,
}

#[derive(Error, Debug, Clone, PartialEq)]
pub enum CompileError {
    #[error("Required bindings {required} are not bound in the rule environment")]
    RequiredBindings { required: Required },
    #[error("Rule {rule} does not bind a variable \"{variable}\"")]
    UnboundVariable {
        rule: DeductiveRule,
        variable: String,
    },
}

#[derive(Error, Debug, Clone, PartialEq)]
pub enum SyntaxError {
    #[error("Attribute format is \"namespace/predicate\", but got \"{actual}\"")]
    InvalidAttributeSyntax { actual: String },
}
