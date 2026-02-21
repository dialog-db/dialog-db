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
    UnboundVariable {
        /// The name of the unbound variable
        variable_name: String,
    },

    /// A rule application is missing required parameters
    #[error("Rule application omits required parameter \"{parameter}\"")]
    MissingRuleParameter {
        /// The missing parameter name
        parameter: String,
    },

    /// A formula evaluation error
    #[error("Formula application omits required parameter: \"{parameter}\"")]
    RequiredFormulaParamater {
        /// The missing parameter name
        parameter: String,
    },

    /// Constraint requirements have not been met (e.g., neither term is bound)
    #[error("Constraint requirements have not been met: {constraint}")]
    ConstraintViolation {
        /// Description of the violated constraint
        constraint: String,
    },

    /// A variable was used inconsistently in a formula
    #[error(
        "Variable inconsistency: {parameter:?} has actual value {actual:?} but expected {expected:?}"
    )]
    VariableInconsistency {
        /// The parameter name where the inconsistency was found
        parameter: String,
        /// The actual term bound to the variable
        actual: Term<Value>,
        /// The expected term for the variable
        expected: Term<Value>,
    },

    /// A variable appears in both input and output of a formula
    #[error("Variable {variable_name:?} cannot appear in both input and output")]
    VariableInputOutputConflict {
        /// The conflicting variable name
        variable_name: String,
    },

    /// Planning failed due to circular dependencies
    #[error("Cannot plan query due to circular dependencies")]
    CircularDependency,

    /// Invalid rule structure
    #[error("Invalid rule: {reason}")]
    InvalidRule {
        /// The reason the rule is invalid
        reason: String,
    },

    /// Serialization/deserialization errors
    #[error("Serialization error: {message}")]
    Serialization {
        /// The serialization error message
        message: String,
    },

    /// Variable not supported in this context
    #[error("Variable not supported: {message}")]
    VariableNotSupported {
        /// Description of why the variable is unsupported
        message: String,
    },

    /// Invalid attribute format
    #[error("Invalid attribute: {attribute}")]
    InvalidAttribute {
        /// The invalid attribute string
        attribute: String,
    },

    /// Invalid term type
    #[error("Invalid term: {message}")]
    InvalidTerm {
        /// Description of the term error
        message: String,
    },

    /// Empty selector error
    #[error("Empty selector: {message}")]
    EmptySelector {
        /// Description of the empty selector error
        message: String,
    },

    /// An error originating from the fact store
    #[error("Fact store: {0}")]
    FactStore(String),

    /// Query planning errors
    #[error("Planning error: {message}")]
    PlanningError {
        /// The planning error message
        message: String,
    },
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

/// Errors arising from inconsistent variable bindings or type mismatches.
///
/// TODO: Large enum variant - TypeMismatch (320 bytes) contains two Value fields which are large
/// (160 bytes each). Consider boxing these fields to reduce the enum size from 320 bytes to ~24 bytes,
/// matching the other error variants.
#[allow(clippy::large_enum_variant)]
#[derive(Error, Debug)]
pub enum InconsistencyError {
    /// A variable's type is inconsistent with its assigned value
    #[error("Variable type is inconsistent with value: {0}")]
    TypeError(String),
    /// A variable cannot be assigned the given value
    #[error("Different variable cannot be assigned: {0}")]
    AssignmentError(String),

    /// Expected and actual values do not match
    #[error("Type mismatch: expected {expected:?}, got {actual:?}")]
    TypeMismatch {
        /// The expected value
        expected: Value,
        /// The actual value encountered
        actual: Value,
    },

    /// A referenced variable has no binding
    #[error("Unbound variable: {0}")]
    UnboundVariableError(String),

    /// The value type does not match the expected type
    #[error("Type mismatch: expected value of type {expected}, got {actual}")]
    UnexpectedType {
        /// The expected type
        expected: Type,
        /// The actual type encountered
        actual: Type,
    },

    /// A fact selector has no constraints, which would match everything
    #[error("Invalid fact selector")]
    UnconstrainedSelector,

    /// A type conversion between value types failed
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
    RequiredParameter {
        /// The missing parameter name
        parameter: String,
    },

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
        /// The parameter name that requires a bound variable
        parameter: String,
        /// The unbound term
        term: Term<Value>,
    },

    /// Attempt to write a value that conflicts with an existing binding
    ///
    /// This occurs when a formula tries to write a value to a variable
    /// that already has a different value bound to it. This maintains
    /// logical consistency in the query evaluation.
    ///
    /// # Example
    /// ```rs
    /// let input = Answer::new()
    ///     .set(Term::var("x"), 5u32).unwrap()
    ///     .set(Term::var("y"), 3u32).unwrap()
    ///     .set(Term::var("result"), 10u32).unwrap(); // Already bound to 10
    ///
    /// // Evaluating a formula that tries to write 8 to "result" (already 10)
    /// // produces VariableInconsistency { parameter: "is", actual: 10, expected: 8 }
    /// ```
    #[error(
        "Variable for the '{parameter}' is bound to {actual} which is inconsistent with value being set: {expected}"
    )]
    VariableInconsistency {
        /// The parameter where the inconsistency was detected
        parameter: String,
        /// The actual term bound to the variable
        actual: Term<Value>,
        /// The expected term for the variable
        expected: Term<Value>,
    },

    /// Type conversion failed when casting a Value to the requested type
    ///
    /// This occurs when using `TryFrom<Value>` to convert a Value to a
    /// specific Rust type, but the Value's actual type is incompatible.
    ///
    /// # Example
    /// ```no_run
    /// # use dialog_query::Value;
    /// let value = Value::String("hello".to_string());
    /// // Attempting to convert a String value to u32 fails:
    /// let number: Result<u32, _> = value.try_into();
    /// // Err(TypeMismatch { expected: UnsignedInt, actual: String })
    /// ```
    #[error("Type mismatch: expected {expected}, got {actual}")]
    TypeMismatch {
        /// The expected type
        expected: Type,
        /// The actual type encountered
        actual: Type,
    },
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
// impl TryFrom<&mut Bindings> for MyFormulaInput {
//     type Error = FormulaEvaluationError;
//     fn try_from(bindings: &mut Bindings) -> Result<Self, Self::Error> {
//         bindings.resolve("field")?.try_into()?
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
    /// A parameter passed to a rule is not used by any of its premises
    #[error("Rule {rule} does not makes use of the \"{parameter}\" parameter")]
    UnusedParameter {
        /// The rule containing the unused parameter
        rule: DeductiveRule,
        /// The unused parameter name
        parameter: String,
    },
    /// A variable referenced in a rule head is not bound by any premise
    #[error("Rule {rule} does not bind a variable \"{variable}\"")]
    UnboundVariable {
        /// The rule with the unbound variable
        rule: DeductiveRule,
        /// The unbound variable name
        variable: String,
    },
    /// A required parameter was not supplied in the rule application
    #[error("Rule {rule} application omits required parameter \"{parameter}\"")]
    OmitsRequiredParameter {
        /// The rule missing the parameter
        rule: DeductiveRule,
        /// The omitted parameter name
        parameter: String,
    },
    /// A local variable used in the rule cannot be provided by any premise
    #[error("Rule {rule} makes use of local {variable} that no premise can provide")]
    RequiredLocalVariable {
        /// The rule referencing the local variable
        rule: DeductiveRule,
        /// The unsatisfiable local variable name
        variable: String,
    },
    /// An unbound term was passed where a bound value is required
    #[error(
        "Rule {rule} application passes unbound {term} into a required parameter \"{parameter}\""
    )]
    UnboundRuleParameter {
        /// The rule with the unbound parameter
        rule: DeductiveRule,
        /// The required parameter name
        parameter: String,
        /// The unbound term that was passed
        term: Term<Value>,
    },

    /// A premise application passes an unbound variable into a required parameter
    #[error(
        "Premise {application} passes unbound variable in a required parameter \"{parameter}\""
    )]
    UnboundParameter {
        /// The premise application containing the error
        application: Box<Application>,
        /// The required parameter name
        parameter: String,
        /// The unbound term that was passed
        term: Term<Value>,
    },

    /// A formula application is missing a required cell
    #[error("Formula {formula} application omits required cell \"{cell}\"")]
    OmitsRequiredCell {
        /// The formula name
        formula: &'static str,
        /// The omitted cell name
        cell: String,
    },
    /// A blank variable was passed into a required formula cell
    #[error(
        "Formula {formula} application can not pass blank '_' variable in required cell \"{cell}\""
    )]
    BlankRequiredCell {
        /// The formula name
        formula: &'static str,
        /// The cell that received a blank
        cell: String,
    },

    /// An unbound variable was passed into a required formula cell
    #[error(
        "Formula {formula} application passes '{variable}' unbound variable into a required cell \"{cell}\""
    )]
    UnboundRequiredCell {
        /// The formula name
        formula: &'static str,
        /// The cell that received an unbound variable
        cell: String,
        /// The unbound variable name
        variable: String,
    },

    /// An unbound parameter was passed into a required formula cell
    #[error(
        "Formula {formula} application passes unbound {parameter} into a required cell \"{cell}\""
    )]
    UnboundFormulaParameter {
        /// The formula name
        formula: &'static str,
        /// The cell that received an unbound parameter
        cell: String,
        /// The unbound parameter term
        parameter: Term<Value>,
    },

    /// An application was provided with no non-blank parameters
    #[error("Application requires at least one non-blank parameter")]
    UnparameterizedApplication,

    /// An unexpected internal error during planning
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
    /// A term has an unexpected type
    #[error("Expected a term with type {expected}, instead got {actual}")]
    TypeMismatch {
        /// The expected type
        expected: Type,
        /// The actual term encountered
        actual: Term<Value>,
    },
    /// A required term was not provided
    #[error("Required term is missing")]
    OmittedRequirement,
    /// A required term was given as blank
    #[error("Required term can not be blank")]
    BlankRequirement,
}
impl TypeError {
    /// Converts this error into a [`SchemaError`] by attaching a binding name.
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
    /// A binding has an unexpected type
    #[error("Expected binding \"{binding}\" with {expected} type, instead got {actual}")]
    TypeError {
        /// The binding name
        binding: String,
        /// The expected type
        expected: Type,
        /// The actual term encountered
        actual: Term<Value>,
    },
    /// A required binding was not provided
    #[error("Required binding \"{binding}\" was omitted")]
    OmittedRequirement {
        /// The omitted binding name
        binding: String,
    },

    /// A required binding was given as blank
    #[error("Required binding \"{binding}\" can not be blank")]
    BlankRequirement {
        /// The blank binding name
        binding: String,
    },

    /// A fact selector has no constraints
    #[error("Unconstrained fact selector")]
    UnconstrainedSelector,
}

/// Errors that can occur during rule compilation
#[derive(Error, Debug, Clone, PartialEq)]
pub enum CompileError {
    /// Required bindings are missing from the rule environment
    #[error("Required bindings {required} are not bound in the rule environment")]
    RequiredBindings {
        /// The set of required but unbound bindings
        required: Required,
    },
    /// A variable referenced in the rule is not bound
    #[error("Rule {rule} does not bind a variable \"{variable}\"")]
    UnboundVariable {
        /// The rule with the unbound variable
        rule: DeductiveRule,
        /// The unbound variable name
        variable: String,
    },
}

/// Errors from parsing syntactic constructs
#[derive(Error, Debug, Clone, PartialEq)]
pub enum SyntaxError {
    /// An attribute string does not match the expected `namespace/predicate` format
    #[error("Attribute format is \"namespace/predicate\", but got \"{actual}\"")]
    InvalidAttributeSyntax {
        /// The invalid attribute string
        actual: String,
    },
}
