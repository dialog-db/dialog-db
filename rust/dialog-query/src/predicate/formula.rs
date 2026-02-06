//! Formula system for type-safe data transformations in queries
//!
//! This module provides a powerful and extensible system for defining formulas that
//! transform data during query evaluation. Formulas enable computed fields, data
//! transformations, and complex calculations while maintaining type safety.
//!
//! # Overview
//!
//! The formula system consists of several key components:
//!
//! - **[`Formula`] trait** - The core trait that all formulas must implement
//! - **[`Compute`] trait** - Optional trait for formulas that compute outputs from inputs
//! - **[`FormulaApplication`]** - Non-generic formula bound to term mappings, integrable with rules
//! - **[`Cursor`](crate::cursor::Cursor)** - Provides read/write access during evaluation
//! - **[`Dependencies`](crate::deductive_rule::Dependencies)** - Declares parameter requirements
//! - **Standard `TryFrom<Value>`** - Type conversion between Value and Rust types
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────┐
//! │   User Query    │
//! └────────┬────────┘
//!          │ Terms mapping: {of: ?x, with: ?y, is: ?result}
//!          ▼
//! ┌─────────────────┐
//! │FormulaApplication│
//! └────────┬────────┘
//!          │ For each input Match
//!          ▼
//! ┌─────────────────┐
//! │     Cursor      │ Reads: ?x → 5, ?y → 3
//! └────────┬────────┘
//!          │
//!          ▼
//! ┌─────────────────┐
//! │  Formula Logic  │ Computes: 5 + 3 = 8
//! └────────┬────────┘
//!          │
//!          ▼
//! ┌─────────────────┐
//! │  Write Results  │ Writes: ?result → 8
//! └─────────────────┘
//! ```
//!
//! # Using Formulas
//!
//! Formulas provide computed transformations during query evaluation.
//! See the formula implementations in `crate::formula` for examples.
//!
//! ```rust
//! use dialog_query::{formula::math::Sum, Formula, Parameters, Term};
//!
//! // Create a Sum formula application that binds variables x, y, and result
//! let mut parameters = Parameters::new();
//! parameters.insert("of".to_string(), Term::var("x"));
//! parameters.insert("with".to_string(), Term::var("y"));
//! parameters.insert("is".to_string(), Term::var("result"));
//!
//! let sum_formula = Sum::apply(parameters).unwrap();
//! // The formula can now be used in query evaluation to compute result = x + y
//! ```
//!
//! # Design Principles
//!
//! 1. **Type Safety** - Formulas work with strongly typed inputs and outputs
//! 2. **Integration** - Non-generic applications integrate seamlessly with rule system
//! 3. **Composability** - Formulas can be chained and combined in queries and rules
//! 4. **Separation of Concerns** - Logic (Compute) is separate from I/O (Cursor)
//! 5. **Dependency Declaration** - Clear parameter requirements for planning
//! 6. **Error Handling** - Clear error types for all failure modes
//! 7. **Performance** - Zero-cost abstractions where possible
//!
//! # Integration with Deductive Rules
//!
//! The non-generic `FormulaApplication` design allows formulas to be seamlessly integrated
//! with the deductive rule system. Formulas can now be used as premises in rules,
//! participate in query planning, and be stored alongside other rule applications.
//!
//! # Future Enhancements
//!
//! The formula system is designed to support future macro generation that will
//! automatically derive the boilerplate code, making formula definition as simple as:
//!
//! ```rust,ignore
//! #[derive(Formula)]
//! struct Sum {
//!     of: u32,
//!     with: u32,
//!     #[computed]
//!     is: u32,
//! }
//!
//! impl Compute for Sum {
//!     fn compute(input: Self::Input) -> Vec<Self> {
//!         vec![Sum {
//!             of: input.of,
//!             with: input.with,
//!             is: input.of + input.with,
//!         }]
//!     }
//! }
//! ```

use std::collections::HashMap;
use std::fmt::Display;

use crate::{Term, Type};
use serde::{Deserialize, Serialize};

use crate::Schema;
use crate::application::formula::FormulaApplication;
use crate::cursor::Cursor;
pub use crate::dsl::{Input, Quarriable};
use crate::error::{FormulaEvaluationError, SchemaError, TypeError};
use crate::selection::Answer;
use crate::types::Scalar;
use crate::{Parameters, Requirement};

/// Core trait for implementing formulas in the query system
///
/// The `Formula` trait defines the interface that all formulas must implement.
/// It provides a type-safe way to transform data during query evaluation.
///
/// # Type Parameters
///
/// - `Input`: The input type that can be constructed from a [`Cursor`].
///   This type should contain all the fields the formula needs to read.
/// - `Match`: Currently unused, reserved for future macro generation that
///   will create match patterns for formula applications.
///
/// # Implementation Guide
///
/// To implement a formula:
///
/// 1. Define an input type that implements `TryFrom<Cursor>`
/// 2. Implement `name()` to return the formula's identifier
/// 3. Implement `dependencies()` to declare parameter requirements
/// 4. Implement `derive` to create output instances from input
/// 5. Implement `write` to write computed values back to the cursor
///
/// Most formulas should also implement the [`Compute`] trait to separate
/// the computation logic from the I/O operations.
///
/// # Example
///
/// See the module-level documentation for a complete example.
pub trait Formula: Quarriable + Output + Sized + Clone {
    /// The input type for this formula
    ///
    /// This type must be constructible from a Cursor and should contain
    /// all the fields that the formula needs to read from the input.
    type Input: In;

    /// Match type for future pattern matching support
    ///
    /// Currently unused. In future versions, this will be used by macros
    /// to generate pattern matching code for formula applications in queries.
    type Match: Match<Formula = Self>;

    // fn dependencies() -> Dependencies;

    fn cost() -> usize;
    fn cells() -> &'static Cells;
    fn operator() -> &'static str;

    fn schema() -> Schema {
        Self::cells().into()
    }

    fn operands(&self) -> impl Iterator<Item = &str> {
        Self::cells().keys()
    }

    /// Convert derived outputs to Answer instances with proper provenance
    ///
    /// This method orchestrates the full formula evaluation:
    /// 1. Calls `derive` to compute outputs
    /// 2. For each output, calls `write` to add values to cursor
    /// 3. Returns the Answer with Factor::Derived provenance
    ///
    /// This default implementation should work for most formulas.
    fn compute(cursor: &mut Cursor) -> Result<Vec<Answer>, FormulaEvaluationError> {
        let mut answers = Vec::new();
        let input: Self::Input = cursor.try_into()?;
        for output in Self::derive(input) {
            let mut cursor = cursor.clone();
            Self::write(&output, &mut cursor)?;
            answers.push(cursor.source);
        }

        Ok(answers)
    }

    /// This method contains actual logic for deriving an output from provided
    /// inputs.
    fn derive(input: Self::Input) -> Vec<Self>;

    /// Create a formula application with term bindings
    ///
    /// This method binds the formula to specific term mappings, creating
    /// a non-generic [`FormulaApplication`] that can be evaluated over streams of matches
    /// and integrated with the deductive rule system.
    ///
    /// # Arguments
    /// * `terms` - Mapping from formula parameter names to query terms
    ///
    /// # Example
    /// ```ignore
    /// let mut terms = Terms::new();
    /// terms.insert("of".to_string(), Term::var("input1"));
    /// terms.insert("with".to_string(), Term::var("input2"));
    /// terms.insert("is".to_string(), Term::var("output"));
    ///
    /// let app = Sum::apply(terms)?;
    /// ```
    fn apply(terms: Parameters) -> Result<FormulaApplication, SchemaError> {
        let cells = Self::cells();

        Ok(FormulaApplication {
            name: Self::operator(),
            cells,
            cost: Self::cost(),
            parameters: cells.conform(terms)?,
            compute: |cursor| Self::compute(cursor),
        })
    }
}

pub trait Interface {
    /// The input type for this formula
    ///
    /// This type must be constructible from a Cursor and should contain
    /// all the fields that the formula needs to read from the input.
    type Input: In;
    type Output: Output;
}

impl<T: Formula> Interface for T {
    type Input = T::Input;
    type Output = T;
}

pub trait In: for<'a> TryFrom<&'a mut Cursor, Error = FormulaEvaluationError> {}
impl<T: for<'a> TryFrom<&'a mut Cursor, Error = FormulaEvaluationError>> In for T {}

pub trait Output {
    /// Write this formula instance's output values to the cursor
    ///
    /// This method is called for each output instance produced by `derive`
    /// to write the computed values back to the cursor.
    ///
    /// # Arguments
    /// * `cursor` - The cursor to write output values to
    ///
    /// # Returns
    /// * `Ok(())` - If all writes succeeded
    /// * `Err(_)` - If writing fails (e.g., due to inconsistency)
    fn write(&self, cursor: &mut Cursor) -> Result<(), FormulaEvaluationError>;
}

pub trait Match: Sized + Clone + Into<Parameters> {
    type Formula: Formula<Match = Self>;
}

impl<T: Match + Clone> From<T> for FormulaApplication {
    fn from(value: T) -> Self {
        FormulaApplication {
            name: T::Formula::operator(),
            cells: T::Formula::cells(),
            cost: T::Formula::cost(),
            parameters: value.into(),
            compute: |cursor| T::Formula::compute(cursor),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Cell {
    /// Name of this cell
    name: String,
    /// Description of this cell
    description: String,
    /// Data type of this cell
    #[serde(rename = "type")]
    content_type: Option<Type>,
    /// Requirement for this cell
    requirement: Requirement,
}

impl Cell {
    pub fn new(name: &'static str, content_type: Option<Type>) -> Self {
        Cell {
            name: name.to_string(),
            description: String::new(),
            content_type,
            requirement: Requirement::Optional,
        }
    }

    pub fn typed(&mut self, content_type: Type) -> &mut Self {
        self.content_type = Some(content_type);
        self
    }

    pub fn the(&mut self, description: &'static str) -> &mut Self {
        self.description = description.to_string();
        self
    }

    pub fn required(&mut self) -> &mut Self {
        self.requirement = Requirement::Required(None);
        self
    }

    pub fn derived(&mut self, _derivation: usize) -> &mut Self {
        self.requirement = Requirement::Optional;
        self
    }

    pub fn done(self) -> Self {
        self
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn description(&self) -> &str {
        &self.description
    }

    pub fn content_type(&self) -> &Option<Type> {
        &self.content_type
    }

    pub fn requirement(&self) -> &Requirement {
        &self.requirement
    }

    /// Type checks that provided term matches cells content type. If term
    pub fn check<'a, T: Scalar>(&self, term: &'a Term<T>) -> Result<&'a Term<T>, TypeError> {
        // First we type check the input to ensure it matches cell's content type
        match (self.content_type(), term.content_type()) {
            // if expected is any (has no type) it checks
            (None, _) => Ok(term),
            // if cell is of some type and we're given term of unknown
            // type that's also fine.
            (_, None) => Ok(term),
            // if expected isn't any (has no type) it must be equal
            // to actual or it's a type missmatch.
            (Some(expected), actual) => {
                if Some(*expected) == actual {
                    Ok(term)
                } else {
                    Err(TypeError::TypeMismatch {
                        expected: *expected,
                        actual: term.as_unknown(),
                    })
                }
            }
        }
    }

    pub fn conform<'a, T: Scalar>(
        &self,
        term: Option<&'a Term<T>>,
    ) -> Result<Option<&'a Term<T>>, TypeError> {
        // We check that cell type matches term type.
        if let Some(term) = term {
            self.check(term)?;
        }

        // Verify that required parameter is provided
        if self.requirement().is_required() {
            match term {
                Some(Term::Constant(_)) => Ok(()),
                Some(Term::Variable { name: Some(_), .. }) => Ok(()),
                Some(Term::Variable { name: None, .. }) => Err(TypeError::BlankRequirement),
                None => Err(TypeError::OmittedRequirement),
            }?;
        };

        Ok(term)
    }
}

impl Display for Cell {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let prefix = if self.requirement.is_required() {
            ""
        } else {
            "?"
        };

        if let Some(content_type) = self.content_type {
            write!(f, "{}{}: {}", prefix, self.name, content_type)
        } else {
            write!(f, "{}{}: Value", prefix, self.name)
        }
    }
}

pub struct CellsBuilder {
    cells: HashMap<String, Cell>,
}

impl CellsBuilder {
    pub fn cell(&mut self, name: &'static str, content_type: Option<Type>) -> &mut Cell {
        let cell = Cell::new(name, content_type);
        self.cells.insert(name.to_string(), cell);
        self.cells.get_mut(name).unwrap()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(transparent)]
pub struct Cells(HashMap<String, Cell>);
impl Cells {
    pub fn define<F>(define: F) -> Self
    where
        F: FnOnce(&mut CellsBuilder),
    {
        let mut builder = CellsBuilder {
            cells: HashMap::new(),
        };
        define(&mut builder);
        Self(builder.cells)
    }

    pub fn insert(&mut self, cell: Cell) {
        self.0.insert(cell.name.clone(), cell);
    }

    pub fn new() -> Self {
        Cells(HashMap::new())
    }

    pub fn from<T: Iterator<Item = Cell>>(source: T) -> Cells {
        let mut cells = Self::default();
        for cell in source {
            cells.0.insert(cell.name.clone(), cell);
        }
        cells
    }

    /// Returns an iterator over all dependencies as (name, requirement) pairs.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &Cell)> {
        self.0.iter().map(|(k, v)| (k.as_str(), v))
    }

    pub fn get(&self, name: &str) -> Option<&Cell> {
        self.0.get(name)
    }

    pub fn count(&self) -> usize {
        self.0.len()
    }

    pub fn keys(&self) -> impl Iterator<Item = &str> {
        self.0.keys().map(|k| k.as_str())
    }

    /// Conforms the provided parameters conform to the schema of the cells.
    pub fn conform(&self, parameters: Parameters) -> Result<Parameters, SchemaError> {
        for (name, cell) in self.iter() {
            let parameter = parameters.get(name);
            cell.conform(parameter).map_err(|e| e.at(name.into()))?;
        }

        Ok(parameters)
    }
}

impl<T: Iterator<Item = Cell>> From<T> for Cells {
    fn from(source: T) -> Self {
        Self::from(source)
    }
}

impl From<&Cells> for Schema {
    fn from(cells: &Cells) -> Self {
        use crate::{Cardinality, Field};
        let mut schema = Schema::new();
        for (name, cell) in cells.iter() {
            schema.insert(
                name.into(),
                Field {
                    description: cell.description.clone(),
                    content_type: cell.content_type,
                    requirement: cell.requirement.clone(),
                    cardinality: Cardinality::One,
                },
            );
        }
        schema
    }
}

#[test]
fn test_cells() -> anyhow::Result<()> {
    let cells = Cells::define(|builder| {
        builder
            .cell("name", Some(Type::String))
            .the("name field")
            .required();

        builder
            .cell("age", Some(Type::UnsignedInt))
            .the("age field")
            .derived(15);
    });

    assert_eq!(cells.count(), 2);
    assert_eq!(cells.get("name").unwrap().name(), "name");
    assert_eq!(
        *cells.get("name").unwrap().content_type(),
        Some(Type::String)
    );
    assert_eq!(cells.get("name").unwrap().description(), "name field");
    assert_eq!(
        cells.get("name").unwrap().requirement(),
        &Requirement::Required(None)
    );

    assert_eq!(cells.get("age").unwrap().name(), "age");
    assert_eq!(
        *cells.get("age").unwrap().content_type(),
        Some(Type::UnsignedInt)
    );
    assert_eq!(cells.get("age").unwrap().description(), "age field");
    assert_eq!(
        cells.get("age").unwrap().requirement(),
        &Requirement::Optional
    );
    Ok(())
}
