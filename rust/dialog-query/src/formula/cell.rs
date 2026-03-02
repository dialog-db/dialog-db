use std::collections::HashMap;
use std::fmt::Display;

use crate::error::{FieldTypeError, TypeError};
use crate::term::Term;
use crate::types::Any;
use crate::{Parameters, Requirement, Schema, Type};
use serde::{Deserialize, Serialize};

/// A single named parameter slot in a formula's schema.
///
/// Each `Cell` declares its name, an optional value type, and whether it is
/// required (must be bound before the formula runs) or optional/derived
/// (will be produced by the formula). The `#[derive(Formula)]` macro
/// generates a [`Cells`] collection from a formula struct's fields —
/// non-`#[derived]` fields become required cells, `#[derived]` fields
/// become optional cells.
///
/// Cells are also used for type checking: [`Cell::check`] validates that a
/// [`Term`](crate::Term) matches the cell's declared type, and
/// [`Cell::conform`] additionally enforces requirement constraints.
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
    /// Creates a new optional cell with the given name and optional content type.
    pub fn new(name: &'static str, content_type: Option<Type>) -> Self {
        Cell {
            name: name.to_string(),
            description: String::new(),
            content_type,
            requirement: Requirement::Optional,
        }
    }

    /// Sets the content type for this cell, returning `self` for chaining.
    pub fn typed(&mut self, content_type: Type) -> &mut Self {
        self.content_type = Some(content_type);
        self
    }

    /// Sets a human-readable description for this cell, returning `self` for chaining.
    pub fn the(&mut self, description: &'static str) -> &mut Self {
        self.description = description.to_string();
        self
    }

    /// Marks this cell as required, returning `self` for chaining.
    pub fn required(&mut self) -> &mut Self {
        self.requirement = Requirement::Required(None);
        self
    }

    /// Marks this cell as derived (optional), returning `self` for chaining.
    pub fn derived(&mut self, _derivation: usize) -> &mut Self {
        self.requirement = Requirement::Optional;
        self
    }

    /// Consumes and returns the cell, finalizing the builder chain.
    pub fn done(self) -> Self {
        self
    }

    /// Returns the name of this cell.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the description of this cell.
    pub fn description(&self) -> &str {
        &self.description
    }

    /// Returns the content type of this cell, if specified.
    pub fn content_type(&self) -> &Option<Type> {
        &self.content_type
    }

    /// Returns the requirement level of this cell.
    pub fn requirement(&self) -> &Requirement {
        &self.requirement
    }

    /// Type checks that the provided parameter matches this cell's content type.
    pub fn check(&self, param: &Term<Any>) -> Result<(), FieldTypeError> {
        // First we type check the input to ensure it matches cell's content type
        match (self.content_type(), param.content_type()) {
            // if expected is any (has no type) it checks
            (None, _) => Ok(()),
            // if cell is of some type and we're given term of unknown
            // type that's also fine.
            (_, None) => Ok(()),
            // if expected isn't any (has no type) it must be equal
            // to actual or it's a type missmatch.
            (Some(expected), actual) => {
                if Some(*expected) == actual {
                    Ok(())
                } else {
                    Err(FieldTypeError::TypeMismatch {
                        expected: *expected,
                        actual: Box::new(param.clone()),
                    })
                }
            }
        }
    }

    /// Validates that a parameter conforms to this cell's type and requirement constraints.
    pub fn conform(&self, param: Option<&Term<Any>>) -> Result<(), FieldTypeError> {
        // We check that cell type matches term type.
        if let Some(param) = param {
            self.check(param)?;
        }

        // Verify that required parameter is provided
        if self.requirement().is_required() {
            match param {
                Some(Term::Constant(_)) => Ok(()),
                Some(Term::Variable { name: Some(_), .. }) => Ok(()),
                Some(Term::Variable { name: None, .. }) => Err(FieldTypeError::BlankRequirement),
                None => Err(FieldTypeError::OmittedRequirement),
            }?;
        };

        Ok(())
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

/// Builder for constructing a [`Cells`] collection via a callback.
pub struct CellsBuilder {
    cells: HashMap<String, Cell>,
}

impl CellsBuilder {
    /// Adds a new cell with the given name and optional type, returning it for further configuration.
    pub fn cell(&mut self, name: &'static str, content_type: Option<Type>) -> &mut Cell {
        let cell = Cell::new(name, content_type);
        self.cells.insert(name.to_string(), cell);
        self.cells.get_mut(name).unwrap()
    }
}

/// The complete set of parameter slots for a formula, keyed by name.
///
/// A `Cells` collection is the formula equivalent of a [`Schema`](crate::Schema):
/// it lists every input and output parameter together with type and
/// requirement metadata. The collection is typically built once (via the
/// `#[derive(Formula)]` macro) and stored in a `static OnceLock` so all
/// applications of the same formula share a single schema instance.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(transparent)]
pub struct Cells(HashMap<String, Cell>);
impl Cells {
    /// Creates a new `Cells` collection using a builder callback.
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

    /// Inserts a cell into this collection, keyed by its name.
    pub fn insert(&mut self, cell: Cell) {
        self.0.insert(cell.name.clone(), cell);
    }

    /// Creates an empty `Cells` collection.
    pub fn new() -> Self {
        Cells(HashMap::new())
    }

    /// Creates a `Cells` collection from an iterator of [`Cell`] values.
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

    /// Returns a reference to the cell with the given name, if it exists.
    pub fn get(&self, name: &str) -> Option<&Cell> {
        self.0.get(name)
    }

    /// Returns the number of cells in this collection.
    pub fn count(&self) -> usize {
        self.0.len()
    }

    /// Returns an iterator over the cell names in this collection.
    pub fn keys(&self) -> impl Iterator<Item = &str> {
        self.0.keys().map(|k| k.as_str())
    }

    /// Conforms the provided parameters conform to the schema of the cells.
    pub fn conform(&self, parameters: Parameters) -> Result<Parameters, TypeError> {
        for (name, cell) in self.iter() {
            cell.conform(parameters.get(name))
                .map_err(|e| e.at(name.into()))?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Requirement, Type};

    #[dialog_common::test]
    fn it_evaluates_cells() -> anyhow::Result<()> {
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
}
