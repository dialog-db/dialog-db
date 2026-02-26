use crate::Term;
use crate::fact::Scalar;
use std::collections::HashSet;
use std::fmt::Display;

/// Variable names that must be bound before a premise can execute.
///
/// During query planning, each premise declares which variables it needs
/// as inputs (via `Requirement::Required` in its schema). The planner
/// tracks these as `Prerequisites` — when the environment gains new
/// bindings from previously-executed premises, satisfied variables are
/// removed. A premise becomes viable once its `Prerequisites` is empty.
///
/// If planning completes with non-empty `Prerequisites`, the planner
/// produces a `CompileError::RequiredBindings` error listing the
/// unsatisfied variable names.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Prerequisites(HashSet<String>);

impl Prerequisites {
    /// Creates an empty set.
    pub fn new() -> Self {
        Self::default()
    }

    /// Removes all entries.
    pub fn clear(&mut self) {
        self.0.clear();
    }

    /// Returns the number of unsatisfied variables.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns `true` if there are no unsatisfied variables.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Adds the variable name from `term`. Constants are ignored.
    /// Panics if the variable is unnamed (blank).
    pub fn insert<T: Scalar>(&mut self, term: &Term<T>) {
        match term {
            Term::Constant(_) => {}
            Term::Variable { name, .. } => {
                let variable = name
                    .clone()
                    .expect("prerequisites must be passed a named variable");
                self.0.insert(variable);
            }
        }
    }

    /// Removes the variable name from the set. Returns `true` if it was present.
    pub fn remove<T: Scalar>(&mut self, term: &Term<T>) -> bool {
        match term {
            Term::Variable {
                name: Some(name), ..
            } => self.0.remove(name),
            _ => false,
        }
    }
}

impl Display for Prerequisites {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut iter = self.0.iter();
        if let Some(name) = iter.next() {
            write!(f, "{}", name)?;
        }

        for name in iter {
            write!(f, ", {}", name)?;
        }

        write!(f, "")
    }
}
