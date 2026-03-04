use crate::premise::Premise;
use std::slice::Iter;
use std::vec::IntoIter;

/// An ordered collection of [`Premise`] values forming a rule's body.
///
/// Built from the return value of a rule's `when` function — either a
/// single premise, a tuple of premises, or an array/vec. The [`When`]
/// trait provides the conversion. At planning time the premises are
/// handed to the [`Planner`](crate::planner::Planner) which reorders
/// them for optimal execution.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Premises(pub(crate) Vec<Premise>);

impl Premises {
    /// Create a new empty When collection
    pub fn new() -> Self {
        Self::default()
    }

    /// Get the number of statements
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Get an iterator over the statements
    pub fn iter(&self) -> impl Iterator<Item = &Premise> {
        self.0.iter()
    }

    /// Add a statement-producing item to this When
    pub fn extend<T: super::When>(&mut self, items: T) {
        self.0.extend(items.into_premises());
    }

    /// Get the inner Vec for compatibility
    pub fn into_vec(self) -> Vec<Premise> {
        self.0
    }

    /// Get reference to inner Vec for compatibility
    pub fn as_vec(&self) -> &Vec<Premise> {
        &self.0
    }
}

impl IntoIterator for Premises {
    type Item = Premise;
    type IntoIter = IntoIter<Premise>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<'a> IntoIterator for &'a Premises {
    type Item = &'a Premise;
    type IntoIter = Iter<'a, Premise>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

impl<T: Into<Premise>> From<Vec<T>> for Premises {
    fn from(source: Vec<T>) -> Self {
        let mut premises = vec![];
        for each in source {
            premises.push(each.into());
        }
        Premises(premises)
    }
}

impl<T: Into<Premise>, const N: usize> From<[T; N]> for Premises {
    fn from(source: [T; N]) -> Self {
        let mut premises = vec![];
        for each in source {
            premises.push(each.into());
        }
        Premises(premises)
    }
}
