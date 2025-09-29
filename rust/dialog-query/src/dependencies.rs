use crate::fact::Scalar;
use crate::Term;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Tracks dependencies and their requirement levels for rules and formulas.
/// Used during analysis to determine execution costs and validate requirements.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Dependencies(HashMap<String, Requirement>);

impl Dependencies {
    /// Creates a new empty dependency set.
    pub fn new() -> Self {
        Dependencies(HashMap::new())
    }

    /// Calculates the total cost of all derived dependencies.
    /// Required dependencies don't contribute to cost as they must be provided.
    pub fn cost(&self) -> usize {
        self.0
            .values()
            .filter_map(|d| match d {
                Requirement::Derived(cost) => Some(*cost),
                _ => None,
            })
            .sum()
    }

    /// Returns an iterator over all dependencies as (name, requirement) pairs.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &Requirement)> {
        self.0.iter().map(|(k, v)| (k.as_str(), v))
    }

    /// Adds or updates a derived dependency with the given cost.
    /// If dependency already exists as derived, keeps the maximum cost.
    pub fn desire(&mut self, dependency: String, cost: usize) {
        let Dependencies(content) = self;
        if let Some(existing) = content.get(&dependency) {
            if let Requirement::Derived(prior) = existing {
                content.insert(dependency, Requirement::Derived(cost.max(*prior)));
            }
        } else {
            content.insert(dependency, Requirement::Derived(cost));
        }
    }

    /// Marks a dependency as provided (zero cost derived).
    pub fn provide(&mut self, dependency: String) {
        self.desire(dependency, 0);
    }

    /// Marks a dependency as required - must be provided externally.
    pub fn require(&mut self, dependency: String) {
        self.0.insert(dependency, Requirement::Required);
    }

    /// Alters the dependency level to the lowest between current and provided
    /// levels. If dependency does not exist yet it is added. General idea
    /// behind picking lower ranking level is that if some premise is able to
    /// fulfill the requirement with a lower budget it will likely be picked
    /// to execute ahead of the ones that are more expensive, hence actual level
    /// is lower (🤔 perhaps average would be more accurate).
    pub fn merge(&mut self, dependency: String, requirement: &Requirement) {
        let Dependencies(content) = self;
        if let Some(existing) = content.get(&dependency) {
            if let Requirement::Derived(prior) = existing {
                if let Requirement::Derived(desire) = requirement {
                    content.insert(dependency, Requirement::Derived(*prior.min(desire)));
                }
            } else {
                content.insert(dependency, requirement.clone());
            }
        }
        // If dependency was previously assumed to be required it is no longer
        else {
            content.insert(dependency, requirement.clone());
        }
    }

    /// Checks if a dependency exists in this set.
    pub fn contains(&self, dependency: &str) -> bool {
        let Dependencies(content) = self;
        content.contains_key(dependency)
    }

    /// Returns an iterator over only the required dependencies.
    pub fn required(&self) -> impl Iterator<Item = (&str, &Requirement)> {
        self.0.iter().filter_map(|(k, v)| match v {
            Requirement::Required => Some((k.as_str(), v)),
            Requirement::Derived(_) => None,
        })
    }

    pub fn lookup(&self, dependency: &str) -> Option<&Requirement> {
        self.0.get(dependency)
    }

    /// Gets the requirement level for a dependency, defaulting to Derived(0) if not present.
    pub fn resolve(&self, name: &str) -> Requirement {
        match self.0.get(name) {
            Some(requirement) => requirement.clone(),
            None => Requirement::Derived(0),
        }
    }
}

/// Represents the requirement level for a dependency in a rule or formula.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Requirement {
    /// Dependency that must be provided externally - cannot be derived.
    Required,
    /// Dependency that could be provided. If not provided it will be derived.
    /// Number represents cost of the derivation.
    Derived(usize),
}

impl Requirement {
    /// Checks if this is a required (non-derivable) dependency.
    pub fn is_required(&self) -> bool {
        matches!(self, Requirement::Required)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dependencies_operations() {
        let mut deps = Dependencies::new();

        // Test basic operations
        assert!(!deps.contains("test"));
        assert_eq!(deps.resolve("test"), Requirement::Derived(0)); // Default value

        // Test desire
        deps.desire("test".into(), 100);
        assert!(deps.contains("test"));
        assert_eq!(deps.resolve("test"), Requirement::Derived(100));

        // Test require
        deps.require("required".into());
        assert_eq!(deps.resolve("required"), Requirement::Required);

        // Test provide
        deps.provide("provided".into());
        assert_eq!(deps.resolve("provided"), Requirement::Derived(0));

        // Test iteration
        let items: Vec<_> = deps.iter().collect();
        assert_eq!(items.len(), 3);
    }

    #[test]
    fn test_dependencies_update_logic() {
        let mut deps = Dependencies::new();

        // Test updating derived with derived - should take minimum cost
        deps.desire("cost".into(), 50);
        deps.merge("cost".into(), &Requirement::Derived(200));
        assert_eq!(deps.resolve("cost"), Requirement::Derived(50)); // Takes minimum

        // Test updating derived with lower cost - should take the new lower cost
        deps.merge("cost".into(), &Requirement::Derived(25));
        assert_eq!(deps.resolve("cost"), Requirement::Derived(25));

        // Test that Required dependency gets overridden when updated with Derived
        deps.require("required_test".into());
        deps.merge("required_test".into(), &Requirement::Derived(100));
        assert_eq!(deps.resolve("required_test"), Requirement::Derived(100));

        // Test adding new dependency via update
        deps.merge("new_dep".into(), &Requirement::Derived(75));
        assert_eq!(deps.resolve("new_dep"), Requirement::Derived(75));
    }

    #[test]
    fn test_requirement_properties() {
        let required = Requirement::Required;
        let derived = Requirement::Derived(100);

        assert!(required.is_required());
        assert!(!derived.is_required());
    }
}
