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
    /// Required dependencies contribute cost only if part of choice group.
    /// Note: With the new cost model, this always returns 0 since costs are
    /// calculated by estimate() methods on Application/Premise types.
    pub fn cost(&self) -> usize {
        0
    }

    /// Returns an iterator over all dependencies as (name, requirement) pairs.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &Requirement)> {
        self.0.iter().map(|(k, v)| (k.as_str(), v))
    }

    /// Adds or updates a derived dependency.
    /// Note: Cost parameter is deprecated but kept for API compatibility.
    pub fn desire(&mut self, dependency: String, _cost: usize) {
        let Dependencies(content) = self;
        if !content.contains_key(&dependency) {
            content.insert(dependency, Requirement::Optional);
        }
    }

    /// Marks a dependency as provided (zero cost derived).
    pub fn provide(&mut self, dependency: String) {
        self.desire(dependency, 0);
    }

    /// Marks a dependency as required - must be provided externally.
    pub fn require(&mut self, dependency: String) {
        self.0.insert(dependency, Requirement::Required(None));
    }

    /// Alters the dependency level. If dependency does not exist yet it is added.
    pub fn merge(&mut self, dependency: String, requirement: &Requirement) {
        let Dependencies(content) = self;
        if let Some(existing) = content.get(&dependency) {
            if matches!(existing, Requirement::Optional)
                && matches!(requirement, Requirement::Optional)
            {
                // Both are derived, keep it as derived
                content.insert(dependency, Requirement::Optional);
            } else {
                content.insert(dependency, requirement.clone());
            }
        } else {
            // If dependency was previously assumed to be required it is no longer
            content.insert(dependency, requirement.clone());
        }
    }

    /// Checks if a dependency exists in this set.
    pub fn contains(&self, dependency: &str) -> bool {
        let Dependencies(content) = self;
        content.contains_key(dependency)
    }

    /// Returns an iterator over only the required dependencies.
    /// Includes both non-choice required and choice-group required dependencies.
    pub fn required(&self) -> impl Iterator<Item = (&str, &Requirement)> {
        self.0.iter().filter_map(|(k, v)| match v {
            Requirement::Required(_) => Some((k.as_str(), v)),
            Requirement::Optional => None,
        })
    }

    pub fn lookup(&self, dependency: &str) -> Option<&Requirement> {
        self.0.get(dependency)
    }

    /// Gets the requirement level for a dependency, defaulting to Derived if not present.
    pub fn resolve(&self, name: &str) -> Requirement {
        match self.0.get(name) {
            Some(requirement) => requirement.clone(),
            None => Requirement::Optional,
        }
    }
}

/// Represents the requirement level for a dependency in a rule or formula.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Requirement {
    /// Dependency that must be provided externally or via choice group.
    /// If Some(group), this is part of a choice group.
    /// If None, must be provided externally (no derivation possible).
    Required(Option<Group>),
    /// Dependency that can be derived if not provided.
    Optional,
}

impl Requirement {
    /// Checks if this is a required (non-derivable) dependency.
    pub fn is_required(&self) -> bool {
        matches!(self, Requirement::Required(_))
    }

    /// Check if this requirement is part of a choice group
    pub fn group(&self) -> Option<Group> {
        match self {
            Requirement::Required(Some(group)) => Some(*group),
            Requirement::Required(None) => None,
            Requirement::Optional => None,
        }
    }

    pub fn required() -> Self {
        Requirement::Required(None)
    }

    pub fn optional() -> Self {
        Requirement::Optional
    }
}

/// Identifier for a choice group
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Group(usize);

impl Group {
    fn new() -> Self {
        use std::sync::atomic::{AtomicUsize, Ordering};
        static NEXT_ID: AtomicUsize = AtomicUsize::new(0);
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        Group(id)
    }

    /// Mark this parameter as part of the choice with this group
    pub fn member(&self) -> Requirement {
        Requirement::Required(Some(*self))
    }
}

/// Static API for creating requirements
pub struct Dependency;

impl Dependency {
    /// Create a requirement group where one of the members is required
    /// in order to derive the rest.
    pub fn some() -> Group {
        Group::new()
    }

    /// Mark parameter as required (must be provided externally)
    pub fn require() -> Requirement {
        Requirement::Required(None)
    }

    /// Mark parameter as derived (can be computed if not provided)
    pub fn optional() -> Requirement {
        Requirement::Optional
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
        assert_eq!(deps.resolve("test"), Requirement::Optional); // Default value

        // Test desire
        deps.desire("test".into(), 100);
        assert!(deps.contains("test"));
        assert_eq!(deps.resolve("test"), Requirement::Optional);

        // Test require
        deps.require("required".into());
        assert_eq!(deps.resolve("required"), Requirement::Required(None));

        // Test provide
        deps.provide("provided".into());
        assert_eq!(deps.resolve("provided"), Requirement::Optional);

        // Test iteration
        let items: Vec<_> = deps.iter().collect();
        assert_eq!(items.len(), 3);
    }

    #[test]
    fn test_dependencies_update_logic() {
        let mut deps = Dependencies::new();

        // Test updating derived with derived - should remain derived
        deps.desire("cost".into(), 50);
        deps.merge("cost".into(), &Requirement::Optional);
        assert_eq!(deps.resolve("cost"), Requirement::Optional);

        // Test that Required dependency gets overridden when updated with Derived
        deps.require("required_test".into());
        deps.merge("required_test".into(), &Requirement::Optional);
        assert_eq!(deps.resolve("required_test"), Requirement::Optional);

        // Test adding new dependency via update
        deps.merge("new_dep".into(), &Requirement::Optional);
        assert_eq!(deps.resolve("new_dep"), Requirement::Optional);
    }

    #[test]
    fn test_requirement_properties() {
        let required = Requirement::Required(None);
        let derived = Requirement::Optional;

        assert!(required.is_required());
        assert!(!derived.is_required());
    }
}
