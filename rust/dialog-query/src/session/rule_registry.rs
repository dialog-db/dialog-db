use crate::Entity;
use crate::QueryError;
use crate::concept::application::ConceptRules;
use crate::concept::predicate::ConceptPredicate;
use crate::rule::deductive::DeductiveRule;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// Thread-safe registry of deductive rules, keyed by the conclusion entity.
///
/// Both [`Session`](super::Session) and [`QuerySession`](super::QuerySession)
/// hold a `RuleRegistry`. When a concept query needs rules, the registry
/// returns a [`ConceptRules`](crate::concept::application::ConceptRules)
/// bundle containing the default rule (derived from the concept's
/// attributes) plus any explicitly installed rules, together with a
/// per-adornment plan cache.
///
/// Cloning a registry is cheap — the underlying `HashMap` is wrapped in
/// `Arc<RwLock<…>>` so all clones share the same rule set and caches.
#[derive(Debug, Clone, Default)]
pub struct RuleRegistry {
    rules: Arc<RwLock<HashMap<Entity, ConceptRules>>>,
}

impl RuleRegistry {
    /// Creates an empty rule registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a deductive rule, deduplicating by equality.
    /// Invalidates cached plans for the affected concept entity.
    pub fn register(&mut self, rule: DeductiveRule) -> Result<(), QueryError> {
        let entity = rule.conclusion().this();
        self.rules
            .write()
            .map_err(|e| QueryError::FactStore(e.to_string()))?
            .entry(entity)
            .or_insert_with(|| ConceptRules::new(rule.conclusion()))
            .install(rule);
        Ok(())
    }

    /// Acquire rules for the given concept. Creates the default rule from
    /// the predicate's attributes on first access — so this always returns
    /// a ConceptRules regardless of whether any rules were explicitly installed.
    pub fn acquire(&self, predicate: &ConceptPredicate) -> Result<ConceptRules, QueryError> {
        let entity = predicate.this();
        Ok(self
            .rules
            .write()
            .map_err(|e| QueryError::FactStore(e.to_string()))?
            .entry(entity)
            .or_insert_with(|| ConceptRules::new(predicate))
            .clone())
    }
}
