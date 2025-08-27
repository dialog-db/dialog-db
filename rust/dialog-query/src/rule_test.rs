//! Tests for rule functionality
//!
//! This module contains tests for the basic rule system functionality,
//! focusing on derived rules and predicate evaluation.

#[cfg(test)]
mod tests {
    use super::*;
    use crate::predicate::{Predicate, PredicateForm};
    use crate::rule::{DerivedRule, Rule};
    use crate::syntax::VariableScope;
    use crate::term::Term;
    use dialog_artifacts::Value;
    use std::collections::BTreeMap;

    #[test]
    fn test_derived_rule_creation() {
        let mut attributes = BTreeMap::new();
        attributes.insert("name".to_string(), "String".to_string());
        attributes.insert("age".to_string(), "u32".to_string());
        
        let rule = DerivedRule::new("person".to_string(), attributes);
        
        assert_eq!(rule.the, "person");
        assert_eq!(rule.attributes.len(), 2);
        assert_eq!(rule.attributes.get("name"), Some(&"String".to_string()));
        assert_eq!(rule.attributes.get("age"), Some(&"u32".to_string()));
    }

    #[test]
    fn test_derived_rule_premises() {
        let mut attributes = BTreeMap::new();
        attributes.insert("name".to_string(), "String".to_string());
        attributes.insert("age".to_string(), "u32".to_string());
        
        let rule = DerivedRule::new("person".to_string(), attributes);
        let premises = rule.premises();
        
        // Should generate one predicate per attribute
        assert_eq!(premises.len(), 2);
        
        // Each premise should be a fact selector
        for premise in premises {
            match premise {
                PredicateForm::FactSelector(_) => {
                    // This is expected
                }
            }
        }
    }

    #[test]
    fn test_derived_rule_premises_empty_attributes() {
        let attributes = BTreeMap::new();
        let rule = DerivedRule::new("tag".to_string(), attributes);
        let premises = rule.premises();
        
        // Should generate a tag predicate for empty attributes
        assert_eq!(premises.len(), 1);
        
        match &premises[0] {
            PredicateForm::FactSelector(selector) => {
                // Should have a "the/tag" attribute and value "tag"
                assert!(selector.the.is_some());
                assert!(selector.of.is_some());
                assert!(selector.is.is_some());
            }
        }
    }

    #[test]
    fn test_derived_rule_match_creation() {
        let mut attributes = BTreeMap::new();
        attributes.insert("name".to_string(), "String".to_string());
        
        let rule = DerivedRule::new("person".to_string(), attributes);
        
        let mut variables = BTreeMap::new();
        variables.insert("this".to_string(), Term::var("person_entity"));
        variables.insert("name".to_string(), Term::from(Value::String("Alice".to_string())));
        
        let rule_match = rule.r#match(variables.clone());
        
        assert_eq!(rule_match.rule.the, "person");
        assert_eq!(rule_match.variables, variables);
    }

    #[test]
    fn test_predicate_form_fact_selector() {
        let entity_term = Term::var("entity");
        let attr_term = Term::from("person/name".parse::<dialog_artifacts::Attribute>().unwrap());
        let value_term = Term::from(Value::String("Alice".to_string()));
        
        let predicate = PredicateForm::fact(
            Some(attr_term),
            Some(entity_term),
            Some(value_term),
        );
        
        match predicate {
            PredicateForm::FactSelector(selector) => {
                assert!(selector.the.is_some());
                assert!(selector.of.is_some());
                assert!(selector.is.is_some());
            }
        }
    }

    #[test]
    fn test_predicate_planning() {
        let entity_term = Term::var("entity");
        let attr_term = Term::from("person/name".parse::<dialog_artifacts::Attribute>().unwrap());
        let value_term = Term::from(Value::String("Alice".to_string()));
        
        let predicate = PredicateForm::fact(
            Some(attr_term),
            Some(entity_term),
            Some(value_term),
        );
        
        let scope = VariableScope::new();
        let plan = predicate.plan(&scope);
        
        // Should successfully create a plan
        assert!(plan.is_ok());
    }
}