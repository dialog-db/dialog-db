//! Rule-based deduction system
//!
//! This module implements the core rule system for dialog-query, allowing
//! declarative specification of derived facts through logical rules.
//!
//! The design is based on the TypeScript implementation in @query/src/plan/rule.js
//! and follows the patterns described in the design document at notes/rules.md.

// use crate::attribute::{Attribute, Match as AttributeMatch};
use crate::artifact::{ArtifactStore, Value};
use crate::concept::Concept;
use crate::error::QueryResult;
use crate::plan::{EvaluationContext, EvaluationPlan, MatchFrame, Plan};
use crate::premise::Premise;
use crate::selection::Selection;
use crate::statement::Statement;
use crate::syntax::VariableScope;
use crate::term::Term;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Utility type that simply gets associated type for the relation.
#[allow(type_alias_bounds)]
pub type Match<T: Concept> = T::Match;
#[allow(type_alias_bounds)]
pub type Claim<T: Concept> = T::Claim;
#[allow(type_alias_bounds)]
pub type Attributes<T: Concept> = T::Attributes;

/// Collection of premises that must be satisfied for a rule to apply.
///
/// This is simply an alias for `Vec<Statement>` to provide semantic meaning
/// for the "when" part of rules - the conditions that must be true for a rule to fire.
///
/// # Design Goal
/// 
/// The goal is to enable clean, readable rule definitions using the standard
/// `vec![]` macro: `vec![premise1, premise2, premise3]`
/// 
/// This provides clean syntax without type annotations or conversion methods,
/// matching idiomatic Rust patterns.
///
/// # Usage Pattern
/// 
/// Rules should return premises using the `vec!` macro:
/// ```rust
/// use dialog_query::{When, Statement, FactSelector, Term, Value};
/// 
/// struct ExampleRule;
/// 
/// impl ExampleRule {
///     fn when(&self) -> When {
///         vec![
///             Statement::select(FactSelector {
///                 the: Some(Term::from("attr1".parse::<dialog_query::artifact::Attribute>().unwrap())),
///                 of: Some(Term::var("entity")),
///                 is: Some(Term::from(Value::String("value1".to_string()))),
///                 fact: None,
///             }),
///             Statement::select(FactSelector {
///                 the: Some(Term::from("attr2".parse::<dialog_query::artifact::Attribute>().unwrap())),
///                 of: Some(Term::var("entity")),
///                 is: Some(Term::var("value2")),
///                 fact: None,
///             }),
///         ]
///     }
/// }
/// ```
pub type When = Vec<Statement>;




/// A rule that derives facts from conditions
///
/// This trait represents the core abstraction for rule-based deduction in the dialog-query system.
/// Rules follow the datalog pattern of "when conditions are met, then conclusions can be drawn".
///
/// # Design Philosophy
///
/// Rules are inspired by datalog and implement conditional logic:
/// - **Conditions (when)**: A set of premises that must all be satisfied
/// - **Conclusions (match)**: What can be derived when conditions are met
///
/// The design follows the patterns described in notes/rules.md, enabling clean,
/// declarative rule definitions that look similar to datalog syntax.
///
/// # Implementation Pattern
///
/// There are two ways to implement clean rule definitions:
///
/// **Option 1: Direct Array Returns (Recommended)**
///
/// Set `type When = [Statement; N]` and return arrays directly:
/// `[premise1, premise2]` (no .into() needed!)
///
/// **Option 2: When Return Type**
///
/// Set `type When = dialog_query::When` and return arrays with conversion:
/// `[premise1, premise2].into()` (.into() required)
///
/// Both approaches provide readable rule definitions that match the patterns in the design documentation.
///
/// # Type Safety
///
/// - Rules should be associated with Concepts for proper type safety
/// - The Match type must implement Premise so it can be used in queries
/// - All premises in when() must be satisfiable for the rule to fire
pub trait Rule: Clone + std::fmt::Debug {
    /// The type of match pattern this rule produces when instantiated
    ///
    /// This represents a specific instance of the rule with variable bindings.
    /// The Match type must implement Premise so it can be used directly in queries,
    /// enabling compositional rule building.
    type Match: Premise + Clone + std::fmt::Debug;

    /// Define the conditions (premises) that must be satisfied for this rule to apply
    ///
    /// This method defines the "when" part of the rule - all returned premises must
    /// be satisfied for the rule to fire. The premises are evaluated as a conjunction
    /// (logical AND).
    ///
    /// # Return Pattern
    ///
    /// Return premises using vec! macro:
    /// `vec![premise1, premise2, premise3]` (no type annotations needed!)
    ///
    /// This provides clean, readable rule definitions that match the syntax
    /// shown in the design documentation.
    ///
    /// # Implementation Notes
    ///
    /// - All premises must be satisfied (AND logic)
    /// - Premises typically share variables to create joins
    /// - Use Statement::fact_selector() for most basic premises
    /// - Variables like Term::var("entity") create joins across premises
    fn when(&self) -> When;

    /// Create a match instance with specific variable bindings
    ///
    /// This instantiates the rule with concrete variable bindings, typically
    /// from query patterns or other rule applications. The resulting Match
    /// can then be used in queries or as premises in other rules.
    ///
    /// # Parameters
    ///
    /// - `variables`: Map of variable names to their bound values
    ///
    /// # Returns
    ///
    /// A Match instance that implements Premise and can be used in:
    /// - Direct queries: `dialog.query(rule_match)`
    /// - Other rule premises: Include the match as a premise
    /// - Compositional rule building
    fn r#match(&self, variables: BTreeMap<String, Term<Value>>) -> Self::Match;
}

/// A derived rule that deduces facts by joining attributes on an entity
///
/// This represents the simplest form of rule - one that derives a composite
/// fact (like a "Person") by requiring that all its constituent attributes
/// exist on the same entity.
///
/// Based on the design document example where a Person rule would match
/// when an entity has both "person/name" and "person/birthday" attributes.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DerivedRule {
    /// The namespace/name for this rule (e.g., "person", "employee")
    pub the: String,

    /// The attributes that must exist for this rule to match
    ///
    /// Maps attribute names to their expected types. For example:
    /// - "name" -> String type
    /// - "birthday" -> u32 type
    pub attributes: BTreeMap<String, String>, // Simple string type names for now
}

impl DerivedRule {
    /// Create a new derived rule
    pub fn new(the: String, attributes: BTreeMap<String, String>) -> Self {
        Self { the, attributes }
    }
}





impl Rule for DerivedRule {
    type Match = DerivedRuleMatch;

    fn when(&self) -> When {
        let mut predicates = Vec::new();

        // Generate a predicate for each attribute
        // All predicates will use the same entity variable to ensure they join
        let entity_var = Term::var("this");

        for (attr_name, _attr_type) in &self.attributes {
            // Create attribute name in the format "namespace/attribute"
            let full_attr_name = format!("{}/{}", self.the, attr_name);
            let attr_term = Term::from(
                full_attr_name
                    .parse::<crate::artifact::Attribute>()
                    .unwrap(),
            );

            // Create a variable for the attribute value
            let value_var = Term::var(attr_name);

            // Create the fact selector predicate
            let selector = crate::fact_selector::FactSelector {
                the: Some(attr_term),
                of: Some(entity_var.clone()),
                is: Some(value_var),
                fact: None,
            };

            predicates.push(Statement::select(selector));
        }

        // If we have no attributes, create a tag predicate
        if predicates.is_empty() {
            let tag_attr = format!("the/{}", self.the);
            let attr_term = Term::from(tag_attr.parse::<crate::artifact::Attribute>().unwrap());
            let value_term = Term::from(Value::String(self.the.clone()));

            let selector = crate::fact_selector::FactSelector {
                the: Some(attr_term),
                of: Some(entity_var),
                is: Some(value_term),
                fact: None,
            };

            predicates.push(Statement::select(selector));
        }

        predicates
    }

    fn r#match(&self, variables: BTreeMap<String, Term<Value>>) -> Self::Match {
        DerivedRuleMatch {
            rule: self.clone(),
            variables,
        }
    }
}

/// A match instance for a derived rule
///
/// Represents a specific application of a derived rule with particular
/// variable bindings. This is what gets evaluated during query execution.
#[derive(Debug, Clone)]
pub struct DerivedRuleMatch {
    /// The rule definition this match is based on
    pub rule: DerivedRule,

    /// Variable bindings for this match
    pub variables: BTreeMap<String, Term<Value>>,
}

impl Premise for DerivedRuleMatch {
    type Plan = DerivedRuleMatchPlan;

    fn plan(&self, scope: &VariableScope) -> QueryResult<Self::Plan> {
        // Create execution plans for each premise predicate
        let mut premise_plans = Vec::new();

        for predicate in self.rule.when() {
            let plan = predicate.plan(scope)?;
            premise_plans.push(plan);
        }

        Ok(DerivedRuleMatchPlan {
            rule_match: self.clone(),
            premise_plans,
        })
    }
}

/// Execution plan for a derived rule match
///
/// Contains the plans needed to evaluate all premise predicates and
/// combine their results through join operations.
#[derive(Debug, Clone)]
pub struct DerivedRuleMatchPlan {
    /// The rule match this plan executes
    pub rule_match: DerivedRuleMatch,

    /// Plans for evaluating each premise statement
    pub premise_plans: Vec<crate::statement::StatementPlan>,
}

impl Plan for DerivedRuleMatchPlan {}

impl EvaluationPlan for DerivedRuleMatchPlan {
    fn cost(&self) -> f64 {
        // Cost is sum of all premise costs plus a small join overhead
        let premise_cost: f64 = self.premise_plans.iter().map(|p| p.cost()).sum();
        premise_cost + (self.premise_plans.len() as f64 * 0.1) // Small join overhead per premise
    }

    fn evaluate<S, M>(&self, _context: EvaluationContext<S, M>) -> impl Selection + '_
    where
        S: ArtifactStore + Clone + Send + 'static,
        M: Selection + 'static,
    {
        // This implements basic rule evaluation by joining premise predicates
        //
        // The algorithm would be:
        // 1. Evaluate each premise predicate against the context
        // 2. Join all results on shared variables (especially the "this" entity variable)
        // 3. Return matches where all premises are satisfied

        // For now, we'll return an empty selection as a placeholder
        // A full implementation would need sophisticated join operations
        crate::selection::EmptySelection::new()
    }
}

/// Rule application - represents a rule being applied to specific terms
///
/// This corresponds to RuleApplication in the TypeScript implementation.
/// It binds rule variables to specific terms from a query pattern.
#[derive(Debug, Clone)]
pub struct RuleApplication<R: Rule> {
    /// The rule being applied
    pub rule: R,

    /// The terms this rule is being applied to
    pub terms: BTreeMap<String, Term<Value>>,

    /// Variable bindings from the application context
    pub bindings: MatchFrame,
}

impl<R: Rule> RuleApplication<R> {
    /// Create a new rule application
    pub fn new(rule: R, terms: BTreeMap<String, Term<Value>>, bindings: MatchFrame) -> Self {
        Self {
            rule,
            terms,
            bindings,
        }
    }

    /// Get the cost of applying this rule
    pub fn cost(&self) -> f64 {
        // Base cost for rule application
        10.0
    }
}

impl<R: Rule + Send> Premise for RuleApplication<R>
where
    R::Match: Send,
{
    type Plan = RuleApplicationPlan<R>;

    fn plan(&self, _scope: &VariableScope) -> QueryResult<Self::Plan> {
        // Create a match instance from the rule
        let rule_match = self.rule.r#match(self.terms.clone());

        Ok(RuleApplicationPlan {
            application: self.clone(),
            rule_match,
        })
    }
}

/// Execution plan for rule applications
#[derive(Debug, Clone)]
pub struct RuleApplicationPlan<R: Rule> {
    /// The rule application being planned
    pub application: RuleApplication<R>,

    /// The rule match instance
    pub rule_match: R::Match,
}

impl<R: Rule + Send> Plan for RuleApplicationPlan<R> where R::Match: Send + Premise {}

impl<R: Rule + Send> EvaluationPlan for RuleApplicationPlan<R>
where
    R::Match: Send + Premise,
{
    fn cost(&self) -> f64 {
        self.application.cost()
    }

    fn evaluate<S, M>(&self, _context: EvaluationContext<S, M>) -> impl Selection + '_
    where
        S: ArtifactStore + Clone + Send + 'static,
        M: Selection + 'static,
    {
        // For now, return empty selection
        // Full implementation would evaluate the rule match
        crate::selection::EmptySelection::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifact::Value;
    use crate::premise::Premise;
    use crate::statement::Statement;
    use crate::syntax::VariableScope;
    use crate::term::Term;
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
        let premises = rule.when();

        // Should generate one predicate per attribute
        assert_eq!(premises.len(), 2);

        // Each premise should be a fact selector
        for premise in premises {
            match premise {
                Statement::Select(_) => {
                    // This is expected
                }
            }
        }
    }

    #[test]
    fn test_derived_rule_premises_empty_attributes() {
        let attributes = BTreeMap::new();
        let rule = DerivedRule::new("tag".to_string(), attributes);
        let premises = rule.when();

        // Should generate a tag predicate for empty attributes
        assert_eq!(premises.len(), 1);

        match &premises[0] {
            Statement::Select(selector) => {
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
        variables.insert(
            "name".to_string(),
            Term::from(Value::String("Alice".to_string())),
        );

        let rule_match = rule.r#match(variables.clone());

        assert_eq!(rule_match.rule.the, "person");
        assert_eq!(rule_match.variables, variables);
    }

    #[test]
    fn test_statement_fact_selector() {
        let entity_term = Term::var("entity");
        let attr_term = Term::from("person/name".parse::<crate::artifact::Attribute>().unwrap());
        let value_term = Term::from(Value::String("Alice".to_string()));

        let statement = Statement::fact(Some(attr_term), Some(entity_term), Some(value_term));

        match statement {
            Statement::Select(selector) => {
                assert!(selector.the.is_some());
                assert!(selector.of.is_some());
                assert!(selector.is.is_some());
            }
        }
    }

    #[test]
    fn test_statement_planning() {
        let entity_term = Term::var("entity");
        let attr_term = Term::from("person/name".parse::<crate::artifact::Attribute>().unwrap());
        let value_term = Term::from(Value::String("Alice".to_string()));

        let statement = Statement::fact(Some(attr_term), Some(entity_term), Some(value_term));

        let scope = VariableScope::new();
        let plan = statement.plan(&scope);

        // Should successfully create a plan
        assert!(plan.is_ok());
    }

    #[test]
    fn test_when_array_literal_api() {
        // Test that we can use array literals to create When collections
        let statement1 = Statement::select(crate::fact_selector::FactSelector {
            the: Some(Term::from(
                "person/name".parse::<crate::artifact::Attribute>().unwrap(),
            )),
            of: Some(Term::var("entity")),
            is: Some(Term::from(Value::String("Alice".to_string()))),
            fact: None,
        });

        let statement2 = Statement::select(crate::fact_selector::FactSelector {
            the: Some(Term::from(
                "person/age".parse::<crate::artifact::Attribute>().unwrap(),
            )),
            of: Some(Term::var("entity")),
            is: Some(Term::from(Value::UnsignedInt(25))),
            fact: None,
        });

        // This is the key test - vec! macro syntax should work
        let when_collection: When = vec![statement1.clone(), statement2.clone()];

        assert_eq!(when_collection.len(), 2);
        assert_eq!(when_collection[0], statement1);
        assert_eq!(when_collection[1], statement2);

        // Test single element vecs
        let single_when: When = vec![statement1.clone()];
        assert_eq!(single_when.len(), 1);
        assert_eq!(single_when[0], statement1);
    }

    #[test]
    fn test_clean_rule_function_api() {
        // Test that demonstrates the clean API we want for rule functions

        // This simulates what a rule function would look like:
        fn example_rule_function() -> When {
            let statement1 = Statement::select(crate::fact_selector::FactSelector {
                the: Some(Term::from(
                    "person/name".parse::<crate::artifact::Attribute>().unwrap(),
                )),
                of: Some(Term::var("entity")),
                is: Some(Term::from(Value::String("John".to_string()))),
                fact: None,
            });

            let statement2 = Statement::select(crate::fact_selector::FactSelector {
                the: Some(Term::from(
                    "person/birthday"
                        .parse::<crate::artifact::Attribute>()
                        .unwrap(),
                )),
                of: Some(Term::var("entity")),
                is: Some(Term::var("birthday")),
                fact: None,
            });

            // Clean vec! macro - no .into() or type annotations needed!
            vec![statement1, statement2]
        }

        // Call our example rule function
        let when_result = example_rule_function();

        // Verify it works correctly
        assert_eq!(when_result.len(), 2);

        // Verify the statements are correct
        if let Statement::Select(ref selector) = when_result[0] {
            assert!(selector.the.is_some());
            assert!(selector.of.is_some());
            assert!(selector.is.is_some());
        } else {
            panic!("Expected FactSelector statement");
        }
    }

    #[test]
    fn test_vec_clean_syntax() {
        // Test the vec! macro for clean rule definition syntax

        #[derive(Debug, Clone)]
        struct VecTestRule;

        impl Rule for VecTestRule {
            type Match = VecTestRuleMatch;

            fn when(&self) -> When {
                let statement1 = Statement::select(crate::fact_selector::FactSelector {
                    the: Some(Term::from(
                        "macro/attr1".parse::<crate::artifact::Attribute>().unwrap(),
                    )),
                    of: Some(Term::var("entity")),
                    is: Some(Term::from(Value::String("value1".to_string()))),
                    fact: None,
                });

                let statement2 = Statement::select(crate::fact_selector::FactSelector {
                    the: Some(Term::from(
                        "macro/attr2".parse::<crate::artifact::Attribute>().unwrap(),
                    )),
                    of: Some(Term::var("entity")),
                    is: Some(Term::var("value2")),
                    fact: None,
                });

                // This is the key test: using vec! macro for clean syntax
                vec![statement1, statement2]
            }

            fn r#match(&self, variables: BTreeMap<String, Term<Value>>) -> Self::Match {
                VecTestRuleMatch { variables }
            }
        }

        #[derive(Debug, Clone)]
        struct VecTestRuleMatch {
            variables: BTreeMap<String, Term<Value>>,
        }

        impl Premise for VecTestRuleMatch {
            type Plan = VecTestRuleMatchPlan;

            fn plan(&self, _scope: &VariableScope) -> QueryResult<Self::Plan> {
                Ok(VecTestRuleMatchPlan)
            }
        }

        #[derive(Debug, Clone)]
        struct VecTestRuleMatchPlan;

        impl crate::plan::Plan for VecTestRuleMatchPlan {}

        impl crate::plan::EvaluationPlan for VecTestRuleMatchPlan {
            fn cost(&self) -> f64 {
                1.0
            }

            fn evaluate<S, M>(
                &self,
                _context: crate::plan::EvaluationContext<S, M>,
            ) -> impl crate::Selection + '_
            where
                S: crate::artifact::ArtifactStore + Clone + Send + 'static,
                M: crate::Selection + 'static,
            {
                crate::selection::EmptySelection::new()
            }
        }

        // Test the rule
        let rule = VecTestRule;
        let when_result = rule.when();

        // Verify it works correctly
        assert_eq!(when_result.len(), 2);

        // Verify the statements are correct
        for statement in &when_result {
            if let Statement::Select(ref selector) = statement {
                assert!(selector.the.is_some());
                assert!(selector.of.is_some());
                assert!(selector.is.is_some());
            } else {
                panic!("Expected FactSelector statement");
            }
        }
    }

    #[test]
    fn test_direct_array_return_without_into() {
        // Test for the new IntoWhen trait that allows direct array returns

        #[derive(Debug, Clone)]
        struct TestRule;

        impl Rule for TestRule {
            type Match = TestRuleMatch;

            fn when(&self) -> When {
                let statement1 = Statement::select(crate::fact_selector::FactSelector {
                    the: Some(Term::from(
                        "test/attr1".parse::<crate::artifact::Attribute>().unwrap(),
                    )),
                    of: Some(Term::var("entity")),
                    is: Some(Term::from(Value::String("value1".to_string()))),
                    fact: None,
                });

                let statement2 = Statement::select(crate::fact_selector::FactSelector {
                    the: Some(Term::from(
                        "test/attr2".parse::<crate::artifact::Attribute>().unwrap(),
                    )),
                    of: Some(Term::var("entity")),
                    is: Some(Term::var("value2")),
                    fact: None,
                });

                // This is the key test: using vec! macro for clean syntax
                vec![statement1, statement2]
            }

            fn r#match(&self, variables: BTreeMap<String, Term<Value>>) -> Self::Match {
                TestRuleMatch { variables }
            }
        }

        #[derive(Debug, Clone)]
        struct TestRuleMatch {
            variables: BTreeMap<String, Term<Value>>,
        }

        impl Premise for TestRuleMatch {
            type Plan = TestRuleMatchPlan;

            fn plan(&self, _scope: &VariableScope) -> QueryResult<Self::Plan> {
                Ok(TestRuleMatchPlan)
            }
        }

        #[derive(Debug, Clone)]
        struct TestRuleMatchPlan;

        impl crate::plan::Plan for TestRuleMatchPlan {}

        impl crate::plan::EvaluationPlan for TestRuleMatchPlan {
            fn cost(&self) -> f64 {
                1.0
            }

            fn evaluate<S, M>(
                &self,
                _context: crate::plan::EvaluationContext<S, M>,
            ) -> impl crate::Selection + '_
            where
                S: crate::artifact::ArtifactStore + Clone + Send + 'static,
                M: crate::Selection + 'static,
            {
                crate::selection::EmptySelection::new()
            }
        }

        // Test the rule
        let rule = TestRule;
        let when_result = rule.when();

        // Verify it works correctly
        assert_eq!(when_result.len(), 2);

        // Verify the statements are correct
        for statement in &when_result {
            if let Statement::Select(ref selector) = statement {
                assert!(selector.the.is_some());
                assert!(selector.of.is_some());
                assert!(selector.is.is_some());
            } else {
                panic!("Expected FactSelector statement");
            }
        }
    }
}
