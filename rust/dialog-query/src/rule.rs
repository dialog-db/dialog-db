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
use crate::error::{QueryError, QueryResult};
use crate::fact_selector::FactSelector;
use crate::plan::{EvaluationContext, EvaluationPlan, MatchFrame};
use crate::premise::Premise;
use crate::query::Store;
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
pub type Claim<T: Concept> = T::Assert;
#[allow(type_alias_bounds)]
pub type Attributes<T: Concept> = T::Attributes;
#[allow(type_alias_bounds)]
pub type Instance<T: Concept> = T::Instance;

/// Collection of premises that must be satisfied for a rule to apply.
///
/// This type represents the "when" part of rules - the conditions that must be true for a rule to fire.
/// It supports multiple clean syntax options for rule definitions.
///
/// # Design Goal
///
/// Enable clean, readable rule definitions through multiple ergonomic approaches:
/// - Array syntax: `[premise1, premise2].into()` (works with any `T: Statements`)
/// - Macro syntax: `when![premise1, premise2]`
/// - Operator chaining: `premise1 & premise2 & premise3`
/// - Mixed approaches for maximum flexibility
///
/// # Usage Patterns
///
/// ```rust,ignore
/// use dialog_query::{When, FactSelector, Term, Value, when, Rule};
/// use std::collections::BTreeMap;
///
/// struct ExampleRule {
///     selector1: FactSelector<Value>,
///     selector2: FactSelector<Value>,
///     selector3: FactSelector<Value>,
/// }
///
/// struct ExampleMatch {
///     selector1: FactSelector<Value>,
///     selector2: FactSelector<Value>,
///     selector3: FactSelector<Value>,
/// }
///
/// impl Rule for ExampleRule {
///     type Match = ExampleMatch;
///
///     fn when(terms: Self::Match) -> When {
///         // Option 1: Array syntax with From trait - clean and direct
///         [terms.selector1, terms.selector2, terms.selector3].into()
///
///         // Option 2: Macro syntax - most concise
///         // when![terms.selector1, terms.selector2, terms.selector3]
///
///         // Option 3: Operator chaining - reads like logical AND
///         // terms.selector1 & terms.selector2 & terms.selector3
///     }
///
///     fn r#match(&self, variables: BTreeMap<String, Term<Value>>) -> Self::Match {
///         ExampleMatch {
///             selector1: self.selector1.clone(),
///             selector2: self.selector2.clone(),
///             selector3: self.selector3.clone(),
///         }
///     }
/// }
/// ```
/// Trait for types that can be converted into multiple statements
///
/// This enables flexible composition where single items, collections, or custom
/// types can all contribute statements to rule conditions.
pub trait Statements {
    type IntoIter: IntoIterator<Item = Statement>;
    fn statements(self) -> Self::IntoIter;
}

#[derive(Debug, Clone, PartialEq)]
pub struct When(Vec<Statement>);

impl When {
    /// Create a new empty When collection
    pub fn new() -> Self {
        When(Vec::new())
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
    pub fn iter(&self) -> impl Iterator<Item = &Statement> {
        self.0.iter()
    }

    /// Add a statement-producing item to this When
    pub fn extend<T: Statements>(&mut self, items: T) {
        self.0.extend(items.statements());
    }

    /// Get the inner Vec for compatibility
    pub fn into_vec(self) -> Vec<Statement> {
        self.0
    }

    /// Get reference to inner Vec for compatibility
    pub fn as_vec(&self) -> &Vec<Statement> {
        &self.0
    }
}

// Implement From for Vec<Statement> - most direct case
impl From<Vec<Statement>> for When {
    fn from(items: Vec<Statement>) -> Self {
        When(items)
    }
}

// Implement From for Vec<FactSelector> - common case
impl From<Vec<FactSelector<crate::artifact::Value>>> for When {
    fn from(items: Vec<FactSelector<crate::artifact::Value>>) -> Self {
        When(items.into_iter().map(Statement::Select).collect())
    }
}

// Generic implementation for arrays - this enables [anything_that_implements_Statements; N].into()
impl<T: Statements, const N: usize> From<[T; N]> for When {
    fn from(items: [T; N]) -> Self {
        let mut statements = Vec::new();
        for item in items {
            statements.extend(item.statements());
        }
        When(statements)
    }
}

// Implement From for single Statement
impl From<Statement> for When {
    fn from(item: Statement) -> Self {
        When(vec![item])
    }
}

// Implement From for single FactSelector
impl From<FactSelector<crate::artifact::Value>> for When {
    fn from(item: FactSelector<crate::artifact::Value>) -> Self {
        When(vec![Statement::Select(item)])
    }
}

// Implement indexing
impl std::ops::Index<usize> for When {
    type Output = Statement;
    fn index(&self, index: usize) -> &Self::Output {
        &self.0[index]
    }
}

// Implement IntoIterator for When
impl IntoIterator for When {
    type Item = Statement;
    type IntoIter = std::vec::IntoIter<Statement>;
    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

// Implement IntoIterator for &When
impl<'a> IntoIterator for &'a When {
    type Item = &'a Statement;
    type IntoIter = std::slice::Iter<'a, Statement>;
    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

impl Default for When {
    fn default() -> Self {
        Self::new()
    }
}

// Implement Statements for single statement types
impl Statements for Statement {
    type IntoIter = std::iter::Once<Statement>;
    fn statements(self) -> Self::IntoIter {
        std::iter::once(self)
    }
}

impl Statements for FactSelector<crate::artifact::Value> {
    type IntoIter = std::iter::Once<Statement>;
    fn statements(self) -> Self::IntoIter {
        std::iter::once(Statement::Select(self))
    }
}

// Implement Statements for collections
impl Statements for Vec<Statement> {
    type IntoIter = std::vec::IntoIter<Statement>;
    fn statements(self) -> Self::IntoIter {
        self.into_iter()
    }
}

impl<const N: usize> Statements for [Statement; N] {
    type IntoIter = std::array::IntoIter<Statement, N>;
    fn statements(self) -> Self::IntoIter {
        self.into_iter()
    }
}

impl<const N: usize> Statements for [FactSelector<crate::artifact::Value>; N] {
    type IntoIter = std::iter::Map<
        std::array::IntoIter<FactSelector<crate::artifact::Value>, N>,
        fn(FactSelector<crate::artifact::Value>) -> Statement,
    >;
    fn statements(self) -> Self::IntoIter {
        self.into_iter().map(|selector| Statement::Select(selector))
    }
}

impl Statements for Vec<FactSelector<crate::artifact::Value>> {
    type IntoIter = std::iter::Map<
        std::vec::IntoIter<FactSelector<crate::artifact::Value>>,
        fn(FactSelector<crate::artifact::Value>) -> Statement,
    >;
    fn statements(self) -> Self::IntoIter {
        self.into_iter().map(|selector| Statement::Select(selector))
    }
}

impl Statements for When {
    type IntoIter = std::vec::IntoIter<Statement>;
    fn statements(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

// Implement BitAnd (&) operators for combining statements
impl std::ops::BitAnd<When> for When {
    type Output = When;
    fn bitand(mut self, rhs: When) -> When {
        self.0.extend(rhs.0);
        self
    }
}

impl std::ops::BitAnd<FactSelector<crate::artifact::Value>> for When {
    type Output = When;
    fn bitand(mut self, rhs: FactSelector<crate::artifact::Value>) -> When {
        self.0.push(Statement::Select(rhs));
        self
    }
}

impl std::ops::BitAnd<Statement> for When {
    type Output = When;
    fn bitand(mut self, rhs: Statement) -> When {
        self.0.push(rhs);
        self
    }
}

// Allow starting chains with fact selectors
impl std::ops::BitAnd<FactSelector<crate::artifact::Value>>
    for FactSelector<crate::artifact::Value>
{
    type Output = When;
    fn bitand(self, rhs: FactSelector<crate::artifact::Value>) -> When {
        vec![self, rhs].into()
    }
}

impl std::ops::BitAnd<When> for FactSelector<crate::artifact::Value> {
    type Output = When;
    fn bitand(self, rhs: When) -> When {
        let mut result = When::new();
        result.0.push(Statement::Select(self));
        result.0.extend(rhs.0);
        result
    }
}

// Allow starting chains with statements
impl std::ops::BitAnd<Statement> for Statement {
    type Output = When;
    fn bitand(self, rhs: Statement) -> When {
        vec![self, rhs].into()
    }
}

impl std::ops::BitAnd<When> for Statement {
    type Output = When;
    fn bitand(self, rhs: When) -> When {
        let mut result = When::new();
        result.0.push(self);
        result.0.extend(rhs.0);
        result
    }
}

/// Macro for creating When collections with clean array-like syntax
///
/// This macro provides the most concise way to create rule conditions:
///
/// ```rust
/// use dialog_query::{when, When, FactSelector, Term, Value};
///
/// fn example() -> When {
///     let selector1 = FactSelector {
///         the: Some(Term::from("attr1".parse::<dialog_query::artifact::Attribute>().unwrap())),
///         of: Some(Term::var("entity")),
///         is: Some(Term::from(Value::String("value1".to_string()))),
///         fact: None,
///     };
///     let selector2 = FactSelector {
///         the: Some(Term::from("attr2".parse::<dialog_query::artifact::Attribute>().unwrap())),
///         of: Some(Term::var("entity")),
///         is: Some(Term::var("value2")),
///         fact: None,
///     };
///     let selector3 = FactSelector {
///         the: Some(Term::from("attr3".parse::<dialog_query::artifact::Attribute>().unwrap())),
///         of: Some(Term::var("entity")),
///         is: Some(Term::var("value3")),
///         fact: None,
///     };
///     when![selector1, selector2, selector3]
/// }
/// ```
#[macro_export]
macro_rules! when {
    [$($item:expr),* $(,)?] => {
        vec![$($item),*].into()
    };
}

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
/// # Type Safety
///
/// Rules are associated with Concepts which provide:
/// - `Match`: The match pattern with Term-wrapped fields for querying
/// - `Claim`: The claim pattern for asserting derived facts
/// - `Attributes`: Builder pattern with attribute matchers
///
/// The Concept association ensures proper type safety and consistent patterns.
pub trait Rule: Concept {
    /// Define the conditions (premises) that must be satisfied for this rule to apply
    ///
    /// This method defines the "when" part of the rule - all returned premises must
    /// be satisfied for the rule to fire. The premises are evaluated as a conjunction
    /// (logical AND).
    ///
    /// # Parameters
    ///
    /// - `terms`: The match pattern with variable bindings that this rule is checking
    ///
    /// # Return Pattern
    ///
    /// Return premises using the clean When syntax:
    /// - `When::from([premise1, premise2])` - Array syntax
    /// - `when![premise1, premise2]` - Macro syntax
    /// - `premise1 & premise2` - Operator chaining
    ///
    /// # Implementation Notes
    ///
    /// - All premises must be satisfied (AND logic)
    /// - Premises typically use variables from the match pattern
    /// - Variables create joins across premises
    /// - The match pattern provides the context for generating appropriate premises
    fn when(terms: Self::Match) -> When;
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

    /// Create a match instance with specific entity and attributes
    pub fn create_match(
        &self,
        entity: Term<crate::artifact::Entity>,
        attributes: BTreeMap<String, Term<Value>>,
    ) -> DerivedRuleMatch {
        DerivedRuleMatch {
            this: entity,
            rule: self.clone(),
            attributes,
        }
    }
}

/// Attributes pattern for DerivedRule - used for building queries with .is() and .not()
#[derive(Debug, Clone)]
pub struct DerivedRuleAttributes {
    pub entity: Term<crate::artifact::Entity>,
    pub rule: Option<DerivedRule>,
}

/// Claim pattern for DerivedRule - used in rule conclusions
#[derive(Debug, Clone)]
pub struct AssertDerivedRule {
    pub attributes: BTreeMap<String, Term<Value>>,
}

#[derive(Debug, Clone)]
pub struct RetractDerivedRule {
    pub attributes: BTreeMap<String, Term<Value>>,
}

impl Concept for DerivedRule {
    type Instance = DerivedRule;
    type Match = DerivedRuleMatch;
    type Assert = AssertDerivedRule;
    type Retract = RetractDerivedRule;
    type Attributes = DerivedRuleAttributes;

    fn name() -> &'static str {
        "DerivedRule"
    }

    fn r#match<T: Into<Term<crate::artifact::Entity>>>(this: T) -> Self::Attributes {
        DerivedRuleAttributes {
            entity: this.into(),
            rule: None, // Will be set when used with a specific rule instance
        }
    }
}

impl crate::concept::Match for DerivedRuleMatch {
    type Instance = DerivedRule;
    type Attributes = DerivedRuleAttributes;

    fn term_for(&self, name: &str) -> Option<&Term<Value>> {
        self.attributes.get(name)
    }

    fn this(&self) -> Term<crate::artifact::Entity> {
        self.this.clone()
    }
}

impl crate::concept::Attributes for DerivedRuleAttributes {
    fn attributes() -> &'static [(&'static str, crate::attribute::Attribute<Value>)] {
        // For now, return empty slice as DerivedRule attributes are dynamic
        &[]
    }
}

impl crate::concept::Instance for DerivedRule {
    fn this(&self) -> crate::artifact::Entity {
        // For now, create a new entity - in practice this would be stored
        crate::artifact::Entity::new().unwrap()
    }
}

impl Rule for DerivedRule {
    fn when(terms: Self::Match) -> When {
        let mut selectors = Vec::new();

        // Use the entity from the match pattern
        let entity_var = terms.this.clone();

        for (attr_name, _attr_type) in &terms.rule.attributes {
            // Create attribute name in the format "namespace/attribute"
            let full_attr_name = format!("{}/{}", terms.rule.the, attr_name);
            let attr_term = Term::from(
                full_attr_name
                    .parse::<crate::artifact::Attribute>()
                    .unwrap(),
            );

            // Get the value variable from the match or create a new one
            let value_var = terms
                .attributes
                .get(attr_name)
                .cloned()
                .unwrap_or_else(|| Term::var(attr_name));

            // Create the fact selector predicate
            let selector = crate::fact_selector::FactSelector {
                the: Some(attr_term),
                of: Some(entity_var.clone()),
                is: Some(value_var),
                fact: None,
            };

            selectors.push(selector);
        }

        // If we have no attributes, create a tag predicate
        if selectors.is_empty() {
            let tag_attr = format!("the/{}", terms.rule.the);
            let attr_term = Term::from(tag_attr.parse::<crate::artifact::Attribute>().unwrap());
            let value_term = Term::from(Value::String(terms.rule.the.clone()));

            let selector = crate::fact_selector::FactSelector {
                the: Some(attr_term),
                of: Some(entity_var),
                is: Some(value_term),
                fact: None,
            };

            selectors.push(selector);
        }

        When::from(selectors)
    }
}

/// A match instance for a derived rule
///
/// Represents a match pattern with Term-wrapped fields for querying.
/// This follows the Concept pattern where Match types have:
/// - A `this` field of type Term<Entity> for the entity being matched
/// - Term-wrapped fields for each attribute
#[derive(Debug, Clone)]
pub struct DerivedRuleMatch {
    /// The entity being matched
    pub this: Term<crate::artifact::Entity>,

    /// The rule definition this match is based on
    pub rule: DerivedRule,

    /// Attribute terms - one Term per attribute defined in the rule
    pub attributes: BTreeMap<String, Term<Value>>,
}

// impl Statements for DerivedRuleMatch {
//     type IntoIter = std::vec::IntoIter<Statement>;
//     fn statements(self) -> Self::IntoIter {
//         DerivedRule::when(self).into_iter()
//     }
// }

// impl Premise for DerivedRuleMatch {
//     type Plan = DerivedRuleMatchPlan;

//     fn plan(&self, scope: &VariableScope) -> QueryResult<Self::Plan> {
//         // Create execution plans for each premise predicate
//         let mut premise_plans = Vec::new();

//         for predicate in DerivedRule::when(self.clone()) {
//             let plan = predicate.plan(scope)?;
//             premise_plans.push(plan);
//         }

//         Ok(DerivedRuleMatchPlan {
//             rule_match: self.clone(),
//             premise_plans,
//         })
//     }
// }

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

impl EvaluationPlan for DerivedRuleMatchPlan {
    fn cost(&self) -> &crate::plan::Cost {
        // For now return a static cost
        &crate::plan::Cost::Estimate(100)
    }

    fn evaluate<S: Store, M: Selection>(
        &self,
        _context: EvaluationContext<S, M>,
    ) -> impl Selection {
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
    R::Match: Send + Premise,
{
    type Plan = RuleApplicationPlan<R>;

    fn plan(&self, _scope: &VariableScope) -> QueryResult<Self::Plan> {
        // For now, create a simple placeholder plan
        // In practice, this would need proper rule evaluation logic
        // We need to create a proper match for the specific rule type R
        // For now, we'll use a simplified approach
        Err(QueryError::PlanningError {
            message: "RuleApplication planning not yet implemented for generic rules".to_string(),
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

impl<R: Rule + Send> EvaluationPlan for RuleApplicationPlan<R>
where
    R::Match: Send + Premise,
{
    fn cost(&self) -> &crate::plan::Cost {
        // For now return a static cost, proper implementation would calculate rule cost
        &crate::plan::Cost::Estimate(100)
    }

    fn evaluate<S: Store, M: Selection>(
        &self,
        _context: EvaluationContext<S, M>,
    ) -> impl Selection {
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

    // #[test]
    // fn test_derived_rule_premises() {
    //     let mut attributes = BTreeMap::new();
    //     attributes.insert("name".to_string(), "String".to_string());
    //     attributes.insert("age".to_string(), "u32".to_string());

    //     let rule = DerivedRule::new("person".to_string(), attributes);
    //     let rule_match = rule.create_match(Term::var("entity"), BTreeMap::new());
    //     let premises = DerivedRule::when(rule_match);

    //     // Should generate one predicate per attribute
    //     assert_eq!(premises.len(), 2);

    //     // Each premise should be a fact selector
    //     for premise in premises {
    //         match premise {
    //             Statement::Select(_) => {
    //                 // This is expected
    //             }
    //             Statement::Query(_) => {
    //                 // Concept selectors are also valid premises
    //             }
    //         }
    //     }
    // }

    // #[test]
    // fn test_derived_rule_premises_empty_attributes() {
    //     let attributes = BTreeMap::new();
    //     let rule = DerivedRule::new("tag".to_string(), attributes);
    //     let rule_match = rule.create_match(Term::var("entity"), BTreeMap::new());
    //     let premises = DerivedRule::when(rule_match);

    //     // Should generate a tag predicate for empty attributes
    //     assert_eq!(premises.len(), 1);

    //     match &premises[0] {
    //         Statement::Select(selector) => {
    //             // Should have a "the/tag" attribute and value "tag"
    //             assert!(selector.the.is_some());
    //             assert!(selector.of.is_some());
    //             assert!(selector.is.is_some());
    //         }
    //         Statement::Query(_) => {
    //             // Query statements are also valid
    //         }
    //     }
    // }

    #[test]
    fn test_derived_rule_match_creation() {
        let mut attributes = BTreeMap::new();
        attributes.insert("name".to_string(), "String".to_string());

        let rule = DerivedRule::new("person".to_string(), attributes);

        let entity = Term::var("person_entity");
        let mut attr_values = BTreeMap::new();
        attr_values.insert(
            "name".to_string(),
            Term::from(Value::String("Alice".to_string())),
        );

        let rule_match = rule.create_match(entity.clone(), attr_values.clone());

        assert_eq!(rule_match.rule.the, "person");
        assert_eq!(rule_match.this, entity);
        assert_eq!(rule_match.attributes, attr_values);
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

        // This is the key test - When::from syntax should work
        let when_collection: When = When::from([statement1.clone(), statement2.clone()]);

        assert_eq!(when_collection.len(), 2);
        assert_eq!(when_collection[0], statement1);
        assert_eq!(when_collection[1], statement2);

        // Test single element vecs
        let single_when: When = When::from([statement1.clone()]);
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

            // Clean When::from - no .into() or type annotations needed!
            When::from([statement1, statement2])
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

    // #[test]
    // fn test_vec_clean_syntax() {
    //     // Test the vec! macro for clean rule definition syntax

    //     #[derive(Debug, Clone)]
    //     struct VecTestRule;

    //     #[derive(Debug, Clone)]
    //     struct VecTestRuleAttributes;

    //     #[derive(Debug, Clone)]
    //     struct AssertVecTestRule;

    //     #[derive(Debug, Clone)]
    //     struct RetractVecTestRule;

    //     impl Statements for VecTestRuleMatch {
    //         type IntoIter = std::vec::IntoIter<Statement>;
    //         fn statements(self) -> Self::IntoIter {
    //             VecTestRule::when(self).into_iter()
    //         }
    //     }

    //     impl Concept for VecTestRule {
    //         type Match = VecTestRuleMatch;
    //         type Assert = AssertVecTestRule;
    //         type Retract = RetractVecTestRule;

    //         fn attributes() -> &'static [crate::attribute::Attribute<crate::artifact::Value>] {
    //             &[]
    //         }
    //         type Attributes = VecTestRuleAttributes;

    //         fn name() -> &'static str {
    //             "VecTestRule"
    //         }

    //         fn r#match<T: Into<Term<crate::artifact::Entity>>>(_this: T) -> Self::Attributes {
    //             VecTestRuleAttributes
    //         }
    //     }

    //     impl Rule for VecTestRule {
    //         fn when(_terms: Self::Match) -> When {
    //             let statement1 = Statement::select(crate::fact_selector::FactSelector {
    //                 the: Some(Term::from(
    //                     "macro/attr1".parse::<crate::artifact::Attribute>().unwrap(),
    //                 )),
    //                 of: Some(Term::var("entity")),
    //                 is: Some(Term::from(Value::String("value1".to_string()))),
    //                 fact: None,
    //             });

    //             let statement2 = Statement::select(crate::fact_selector::FactSelector {
    //                 the: Some(Term::from(
    //                     "macro/attr2".parse::<crate::artifact::Attribute>().unwrap(),
    //                 )),
    //                 of: Some(Term::var("entity")),
    //                 is: Some(Term::var("value2")),
    //                 fact: None,
    //             });

    //             // This is the key test: using When::from for clean syntax
    //             When::from([statement1, statement2])
    //         }
    //     }

    //     #[derive(Debug, Clone)]
    //     struct VecTestRuleMatch {
    //         this: Term<crate::artifact::Entity>,
    //     }

    //     impl Premise for VecTestRuleMatch {
    //         type Plan = VecTestRuleMatchPlan;

    //         fn plan(&self, _scope: &VariableScope) -> QueryResult<Self::Plan> {
    //             Ok(VecTestRuleMatchPlan)
    //         }
    //     }

    //     #[derive(Debug, Clone)]
    //     struct VecTestRuleMatchPlan;

    //     impl crate::plan::Plan for VecTestRuleMatchPlan {}

    //     impl crate::plan::EvaluationPlan for VecTestRuleMatchPlan {
    //         fn cost(&self) -> f64 {
    //             1.0
    //         }

    //         fn evaluate<S, M>(
    //             &self,
    //             _context: crate::plan::EvaluationContext<S, M>,
    //         ) -> impl crate::Selection + '_
    //         where
    //             S: crate::artifact::ArtifactStore + Clone + Send + 'static,
    //             M: crate::Selection + 'static,
    //         {
    //             crate::selection::EmptySelection::new()
    //         }
    //     }

    //     // Test the rule
    //     let rule_match = VecTestRuleMatch {
    //         this: Term::var("test_entity"),
    //     };
    //     let when_result = VecTestRule::when(rule_match);

    //     // Verify it works correctly
    //     assert_eq!(when_result.len(), 2);

    //     // Verify the statements are correct
    //     for statement in &when_result {
    //         if let Statement::Select(ref selector) = statement {
    //             assert!(selector.the.is_some());
    //             assert!(selector.of.is_some());
    //             assert!(selector.is.is_some());
    //         } else {
    //             panic!("Expected FactSelector statement");
    //         }
    //     }
    // }

    #[test]
    fn test_new_when_api_comprehensive() {
        // Test comprehensive When API with all syntax options

        let selector1 = crate::fact_selector::FactSelector {
            the: Some(Term::from(
                "test/attr1".parse::<crate::artifact::Attribute>().unwrap(),
            )),
            of: Some(Term::var("entity")),
            is: Some(Term::from(Value::String("value1".to_string()))),
            fact: None,
        };

        let selector2 = crate::fact_selector::FactSelector {
            the: Some(Term::from(
                "test/attr2".parse::<crate::artifact::Attribute>().unwrap(),
            )),
            of: Some(Term::var("entity")),
            is: Some(Term::var("value2")),
            fact: None,
        };

        let selector3 = crate::fact_selector::FactSelector {
            the: Some(Term::from(
                "test/attr3".parse::<crate::artifact::Attribute>().unwrap(),
            )),
            of: Some(Term::var("entity")),
            is: Some(Term::var("value3")),
            fact: None,
        };

        // Test 1: From trait with Vec<FactSelector>
        let when1: When = vec![selector1.clone(), selector2.clone()].into();
        assert_eq!(when1.len(), 2);

        // Test 2: From trait with array of FactSelectors
        let when2: When = [selector1.clone(), selector2.clone()].into();
        assert_eq!(when2.len(), 2);

        // Test 3: when! macro
        let when3: When = when![selector1.clone(), selector2.clone(), selector3.clone()];
        assert_eq!(when3.len(), 3);

        // Test 4: & operator chaining
        let when4 = selector1.clone() & selector2.clone() & selector3.clone();
        assert_eq!(when4.len(), 3);

        // Test 5: Mixed & operations
        let when5 = when1 & selector3.clone();
        assert_eq!(when5.len(), 3);

        // Test 6: When & When
        let when6 = when2 & when3;
        assert_eq!(when6.len(), 5);

        // Test 7: Verify all statements are correct
        for statement in &when4 {
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
    fn test_generic_statements_array() {
        // Test the generic From<[T: Statements; N]> implementation
        // This demonstrates that we can mix different types that implement Statements

        let fact_selector = FactSelector {
            the: Some(Term::from(
                "test/attr1".parse::<crate::artifact::Attribute>().unwrap(),
            )),
            of: Some(Term::var("entity")),
            is: Some(Term::from(Value::String("value1".to_string()))),
            fact: None,
        };

        let statement = Statement::select(FactSelector {
            the: Some(Term::from(
                "test/attr2".parse::<crate::artifact::Attribute>().unwrap(),
            )),
            of: Some(Term::var("entity")),
            is: Some(Term::var("value2")),
            fact: None,
        });

        // This works because both FactSelector and Statement implement Statements
        // However, Rust requires that all array elements have the same type
        // So we need to convert one type to the other or use collections

        // Test 1: Array of FactSelectors
        let when1: When = [fact_selector.clone(), fact_selector.clone()].into();
        assert_eq!(when1.len(), 2);

        // Test 2: Array of Statements
        let when2: When = [statement.clone(), statement.clone()].into();
        assert_eq!(when2.len(), 2);

        // Test 3: Vec of FactSelectors using From trait
        let vec_selectors = vec![fact_selector.clone(), fact_selector.clone()];
        let when3: When = vec_selectors.into();
        assert_eq!(when3.len(), 2);

        // Verify all statements are properly converted
        for when_result in [when1, when2, when3] {
            for stmt in when_result {
                if let Statement::Select(ref selector) = stmt {
                    assert!(selector.the.is_some());
                    assert!(selector.of.is_some());
                    assert!(selector.is.is_some());
                } else {
                    panic!("Expected Statement::Select");
                }
            }
        }
    }

    // #[test]
    // fn test_direct_array_return_without_into() {
    //     // Test for the new IntoWhen trait that allows direct array returns

    //     #[derive(Debug, Clone)]
    //     struct TestRule;

    //     #[derive(Debug, Clone)]
    //     struct TestRuleAttributes;

    //     #[derive(Debug, Clone)]
    //     struct TestRuleAssert;

    //     #[derive(Debug, Clone)]
    //     struct TestRuleRetract;

    //     impl Concept for TestRule {
    //         type Match = TestRuleMatch;
    //         type Assert = TestRuleAssert;
    //         type Retract = TestRuleRetract;
    //         type Attributes = TestRuleAttributes;

    //         fn attributes() -> &'static [crate::attribute::Attribute<crate::artifact::Value>] {
    //             &[]
    //         }

    //         fn name() -> &'static str {
    //             "TestRule"
    //         }

    //         fn r#match<T: Into<Term<crate::artifact::Entity>>>(_this: T) -> Self::Attributes {
    //             TestRuleAttributes
    //         }
    //     }

    //     impl Rule for TestRule {
    //         fn when(_terms: Self::Match) -> When {
    //             let statement1 = Statement::select(crate::fact_selector::FactSelector {
    //                 the: Some(Term::from(
    //                     "test/attr1".parse::<crate::artifact::Attribute>().unwrap(),
    //                 )),
    //                 of: Some(Term::var("entity")),
    //                 is: Some(Term::from(Value::String("value1".to_string()))),
    //                 fact: None,
    //             });

    //             let statement2 = Statement::select(crate::fact_selector::FactSelector {
    //                 the: Some(Term::from(
    //                     "test/attr2".parse::<crate::artifact::Attribute>().unwrap(),
    //                 )),
    //                 of: Some(Term::var("entity")),
    //                 is: Some(Term::var("value2")),
    //                 fact: None,
    //             });

    //             // This is the key test: using When::from for clean syntax
    //             When::from(vec![statement1, statement2])
    //         }
    //     }

    //     #[derive(Debug, Clone)]
    //     struct TestRuleMatch {
    //         this: Term<crate::artifact::Entity>,
    //     }

    //     impl Statements for TestRuleMatch {
    //         type IntoIter = std::vec::IntoIter<Statement>;
    //         fn statements(self) -> Self::IntoIter {
    //             TestRule::when(self).into_iter()
    //         }
    //     }

    //     impl Premise for TestRuleMatch {
    //         type Plan = TestRuleMatchPlan;

    //         fn plan(&self, _scope: &VariableScope) -> QueryResult<Self::Plan> {
    //             Ok(TestRuleMatchPlan)
    //         }
    //     }

    //     #[derive(Debug, Clone)]
    //     struct TestRuleMatchPlan;

    //     impl crate::plan::Plan for TestRuleMatchPlan {}

    //     impl crate::plan::EvaluationPlan for TestRuleMatchPlan {
    //         fn cost(&self) -> f64 {
    //             1.0
    //         }

    //         fn evaluate<S, M>(
    //             &self,
    //             _context: crate::plan::EvaluationContext<S, M>,
    //         ) -> impl crate::Selection + '_
    //         where
    //             S: crate::artifact::ArtifactStore + Clone + Send + 'static,
    //             M: crate::Selection + 'static,
    //         {
    //             crate::selection::EmptySelection::new()
    //         }
    //     }

    //     // Test the rule
    //     let rule_match = TestRuleMatch {
    //         this: Term::var("test_entity"),
    //     };
    //     let when_result = TestRule::when(rule_match);

    //     // Verify it works correctly
    //     assert_eq!(when_result.len(), 2);

    //     // Verify the statements are correct
    //     for statement in &when_result {
    //         if let Statement::Select(ref selector) = statement {
    //             assert!(selector.the.is_some());
    //             assert!(selector.of.is_some());
    //             assert!(selector.is.is_some());
    //         } else {
    //             panic!("Expected FactSelector statement");
    //         }
    //     }
    // }
}
