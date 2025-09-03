//! Rule-based deduction system
//!
//! This module implements the core rule system for dialog-query, allowing
//! declarative specification of derived facts through logical rules.
//!
//! The design is based on the TypeScript implementation in @query/src/plan/rule.js
//! and follows the patterns described in the design document at notes/rules.md.

use crate::concept::Concept;
use crate::fact_selector::FactSelector;
use crate::statement::Statement;

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
/// ```rust
/// use dialog_query::{When, FactSelector, Term, Value};
///
/// // Example of creating When collections with different syntax options
/// fn demonstrate_when_creation() -> When {
///     let selector1 = FactSelector::<Value> {
///         the: Some(Term::from("example/field1".parse::<dialog_artifacts::Attribute>().unwrap())),
///         of: Some(Term::var("entity")),
///         is: Some(Term::var("value1")),
///         fact: None,
///     };
///
///     let selector2 = FactSelector::<Value> {
///         the: Some(Term::from("example/field2".parse::<dialog_artifacts::Attribute>().unwrap())),
///         of: Some(Term::var("entity")),
///         is: Some(Term::var("value2")),
///         fact: None,
///     };
///
///     // Multiple syntax options for creating When:
///
///     // Option 1: Array syntax with From trait - clean and direct
///     let when1: When = [selector1.clone(), selector2.clone()].into();
///
///     // Option 2: Vec syntax
///     let when2: When = vec![selector1.clone(), selector2.clone()].into();
///
///     // Option 3: Operator chaining - reads like logical AND
///     let when3 = selector1.clone() & selector2.clone();
///
///     // All approaches create equivalent When collections
///     assert_eq!(when1.len(), 2);
///     assert_eq!(when2.len(), 2);
///     assert_eq!(when3.len(), 2);
///
///     when1
/// }
///
/// // For generated Rule structs, use the derive macro and Attributes::of pattern:
/// // #[derive(Rule, Debug, Clone)]
/// // struct Person { name: String, age: u32 }
/// // let query = PersonAttributes::of(Term::var("entity"));
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifact::Value;
    use crate::premise::Premise;
    use crate::statement::Statement;
    use crate::syntax::VariableScope;
    use crate::term::Term;

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
        match plan {
            Ok(_) => {},
            Err(_) => panic!("Expected ready plan"),
        }
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
        let Statement::Select(ref selector) = when_result[0];

        assert!(selector.the.is_some());
        assert!(selector.of.is_some());
        assert!(selector.is.is_some());
    }

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
            let Statement::Select(ref selector) = statement;
            assert!(selector.the.is_some());
            assert!(selector.of.is_some());
            assert!(selector.is.is_some());
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
                let Statement::Select(ref selector) = stmt;
                assert!(selector.the.is_some());
                assert!(selector.of.is_some());
                assert!(selector.is.is_some());
            }
        }
    }
}
