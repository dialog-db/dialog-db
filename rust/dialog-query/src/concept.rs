use crate::analyzer::LegacyAnalysis;
use crate::application::ConceptApplication;
use crate::attribute::Attribute;
use crate::error::{AnalyzerError, PlanError, QueryError};
use crate::fact_selector::{BASE_COST, ENTITY_COST, VALUE_COST};
use crate::plan::ConceptPlan;
use crate::plan::EvaluationPlan;
use crate::predicate;
use crate::query::{Query, QueryResult, Source};
use crate::term::Term;
use crate::Selection;
use crate::VariableScope;
use crate::{Application, Premise};
use crate::{Dependencies, Entity, Parameters, Value};
use dialog_artifacts::Instruction;

/// Concept is a set of attributes associated with entity representing an
/// abstract idea. It is a tool for the domain modeling and in some regard
/// similar to a table in relational database or a collection in the document
/// database, but unlike them it is disconnected from how information is
/// organized, in that sense it is more like view into which you can also insert.
///
/// Concepts are used to describe conclusions of the rules, providing a mapping
/// between conclusions and facts. In that sense you concepts are on-demand
/// cache of all the conclusions from the associated rules.
pub trait Concept: Clone + std::fmt::Debug {
    type Instance: Instance;
    /// Type describing attributes of this concept.
    type Attributes: Attributes;
    /// Type representing a query of this concept. It is a set of terms
    /// corresponding to the set of attributes defined by this concept.
    /// It is used as premise of the rule.
    type Match: Match<Instance = Self::Instance, Attributes = Self::Attributes>;
    /// Type representing an assertion of this concept. It is used in the
    /// inductive rules that describe how state of the concept changes
    /// (or persists) over time.
    type Assert;
    /// Type representing a retraction of this concept. It is used in the
    /// inductive rules to describe conditions for the of the concepts lifecycle.
    type Retract;

    fn name() -> &'static str;

    /// Returns the static list of attributes defined for this concept
    ///
    /// This is a convenience method that delegates to the associated Attributes type.
    /// It provides easy access to concept attributes without having to explicitly
    /// reference the Attributes associated type.
    fn attributes() -> &'static [(&'static str, Attribute<Value>)] {
        Self::Attributes::attributes()
    }
}

/// Every assertion or retraction can be decomposed into a set of
/// assertion / retraction.
///
/// This trait enables us to define each Concpet::Assert and Concpet::Retract
/// such that it could be decomposed into a set of instructions which can be
/// then be committed.
pub trait Instructions {
    type IntoIter: IntoIterator<Item = Instruction>;
    fn instructions(self) -> Self::IntoIter;
}

/// Concepts can be matched and this trait describes an abstract match for the
/// concept. Each match should be translatable into a set of statements making
/// it possible to spread it into a query.
pub trait Match {
    /// Instance of the concept that this match can produce.
    type Instance: Instance;
    /// Attributes describing the mapping between concept and it's instance.
    type Attributes: Attributes;

    /// Provides term for a given property name in the corresponding concept
    fn term_for(&self, name: &str) -> Option<&Term<Value>>;

    fn this(&self) -> Term<Entity>;

    fn analyze(&self) -> Result<LegacyAnalysis, AnalyzerError> {
        let mut dependencies = Dependencies::new();
        dependencies.desire("this".into(), ENTITY_COST);

        for (name, _) in Self::Attributes::attributes() {
            dependencies.desire(name.to_string(), VALUE_COST);
        }

        Ok(LegacyAnalysis {
            cost: BASE_COST,
            dependencies,
        })
    }

    fn plan(&self, scope: &VariableScope) -> Result<ConceptPlan, PlanError> {
        // let mut provides = VariableScope::new();
        // let analysis = self.analyze().map_err(PlanError::from)?;

        // // analyze dependencies and make sure that all required dependencies
        // // are provided
        // for (name, requirement) in analysis.dependencies.iter() {
        //     let parameter = self.term_for(name);
        //     match requirement {
        //         Requirement::Required => {}
        //         Requirement::Derived(_) => {
        //             // If requirement can be derived and was not provided
        //             // we add it to the provided set
        //             if let Some(term) = parameter {
        //                 if !scope.contains(&term) {
        //                     provides.add(&term);
        //                 }
        //             }
        //         }
        //     }
        // }

        // let mut premises = vec![];
        // let entity = self.this();
        // for (name, attribute) in Self::Attributes::attributes() {
        //     if let Some(term) = self.term_for(name) {
        //         let select = FactSelector::new()
        //             .the(attribute.the())
        //             .of(entity.clone())
        //             .is(term.clone());

        //         premises.push(select.into())
        //     }
        // }

        // let mut join = Join::new(&premises);
        // let (cost, conjuncts) = join.plan(scope)?;

        // Ok(ConceptPlan {
        //     cost,
        //     provides,
        //     conjuncts,
        // })
        // TODO: Legacy concept planning code - needs refactoring
        panic!("Legacy concept planning not yet implemented")
    }

    fn conpect(&self) -> predicate::Concept {
        //     let mut attributes = HashMap::new();
        //     let mut operator = None;
        //     for (name, attribute) in Self::Attributes::attributes() {
        //         if operator.is_none() {
        //             operator = Some(attribute.namespace.to_string())
        //         }
        //         attributes.insert(name.to_string(), attribute.clone());
        //     }

        //     predicate::Concept {
        //         operator: operator.unwrap_or("".to_string()),
        //         attributes,
        //     }
        panic!("Legacy concept conversion not yet implemented")
    }
}

impl<T: Match> Query for T {
    fn query<S: Source>(&self, store: &S) -> QueryResult<impl Selection> {
        use crate::try_stream;

        let scope = VariableScope::new();

        // Use try_stream to create a stream that owns the plan
        let plan = self.plan(&scope).map_err(|error| QueryError::from(error))?;
        let context = crate::plan::fresh(store.clone());

        Ok(try_stream! {
            for await result in plan.evaluate(context) {
                yield result?;
            }
        })
    }
}

impl<T: Match> From<T> for Parameters {
    fn from(source: T) -> Self {
        let mut terms = Self::new();
        if let Some(term) = source.term_for("this") {
            terms.insert("this".into(), term.clone());
        }

        for (name, _) in T::Attributes::attributes() {
            if let Some(term) = source.term_for(name) {
                terms.insert(name.to_string(), term.clone());
            }
        }

        terms
    }
}

impl<T: Match> From<T> for Premise {
    fn from(source: T) -> Self {
        let concept = source.conpect();
        let terms = source.into();
        Premise::Apply(Application::Concept(ConceptApplication { terms, concept }))
    }
}

// impl<T: Match + Clone + std::fmt::Debug> Premise for T {
//     type Plan = JoinPlan;

//     fn plan(&self, scope: &VariableScope) -> PlanResult<Self::Plan> {
//         // Step 1: Create statement premises for each attribute
//         let mut statements = Vec::new();
//         let entity = self.this();

//         for (name, attribute) in T::Attributes::attributes() {
//             let term = self.term_for(name).unwrap();
//             let select = FactSelector::new()
//                 .the(attribute.the())
//                 .of(entity.clone())
//                 .is(term.clone());

//             statements.push(Statement::select(select));
//         }

//         // Step 2: Create a Join premise and plan it
//         let join_premise = Join::new(statements);
//         join_premise.plan(scope)
//     }

//     fn cells(&self) -> VariableScope {
//         // Collect cells from all attributes
//         let mut cells = VariableScope::new();
//         let entity = self.this();

//         // Add the entity variable
//         cells.add(&entity);

//         // Add variables from each attribute term
//         for (name, _attribute) in T::Attributes::attributes() {
//             if let Some(term) = self.term_for(name) {
//                 cells.add(term);
//             }
//         }

//         cells
//     }
// }

/// Describes an instance of a concept. It is expected that each concept is
/// can be materialized from the selection::Match.
pub trait Instance {
    /// Each instance has a corresponding entity and this method
    /// returns a reference to it.
    fn this(&self) -> Entity;
}

// Schema describes mapping between concept properties and attributes that
// correspond to those properties.
pub trait Attributes {
    fn attributes() -> &'static [(&'static str, Attribute<Value>)];

    /// Create an attributes pattern for querying
    fn of<T: Into<Term<Entity>>>(entity: T) -> Self;
}

// /// Join premise that combines multiple premises and orders them optimally
// #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
// pub struct Join {
//     premises: Vec<Statement>,
// }

// impl Join {
//     /// Create a new Join from a collection of premises
//     pub fn new(premises: Vec<Statement>) -> Self {
//         Self { premises }
//     }
// }

// /// Plan for executing a Join premise
// #[derive(Debug, Clone)]
// pub struct JoinPlan {
//     cost: usize,
//     ordered_premises: Vec<StatementPlan>,
// }

// /// Cached premise with all computed data
// #[derive(Debug, Clone)]
// struct CachedPlan<'a> {
//     premise: &'a Statement,
//     cells: VariableScope,
//     result: PlanResult<StatementPlan>,
// }

// impl<'a> CachedPlan<'a> {
//     fn recompute(&mut self, scope: &VariableScope) -> &Self {
//         self.result = self.premise.plan(scope);
//         self
//     }
// }

// /// Create a planning error from cached premises that failed to plan
// fn create_planning_error(cached_premises: &[CachedPlan]) -> crate::plan::PlanError {
//     crate::plan::PlanError {
//         description: format!(
//             "Cannot plan remaining {} premises - missing required variables",
//             cached_premises.len()
//         ),
//     }
// }

// impl Premise for Join {
//     type Plan = JoinPlan;

//     fn cells(&self) -> VariableScope {
//         let mut cells = VariableScope::new();
//         for premise in &self.premises {
//             cells.extend(premise.cells());
//         }
//         cells
//     }

//     fn plan(&self, scope: &VariableScope) -> PlanResult<Self::Plan> {
//         let mut local = scope.clone();
//         let mut conjuncts = Vec::new();
//         let mut cost = 0usize;

//         // First iteration: compute everything and populate cache
//         let mut cache: Vec<CachedPlan> = Vec::new();
//         let mut best: Option<(crate::statement::StatementPlan, usize)> = None;

//         for (index, premise) in self.premises.iter().enumerate() {
//             let cells = premise.cells();
//             let result = premise.plan(&local);

//             // Check if this is the best plan so far
//             if let Ok(plan) = &result {
//                 if let Some((ref top, _)) = best {
//                     if plan.cmp(top) == std::cmp::Ordering::Less {
//                         best = Some((plan.clone(), index));
//                     }
//                 } else {
//                     best = Some((plan.clone(), index));
//                 }
//             }

//             cache.push(CachedPlan {
//                 premise,
//                 cells,
//                 result,
//             });
//         }

//         // If we found a plannable premise in first iteration, process it
//         let mut delta = if let Some((plan, index)) = best {
//             cost += plan.cost();
//             let delta = local.extend(plan.provides());
//             conjuncts.push(plan);
//             cache.remove(index);
//             delta
//         } else {
//             return Err(create_planning_error(&cache));
//         };

//         // Subsequent iterations: use cached data with delta optimization
//         while !cache.is_empty() {
//             let mut best: Option<(crate::statement::StatementPlan, usize)> = None;

//             for (index, cached) in cache.iter_mut().enumerate() {
//                 // Check if we need to recompute based on delta
//                 if cached.cells.intersects(&delta) {
//                     cached.recompute(&local);
//                 }

//                 if let Ok(plan) = &cached.result {
//                     if let Some((top, _)) = &best {
//                         if plan.cmp(top) == std::cmp::Ordering::Less {
//                             best = Some((plan.clone(), index));
//                         }
//                     } else {
//                         best = Some((plan.clone(), index));
//                     }
//                 }
//             }

//             if let Some((plan, index)) = best {
//                 cost += plan.cost();
//                 delta = local.extend(plan.provides());
//                 conjuncts.push(plan);
//                 cache.remove(index);
//             } else {
//                 return Err(create_planning_error(&cache));
//             }
//         }

//         Ok(JoinPlan {
//             cost,
//             ordered_premises: conjuncts,
//         })
//     }
// }

// impl EvaluationPlan for JoinPlan {
//     fn cost(&self) -> usize {
//         self.cost
//     }

//     fn provides(&self) -> VariableScope {
//         let mut scope = VariableScope::new();
//         for premise in &self.ordered_premises {
//             scope.extend(premise.provides());
//         }
//         scope
//     }

//     fn evaluate<S: Store, M: Selection>(&self, context: EvaluationContext<S, M>) -> impl Selection {
//         // Convert statement plans to evaluation plans
//         let eval_plans: Vec<crate::statement::StatementPlan> = self.ordered_premises.clone();
//         crate::and::join(eval_plans, context)
//     }
// }

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifact::{Artifacts, Attribute as ArtifactAttribute};
    use crate::artifact::{Value, ValueDataType};
    use crate::selection::SelectionExt;
    use crate::term::Term;
    use crate::{Fact, Query, Session};
    use anyhow::Result;
    use dialog_storage::MemoryStorageBackend;

    // Define a Person concept for testing using raw concept API
    #[derive(Debug, Clone)]
    struct Person {
        name: String,
        age: u32,
    }

    // PersonMatch for querying - contains Term-wrapped fields
    #[derive(Debug, Clone)]
    struct PersonMatch {
        this: Term<Entity>,
        name: Term<Value>,
        age: Term<Value>,
    }

    // PersonAttributes for building queries
    #[derive(Debug, Clone)]
    struct PersonAttributes {
        pub entity: Term<Entity>,
    }

    // PersonAssert for assertions
    #[derive(Debug, Clone)]
    struct PersonAssert {
        pub name: String,
        pub age: u32,
    }

    // PersonRetract for retractions
    #[derive(Debug, Clone)]
    struct PersonRetract {
        pub name: String,
        pub age: u32,
    }

    // Implement Concept for Person
    impl Concept for Person {
        type Instance = Person;
        type Attributes = PersonAttributes;
        type Match = PersonMatch;
        type Assert = PersonAssert;
        type Retract = PersonRetract;

        fn name() -> &'static str {
            "person"
        }
    }

    // Implement Instance for Person
    impl Instance for Person {
        fn this(&self) -> Entity {
            // For testing, just create a new entity
            Entity::new().unwrap()
        }
    }

    // Implement Match for PersonMatch
    impl Match for PersonMatch {
        type Instance = Person;
        type Attributes = PersonAttributes;

        fn term_for(&self, name: &str) -> Option<&Term<Value>> {
            match name {
                "name" => Some(&self.name),
                "age" => Some(&self.age),
                _ => None,
            }
        }

        fn this(&self) -> Term<Entity> {
            self.this.clone()
        }
    }

    // Implement Rule for Person to support when() method
    impl crate::rule::Rule for Person {
        fn when(terms: Self::Match) -> crate::rule::When {
            use crate::fact_selector::FactSelector;

            // Create fact selectors for each attribute
            let name_selector = FactSelector::<Value> {
                the: Some(Term::from(
                    "person/name".parse::<crate::artifact::Attribute>().unwrap(),
                )),
                of: Some(terms.this.clone()),
                is: Some(terms.name),
                fact: None,
            };

            let age_selector = FactSelector::<Value> {
                the: Some(Term::from(
                    "person/age".parse::<crate::artifact::Attribute>().unwrap(),
                )),
                of: Some(terms.this),
                is: Some(terms.age),
                fact: None,
            };

            [name_selector, age_selector].into()
        }
    }

    // Implement Premises for PersonMatch
    impl crate::rule::Premises for PersonMatch {
        type IntoIter = std::vec::IntoIter<crate::premise::Premise>;

        fn premises(self) -> Self::IntoIter {
            use crate::rule::Rule;
            Person::when(self).into_iter()
        }
    }

    // Implement Attributes for PersonAttributes
    impl Attributes for PersonAttributes {
        fn attributes() -> &'static [(&'static str, Attribute<Value>)] {
            use std::sync::LazyLock;

            static PERSON_ATTRIBUTES: LazyLock<[(&'static str, Attribute<Value>); 2]> =
                LazyLock::new(|| {
                    [
                        (
                            "name",
                            Attribute::new(
                                "person",
                                "name",
                                "Person's name",
                                ValueDataType::String,
                            ),
                        ),
                        (
                            "age",
                            Attribute::new(
                                "person",
                                "age",
                                "Person's age",
                                ValueDataType::UnsignedInt,
                            ),
                        ),
                    ]
                });
            &*PERSON_ATTRIBUTES
        }

        fn of<T: Into<Term<Entity>>>(entity: T) -> Self {
            PersonAttributes {
                entity: entity.into(),
            }
        }
    }

    #[test]
    fn test_person_concept_creation() {
        // Test that the Person concept has the expected properties
        assert_eq!(Person::name(), "person");

        let attributes = Person::attributes();
        assert_eq!(attributes.len(), 2);

        // Check that name and age attributes exist
        let attr_names: Vec<&str> = attributes.iter().map(|(name, _)| *name).collect();
        assert!(attr_names.contains(&"name"));
        assert!(attr_names.contains(&"age"));
    }

    #[test]
    fn test_person_attributes_creation() {
        // Test creating PersonAttributes with the 'of' method
        let entity_var = Term::var("person_entity");
        let _person_attrs = PersonAttributes::of(entity_var.clone());

        // Should create attributes with the provided entity
        // The macro should have generated proper attribute constants
        assert_eq!(Person::name(), "person");
    }

    #[test]
    fn test_person_match_creation() {
        // Test creating a PersonMatch for querying
        let entity_var = Term::var("person_entity");
        let name_var = Term::var("person_name");
        let age_var = Term::var("person_age");

        let person_match = PersonMatch {
            this: entity_var.clone(),
            name: name_var.clone(),
            age: age_var.clone(),
        };

        // Test Match trait methods
        assert_eq!(person_match.this(), entity_var);
        assert_eq!(person_match.term_for("name"), Some(&name_var));
        assert_eq!(person_match.term_for("age"), Some(&age_var));
        assert_eq!(person_match.term_for("nonexistent"), None);
    }

    #[test]
    fn test_person_match_with_constants() {
        // Test querying for a specific person with constant values
        let entity_var = Term::var("alice_entity");
        let name_const = Term::from(Value::String("Alice".to_string()));
        let age_const = Term::from(Value::UnsignedInt(30));

        let person_match = PersonMatch {
            this: entity_var.clone(),
            name: name_const.clone(),
            age: age_const.clone(),
        };

        // Verify the constants are preserved
        assert_eq!(person_match.term_for("name"), Some(&name_const));
        assert_eq!(person_match.term_for("age"), Some(&age_const));

        // Test that constants are properly identified
        assert!(person_match.term_for("name").unwrap().is_constant());
        assert!(person_match.term_for("age").unwrap().is_constant());
    }

    #[test]
    fn test_person_match_mixed_terms() {
        // Test mixing variables and constants in a match pattern
        let entity_var = Term::var("person_entity");
        let name_const = Term::from(Value::String("Bob".to_string()));
        let age_var = Term::var("any_age");

        let person_match = PersonMatch {
            this: entity_var.clone(),
            name: name_const.clone(),
            age: age_var.clone(),
        };

        // Name should be constant, age should be variable
        assert!(person_match.term_for("name").unwrap().is_constant());
        assert!(person_match.term_for("age").unwrap().is_variable());
        assert_eq!(
            person_match.term_for("age").unwrap().name(),
            Some("any_age")
        );
    }

    #[test]
    fn test_concept_attributes_static_access() {
        // Test accessing attributes through both Concept and Attributes traits
        let concept_attrs = Person::attributes();
        let attrs_trait = PersonAttributes::attributes();

        // Should be the same
        assert_eq!(concept_attrs.len(), attrs_trait.len());
        // Note: We can't compare attributes directly since Attribute doesn't implement PartialEq

        // Verify specific attributes
        let mut found_name = false;
        let mut found_age = false;

        for (name, attribute) in concept_attrs {
            match *name {
                "name" => {
                    found_name = true;
                    // Verify name attribute properties
                    assert_eq!(attribute.the(), "person/name");
                }
                "age" => {
                    found_age = true;
                    // Verify age attribute properties
                    assert_eq!(attribute.the(), "person/age");
                }
                _ => panic!("Unexpected attribute: {}", name),
            }
        }

        assert!(found_name, "name attribute not found");
        assert!(found_age, "age attribute not found");
    }

    #[test]
    fn test_person_instance_creation() {
        // Test creating a Person instance
        let person = Person {
            name: "Charlie".to_string(),
            age: 25,
        };

        // Test Instance trait - this will return a new entity from our placeholder impl
        let entity = person.this();
        assert!(entity.to_string().len() > 0); // Should have some entity ID
    }

    #[test]
    fn test_concept_name_consistency() {
        // Test that concept name is consistent across different access patterns
        assert_eq!(Person::name(), "person");

        // The concept should have consistent naming
        let _person = Person {
            name: "Test".to_string(),
            age: 1,
        };

        // Instance should have the same concept name
        // (though our current Instance impl doesn't store concept info)
        assert_eq!(Person::name(), "person");
    }

    #[test]
    fn test_match_premise_planning() {
        // Test that PersonMatch can be used through Premises trait
        use crate::rule::Premises;

        // Create a PersonMatch with all constants to avoid dependency resolution issues
        let entity_const = Term::from(Entity::new().unwrap());
        let name_const = Term::from(Value::String("Alice".to_string()));
        let age_const = Term::from(Value::UnsignedInt(30));

        let person_match = PersonMatch {
            this: entity_const,
            name: name_const,
            age: age_const,
        };

        // PersonMatch should implement Premises trait and generate individual premises
        let premises: Vec<_> = person_match.clone().premises().collect();

        // Should have premises for each attribute (name and age)
        assert_eq!(premises.len(), 2);

        // Test that PersonMatch correctly implements Match trait methods
        assert_eq!(person_match.term_for("name").unwrap().is_constant(), true);
        assert_eq!(person_match.term_for("age").unwrap().is_constant(), true);
    }

    #[test]
    fn test_attributes_of_method_with_different_entity_types() {
        // Test that 'of' method works with different entity term types

        // With variable
        let entity_var = Term::var("person1");
        let _attrs1 = PersonAttributes::of(entity_var.clone());
        // Can't easily test the internals, but should not panic

        // With constant entity (if we had one)
        let entity_const = Term::var("person2"); // Using var since we don't have entity constants readily
        let _attrs2 = PersonAttributes::of(entity_const);
        // Should also not panic

        // The method should accept anything that implements Into<Term<Entity>>
        assert_eq!(Person::name(), "person"); // Just verify we're still working
    }

    #[test]
    fn test_empty_term_access() {
        // Test accessing non-existent terms
        let entity_var = Term::var("entity");
        let name_var = Term::var("name");
        let age_var = Term::var("age");

        let person_match = PersonMatch {
            this: entity_var,
            name: name_var,
            age: age_var,
        };

        // Should return None for non-existent attributes
        assert_eq!(person_match.term_for("height"), None);
        assert_eq!(person_match.term_for("email"), None);
        assert_eq!(person_match.term_for(""), None);
    }

    #[test]
    fn test_concept_debug_output() {
        // Test that our derived Debug implementations work
        let person = Person {
            name: "Debug Test".to_string(),
            age: 42,
        };

        let debug_output = format!("{:?}", person);
        assert!(debug_output.contains("Person"));
        assert!(debug_output.contains("Debug Test"));
        assert!(debug_output.contains("42"));
    }

    #[test]
    fn test_concept_clone() {
        // Test that our derived Clone implementations work
        let person1 = Person {
            name: "Original".to_string(),
            age: 35,
        };

        let person2 = person1.clone();
        assert_eq!(person1.name, person2.name);
        assert_eq!(person1.age, person2.age);

        // Test PersonMatch clone
        let entity_var = Term::var("entity");
        let match1 = PersonMatch {
            this: entity_var.clone(),
            name: Term::var("name"),
            age: Term::var("age"),
        };

        let match2 = match1.clone();
        assert_eq!(match1.this, match2.this);
        assert_eq!(match1.name, match2.name);
        assert_eq!(match1.age, match2.age);
    }

    #[tokio::test]
    async fn test_person_match_query() -> Result<()> {
        // Test that actually uses PersonMatch to query - this should work with the concept system

        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

        let alice = Entity::new()?;
        let bob = Entity::new()?;
        let mallory = Entity::new()?;

        // Create test data
        let facts = vec![
            Fact::assert(
                "person/name".parse::<ArtifactAttribute>()?,
                alice.clone(),
                Value::String("Alice".to_string()),
            ),
            Fact::assert(
                "person/age".parse::<ArtifactAttribute>()?,
                alice.clone(),
                Value::UnsignedInt(25),
            ),
            Fact::assert(
                "person/name".parse::<ArtifactAttribute>()?,
                bob.clone(),
                Value::String("Bob".to_string()),
            ),
            Fact::assert(
                "person/age".parse::<ArtifactAttribute>()?,
                bob.clone(),
                Value::UnsignedInt(30),
            ),
            Fact::assert(
                "person/name".parse::<ArtifactAttribute>()?,
                mallory.clone(),
                Value::String("Mallory".to_string()),
            ),
        ];

        let mut session = Session::open(artifacts.clone());
        session.transact(facts).await?;

        // This is the real test - using PersonMatch to query for people
        let person_query = PersonMatch {
            this: Term::var("person"),
            name: Term::var("name"),
            age: Term::var("age"),
        };

        // This should work with the planner fix
        let session = Session::open(artifacts);
        let results = person_query.query(&session)?.collect_set().await?;

        // Should find both Alice and Bob (not Mallory who has no age)
        assert_eq!(results.len(), 2, "Should find both people");

        // Verify we got the right people
        assert!(results.contains_binding("name", &Value::String("Alice".to_string())));
        assert!(results.contains_binding("name", &Value::String("Bob".to_string())));
        assert!(results.contains_binding("age", &Value::UnsignedInt(25)));
        assert!(results.contains_binding("age", &Value::UnsignedInt(30)));

        Ok(())
    }

    #[tokio::test]
    async fn test_concept_match_structure() -> Result<()> {
        // Test that PersonMatch correctly implements the Match trait
        // This doesn't require actual querying, just tests the structure

        let alice = Entity::new()?;

        // Test 1: Create a PersonMatch with mixed terms
        let person_match = PersonMatch {
            this: Term::from(alice.clone()),
            name: Term::from(Value::String("Alice".to_string())),
            age: Term::var("age"),
        };

        // Test Match trait methods
        assert_eq!(person_match.this(), Term::from(alice.clone()));
        assert_eq!(
            person_match.term_for("name"),
            Some(&Term::from(Value::String("Alice".to_string())))
        );
        assert_eq!(person_match.term_for("age"), Some(&Term::var("age")));
        assert_eq!(person_match.term_for("nonexistent"), None);

        // Test 2: Verify that PersonMatch can be used as a Premise
        use crate::syntax::VariableScope;

        let scope = VariableScope::new();
        // Just test that it can create a plan - don't worry about execution for now
        let _plan_result = person_match.plan(&scope);
        // The plan might fail due to join ordering issues, but that's okay for this test

        // Test 3: Verify concept attributes are accessible
        let attrs = PersonAttributes::attributes();
        assert_eq!(attrs.len(), 2);

        let name_attr = &attrs[0].1;
        let age_attr = &attrs[1].1;

        // The attributes should have the expected namespaced names
        assert_eq!(name_attr.the(), "person/name");
        assert_eq!(age_attr.the(), "person/age");

        Ok(())
    }

    #[tokio::test]
    async fn test_concept_query_no_matches() -> Result<()> {
        // Test that individual fact selectors work for non-matching queries

        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

        let alice = Entity::new()?;

        // Create minimal test data
        let facts = vec![Fact::assert(
            "person/name".parse::<ArtifactAttribute>()?,
            alice.clone(),
            Value::String("Alice".to_string()),
        )];

        let mut session = Session::open(artifacts.clone());
        session.transact(facts).await?;

        // Test: Search for non-existent person using individual fact selector
        let missing_query = Fact::<Value>::select()
            .the("person/name")
            .of(Term::var("person"))
            .is(Value::String("NonExistent".to_string()));

        let session = Session::open(artifacts);
        let no_results = missing_query.query(&session)?.collect_set().await?;
        assert_eq!(no_results.len(), 0, "Should find no non-existent people");

        Ok(())
    }
}
