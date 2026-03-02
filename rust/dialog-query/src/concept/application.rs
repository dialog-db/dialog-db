/// Adornment types for parameter binding pattern caching.
pub mod adornment;
/// Per-concept rule management with adornment-keyed plan caching.
pub mod rules;

pub use rules::ConceptRules;

use crate::attribute::AttributeDescriptor;
use crate::concept::descriptor::ConceptDescriptor;
use crate::error::InconsistencyError;
use crate::planner::Disjunction;
use crate::schema::CONCEPT_OVERHEAD;
use crate::selection::Answers;
use crate::selection::{Answer, Evidence};
use crate::{
    Cardinality, Environment, Parameter, Parameters, QueryError, Schema, Source, try_stream,
};
use std::fmt::Display;

/// Extract an Answer with parameter names from an Answer with user variable names
///
/// This maps values from user-specified variable names to internal parameter names
/// for scoped evaluation. All factors are copied with their original provenance.
fn extract_parameters(source: &Answer, terms: &Parameters) -> Result<Answer, InconsistencyError> {
    let mut answer = Answer::new();

    for (param_name, user_param) in terms.iter() {
        match user_param {
            Parameter::Variable { name: Some(_), .. } => {
                // For named variables, map from user variable to parameter variable
                let param = Parameter::var(param_name);
                if let Some(factors) = source.lookup(user_param) {
                    answer.merge(Evidence::Parameter {
                        term: &param,
                        value: &factors.content(),
                    })?;
                }
            }
            Parameter::Constant(value) => {
                // For constants, directly bind the parameter variable to the constant value
                let param = Parameter::var(param_name);
                answer.merge(Evidence::Parameter {
                    term: &param,
                    value,
                })?;
            }
            Parameter::Variable { name: None, .. } => {}
        }
    }

    Ok(answer)
}

/// Merge an Answer with parameter names back into an Answer with user variable names
///
/// This maps values from internal parameter names back to user-specified variable names
/// after evaluation. All factors are copied with their original provenance.
fn merge_parameters(
    base: &Answer,
    result: &Answer,
    terms: &Parameters,
) -> Result<Answer, InconsistencyError> {
    let mut merged = base.clone();

    // Map through parameters: for each parameter, if it exists in result,
    // merge it into base under the user variable name
    for (param_name, user_param) in terms.iter() {
        // Skip constants - they were input parameters, not results to merge back
        if matches!(user_param, Parameter::Constant(_)) {
            continue;
        }

        // Try to get the factors for the parameter name from result
        let param = Parameter::var(param_name);
        if let Some(factors) = result.lookup(&param) {
            // Merge all factors under the user's variable name, preserving provenance
            for factor in factors.evidence() {
                merged.assign(user_param, factor)?;
            }
        }
    }

    Ok(merged)
}

/// Represents an application of a concept with specific term bindings.
/// This is used when querying for entities that match a concept pattern.
/// Note: The name has a typo (should be ConceptQuery) but is kept for compatibility.
#[derive(Debug, Clone, PartialEq)]
pub struct ConceptQuery {
    /// The term bindings for this concept application.
    pub terms: Parameters,
    /// The concept predicate being applied.
    pub predicate: ConceptDescriptor,
}

impl ConceptQuery {
    /// Estimate the cost of this concept application given the current environment.
    /// A concept is essentially a join over N fact lookups (one per attribute).
    /// Each fact lookup has the form: (this, attribute_i, value_i).
    ///
    /// Cost model:
    /// - If "this" is bound: Sum of costs for each attribute lookup
    ///   - For both 2/3 and 3/3 constraint:
    ///     - Cardinality::One: SEGMENT_READ_COST (same lookup cost)
    ///     - Cardinality::Many: RANGE_SCAN_COST (still need to scan)
    ///
    /// - If "this" is unbound but any attribute value is bound:
    ///   - Prefer Cardinality::One attribute (nearly free - just returns `this`)
    ///   - Otherwise use Cardinality::Many (expensive - scan + lookups for each result)
    ///
    /// - If nothing is bound: Returns None (should be blocked)
    pub fn estimate(&self, env: &Environment) -> Option<usize> {
        // Check if "this" parameter is bound
        let this_bound = if let Some(this) = self.terms.get("this") {
            this.is_bound(env)
        } else {
            false
        };

        if this_bound {
            // Entity is known - each attribute is a lookup (the + of known)
            let mut total = CONCEPT_OVERHEAD; // Add overhead for potential rule evaluation
            for (name, attribute) in self.predicate.with().iter() {
                // Check if this attribute's value is also bound
                total += attribute.estimate(
                    true,
                    if let Some(param) = self.terms.get(name) {
                        param.is_bound(env)
                    } else {
                        false
                    },
                );
            }
            Some(total)
        } else {
            // Entity is not bound - categorize attributes to find best execution strategy
            let mut bound_one: Option<&AttributeDescriptor> = None;
            let mut bound_many: Option<&AttributeDescriptor> = None;
            let mut unbound_one: Option<&AttributeDescriptor> = None;
            let mut unbound_many: Option<&AttributeDescriptor> = None;

            for (name, attribute) in self.predicate.with().iter() {
                if let Some(param) = self.terms.get(name) {
                    if param.is_bound(env) {
                        match attribute.cardinality() {
                            Cardinality::One => {
                                bound_one = Some(attribute);
                                break; // Best case found, stop searching
                            }
                            Cardinality::Many if bound_many.is_none() => {
                                bound_many = Some(attribute);
                            }
                            _ => {}
                        }
                    } else {
                        // Term exists but not bound
                        match attribute.cardinality() {
                            Cardinality::One if unbound_one.is_none() => {
                                unbound_one = Some(attribute);
                            }
                            Cardinality::Many if unbound_many.is_none() => {
                                unbound_many = Some(attribute);
                            }
                            _ => {}
                        }
                    }
                } else {
                    // No term at all
                    match attribute.cardinality() {
                        Cardinality::One if unbound_one.is_none() => {
                            unbound_one = Some(attribute);
                        }
                        Cardinality::Many if unbound_many.is_none() => {
                            unbound_many = Some(attribute);
                        }
                        _ => {}
                    }
                }
            }

            // Determine initial scan strategy based on what we found
            // For lead attribute: of=false (finding entity), is=bound (value bound or not)
            let (lead, bound) = if let Some(attribute) = bound_one {
                // Best case: bound Cardinality::One - lookup returns `this` directly
                (attribute, true)
            } else if let Some(attribute) = bound_many {
                // Bound Cardinality::Many - scan with value constraint
                (attribute, true)
            } else if let Some(attribute) = unbound_one {
                // No bound attributes but have Cardinality::One - cheaper scan
                (attribute, false)
            } else if let Some(attribute) = unbound_many {
                // Worst case: use unbound Cardinality::Many
                (attribute, false)
            } else {
                unreachable!("concept without attributes is not possible")
            };

            // Start with initial cost including overhead for potential rule evaluation
            // of=false (finding entity), is=bound
            let mut total = CONCEPT_OVERHEAD + lead.estimate(false, bound);

            for (name, attribute) in self.predicate.with().iter() {
                if lead != attribute {
                    total += attribute.estimate(
                        true,
                        if let Some(param) = self.terms.get(name) {
                            param.is_bound(env)
                        } else {
                            false
                        },
                    );
                }
            }

            Some(total)
        }
    }

    /// Returns the parameters for this concept application
    pub fn parameters(&self) -> Parameters {
        self.terms.clone()
    }

    /// Returns the schema describing this concept's attributes and their types.
    pub fn schema(&self) -> Schema {
        self.predicate.schema()
    }

    /// Evaluates this concept application within the given context, producing
    /// a stream of answers.
    ///
    /// Rather than threading a scope through the entire evaluation pipeline,
    /// we derive the binding pattern (adornment) from the first answer and
    /// use it to obtain a specialized, cached execution plan. This is the
    /// key insight from magic set optimization applied locally: the adornment
    /// is computed at the point of use from what's actually bound, rather
    /// than carried globally through every evaluation step.
    pub fn evaluate<S: Source, M: Answers>(self, answers: M, source: &S) -> impl Answers {
        let app = self;
        let source = source.clone();

        try_stream! {
            let mut plan = None;

            for await each in answers {
                let input = each?;

                // Derive the binding pattern from the first answer and cache the
                // plan. All answers in the selection share the same binding pattern
                // (same variables bound), only the values differ.
                if plan.is_none() {
                    let rules = source.acquire(&app.predicate)?;
                    plan = Some(rules.plan(&app.terms, &input));
                }
                let plan = plan.as_ref().unwrap();

                // Extract answer with parameter names for scoped evaluation
                // Maps user variable names → internal parameter names
                let initial_answer = extract_parameters(&input, &app.terms)
                    .map_err(|e| QueryError::FactStore(e.to_string()))?;
                let single_answers = initial_answer.seed();

                // Merge results back, mapping parameter names → user variable names
                // All factors are copied with their original provenance
                for await result in Disjunction::clone(plan).evaluate(single_answers, &source) {
                    let result_answer = result?;
                    let merged = merge_parameters(&input, &result_answer, &app.terms)
                        .map_err(|e| QueryError::FactStore(e.to_string()))?;
                    yield merged;
                }
            }
        }
    }
}

impl Display for ConceptQuery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {{", self.predicate.this())?;
        for (name, term) in self.terms.iter() {
            write!(f, "{}: {},", name, term)?;
        }

        write!(f, "}}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::concept::descriptor::ConceptDescriptor;
    use crate::relation::query::RelationQuery;
    use crate::selection::Answer;
    use crate::the;
    use crate::{
        Association, AttributeDescriptor, Cardinality, DeductiveRule, Negation, Parameter,
        Parameters, Premise, Proposition, Session, Term, Type, Value,
    };

    // Note: Async tests are commented out due to Rust recursion limit issues in test compilation
    // with deeply nested async streams. The functionality is tested indirectly through integration
    // tests and the planning tests above verify the core logic.

    #[dialog_common::test]
    async fn it_executes_concept_query() -> anyhow::Result<()> {
        use dialog_artifacts::{Artifacts, Entity};
        use dialog_storage::MemoryStorageBackend;

        // Create a store and session
        let backend = MemoryStorageBackend::default();
        let store = Artifacts::anonymous(backend).await?;
        let mut session = Session::open(store);

        let alice = Entity::new()?;
        let bob = Entity::new()?;

        session
            .transact(vec![
                Association {
                    the: the!("person/name"),
                    of: alice.clone(),
                    is: Value::String("Alice".to_string()),
                },
                Association {
                    the: the!("person/age"),
                    of: alice.clone(),
                    is: Value::UnsignedInt(25),
                },
                Association {
                    the: the!("person/name"),
                    of: bob.clone(),
                    is: Value::String("Bob".to_string()),
                },
                Association {
                    the: the!("person/age"),
                    of: bob.clone(),
                    is: Value::UnsignedInt(30),
                },
            ])
            .await?;

        // Create a person concept
        let concept = ConceptDescriptor::from(vec![
            (
                "name",
                AttributeDescriptor::new(
                    the!("person/name"),
                    "",
                    Cardinality::One,
                    Some(Type::String),
                ),
            ),
            (
                "age",
                AttributeDescriptor::new(
                    the!("person/age"),
                    "",
                    Cardinality::One,
                    Some(Type::UnsignedInt),
                ),
            ),
        ]);

        let mut terms = Parameters::new();
        terms.insert("this".to_string(), Parameter::var("person"));
        terms.insert("name".to_string(), Parameter::var("name"));
        terms.insert("age".to_string(), Parameter::var("age"));

        let application = ConceptQuery {
            terms,
            predicate: concept,
        };

        // Execute the query
        let selection = futures_util::TryStreamExt::try_collect::<Vec<_>>(
            application.evaluate(Answer::new().seed(), &session),
        )
        .await?;

        // Should find both Alice and Bob with their name and age
        assert_eq!(selection.len(), 2, "Should find 2 people");

        let name_param = Parameter::var("name");
        let age_param = Parameter::var("age");

        let mut found_alice = false;
        let mut found_bob = false;

        for match_result in selection.iter() {
            let name = match_result.resolve(&name_param)?;
            let age = match_result.resolve(&age_param)?;

            match name {
                Value::String(n) if n == "Alice" => {
                    assert_eq!(age, Value::UnsignedInt(25), "Alice should be 25");
                    found_alice = true;
                }
                Value::String(n) if n == "Bob" => {
                    assert_eq!(age, Value::UnsignedInt(30), "Bob should be 30");
                    found_bob = true;
                }
                _ => panic!("Unexpected person: {:?}", name),
            }
        }

        assert!(found_alice, "Should find Alice");
        assert!(found_bob, "Should find Bob");

        Ok(())
    }

    #[dialog_common::test]
    async fn it_executes_query_with_bound_entity() -> anyhow::Result<()> {
        use dialog_artifacts::{Artifacts, Entity};
        use dialog_storage::MemoryStorageBackend;

        // Create a store and session
        let backend = MemoryStorageBackend::default();
        let store = Artifacts::anonymous(backend).await?;
        let mut session = Session::open(store);

        let alice = Entity::new()?;

        session
            .transact(vec![
                Association {
                    the: the!("person/name"),
                    of: alice.clone(),
                    is: Value::String("Alice".to_string()),
                },
                Association {
                    the: the!("person/age"),
                    of: alice.clone(),
                    is: Value::UnsignedInt(25),
                },
            ])
            .await?;

        // Create a person concept
        let concept = ConceptDescriptor::from(vec![
            (
                "name",
                AttributeDescriptor::new(
                    the!("person/name"),
                    "",
                    Cardinality::One,
                    Some(Type::String),
                ),
            ),
            (
                "age",
                AttributeDescriptor::new(
                    the!("person/age"),
                    "",
                    Cardinality::One,
                    Some(Type::UnsignedInt),
                ),
            ),
        ]);

        let mut terms = Parameters::new();
        terms.insert("this".to_string(), Parameter::var("person"));
        terms.insert("name".to_string(), Parameter::var("name"));
        terms.insert("age".to_string(), Parameter::var("age"));

        let application = ConceptQuery {
            terms,
            predicate: concept,
        };

        // Create evaluation context with bound entity in the answer
        let mut answer = Answer::new();
        let person_param = Parameter::var("person");
        answer.merge(Evidence::Parameter {
            term: &person_param,
            value: &Value::from(alice),
        })?;

        let answers = answer.seed();

        // Execute with bound entity via answer
        futures_util::TryStreamExt::try_collect::<Vec<_>>(application.evaluate(answers, &session))
            .await?;

        Ok(())
    }

    #[dialog_common::test]
    fn it_operates_on_concept_conclusion() {
        let concept = ConceptDescriptor::from(vec![
            (
                "name",
                AttributeDescriptor::new(
                    the!("person/name"),
                    "Person name",
                    Cardinality::One,
                    Some(Type::String),
                ),
            ),
            (
                "age",
                AttributeDescriptor::new(
                    the!("person/age"),
                    "Person age",
                    Cardinality::One,
                    Some(Type::UnsignedInt),
                ),
            ),
        ]);

        // Test that attributes are present
        let param_names: Vec<&str> = concept.with().keys().collect();
        assert!(param_names.contains(&"name"));
        assert!(param_names.contains(&"age"));
        assert!(!param_names.contains(&"height"));
        // "this" parameter is implied but not in attributes
    }

    #[dialog_common::test]
    fn it_creates_concept_descriptor() {
        let concept = ConceptDescriptor::from(vec![(
            "name".to_string(),
            AttributeDescriptor::new(
                the!("person/name"),
                "Person name",
                Cardinality::One,
                Some(Type::String),
            ),
        )]);

        // Operator is now a computed URI
        assert!(
            concept.this().to_string().starts_with("concept:"),
            "Operator should be a concept URI"
        );
        assert_eq!(concept.with().iter().count(), 1);
        assert!(concept.with().keys().any(|k| k == "name"));
    }

    #[dialog_common::test]
    fn it_analyzes_concept_application() {
        let concept = ConceptDescriptor::from(vec![
            (
                "name".to_string(),
                AttributeDescriptor::new(
                    the!("person/name"),
                    "Person name",
                    Cardinality::One,
                    Some(Type::String),
                ),
            ),
            (
                "age".to_string(),
                AttributeDescriptor::new(
                    the!("person/age"),
                    "Person age",
                    Cardinality::One,
                    Some(Type::UnsignedInt),
                ),
            ),
        ]);

        let mut terms = Parameters::new();
        terms.insert("name".to_string(), Parameter::var("person_name"));
        terms.insert("age".to_string(), Parameter::var("person_age"));

        let concept_app = ConceptQuery {
            terms,
            predicate: concept,
        };

        let cost = concept_app.estimate(&Environment::new());
        assert_eq!(cost, Some(2100));

        let schema = concept_app.schema();

        assert_eq!(schema.iter().count(), 3);
        assert!(schema.get("this").is_some());
        assert!(schema.get("name").is_some());
        assert!(schema.get("age").is_some());
    }

    #[dialog_common::test]
    fn it_extracts_deductive_rule_parameters() {
        use std::collections::HashSet;

        let predicate = ConceptDescriptor::from([
            (
                "name".to_string(),
                AttributeDescriptor::new(
                    the!("person/name"),
                    "Person name",
                    Cardinality::One,
                    Some(Type::String),
                ),
            ),
            (
                "age".to_string(),
                AttributeDescriptor::new(
                    the!("person/age"),
                    "Person age",
                    Cardinality::One,
                    Some(Type::UnsignedInt),
                ),
            ),
        ]);
        let rule = DeductiveRule::from(&predicate);

        let params: HashSet<&str> = rule.parameters().collect();
        assert!(params.contains("this"));
        assert!(params.contains("name"));
        assert!(params.contains("age"));
        assert_eq!(params.len(), 3);
    }

    #[dialog_common::test]
    fn it_constructs_premises() {
        let relation = RelationQuery::new(
            Term::Constant(the!("person/name")),
            Term::var("person"),
            Parameter::from("Alice".to_string()),
            Term::blank(),
            None,
        );

        let premise = Premise::from(relation);

        match premise {
            Premise::Assert(Proposition::Relation(_)) => {
                // Expected case - RelationQuery produces Relation premise
            }
            _ => panic!("Expected Relation application"),
        }
    }

    #[dialog_common::test]
    fn it_produces_expected_error_types() {
        use crate::QueryError;
        use crate::error::{AnalyzerError, PlanError};

        // Test AnalyzerError creation
        let predicate = ConceptDescriptor::from(vec![(
            "name",
            AttributeDescriptor::new(the!("test/name"), "", Cardinality::One, Some(Type::String)),
        )]);
        let rule = DeductiveRule::from(&predicate);

        let analyzer_error = AnalyzerError::UnusedParameter {
            rule: rule.clone(),
            parameter: "test_param".to_string(),
        };

        // Test conversion to PlanError
        let plan_error: PlanError = analyzer_error.into();
        match &plan_error {
            PlanError::UnusedParameter { rule: r, parameter } => {
                // Operator is now a computed URI
                assert!(
                    r.conclusion().this().to_string().starts_with("concept:"),
                    "Operator should be a concept URI"
                );
                assert_eq!(parameter, "test_param");
            }
            _ => panic!("Expected UnusedParameter variant"),
        }

        // Test conversion to QueryError
        let query_error: QueryError = plan_error.into();
        match query_error {
            QueryError::PlanningError { .. } => {
                // Expected
            }
            _ => panic!("Expected PlanningError variant"),
        }
    }

    #[dialog_common::test]
    fn it_handles_application_variants() {
        // Test Relation application
        let relation = RelationQuery::new(
            Term::Constant(the!("test/attr")),
            Term::blank(),
            Parameter::blank(),
            Term::blank(),
            None,
        );
        let app = Proposition::Relation(Box::new(relation));

        match app {
            Proposition::Relation(_) => {
                // Expected
            }
            _ => panic!("Expected Relation variant"),
        }

        // Test other variants exist
        let mut terms = Parameters::new();
        terms.insert("test".to_string(), Parameter::var("test_var"));
        let concept_app = Proposition::Concept(ConceptQuery {
            terms,
            predicate: ConceptDescriptor::from([(
                "name",
                AttributeDescriptor::new(
                    the!("test/name"),
                    "Test name",
                    Cardinality::One,
                    Some(Type::String),
                ),
            )]),
        });

        match concept_app {
            Proposition::Concept(_) => {
                // Expected
            }
            _ => panic!("Expected Realize variant"),
        }
    }

    #[dialog_common::test]
    fn it_constructs_negation() {
        let relation = RelationQuery::new(
            Term::Constant(the!("test/attr")),
            Term::blank(),
            Parameter::blank(),
            Term::blank(),
            None,
        );
        let app = Proposition::Relation(Box::new(relation));
        let negation = Negation(app);

        // Test that negation wraps the application
        match negation {
            Negation(Proposition::Relation(_)) => {
                // Expected
            }
            _ => panic!("Expected wrapped Relation application"),
        }
    }

    #[dialog_common::test]
    async fn it_respects_constant_entity_parameter() -> anyhow::Result<()> {
        use dialog_artifacts::{Artifacts, Entity};
        use dialog_storage::MemoryStorageBackend;

        let backend = MemoryStorageBackend::default();
        let store = Artifacts::anonymous(backend).await?;
        let mut session = Session::open(store);

        let alice = Entity::new()?;
        let bob = Entity::new()?;

        session
            .transact(vec![
                Association {
                    the: the!("person/name"),
                    of: alice.clone(),
                    is: Value::String("Alice".to_string()),
                },
                Association {
                    the: the!("person/name"),
                    of: bob.clone(),
                    is: Value::String("Bob".to_string()),
                },
            ])
            .await?;

        let concept = ConceptDescriptor::from(vec![(
            "name",
            AttributeDescriptor::new(
                the!("person/name"),
                "Person name",
                Cardinality::One,
                Some(Type::String),
            ),
        )]);

        // Query with constant entity - should only return Alice
        let mut terms = Parameters::new();
        terms.insert(
            "this".to_string(),
            Parameter::Constant(Value::Entity(alice.clone())),
        );
        terms.insert("name".to_string(), Parameter::var("name"));

        let app = ConceptQuery {
            terms,
            predicate: concept,
        };
        let selection = futures_util::TryStreamExt::try_collect::<Vec<_>>(
            app.evaluate(Answer::new().seed(), &session),
        )
        .await?;

        assert_eq!(
            selection.len(),
            1,
            "Should find only Alice, not both people"
        );
        assert_eq!(
            selection[0].resolve(&Parameter::var("name"))?,
            Value::String("Alice".to_string())
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn it_respects_constant_attribute_parameter() -> anyhow::Result<()> {
        use dialog_artifacts::{Artifacts, Entity};
        use dialog_storage::MemoryStorageBackend;

        let backend = MemoryStorageBackend::default();
        let store = Artifacts::anonymous(backend).await?;
        let mut session = Session::open(store);

        let alice = Entity::new()?;
        let bob = Entity::new()?;

        session
            .transact(vec![
                Association {
                    the: the!("person/name"),
                    of: alice.clone(),
                    is: Value::String("Alice".to_string()),
                },
                Association {
                    the: the!("person/age"),
                    of: alice.clone(),
                    is: Value::UnsignedInt(25),
                },
                Association {
                    the: the!("person/name"),
                    of: bob.clone(),
                    is: Value::String("Bob".to_string()),
                },
                Association {
                    the: the!("person/age"),
                    of: bob.clone(),
                    is: Value::UnsignedInt(30),
                },
            ])
            .await?;

        let concept = ConceptDescriptor::from(vec![
            (
                "name",
                AttributeDescriptor::new(
                    the!("person/name"),
                    "Person name",
                    Cardinality::One,
                    Some(Type::String),
                ),
            ),
            (
                "age",
                AttributeDescriptor::new(
                    the!("person/age"),
                    "Person age",
                    Cardinality::One,
                    Some(Type::UnsignedInt),
                ),
            ),
        ]);

        // Query with constant name value - should only return Bob
        let mut terms = Parameters::new();
        terms.insert("this".to_string(), Parameter::var("entity"));
        terms.insert("name".to_string(), Parameter::from("Bob".to_string()));
        terms.insert("age".to_string(), Parameter::var("age"));

        let app = ConceptQuery {
            terms,
            predicate: concept,
        };
        let selection = futures_util::TryStreamExt::try_collect::<Vec<_>>(
            app.evaluate(Answer::new().seed(), &session),
        )
        .await?;

        assert_eq!(selection.len(), 1, "Should find only Bob");
        assert_eq!(
            selection[0].resolve(&Parameter::var("entity"))?,
            Value::Entity(bob.clone())
        );
        assert_eq!(
            selection[0].resolve(&Parameter::var("age"))?,
            Value::UnsignedInt(30)
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn it_respects_multiple_constant_parameters() -> anyhow::Result<()> {
        use dialog_artifacts::{Artifacts, Entity};
        use dialog_storage::MemoryStorageBackend;

        let backend = MemoryStorageBackend::default();
        let store = Artifacts::anonymous(backend).await?;
        let mut session = Session::open(store);

        let alice = Entity::new()?;
        let bob = Entity::new()?;

        session
            .transact(vec![
                Association {
                    the: the!("person/name"),
                    of: alice.clone(),
                    is: Value::String("Alice".to_string()),
                },
                Association {
                    the: the!("person/age"),
                    of: alice.clone(),
                    is: Value::UnsignedInt(25),
                },
                Association {
                    the: the!("person/name"),
                    of: bob.clone(),
                    is: Value::String("Bob".to_string()),
                },
                Association {
                    the: the!("person/age"),
                    of: bob.clone(),
                    is: Value::UnsignedInt(30),
                },
            ])
            .await?;

        let concept = ConceptDescriptor::from(vec![
            (
                "name",
                AttributeDescriptor::new(
                    the!("person/name"),
                    "Person name",
                    Cardinality::One,
                    Some(Type::String),
                ),
            ),
            (
                "age",
                AttributeDescriptor::new(
                    the!("person/age"),
                    "Person age",
                    Cardinality::One,
                    Some(Type::UnsignedInt),
                ),
            ),
        ]);

        // Query with both name and age constants - should only match Alice
        let mut terms = Parameters::new();
        terms.insert("this".to_string(), Parameter::var("entity"));
        terms.insert("name".to_string(), Parameter::from("Alice".to_string()));
        terms.insert("age".to_string(), Parameter::from(25u32));

        let app = ConceptQuery {
            terms,
            predicate: concept,
        };
        let selection = futures_util::TryStreamExt::try_collect::<Vec<_>>(
            app.evaluate(Answer::new().seed(), &session),
        )
        .await?;

        assert_eq!(
            selection.len(),
            1,
            "Should find only Alice with exact name and age match"
        );
        assert_eq!(
            selection[0].resolve(&Parameter::var("entity"))?,
            Value::Entity(alice.clone())
        );

        Ok(())
    }
}
