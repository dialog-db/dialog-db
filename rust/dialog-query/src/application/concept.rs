use super::fact::CONCEPT_OVERHEAD;
use crate::DeductiveRule;
use crate::attribute::AttributeSchema;
use crate::context::new_context;
use crate::planner::{Fork, Join};
use crate::predicate::ConceptDescriptor;
use crate::selection::{Answer, Evidence};
use crate::{Environment, EvaluationContext, Parameters, Schema, Source, Term, Value, try_stream};
use std::fmt::Display;

/// Extract an Answer with parameter names from an Answer with user variable names
///
/// This maps values from user-specified variable names to internal parameter names
/// for scoped evaluation. All factors are copied with their original provenance.
fn extract_parameters(
    source: &Answer,
    terms: &Parameters,
) -> Result<Answer, crate::InconsistencyError> {
    let mut answer = Answer::new();

    for (param_name, user_term) in terms.iter() {
        match user_term {
            Term::Variable { name: Some(_), .. } => {
                // For variables, map from user variable to parameter variable
                let param_term = Term::<Value>::var(param_name);
                if let Some(factors) = source.lookup(user_term) {
                    answer.merge(Evidence::Parameter {
                        term: &param_term,
                        value: &factors.content(),
                    })?;
                }
            }
            Term::Constant(value) => {
                // For constants, directly bind the parameter variable to the constant value
                let param_term = Term::<Value>::var(param_name);
                answer.merge(Evidence::Parameter {
                    term: &param_term,
                    value,
                })?;
            }
            Term::Variable { name: None, .. } => {}
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
) -> Result<Answer, crate::InconsistencyError> {
    let mut merged = base.clone();

    // Map through parameters: for each parameter, if it exists in result,
    // merge it into base under the user variable name
    for (param_name, user_term) in terms.iter() {
        // Skip constants - they were input parameters, not results to merge back
        if matches!(user_term, Term::Constant(_)) {
            continue;
        }

        // Try to get the factors for the parameter name from result
        let param_term = Term::<Value>::var(param_name);
        if let Some(factors) = result.lookup(&param_term) {
            // Merge all factors under the user's variable name, preserving provenance
            for factor in factors.evidence() {
                merged.assign(user_term, factor)?;
            }
        }
    }

    Ok(merged)
}

/// Represents an application of a concept with specific term bindings.
/// This is used when querying for entities that match a concept pattern.
/// Note: The name has a typo (should be ConceptApplication) but is kept for compatibility.
#[derive(Debug, Clone, PartialEq)]
pub struct ConceptApplication {
    /// The term bindings for this concept application.
    pub terms: Parameters,
    /// The concept being applied.
    pub concept: ConceptDescriptor,
}

impl ConceptApplication {
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
        let this_bound = if let Some(this_term) = self.terms.get("this") {
            env.contains(this_term)
        } else {
            false
        };

        if this_bound {
            // Entity is known - each attribute is a lookup (the + of known)
            let mut total = CONCEPT_OVERHEAD; // Add overhead for potential rule evaluation
            for (name, attribute) in self.concept.attributes().iter() {
                // Check if this attribute's value is also bound
                total += attribute.estimate(
                    true,
                    if let Some(term) = self.terms.get(name) {
                        env.contains(term)
                    } else {
                        false
                    },
                );
            }
            Some(total)
        } else {
            // Entity is not bound - categorize attributes to find best execution strategy
            let mut bound_one: Option<&AttributeSchema<Value>> = None;
            let mut bound_many: Option<&AttributeSchema<Value>> = None;
            let mut unbound_one: Option<&AttributeSchema<Value>> = None;
            let mut unbound_many: Option<&AttributeSchema<Value>> = None;

            for (name, attribute) in self.concept.attributes().iter() {
                if let Some(term) = self.terms.get(name) {
                    if env.contains(term) {
                        match attribute.cardinality {
                            crate::Cardinality::One => {
                                bound_one = Some(attribute);
                                break; // Best case found, stop searching
                            }
                            crate::Cardinality::Many if bound_many.is_none() => {
                                bound_many = Some(attribute);
                            }
                            _ => {}
                        }
                    } else {
                        // Term exists but not bound
                        match attribute.cardinality {
                            crate::Cardinality::One if unbound_one.is_none() => {
                                unbound_one = Some(attribute);
                            }
                            crate::Cardinality::Many if unbound_many.is_none() => {
                                unbound_many = Some(attribute);
                            }
                            _ => {}
                        }
                    }
                } else {
                    // No term at all
                    match attribute.cardinality {
                        crate::Cardinality::One if unbound_one.is_none() => {
                            unbound_one = Some(attribute);
                        }
                        crate::Cardinality::Many if unbound_many.is_none() => {
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

            for (name, attribute) in self.concept.attributes().iter() {
                if lead != attribute {
                    total += attribute.estimate(
                        true,
                        if let Some(term) = self.terms.get(name) {
                            env.contains(term)
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
        self.concept.schema()
    }

    /// Evaluates this concept application within the given context, producing a stream of answers.
    pub fn evaluate<S: Source, M: crate::selection::Answers>(
        self,
        context: EvaluationContext<S, M>,
    ) -> impl crate::selection::Answers {
        let app = self;
        let concept = app.concept.clone();

        let mut rules = vec![DeductiveRule::from(&concept)];
        rules.extend(context.source.resolve_rules(&concept.operator()));
        let plan = rules
            .iter()
            .map(|rule| Join::from(&rule.premises))
            .map(|join| join.plan(&context.scope).unwrap_or(join))
            .fold(Fork::new(), |fork, join| fork.or(join));

        try_stream! {
            for await each in context.selection {
                let input = each?;

                // Extract answer with parameter names for scoped evaluation
                // Maps user variable names → internal parameter names
                let initial_answer = extract_parameters(&input, &app.terms)
                    .map_err(|e| crate::QueryError::FactStore(e.to_string()))?;
                let single_answers = futures_util::stream::once(async move { Ok(initial_answer) });
                let eval_context = EvaluationContext {
                    selection: single_answers,
                    source: context.source.clone(),
                    scope: context.scope.clone(),
                };

                // Merge results back, mapping parameter names → user variable names
                // All factors are copied with their original provenance
                for await result in plan.clone().evaluate(eval_context) {
                    let result_answer = result?;
                    let merged = merge_parameters(&input, &result_answer, &app.terms)
                        .map_err(|e| crate::QueryError::FactStore(e.to_string()))?;
                    yield merged;
                }
            }
        }
    }

    /// Queries a source for entities matching this concept application.
    pub fn query<S: Source>(self, source: S) -> impl crate::selection::Answers {
        let store = source.clone();
        let context = new_context(store);
        self.evaluate(context)
    }
}

impl Display for ConceptApplication {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {{", self.concept.operator())?;
        for (name, term) in self.terms.iter() {
            write!(f, "{}: {},", name, term)?;
        }

        write!(f, "}}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::predicate::ConceptDescriptor;
    use crate::{AttributeSchema, Parameters, Term, Type, Value};

    // Note: Async tests are commented out due to Rust recursion limit issues in test compilation
    // with deeply nested async streams. The functionality is tested indirectly through integration
    // tests and the planning tests above verify the core logic.

    #[dialog_common::test]
    async fn test_concept_application_query_execution() -> anyhow::Result<()> {
        use crate::{Relation, Session};
        use dialog_artifacts::{Artifacts, Attribute as ArtifactAttribute, Entity};
        use dialog_storage::MemoryStorageBackend;

        // Create a store and session
        let backend = MemoryStorageBackend::default();
        let store = Artifacts::anonymous(backend).await?;
        let mut session = Session::open(store);

        let alice = Entity::new()?;
        let bob = Entity::new()?;

        session
            .transact(vec![
                Relation {
                    the: "person/name".parse::<ArtifactAttribute>()?,
                    of: alice.clone(),
                    is: Value::String("Alice".to_string()),
                },
                Relation {
                    the: "person/age".parse::<ArtifactAttribute>()?,
                    of: alice.clone(),
                    is: Value::UnsignedInt(25),
                },
                Relation {
                    the: "person/name".parse::<ArtifactAttribute>()?,
                    of: bob.clone(),
                    is: Value::String("Bob".to_string()),
                },
                Relation {
                    the: "person/age".parse::<ArtifactAttribute>()?,
                    of: bob.clone(),
                    is: Value::UnsignedInt(30),
                },
            ])
            .await?;

        // Create a person concept
        let concept = ConceptDescriptor::Dynamic {
            description: String::new(),
            attributes: vec![
                (
                    "name",
                    AttributeSchema::new("person", "name", "", Type::String),
                ),
                (
                    "age",
                    AttributeSchema::new("person", "age", "", Type::UnsignedInt),
                ),
            ]
            .into(),
        };

        let mut terms = Parameters::new();
        terms.insert("this".to_string(), Term::var("person"));
        terms.insert("name".to_string(), Term::var("name"));
        terms.insert("age".to_string(), Term::var("age"));

        let application = ConceptApplication { terms, concept };

        // Execute the query
        let selection =
            futures_util::TryStreamExt::try_collect::<Vec<_>>(application.query(session)).await?;

        // Should find both Alice and Bob with their name and age
        assert_eq!(selection.len(), 2, "Should find 2 people");

        let name_var: Term<Value> = Term::var("name");
        let age_var: Term<Value> = Term::var("age");

        let mut found_alice = false;
        let mut found_bob = false;

        for match_result in selection.iter() {
            let name = match_result.resolve(&name_var)?;
            let age = match_result.resolve(&age_var)?;

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
    async fn test_concept_application_with_bound_entity_query() -> anyhow::Result<()> {
        use crate::context::new_context;
        use crate::{Relation, Session};
        use dialog_artifacts::{Artifacts, Attribute as ArtifactAttribute, Entity};
        use dialog_storage::MemoryStorageBackend;

        // Create a store and session
        let backend = MemoryStorageBackend::default();
        let store = Artifacts::anonymous(backend).await?;
        let mut session = Session::open(store);

        let alice = Entity::new()?;

        session
            .transact(vec![
                Relation {
                    the: "person/name".parse::<ArtifactAttribute>()?,
                    of: alice.clone(),
                    is: Value::String("Alice".to_string()),
                },
                Relation {
                    the: "person/age".parse::<ArtifactAttribute>()?,
                    of: alice.clone(),
                    is: Value::UnsignedInt(25),
                },
            ])
            .await?;

        // Create a person concept
        let concept = ConceptDescriptor::Dynamic {
            description: String::new(),
            attributes: vec![
                (
                    "name",
                    AttributeSchema::new("person", "name", "", Type::String),
                ),
                (
                    "age",
                    AttributeSchema::new("person", "age", "", Type::UnsignedInt),
                ),
            ]
            .into(),
        };

        let mut terms = Parameters::new();
        terms.insert("this".to_string(), Term::var("person"));
        terms.insert("name".to_string(), Term::var("name"));
        terms.insert("age".to_string(), Term::var("age"));

        let application = ConceptApplication { terms, concept };

        // Create a scope with the entity already bound
        let mut scope = Environment::new();
        let person_var: Term<Value> = Term::var("person");
        scope.add(&person_var);

        // Create evaluation context with bound entity
        let context = new_context(session);
        let context_with_scope = EvaluationContext {
            source: context.source,
            selection: context.selection,
            scope,
        };

        // Execute with bound entity scope
        futures_util::TryStreamExt::try_collect::<Vec<_>>(application.evaluate(context_with_scope))
            .await?;

        Ok(())
    }

    #[dialog_common::test]
    fn test_concept_as_conclusion_operations() {
        use crate::predicate::concept::Attributes;

        let concept = ConceptDescriptor::Dynamic {
            description: String::new(),
            attributes: Attributes::from(vec![
                (
                    "name",
                    AttributeSchema::<Value>::new("person", "name", "Person name", Type::String),
                ),
                (
                    "age",
                    AttributeSchema::<Value>::new("person", "age", "Person age", Type::UnsignedInt),
                ),
            ]),
        };

        // Test that attributes are present
        let param_names: Vec<&str> = concept.attributes().keys().collect();
        assert!(param_names.contains(&"name"));
        assert!(param_names.contains(&"age"));
        assert!(!param_names.contains(&"height"));
        // "this" parameter is implied but not in attributes
    }

    #[dialog_common::test]
    fn test_concept_creation() {
        use crate::predicate::concept::Attributes;

        let concept = ConceptDescriptor::Dynamic {
            description: String::new(),
            attributes: Attributes::from(vec![(
                "name".to_string(),
                AttributeSchema::<Value>::new("person", "name", "Person name", Type::String),
            )]),
        };

        // Operator is now a computed URI
        assert!(
            concept.operator().starts_with("concept:"),
            "Operator should be a concept URI"
        );
        assert_eq!(concept.attributes().count(), 1);
        assert!(concept.attributes().keys().any(|k| k == "name"));
    }

    #[dialog_common::test]
    fn test_concept_application_analysis() {
        use crate::predicate::concept::Attributes;

        let concept = ConceptDescriptor::Dynamic {
            description: String::new(),
            attributes: Attributes::from(vec![
                (
                    "name".to_string(),
                    AttributeSchema::<Value>::new("person", "name", "Person name", Type::String),
                ),
                (
                    "age".to_string(),
                    AttributeSchema::<Value>::new("person", "age", "Person age", Type::UnsignedInt),
                ),
            ]),
        };

        let mut terms = Parameters::new();
        terms.insert("name".to_string(), Term::var("person_name"));
        terms.insert("age".to_string(), Term::var("person_age"));

        let concept_app = ConceptApplication { terms, concept };

        let cost = concept_app.estimate(&Environment::new());
        assert_eq!(cost, Some(2100));

        let schema = concept_app.schema();

        assert_eq!(schema.iter().count(), 3);
        assert!(schema.get("this").is_some());
        assert!(schema.get("name").is_some());
        assert!(schema.get("age").is_some());
    }

    #[dialog_common::test]
    fn test_deductive_rule_parameters() {
        use std::collections::HashSet;

        let rule = DeductiveRule {
            conclusion: ConceptDescriptor::Dynamic {
                description: String::new(),
                attributes: [
                    (
                        "name".to_string(),
                        AttributeSchema::new("person", "name", "Person name", Type::String),
                    ),
                    (
                        "age".to_string(),
                        AttributeSchema::new("person", "age", "Person age", Type::UnsignedInt),
                    ),
                ]
                .into(),
            },
            premises: vec![],
        };

        let params: HashSet<&str> = rule.parameters().collect();
        assert!(params.contains("this"));
        assert!(params.contains("name"));
        assert!(params.contains("age"));
        assert_eq!(params.len(), 3);
    }

    #[dialog_common::test]
    fn test_premise_construction() {
        use crate::predicate::fact::FactSelector;
        use crate::{Application, Premise};

        let fact = FactSelector::select()
            .the("person/name")
            .of(Term::var("person"))
            .is(Value::String("Alice".to_string()));

        let premise = Premise::from(fact);

        match premise {
            Premise::Apply(Application::Relation(_)) => {
                // Expected case - FactSelector now produces RelationApplication
            }
            _ => panic!("Expected Relation application"),
        }
    }

    #[dialog_common::test]
    fn test_error_types() {
        use crate::error::{AnalyzerError, PlanError, QueryError};
        use crate::predicate::concept::Attributes;

        // Test AnalyzerError creation
        let rule = DeductiveRule {
            conclusion: ConceptDescriptor::Dynamic {
                description: String::new(),
                attributes: Attributes::new(),
            },
            premises: vec![],
        };

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
                    r.conclusion.operator().starts_with("concept:"),
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
    fn test_application_variants() {
        use crate::Application;
        use crate::predicate::concept::Attributes;
        use crate::predicate::fact::FactSelector;

        // Test Relation application (FactSelector now produces RelationApplication)
        let fact = FactSelector::select().the("test/attr");
        let relation: crate::application::RelationApplication = fact.into();
        let app = Application::Relation(relation);

        match app {
            Application::Relation(_) => {
                // Expected
            }
            _ => panic!("Expected Relation variant"),
        }

        // Test other variants exist
        let mut terms = Parameters::new();
        terms.insert("test".to_string(), Term::var("test_var"));
        let concept = ConceptDescriptor::Dynamic {
            description: String::new(),
            attributes: Attributes::new(),
        };
        let concept_app = Application::Concept(ConceptApplication { terms, concept });

        match concept_app {
            Application::Concept(_) => {
                // Expected
            }
            _ => panic!("Expected Realize variant"),
        }
    }

    #[dialog_common::test]
    fn test_negation_construction() {
        use crate::predicate::fact::FactSelector;
        use crate::{Application, Negation};

        let fact = FactSelector::select().the("test/attr");
        let relation: crate::application::RelationApplication = fact.into();
        let app = Application::Relation(relation);
        let negation = Negation(app);

        // Test that negation wraps the application
        match negation {
            Negation(Application::Relation(_)) => {
                // Expected
            }
            _ => panic!("Expected wrapped Relation application"),
        }
    }

    #[dialog_common::test]
    async fn test_concept_application_respects_constant_entity_parameter() -> anyhow::Result<()> {
        use crate::application::concept::ConceptApplication;
        use crate::predicate::concept::ConceptDescriptor;
        use crate::{Relation, Session, Term, Value};
        use dialog_artifacts::{Artifacts, Attribute, Entity};
        use dialog_storage::MemoryStorageBackend;

        let backend = MemoryStorageBackend::default();
        let store = Artifacts::anonymous(backend).await?;
        let mut session = Session::open(store);

        let alice = Entity::new()?;
        let bob = Entity::new()?;

        session
            .transact(vec![
                Relation {
                    the: "person/name".parse::<Attribute>()?,
                    of: alice.clone(),
                    is: Value::String("Alice".to_string()),
                },
                Relation {
                    the: "person/name".parse::<Attribute>()?,
                    of: bob.clone(),
                    is: Value::String("Bob".to_string()),
                },
            ])
            .await?;

        let concept = ConceptDescriptor::Dynamic {
            description: String::new(),
            attributes: vec![(
                "name",
                crate::attribute::AttributeSchema::new(
                    "person",
                    "name",
                    "Person name",
                    crate::Type::String,
                ),
            )]
            .into(),
        };

        // Query with constant entity - should only return Alice
        let mut terms = Parameters::new();
        terms.insert(
            "this".to_string(),
            Term::Constant(Value::Entity(alice.clone())),
        );
        terms.insert("name".to_string(), Term::var("name"));

        let app = ConceptApplication { terms, concept };
        let selection =
            futures_util::TryStreamExt::try_collect::<Vec<_>>(app.query(session)).await?;

        assert_eq!(
            selection.len(),
            1,
            "Should find only Alice, not both people"
        );
        assert_eq!(
            selection[0].resolve(&Term::<Value>::var("name"))?,
            Value::String("Alice".to_string())
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn test_concept_application_respects_constant_attribute_parameter() -> anyhow::Result<()>
    {
        use crate::application::concept::ConceptApplication;
        use crate::predicate::concept::ConceptDescriptor;
        use crate::{Relation, Session, Term, Value};
        use dialog_artifacts::{Artifacts, Attribute, Entity};
        use dialog_storage::MemoryStorageBackend;

        let backend = MemoryStorageBackend::default();
        let store = Artifacts::anonymous(backend).await?;
        let mut session = Session::open(store);

        let alice = Entity::new()?;
        let bob = Entity::new()?;

        session
            .transact(vec![
                Relation {
                    the: "person/name".parse::<Attribute>()?,
                    of: alice.clone(),
                    is: Value::String("Alice".to_string()),
                },
                Relation {
                    the: "person/age".parse::<Attribute>()?,
                    of: alice.clone(),
                    is: Value::UnsignedInt(25),
                },
                Relation {
                    the: "person/name".parse::<Attribute>()?,
                    of: bob.clone(),
                    is: Value::String("Bob".to_string()),
                },
                Relation {
                    the: "person/age".parse::<Attribute>()?,
                    of: bob.clone(),
                    is: Value::UnsignedInt(30),
                },
            ])
            .await?;

        let concept = ConceptDescriptor::Dynamic {
            description: String::new(),
            attributes: vec![
                (
                    "name",
                    crate::attribute::AttributeSchema::new(
                        "person",
                        "name",
                        "Person name",
                        crate::Type::String,
                    ),
                ),
                (
                    "age",
                    crate::attribute::AttributeSchema::new(
                        "person",
                        "age",
                        "Person age",
                        crate::Type::UnsignedInt,
                    ),
                ),
            ]
            .into(),
        };

        // Query with constant name value - should only return Bob
        let mut terms = Parameters::new();
        terms.insert("this".to_string(), Term::var("entity"));
        terms.insert(
            "name".to_string(),
            Term::Constant(Value::String("Bob".to_string())),
        );
        terms.insert("age".to_string(), Term::var("age"));

        let app = ConceptApplication { terms, concept };
        let selection =
            futures_util::TryStreamExt::try_collect::<Vec<_>>(app.query(session)).await?;

        assert_eq!(selection.len(), 1, "Should find only Bob");
        assert_eq!(
            selection[0].resolve(&Term::<Value>::var("entity"))?,
            Value::Entity(bob.clone())
        );
        assert_eq!(
            selection[0].resolve(&Term::<Value>::var("age"))?,
            Value::UnsignedInt(30)
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn test_concept_application_respects_multiple_constant_parameters() -> anyhow::Result<()>
    {
        use crate::application::concept::ConceptApplication;
        use crate::predicate::concept::ConceptDescriptor;
        use crate::{Relation, Session, Term, Value};
        use dialog_artifacts::{Artifacts, Attribute, Entity};
        use dialog_storage::MemoryStorageBackend;

        let backend = MemoryStorageBackend::default();
        let store = Artifacts::anonymous(backend).await?;
        let mut session = Session::open(store);

        let alice = Entity::new()?;
        let bob = Entity::new()?;

        session
            .transact(vec![
                Relation {
                    the: "person/name".parse::<Attribute>()?,
                    of: alice.clone(),
                    is: Value::String("Alice".to_string()),
                },
                Relation {
                    the: "person/age".parse::<Attribute>()?,
                    of: alice.clone(),
                    is: Value::UnsignedInt(25),
                },
                Relation {
                    the: "person/name".parse::<Attribute>()?,
                    of: bob.clone(),
                    is: Value::String("Bob".to_string()),
                },
                Relation {
                    the: "person/age".parse::<Attribute>()?,
                    of: bob.clone(),
                    is: Value::UnsignedInt(30),
                },
            ])
            .await?;

        let concept = ConceptDescriptor::Dynamic {
            description: String::new(),
            attributes: vec![
                (
                    "name",
                    crate::attribute::AttributeSchema::new(
                        "person",
                        "name",
                        "Person name",
                        crate::Type::String,
                    ),
                ),
                (
                    "age",
                    crate::attribute::AttributeSchema::new(
                        "person",
                        "age",
                        "Person age",
                        crate::Type::UnsignedInt,
                    ),
                ),
            ]
            .into(),
        };

        // Query with both name and age constants - should only match Alice
        let mut terms = Parameters::new();
        terms.insert("this".to_string(), Term::var("entity"));
        terms.insert(
            "name".to_string(),
            Term::Constant(Value::String("Alice".to_string())),
        );
        terms.insert("age".to_string(), Term::Constant(Value::UnsignedInt(25)));

        let app = ConceptApplication { terms, concept };
        let selection =
            futures_util::TryStreamExt::try_collect::<Vec<_>>(app.query(session)).await?;

        assert_eq!(
            selection.len(),
            1,
            "Should find only Alice with exact name and age match"
        );
        assert_eq!(
            selection[0].resolve(&Term::<Value>::var("entity"))?,
            Value::Entity(alice.clone())
        );

        Ok(())
    }
}
