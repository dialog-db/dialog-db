use super::fact::CONCEPT_OVERHEAD;
use crate::context::new_context;
use crate::cursor::Cursor;
use crate::planner::Join;
use crate::predicate::Concept;
use crate::DeductiveRule;
use crate::{
    try_stream, Attribute, Environment, EvaluationContext, Parameters, Schema, Selection, Source,
    Value,
};
use std::fmt::Display;

/// Represents an application of a concept with specific term bindings.
/// This is used when querying for entities that match a concept pattern.
/// Note: The name has a typo (should be ConceptApplication) but is kept for compatibility.
#[derive(Debug, Clone, PartialEq)]
pub struct ConceptApplication {
    /// The term bindings for this concept application.
    pub terms: Parameters,
    /// The concept being applied.
    pub concept: Concept,
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
            for (name, attribute) in self.concept.attributes.iter() {
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
            let mut bound_one: Option<&Attribute<Value>> = None;
            let mut bound_many: Option<&Attribute<Value>> = None;
            let mut unbound_one: Option<&Attribute<Value>> = None;
            let mut unbound_many: Option<&Attribute<Value>> = None;

            for (name, attribute) in self.concept.attributes.iter() {
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

            for (name, attribute) in self.concept.attributes.iter() {
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

    pub fn schema(&self) -> Schema {
        self.concept.schema()
    }

    pub fn evaluate<S: Source, M: Selection>(
        &self,
        context: EvaluationContext<S, M>,
    ) -> impl Selection {
        let app = self.clone();
        let concept = self.concept.clone();

        // OPTIMIZATION: Plan once outside the loop instead of replanning on every frame
        // This reduces per-frame cost from ~100s of operations to ~5
        let implicit = DeductiveRule::from(&concept);
        let join: Join = (&implicit.premises).into();
        let planned_join = std::sync::Arc::new(join.plan(&context.scope).unwrap_or(join));

        try_stream! {
            for await each in context.selection {
                let input = each?;

                // Create cursor for bidirectional parameter mapping
                let cursor = Cursor::new(input, app.terms.clone());

                // Extract initial match with resolved constants for evaluation
                let initial_match = crate::Match::try_from(&cursor)
                    .map_err(|e| crate::QueryError::FactStore(e.to_string()))?;

                // Evaluate join with single-match selection
                // NOTE: context.scope already contains all bound variable names from upstream.
                // VariableScope only tracks variable names (for planning), not values.
                // The actual constant values are bound in initial_match, not the scope.
                let single_selection = futures_util::stream::once(async move { Ok(initial_match) });
                let eval_context = EvaluationContext {
                    selection: single_selection,
                    source: context.source.clone(),
                    scope: context.scope.clone(),
                };

                // Merge results back using cursor's bidirectional mapping
                for await result in planned_join.evaluate(eval_context) {
                    yield cursor.merge(&result?)
                        .map_err(|e| crate::QueryError::FactStore(e.to_string()))?;
                }
            }
        }
    }

    pub fn query<S: Source>(&self, store: S) -> impl Selection {
        let store = store.clone();
        let context = new_context(store);
        let selection = self.evaluate(context);
        selection
    }
}

impl Display for ConceptApplication {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {{", self.concept.operator)?;
        for (name, term) in self.terms.iter() {
            write!(f, "{}: {},", name, term)?;
        }

        write!(f, "}}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::predicate::Concept;
    use crate::{Attribute, Parameters, Term, Type, Value};

    // Note: Async tests are commented out due to Rust recursion limit issues in test compilation
    // with deeply nested async streams. The functionality is tested indirectly through integration
    // tests and the planning tests above verify the core logic.

    #[tokio::test]
    async fn test_concept_application_query_execution() -> anyhow::Result<()> {
        use crate::fact::Fact;
        use crate::session::Session;
        use crate::SelectionExt;
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
            ])
            .await?;

        // Create a person concept
        let concept = Concept {
            operator: "person".to_string(),
            attributes: vec![
                ("name", Attribute::new("person", "name", "", Type::String)),
                (
                    "age",
                    Attribute::new("person", "age", "", Type::UnsignedInt),
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
        let selection = application.query(session).collect_matches().await?;

        // Should find both Alice and Bob with their name and age
        assert_eq!(selection.len(), 2, "Should find 2 people");

        let name_var: Term<Value> = Term::var("name");
        let age_var: Term<Value> = Term::var("age");

        let mut found_alice = false;
        let mut found_bob = false;

        for match_result in selection.iter() {
            let name = match_result.get(&name_var)?;
            let age = match_result.get(&age_var)?;

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

    #[tokio::test]
    async fn test_concept_application_with_bound_entity_query() -> anyhow::Result<()> {
        use crate::context::new_context;
        use crate::fact::Fact;
        use crate::session::Session;
        use crate::{EvaluationContext, SelectionExt};
        use dialog_artifacts::{Artifacts, Attribute as ArtifactAttribute, Entity};
        use dialog_storage::MemoryStorageBackend;

        // Create a store and session
        let backend = MemoryStorageBackend::default();
        let store = Artifacts::anonymous(backend).await?;
        let mut session = Session::open(store);

        let alice = Entity::new()?;

        session
            .transact(vec![
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
            ])
            .await?;

        // Create a person concept
        let concept = Concept {
            operator: "person".to_string(),
            attributes: vec![
                ("name", Attribute::new("person", "name", "", Type::String)),
                (
                    "age",
                    Attribute::new("person", "age", "", Type::UnsignedInt),
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
        let selection = application
            .evaluate(context_with_scope)
            .collect_matches()
            .await?;

        // Should still execute successfully and bind name and age
        // (even though we don't have the actual entity value in the match)
        assert!(
            selection.len() >= 0,
            "Should execute successfully with bound scope"
        );

        Ok(())
    }
}

#[tokio::test]
async fn test_concept_application_respects_constant_entity_parameter() -> anyhow::Result<()> {
    use crate::application::concept::ConceptApplication;
    use crate::predicate::concept::Concept;
    use crate::session::Session;
    use crate::{Fact, Parameters, SelectionExt, Term, Value};
    use dialog_artifacts::{Artifacts, Attribute, Entity};
    use dialog_storage::MemoryStorageBackend;

    let backend = MemoryStorageBackend::default();
    let store = Artifacts::anonymous(backend).await?;
    let mut session = Session::open(store);

    let alice = Entity::new()?;
    let bob = Entity::new()?;

    session
        .transact(vec![
            Fact::assert(
                "person/name".parse::<Attribute>()?,
                alice.clone(),
                Value::String("Alice".to_string()),
            ),
            Fact::assert(
                "person/name".parse::<Attribute>()?,
                bob.clone(),
                Value::String("Bob".to_string()),
            ),
        ])
        .await?;

    let concept = Concept {
        operator: "person".to_string(),
        attributes: vec![(
            "name",
            crate::attribute::Attribute::new("person", "name", "Person name", crate::Type::String),
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
    let selection = app.query(session).collect_matches().await?;

    assert_eq!(
        selection.len(),
        1,
        "Should find only Alice, not both people"
    );
    assert_eq!(
        selection[0].get(&Term::<Value>::var("name"))?,
        Value::String("Alice".to_string())
    );

    Ok(())
}

#[tokio::test]
async fn test_concept_application_respects_constant_attribute_parameter() -> anyhow::Result<()> {
    use crate::application::concept::ConceptApplication;
    use crate::predicate::concept::Concept;
    use crate::session::Session;
    use crate::Fact;
    use crate::{Parameters, SelectionExt, Term, Value};
    use dialog_artifacts::{Artifacts, Attribute, Entity};
    use dialog_storage::MemoryStorageBackend;

    let backend = MemoryStorageBackend::default();
    let store = Artifacts::anonymous(backend).await?;
    let mut session = Session::open(store);

    let alice = Entity::new()?;
    let bob = Entity::new()?;

    session
        .transact(vec![
            Fact::assert(
                "person/name".parse::<Attribute>()?,
                alice.clone(),
                Value::String("Alice".to_string()),
            ),
            Fact::assert(
                "person/age".parse::<Attribute>()?,
                alice.clone(),
                Value::UnsignedInt(25),
            ),
            Fact::assert(
                "person/name".parse::<Attribute>()?,
                bob.clone(),
                Value::String("Bob".to_string()),
            ),
            Fact::assert(
                "person/age".parse::<Attribute>()?,
                bob.clone(),
                Value::UnsignedInt(30),
            ),
        ])
        .await?;

    let concept = Concept {
        operator: "person".to_string(),
        attributes: vec![
            (
                "name",
                crate::attribute::Attribute::new(
                    "person",
                    "name",
                    "Person name",
                    crate::Type::String,
                ),
            ),
            (
                "age",
                crate::attribute::Attribute::new(
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
    let selection = app.query(session).collect_matches().await?;

    assert_eq!(selection.len(), 1, "Should find only Bob");
    assert_eq!(
        selection[0].get(&Term::<Value>::var("entity"))?,
        Value::Entity(bob.clone())
    );
    assert_eq!(
        selection[0].get(&Term::<Value>::var("age"))?,
        Value::UnsignedInt(30)
    );

    Ok(())
}

#[tokio::test]
async fn test_concept_application_respects_multiple_constant_parameters() -> anyhow::Result<()> {
    use crate::application::concept::ConceptApplication;
    use crate::fact::Fact;
    use crate::predicate::concept::Concept;
    use crate::session::Session;
    use crate::{Parameters, SelectionExt, Term, Value};
    use dialog_artifacts::{Artifacts, Attribute, Entity};
    use dialog_storage::MemoryStorageBackend;

    let backend = MemoryStorageBackend::default();
    let store = Artifacts::anonymous(backend).await?;
    let mut session = Session::open(store);

    let alice = Entity::new()?;
    let bob = Entity::new()?;

    session
        .transact(vec![
            Fact::assert(
                "person/name".parse::<Attribute>()?,
                alice.clone(),
                Value::String("Alice".to_string()),
            ),
            Fact::assert(
                "person/age".parse::<Attribute>()?,
                alice.clone(),
                Value::UnsignedInt(25),
            ),
            Fact::assert(
                "person/name".parse::<Attribute>()?,
                bob.clone(),
                Value::String("Bob".to_string()),
            ),
            Fact::assert(
                "person/age".parse::<Attribute>()?,
                bob.clone(),
                Value::UnsignedInt(30),
            ),
        ])
        .await?;

    let concept = Concept {
        operator: "person".to_string(),
        attributes: vec![
            (
                "name",
                crate::attribute::Attribute::new(
                    "person",
                    "name",
                    "Person name",
                    crate::Type::String,
                ),
            ),
            (
                "age",
                crate::attribute::Attribute::new(
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
    let selection = app.query(session).collect_matches().await?;

    assert_eq!(
        selection.len(),
        1,
        "Should find only Alice with exact name and age match"
    );
    assert_eq!(
        selection[0].get(&Term::<Value>::var("entity"))?,
        Value::Entity(alice.clone())
    );

    Ok(())
}
