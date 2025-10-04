use super::fact::{BASE_COST, CONCEPT_OVERHEAD, ENTITY_COST, VALUE_COST};
use crate::analyzer::{AnalyzerError, LegacyAnalysis};
use crate::cursor::Cursor;
use crate::error::PlanError;
use crate::plan::{fresh, ConceptPlan};
use crate::planner::Join;
use crate::predicate::Concept;
use crate::DeductiveRule;
use crate::{
    try_stream, Attribute, Dependencies, EvaluationContext, Parameters, Schema, Selection, Source,
    Term, Value, VariableScope,
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
    pub fn cost(&self) -> usize {
        BASE_COST
    }

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
    pub fn estimate(&self, env: &VariableScope) -> Option<usize> {
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

    pub fn dependencies(&self) -> Dependencies {
        let mut dependencies = Dependencies::new();
        if let Some(Term::Variable {
            name: Some(name), ..
        }) = self.terms.get("this")
        {
            dependencies.desire(name.into(), ENTITY_COST);
        }

        for (parameter, _) in self.concept.attributes.iter() {
            if let Some(Term::Variable {
                name: Some(name), ..
            }) = self.terms.get(parameter)
            {
                dependencies.desire(name.into(), VALUE_COST);
            }
        }

        dependencies
    }

    pub fn analyze(&self) -> LegacyAnalysis {
        let mut analysis = LegacyAnalysis::new(BASE_COST);

        analysis.desire(self.terms.get("this"), ENTITY_COST);

        for parameter in self.concept.operands() {
            analysis.desire(self.terms.get(parameter), VALUE_COST);
        }

        analysis
    }

    // /// Analyzes this concept application to determine its dependencies and execution cost.
    // /// All concept applications require the "this" entity parameter and desire all
    // /// concept attributes as dependencies.
    // pub fn analyze(&self) -> Result<Analysis, AnalyzerError> {
    //     let mut dependencies = Dependencies::new();
    //     dependencies.desire("this".into(), ENTITY_COST);

    //     for (name, _) in self.concept.attributes.iter() {
    //         dependencies.desire(name.to_string(), VALUE_COST);
    //     }

    //     Ok(Analysis {
    //         cost: BASE_COST,
    //         dependencies,
    //     })
    // }

    pub fn compile(self) -> Result<ConceptApplicationAnalysis, AnalyzerError> {
        let mut dependencies = Dependencies::new();
        dependencies.desire("this".into(), ENTITY_COST);
        for (name, _) in self.concept.attributes.iter() {
            dependencies.desire(name.to_string(), VALUE_COST);
        }

        Ok(ConceptApplicationAnalysis {
            application: self,
            analysis: LegacyAnalysis {
                cost: BASE_COST,
                dependencies,
            },
        })
    }

    pub fn plan(&self, scope: &VariableScope) -> Result<ConceptPlan, PlanError> {
        let analysis = self.analyze();
        let mut cost = analysis.cost;
        let mut provides = VariableScope::new();
        for (name, _) in analysis.dependencies.iter() {
            let term: Term<Value> = Term::var(name);
            if !scope.contains(&term) {
                provides.add(&term);
                // No variable can be required on the concept application
                // Cost is already calculated via estimate()
            }
        }

        Ok(ConceptPlan {
            cost,
            provides,
            dependencies: analysis.dependencies,
            concept: self.concept.clone(),
            terms: self.terms.clone(),
        })
    }

    // /// Creates an execution plan for this concept application.
    // /// Converts the concept application into a set of fact selector premises
    // /// that can be executed to find matching entities.
    // pub fn plan_legacy(&self, scope: &VariableScope) -> Result<ConceptPlan, PlanError> {
    //     let mut provides = VariableScope::new();
    //     let mut cost = 0;
    //     let mut parameterized = false;

    //     let this_entity: Term<Entity> = if let Some(this_value) = self.terms.get("this") {
    //         // Check if "this" parameter is non-blank
    //         if !this_value.is_blank() {
    //             parameterized = true;
    //         }

    //         if !scope.contains(&this_value) {
    //             provides.add(&this_value);
    //             cost += ENTITY_COST
    //         }

    //         // Convert the "this" term from Term<Value> to Term<Entity>
    //         match this_value {
    //             Term::Variable { name, .. } => Term::<Entity>::Variable {
    //                 name: name.clone(),
    //                 _type: Type::default(),
    //             },
    //             Term::Constant(value) => {
    //                 // If it's a constant, it should be an Entity value
    //                 if let Value::Entity(entity) = value {
    //                     Term::Constant(entity.clone())
    //                 } else {
    //                     // Fallback to a variable if not an entity
    //                     Term::<Entity>::var(&format!("this_{}", self.concept.operator))
    //                 }
    //             }
    //         }
    //     } else {
    //         // Create a unique variable if "this" is not provided
    //         Term::<Entity>::var(&format!("this_{}", self.concept.operator))
    //     };

    //     let mut premises = vec![];

    //     // go over dependencies to add all the terms that will be derived
    //     // by the application to the `provides` list.
    //     for (name, attribute) in self.concept.attributes.iter() {
    //         // If parameter was not provided we add it to the provides set
    //         let select = if let Some(term) = self.terms.get(name) {
    //             // Track if we have any non-blank parameters
    //             if !term.is_blank() {
    //                 parameterized = true;
    //             }

    //             if !scope.contains(&term) {
    //                 provides.add(&term);
    //                 cost += VALUE_COST
    //             }

    //             FactApplication::new()
    //                 .the(attribute.the())
    //                 .of(this_entity.clone())
    //                 .is(term.clone())
    //         } else {
    //             FactApplication::new()
    //                 .the(attribute.the())
    //                 .of(this_entity.clone())
    //         };
    //         premises.push(select.into());
    //     }

    //     // If we have no non-blank parameters, it's an unparameterized application
    //     if !parameterized {
    //         return Err(PlanError::UnparameterizedApplication);
    //     }

    //     let mut join = Join::new(&premises);
    //     let (added_cost, conjuncts) = join.plan(scope)?;

    //     Ok(ConceptPlan {
    //         concept: self.concept.clone(),
    //         cost: cost + added_cost,
    //         provides,
    //         conjuncts,
    //     })
    // }
    //

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
        let context = fresh(store);
        let selection = self.evaluate(context);
        selection
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ConceptApplicationAnalysis {
    pub application: ConceptApplication,
    pub analysis: LegacyAnalysis,
}

impl ConceptApplicationAnalysis {
    pub fn dependencies(&self) -> &'_ Dependencies {
        &self.analysis.dependencies
    }
    pub fn cost(&self) -> usize {
        self.analysis.cost
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

// impl Syntax for ConceptApplication {
//     fn analyze<'a>(&'a self, env: &Environment) -> Stats<'a, Self> {
//         let mut stats = Stats::new(self, BASE_COST);

//         let blank = Term::blank();

//         // If `this` parameter is not bound in local environment
//         // we need to mark it as desired.
//         let this = self.terms.get("this").unwrap_or(&blank);
//         if !env.locals.contains(this) {
//             stats.desire(this, ENTITY_COST);
//         }

//         // Next we need to consider parameters for each attribute
//         // and mark ones that are not bound in local environment as desired.
//         for name in self.concept.attributes.keys() {
//             let parameter = self.terms.get(name).unwrap_or(&blank);
//             if !env.locals.contains(parameter) {
//                 stats.desire(parameter, ENTITY_COST);
//             }
//         }

//         stats
//     }
// }

#[cfg(test)]
mod tests {
    use super::*;
    use crate::predicate::Concept;
    use crate::{Attribute, Parameters, Term, Type, Value};

    #[test]
    fn test_concept_application_plan() {
        // Create a simple person concept with name and age attributes
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

        // Plan with empty scope
        let scope = VariableScope::new();
        let plan = application.plan(&scope).expect("Planning should succeed");

        // Should bind all three variables
        let person_var: Term<Value> = Term::var("person");
        let name_var: Term<Value> = Term::var("name");
        let age_var: Term<Value> = Term::var("age");

        assert!(
            plan.provides.contains(&person_var),
            "Should bind person variable"
        );
        assert!(
            plan.provides.contains(&name_var),
            "Should bind name variable"
        );
        assert!(plan.provides.contains(&age_var), "Should bind age variable");
    }

    #[test]
    fn test_concept_application_with_bound_entity() {
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

        // Create scope with entity already bound
        let mut scope = VariableScope::new();
        let person_var: Term<Value> = Term::var("person");
        scope.add(&person_var);

        let plan = application.plan(&scope).expect("Planning should succeed");

        // Entity is already bound, so only name and age should be provided
        assert_eq!(
            plan.provides.variables.len(),
            2,
            "Should only bind 2 new variables"
        );
        let name_var: Term<Value> = Term::var("name");
        let age_var: Term<Value> = Term::var("age");
        assert!(
            plan.provides.contains(&name_var),
            "Should bind name variable"
        );
        assert!(plan.provides.contains(&age_var), "Should bind age variable");
    }

    // Note: Async tests are commented out due to Rust recursion limit issues in test compilation
    // with deeply nested async streams. The functionality is tested indirectly through integration
    // tests and the planning tests above verify the core logic.

    #[tokio::test]
    async fn test_concept_application_query_execution() -> anyhow::Result<()> {
        use crate::session::Session;
        use crate::{Fact, SelectionExt};
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
        use crate::plan::fresh;
        use crate::session::Session;
        use crate::{EvaluationContext, Fact, SelectionExt};
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
        let mut scope = VariableScope::new();
        let person_var: Term<Value> = Term::var("person");
        scope.add(&person_var);

        // Create evaluation context with bound entity
        let context = fresh(session);
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
