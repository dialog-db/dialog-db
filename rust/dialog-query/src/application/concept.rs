use super::fact::{FactApplication, BASE_COST, ENTITY_COST, VALUE_COST};
use super::Join;
use crate::analyzer::{Analysis, AnalyzerError};
use crate::error::PlanError;
use crate::plan::ConceptPlan;
use crate::predicate::Concept;
use crate::{Dependencies, Entity, Parameters, Term, Type, Value, VariableScope};
use std::fmt::Display;

/// Represents an application of a concept with specific term bindings.
/// This is used when querying for entities that match a concept pattern.
/// Note: The name has a typo (should be ConceptApplication) but is kept for compatibility.
#[derive(Debug, Clone, PartialEq)]
pub struct ConcetApplication {
    /// The term bindings for this concept application.
    pub terms: Parameters,
    /// The concept being applied.
    pub concept: Concept,
}

impl ConcetApplication {
    /// Analyzes this concept application to determine its dependencies and execution cost.
    /// All concept applications require the "this" entity parameter and desire all
    /// concept attributes as dependencies.
    pub fn analyze(&self) -> Result<Analysis, AnalyzerError> {
        let mut dependencies = Dependencies::new();
        dependencies.desire("this".into(), ENTITY_COST);

        for (name, _) in self.concept.attributes.iter() {
            dependencies.desire(name.to_string(), VALUE_COST);
        }

        Ok(Analysis {
            cost: BASE_COST,
            dependencies,
        })
    }

    /// Creates an execution plan for this concept application.
    /// Converts the concept application into a set of fact selector premises
    /// that can be executed to find matching entities.
    pub fn plan(&self, scope: &VariableScope) -> Result<ConceptPlan, PlanError> {
        let mut provides = VariableScope::new();
        let mut cost = 0;
        if let Some(this) = self.terms.get("this") {
            if !scope.contains(&this) {
                provides.add(&this);
                cost += ENTITY_COST
            }
        }

        // Convert the "this" term from Term<Value> to Term<Entity>
        let this_entity: Term<Entity> = if let Some(this_value) = self.terms.get("this") {
            match this_value {
                Term::Variable { name, .. } => Term::<Entity>::Variable {
                    name: name.clone(),
                    _type: Type::default(),
                },
                Term::Constant(value) => {
                    // If it's a constant, it should be an Entity value
                    if let Value::Entity(entity) = value {
                        Term::Constant(entity.clone())
                    } else {
                        // Fallback to a variable if not an entity
                        Term::<Entity>::var(&format!("this_{}", self.concept.operator))
                    }
                }
            }
        } else {
            // Create a unique variable if "this" is not provided
            Term::<Entity>::var(&format!("this_{}", self.concept.operator))
        };

        let mut premises = vec![];

        // go over dependencies to add all the terms that will be derived
        // by the application to the `provides` list.
        for (name, attribute) in self.concept.attributes.iter() {
            let parameter = self.terms.get(name);
            // If parameter was not provided we add it to the provides set
            if let Some(term) = parameter {
                if !scope.contains(&term) {
                    provides.add(&term);
                    cost += VALUE_COST
                }

                let select = FactApplication::new()
                    .the(attribute.the())
                    .of(this_entity.clone())
                    .is(term.clone());

                premises.push(select.into());
            }
        }

        let mut join = Join::new(&premises);
        let (added_cost, conjuncts) = join.plan(scope)?;

        Ok(ConceptPlan {
            cost: cost + added_cost,
            provides,
            conjuncts,
        })
    }
}

impl Display for ConcetApplication {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {{", self.concept.operator)?;
        for (name, term) in self.terms.iter() {
            write!(f, "{}: {},", name, term)?;
        }

        write!(f, "}}")
    }
}
