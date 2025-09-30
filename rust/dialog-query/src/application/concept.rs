use super::fact::{BASE_COST, ENTITY_COST, VALUE_COST};
use crate::analyzer::Planner;
use crate::analyzer::{AnalyzerError, LegacyAnalysis};
use crate::error::PlanError;
use crate::plan::ConceptPlan;
use crate::predicate::Concept;
use crate::{
    Dependencies, EvaluationContext, Parameters, Requirement, Selection, Source, Term, Value,
    VariableScope,
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
        for (name, requirement) in analysis.dependencies.iter() {
            let term: Term<Value> = Term::var(name);
            if !scope.contains(&term) {
                provides.add(&term);
                // No variable can be required on the concept application
                if let Requirement::Derived(overhead) = requirement {
                    cost += overhead;
                }
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

    fn evaluate<S: Source, M: Selection>(
        &self,
        context: EvaluationContext<S, M>,
    ) -> impl Selection {
        // let mut scope = VariableScope::new();
        // // If we some parameters are bound to constants we can optimize
        // // evaluation order
        // for (name, term) in self.terms.iter() {
        //     if matches!(term, Term::Constant(_)) {
        //         scope.add(&Term::var(name));
        //     }
        // }
        // TODO: Phase 7 - Implement concept evaluation using context.scope
        // This needs to resolve the concept's rule, plan execution order based on bound variables,
        // and evaluate the premises

        // let implicit = DeductiveRule::from(&self.concept);
        // let join = Join::new(&implicit.premises).plan(&scope);
        context.selection
    }
}

impl Planner for ConceptApplication {
    fn init(&self, plan: &mut crate::analyzer::Analysis, env: &VariableScope) {
        let blank = Term::blank();
        for operand in self.concept.operands() {
            let term = self.terms.get(operand).unwrap_or(&blank);
            if env.contains(term) {
                plan.desire(term, 0);
            } else {
                plan.desire(term, VALUE_COST);
            }
        }
    }

    fn update(&self, plan: &mut crate::analyzer::Analysis, env: &VariableScope) {
        let blank = Term::blank();
        for operand in self.concept.operands() {
            let term = self.terms.get(operand).unwrap_or(&blank);
            if env.contains(term) {
                plan.desire(term, 0);
            }
        }
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
