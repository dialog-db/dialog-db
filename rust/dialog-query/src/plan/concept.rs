use crate::{try_stream, Dependencies};

use crate::{DeductiveRule, Parameters};

use super::super::predicate::Concept;
use super::{EvaluationContext, EvaluationPlan, Selection, Source, VariableScope};
use crate::{Term, Value};

/// Execution plan for a concept application.
/// Contains the cost estimate, variables that will be provided by execution,
/// and the individual sub-plans that need to be executed and joined.
#[derive(Debug, Clone, PartialEq)]
pub struct ConceptPlan {
    pub concept: Concept,
    pub terms: Parameters,

    /// Estimated execution cost for this plan.
    pub cost: usize,
    /// Variables that will be bound by executing this plan.
    pub provides: VariableScope,

    pub dependencies: Dependencies,
    // /// Individual sub-plans that must all succeed for the concept to match.
    // pub conjuncts: Vec<Plan>,
}
impl EvaluationPlan for ConceptPlan {
    fn cost(&self) -> usize {
        self.cost
    }
    fn provides(&self) -> &VariableScope {
        &self.provides
    }

    fn evaluate<S: Source, M: Selection>(
        &self,
        context: EvaluationContext<S, M>,
    ) -> impl Selection {
        let implicit = DeductiveRule::try_from(&self.concept).expect("Failed to compile implicit rule");
        let mut scope = VariableScope::new();
        let application = implicit.apply(self.terms.clone()).expect("Failed to apply rule");
        for (name, term) in self.terms.iter() {
            if term.is_constant() {
                scope.add(&Term::<Value>::var(name));
            }
        }
        let plan = application.plan(&scope).expect("Failed to plan application");
        try_stream! {
            for await item in plan.evaluate(context) {
                yield item?;
            }
        }

        // let mut _disjuncts = context.source.resolve_rules(&self.concept.operator);
        // _disjuncts.push(DeductiveRule::from(&self.concept));

        // for rule in disjuncts {
        //     rule.apply()
        // }

        // let join = Join::from(self.conjuncts.clone());
        // join.evaluate(context)
    }
}
