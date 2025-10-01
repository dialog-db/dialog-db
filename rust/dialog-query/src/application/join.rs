pub use crate::error::{AnalyzerError, PlanError};
pub use crate::plan::{EvaluationPlan, Plan};
pub use crate::premise::Premise;
pub use crate::VariableScope;

/// Query planner that optimizes the order of premise execution based on cost
/// and dependency analysis. Uses a state machine approach to iteratively
/// select the best premise to execute next.
pub enum Join<'a> {
    /// Initial state with unprocessed premises.
    Idle { premises: &'a Vec<Premise> },
    /// Processing state with cached candidates and current scope.
    Active {
        candidates: Vec<PlanCandidate<'a>>,
        scope: VariableScope,
    },
}

impl<'a> Join<'a> {
    /// Creates a new planner for the given premises.
    pub fn new(premises: &'a Vec<Premise>) -> Self {
        Self::Idle { premises }
    }

    /// Helper to create a planning error from failed candidates.
    /// Returns the first error found, or UnexpectedError if none.
    fn fail(candidates: &[PlanCandidate]) -> Result<Plan, PlanError> {
        for candidate in candidates {
            match &candidate.result {
                Err(error) => {
                    return Err(error.clone());
                }
                _ => {}
            }
        }

        return Err(PlanError::UnexpectedError);
    }

    /// Checks if planning is complete (all premises have been planned).
    fn done(&self) -> bool {
        match self {
            Self::Idle { .. } => false,
            Self::Active { candidates, .. } => candidates.len() == 0,
        }
    }

    /// Creates an optimized execution plan for all premises.
    /// Returns the total cost and ordered list of sub-plans to execute.
    pub fn plan(&mut self, scope: &VariableScope) -> Result<(usize, Vec<Plan>), PlanError> {
        let plan = self.top(scope)?;
        let mut cost = plan.cost();

        let mut scope = scope.clone();
        let mut delta = scope.extend(plan.provides());
        let mut conjuncts = vec![plan];

        while !self.done() {
            let plan = self.top(&delta)?;

            cost += plan.cost();
            delta = scope.extend(plan.provides());

            conjuncts.push(plan);
        }

        Ok((cost, conjuncts))
    }
    /// Selects and returns the best premise to execute next based on cost.
    /// Updates the planner state by removing the selected premise from candidates.
    fn top(&mut self, differential: &VariableScope) -> Result<Plan, PlanError> {
        match self {
            Join::Idle { premises } => {
                let mut best: Option<(Plan, usize)> = None;
                let mut candidates = vec![];
                for (index, premise) in premises.iter().enumerate() {
                    let analysis = premise.analyze_legacy();
                    let result = premise.plan(differential);

                    // Check if this is the best plan so far
                    if let Ok(plan) = &result {
                        if let Some((top, _)) = &best {
                            if plan < top {
                                best = Some((plan.clone(), index));
                            }
                        } else {
                            best = Some((plan.clone(), index));
                        }
                    }

                    let mut dependencies = VariableScope::new();

                    for (name, _) in analysis.dependencies.iter() {
                        dependencies.variables.insert(name.into());
                    }

                    candidates.push(PlanCandidate {
                        premise,
                        dependencies,
                        result,
                    });
                }

                if let Some((plan, index)) = best {
                    candidates.remove(index);
                    *self = Join::Active {
                        candidates,
                        scope: differential.clone(),
                    };

                    Ok(plan)
                } else {
                    Self::fail(&candidates)
                }
            }
            Join::Active {
                candidates, scope, ..
            } => {
                let mut best: Option<(Plan, usize)> = None;
                for (index, candidate) in candidates.iter_mut().enumerate() {
                    // Check if we need to recompute based on delta
                    if candidate.dependencies.intersects(&differential) {
                        candidate.plan(&scope);
                    }

                    if let Ok(plan) = &candidate.result {
                        if let Some((top, _)) = &best {
                            if plan < top {
                                best = Some((plan.clone(), index));
                            }
                        } else {
                            best = Some((plan.clone(), index));
                        }
                    }
                }

                if let Some((plan, index)) = best {
                    candidates.remove(index);

                    Ok(plan)
                } else {
                    Self::fail(&candidates)
                }
            }
        }
    }
}

/// Represents a premise candidate during query planning.
/// Caches the premise's dependencies and planning result to avoid recomputation.
#[derive(Debug, Clone)]
pub struct PlanCandidate<'a> {
    /// Reference to the premise being planned.
    pub premise: &'a Premise,
    /// Variables that this premise depends on.
    pub dependencies: VariableScope,
    /// Cached planning result for this premise.
    pub result: Result<Plan, PlanError>,
}

impl<'a> PlanCandidate<'a> {
    /// Re-plans this premise with the given scope and updates the cached result.
    fn plan(&mut self, scope: &VariableScope) -> &Self {
        self.result = self.premise.plan(scope);
        self
    }
}

#[test]
fn test_planner_creation() {
    let premises = vec![];
    let join = Join::new(&premises);

    match join {
        Join::Idle { premises: p } => {
            assert_eq!(p.len(), 0);
        }
        _ => panic!("Expected Idle state"),
    }
}
