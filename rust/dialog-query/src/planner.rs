use crate::analyzer::{Analysis, JoinPlan, Plan};
use crate::artifact::Value;
use crate::error::CompileError;
pub use crate::error::{AnalyzerError, PlanError};
pub use crate::plan::EvaluationPlan;
pub use crate::premise::Premise;
pub use crate::term::Term;
pub use crate::VariableScope;

/// Query planner that optimizes the order of premise execution based on cost
/// and dependency analysis. Uses a state machine approach to iteratively
/// select the best premise to execute next.
pub enum Join {
    /// Initial state with unprocessed premises.
    Idle { premises: Vec<Premise> },
    /// Processing state with cached candidates and current scope.
    Active { candidates: Vec<Analysis> },
}

impl Join {
    /// Creates a new planner for the given premises.
    pub fn new(premises: Vec<Premise>) -> Self {
        Self::Idle { premises }
    }

    /// Helper to create a planning error from failed candidates.
    fn fail(analyses: &[Analysis]) -> Result<Plan, CompileError> {
        // If there are no candidates at all, return empty Required
        if analyses.is_empty() {
            return Err(CompileError::RequiredBindings {
                required: crate::analyzer::Required::new(),
            });
        }

        // Return the first required bindings error we find
        for analysis in analyses {
            if let Analysis::Blocked { requires, .. } = analysis {
                if requires.count() > 0 {
                    return Err(CompileError::RequiredBindings {
                        required: requires.clone(),
                    });
                }
            }
        }

        unreachable!("Should have had at least one blocked candidate with requirements");
    }

    /// Checks if planning is complete (all premises have been planned).
    fn done(&self) -> bool {
        match self {
            Self::Idle { .. } => false,
            Self::Active { candidates } => candidates.len() == 0,
        }
    }

    /// Creates an optimized execution plan for all premises.
    /// Returns a JoinPlan with the ordered steps, cost, and variable scopes.
    pub fn plan(
        &mut self,
        scope: &VariableScope,
    ) -> Result<JoinPlan, CompileError> {
        let env = scope.clone();
        let mut bound = scope.clone();
        let mut steps = vec![];
        let mut cost = 0;

        while !self.done() {
            let plan = self.top(&bound)?;

            cost += plan.cost;
            // Extend the scope with what this premise binds
            bound.extend(&plan.binds);

            steps.push(plan);
        }

        // binds is the difference between final scope and initial env
        let mut binds = VariableScope::new();
        for var_name in &bound.variables {
            let var: Term<Value> = Term::var(var_name);
            if !env.contains(&var) {
                binds.add(&var);
            }
        }

        Ok(JoinPlan {
            steps,
            cost,
            binds,
            env,
        })
    }
    /// Selects and returns the best premise to execute next based on cost.
    /// Updates the planner state by removing the selected premise from candidates.
    pub fn top(&mut self, env: &VariableScope) -> Result<Plan, CompileError> {
        match self {
            Join::Idle { premises } => {
                let mut candidates = vec![];
                let mut best: Option<(usize, usize)> = None; // (cost, index)

                // Analyze each premise to create initial candidates
                for (index, premise) in premises.iter().enumerate() {
                    let analysis = premise.analyze(env);

                    // Check if this analysis is viable
                    if analysis.is_viable() {
                        let cost = analysis.cost();

                        if let Some((best_cost, _)) = &best {
                            if cost < *best_cost {
                                best = Some((cost, index));
                            }
                        } else {
                            best = Some((cost, index));
                        }
                    }

                    candidates.push(analysis);
                }

                if let Some((_, best_index)) = best {
                    let analysis = candidates.remove(best_index);
                    *self = Join::Active { candidates };
                    Plan::try_from(analysis)
                } else {
                    Self::fail(&candidates)
                }
            }
            Join::Active { candidates } => {
                let mut best: Option<(usize, usize)> = None; // (cost, index)

                // Update all candidates with new bindings
                for (index, analysis) in candidates.iter_mut().enumerate() {
                    // Update this analysis with the current environment
                    analysis.update(env);

                    // Check if this analysis is now viable
                    if analysis.is_viable() {
                        let cost = analysis.cost();

                        if let Some((best_cost, _)) = &best {
                            if cost < *best_cost {
                                best = Some((cost, index));
                            }
                        } else {
                            best = Some((cost, index));
                        }
                    }
                }

                if let Some((_, best_index)) = best {
                    let analysis = candidates.remove(best_index);
                    Plan::try_from(analysis)
                } else {
                    Self::fail(&candidates)
                }
            }
        }
    }
}
