use crate::analyzer::{Analysis, Planner, PremisePlan, Viable};
use crate::error::CompileError;
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
        candidates: Vec<(&'a Premise, Analysis)>,
    },
}

pub struct Candidate<'a> {
    /// Things we have inferred during planning of the premise.
    analysis: Analysis,
    /// Premise that we have analyzed.
    premise: &'a Premise,
}

impl<'a> Join<'a> {
    /// Creates a new planner for the given premises.
    pub fn new(premises: &'a Vec<Premise>) -> Self {
        Self::Idle { premises }
    }

    /// Helper to create a planning error from failed candidates.
    /// Returns the first error found, or UnexpectedError if none.
    fn fail(
        candidates: &[(&'_ Premise, Analysis)],
    ) -> Result<(&'a Premise, VariableScope), CompileError> {
        for (_, plan) in candidates {
            match plan {
                Analysis::Incomplete { required, .. } => Err(CompileError::RequiredBindings {
                    required: required.clone(),
                }),
                Analysis::Candidate { .. } => Ok(()),
            }?;
        }

        unreachable!("Shound have had at least on candidate");
    }

    /// Checks if planning is complete (all premises have been planned).
    fn done(&self) -> bool {
        match self {
            Self::Idle { .. } => false,
            Self::Active { candidates } => candidates.len() == 0,
        }
    }

    /// Creates an optimized execution plan for all premises.
    /// Returns ordered list of ready plans and the final variable scope.
    pub fn plan(
        &mut self,
        scope: &VariableScope,
    ) -> Result<(Vec<PremisePlan<Viable>>, VariableScope), CompileError> {
        let mut bound = scope.clone();
        let mut delta = scope.clone();
        let mut plans = vec![];

        while !self.done() {
            let (premise, provides) = self.top(&delta)?;

            delta = bound.extend(&provides);

            // Create a ready plan from the premise
            // We know it's ready because top() only returns Analysis::Candidate
            let analysis = Planner::plan(premise, &bound);
            let ready_plan = analysis
                .into_ready_plan(premise.clone())
                .expect("Premise from top() should be ready");

            plans.push(ready_plan);

            bound = delta.clone();
        }

        Ok((plans, bound))
    }
    /// Selects and returns the best premise to execute next based on cost.
    /// Updates the planner state by removing the selected premise from candidates.
    pub fn top(
        &mut self,
        differential: &VariableScope,
    ) -> Result<(&'_ Premise, VariableScope), CompileError> {
        let mut best: Option<(usize, &'_ Premise, Analysis, VariableScope, usize)> = None;
        match self {
            Join::Idle { premises } => {
                let mut candidates = vec![];
                let env = differential.clone();
                for (index, premise) in premises.iter().enumerate() {
                    let plan = Planner::plan(premise, &env);

                    match &plan {
                        Analysis::Candidate { cost, desired, .. } => {
                            if let Some((top, _, _, _, _)) = &best {
                                if cost < top {
                                    best = Some((
                                        *cost,
                                        premise,
                                        plan.clone(),
                                        desired.clone().into(),
                                        index,
                                    ));
                                }
                            } else {
                                best = Some((
                                    *cost,
                                    premise,
                                    plan.clone(),
                                    desired.clone().into(),
                                    index,
                                ));
                            }
                        }
                        Analysis::Incomplete { .. } => {}
                    }

                    candidates.push((premise, plan));
                }

                if let Some((_, premise, _, provides, index)) = best {
                    candidates.remove(index);
                    *self = Join::Active { candidates };

                    Ok((premise, provides))
                } else {
                    Self::fail(&candidates)
                }
            }
            Join::Active { candidates } => {
                for (index, (premise, plan)) in candidates.iter_mut().enumerate() {
                    Planner::update(*premise, plan, &differential);

                    // Clone the plan before matching to avoid borrow issues
                    let plan_copy = plan.clone();

                    match &plan_copy {
                        Analysis::Candidate { cost, desired, .. } => {
                            let provides = desired.clone().into();
                            if let Some((top, _, _, _, _)) = best {
                                if *cost < top {
                                    best = Some((*cost, premise, plan_copy, provides, index));
                                }
                            } else {
                                best = Some((*cost, premise, plan_copy, provides, index));
                            }
                        }
                        Analysis::Incomplete { .. } => {}
                    }
                }

                if let Some((_, premise, _, scope, index)) = best {
                    candidates.remove(index);

                    Ok((premise, scope))
                } else {
                    Self::fail(&candidates)
                }
            }
        }
    }
}
