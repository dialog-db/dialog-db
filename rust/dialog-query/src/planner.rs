pub use crate::analyzer::{Environment, Stats, Syntax};
use crate::analyzer::{Planner, SyntaxAnalysis};
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
        candidates: Vec<(&'a Premise, SyntaxAnalysis)>,
    },
}

impl<'a> Join<'a> {
    /// Creates a new planner for the given premises.
    pub fn new(premises: &'a Vec<Premise>) -> Self {
        Self::Idle { premises }
    }

    /// Helper to create a planning error from failed candidates.
    /// Returns the first error found, or UnexpectedError if none.
    fn fail(
        candidates: &[(&'_ Premise, SyntaxAnalysis)],
    ) -> Result<(&'_ Premise, VariableScope), CompileError> {
        for (premise, plan) in candidates {
            match plan {
                SyntaxAnalysis::Incomplete {
                    cost,
                    required,
                    desired,
                } => Err(CompileError::RequiredBindings { required }),
                SyntaxAnalysis::Complete {
                    cost,
                    required,
                    desired,
                } => Ok(()),
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
    /// Returns the total cost and ordered list of sub-plans to execute.
    pub fn plan(
        &mut self,
        scope: &VariableScope,
    ) -> Result<(Vec<Premise>, VariableScope), CompileError> {
        let mut bound = scope.clone();
        let mut delta = scope;
        let mut conjuncts = vec![];

        while !self.done() {
            let (premise, provides) = self.top(delta)?;

            delta = &bound.extend(provides);

            conjuncts.push(premise.to_owned());
        }

        Ok((conjuncts, bound))
    }
    /// Selects and returns the best premise to execute next based on cost.
    /// Updates the planner state by removing the selected premise from candidates.
    pub fn top(
        &mut self,
        differential: &VariableScope,
    ) -> Result<(&'_ Premise, VariableScope), CompileError> {
        let mut best: Option<(usize, &'_ Premise, SyntaxAnalysis, VariableScope, usize)> = None;
        match self {
            Join::Idle { premises } => {
                let mut candidates = vec![];
                let env = differential.clone();
                for (index, premise) in premises.iter().enumerate() {
                    let plan = Planner::plan(premise, &env);

                    match plan {
                        SyntaxAnalysis::Candidate { cost, desired } => {
                            if let Some((top, _, _, _, _)) = &best {
                                if cost < *top {
                                    best = Some((cost, premise, plan, desired.into(), index));
                                }
                            } else {
                                best = Some((cost, premise, plan, desired.into(), index));
                            }
                        }
                        SyntaxAnalysis::Incomplete { .. } => {}
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
                    Planner::update(&self, plan, &differential);

                    match plan {
                        SyntaxAnalysis::Candidate { cost, desired } => {
                            if let Some((top, _, _, _, _)) = best {
                                if *cost < top {
                                    best =
                                        Some((*cost, premise, plan.into(), desired.into(), index));
                                }
                            } else {
                                best = Some((*cost, premise, plan.into(), desired.into(), index));
                            }
                        }
                        SyntaxAnalysis::Incomplete { .. } => {}
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
