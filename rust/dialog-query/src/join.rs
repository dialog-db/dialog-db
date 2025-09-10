use crate::plan::EvaluationPlan;

use crate::{
    try_stream, EvaluationContext, Match, QueryError, Selection, Store, Term, Type, Value,
};
use futures_util::stream::BoxStream;

use crate::deductive_rule::Plan;

/// Represents a join operation that combines multiple query plans.
/// Uses a recursive structure to chain plans together.
#[derive(Debug, Clone)]
pub enum Join {
    /// Base case - passes through the selection unchanged.
    Zero,
    One(Plan),
    Two(Plan, Plan),
    Three(Plan, Plan, Plan),
    Four(Plan, Plan, Plan, Plan),
    Five(Plan, Plan, Plan, Plan, Plan),
    Six(Plan, Plan, Plan, Plan, Plan, Plan),
    Seven(Plan, Plan, Plan, Plan, Plan, Plan, Plan),
    Eight(Plan, Plan, Plan, Plan, Plan, Plan, Plan, Plan),
    Nine(Plan, Plan, Plan, Plan, Plan, Plan, Plan, Plan, Plan),
    Ten(Plan, Plan, Plan, Plan, Plan, Plan, Plan, Plan, Plan, Plan),
    /// Recursive case - joins a plan with the rest of the join chain.
    More(Box<Join>, Plan),
}

impl Join {
    pub fn new() -> Self {
        Join::Zero
    }

    pub fn from(plans: Vec<Plan>) -> Self {
        plans
            .into_iter()
            .fold(Join::Zero, |join, plan| join.and(plan))
    }

    pub fn and(self, plan: Plan) -> Self {
        match self {
            Self::Zero => Self::One(plan),
            Self::One(p0) => Self::Two(p0, plan),
            Self::Two(p0, p1) => Self::Three(p0, p1, plan),
            Self::Three(p0, p1, p2) => Self::Four(p0, p1, p2, plan),
            Self::Four(p0, p1, p2, p3) => Self::Five(p0, p1, p2, p3, plan),
            Self::Five(p0, p1, p2, p3, p4) => Self::Six(p0, p1, p2, p3, p4, plan),
            Self::Six(p0, p1, p2, p3, p4, p5) => Self::Seven(p0, p1, p2, p3, p4, p5, plan),
            Self::Seven(p0, p1, p2, p3, p4, p5, p6) => {
                Self::Eight(p0, p1, p2, p3, p4, p5, p6, plan)
            }
            Self::Eight(p0, p1, p2, p3, p4, p5, p6, p7) => {
                Self::Nine(p0, p1, p2, p3, p4, p5, p6, p7, plan)
            }
            Self::Nine(p0, p1, p2, p3, p4, p5, p6, p7, p8) => {
                Self::Ten(p0, p1, p2, p3, p4, p5, p6, p7, p8, plan)
            }
            Self::Ten(_, _, _, _, _, _, _, _, _, _) => Self::More(Box::new(self), plan),
            Self::More(_, _) => Self::More(Box::new(self), plan),
        }
    }

    /// Evaluates the join by executing each plan in sequence,
    /// feeding the output of one plan as input to the next.
    pub fn evaluate<S: Store, M: Selection>(
        self,
        context: EvaluationContext<S, M>,
    ) -> impl Selection {
        Self::execute(self, context)
    }

    pub fn execute<S: Store, M: Selection>(
        join: Join,
        context: EvaluationContext<S, M>,
    ) -> BoxStream<'static, Result<Match, QueryError>> {
        match join {
            Join::Zero => Box::pin(try_stream! {
                for await each in context.selection {
                    yield each?;
                }
            }),
            Join::One(plan) => Box::pin(try_stream! {
                for await each in plan.evaluate(context) {
                    yield each?;
                }
            }),
            Join::Two(p0, p1) => Box::pin(try_stream! {
                let s1 = p0.evaluate(EvaluationContext {
                   store: context.store.clone(),
                   selection: context.selection
                });

                let s2 = p1.evaluate(EvaluationContext {
                   store: context.store,
                   selection: s1
                });

                for await each in s2 {
                    yield each?;
                }
            }),
            Join::Three(p0, p1, p2) => Box::pin(try_stream! {
                let s1 = p0.evaluate(EvaluationContext {
                   store: context.store.clone(),
                   selection: context.selection
                });

                let s2 = p1.evaluate(EvaluationContext {
                   store: context.store.clone(),
                   selection: s1
                });

                let s3 = p2.evaluate(EvaluationContext {
                   store: context.store,
                   selection: s2
                });

                for await each in s3 {
                    yield each?;
                }
            }),
            Join::Four(p0, p1, p2, p3) => Box::pin(try_stream! {
                let s1 = p0.evaluate(EvaluationContext {
                   store: context.store.clone(),
                   selection: context.selection
                });
                let s2 = p1.evaluate(EvaluationContext {
                   store: context.store.clone(),
                   selection: s1
                });
                let s3 = p2.evaluate(EvaluationContext {
                   store: context.store.clone(),
                   selection: s2
                });
                let s4 = p3.evaluate(EvaluationContext {
                   store: context.store,
                   selection: s3
                });
                for await each in s4 {
                    yield each?;
                }
            }),
            Join::Five(p0, p1, p2, p3, p4) => Box::pin(try_stream! {
                let s1 = p0.evaluate(EvaluationContext {
                   store: context.store.clone(),
                   selection: context.selection
                });
                let s2 = p1.evaluate(EvaluationContext {
                   store: context.store.clone(),
                   selection: s1
                });
                let s3 = p2.evaluate(EvaluationContext {
                   store: context.store.clone(),
                   selection: s2
                });
                let s4 = p3.evaluate(EvaluationContext {
                   store: context.store.clone(),
                   selection: s3
                });
                let s5 = p4.evaluate(EvaluationContext {
                   store: context.store,
                   selection: s4
                });
                for await each in s5 {
                    yield each?;
                }
            }),
            Join::Six(p0, p1, p2, p3, p4, p5) => Box::pin(try_stream! {
                let s1 = p0.evaluate(EvaluationContext {
                   store: context.store.clone(),
                   selection: context.selection
                });
                let s2 = p1.evaluate(EvaluationContext {
                   store: context.store.clone(),
                   selection: s1
                });
                let s3 = p2.evaluate(EvaluationContext {
                   store: context.store.clone(),
                   selection: s2
                });
                let s4 = p3.evaluate(EvaluationContext {
                   store: context.store.clone(),
                   selection: s3
                });
                let s5 = p4.evaluate(EvaluationContext {
                   store: context.store.clone(),
                   selection: s4
                });
                let s6 = p5.evaluate(EvaluationContext {
                   store: context.store,
                   selection: s5
                });
                for await each in s6 {
                    yield each?;
                }
            }),
            Join::Seven(p0, p1, p2, p3, p4, p5, p6) => Box::pin(try_stream! {
                let s1 = p0.evaluate(EvaluationContext {
                   store: context.store.clone(),
                   selection: context.selection
                });
                let s2 = p1.evaluate(EvaluationContext {
                   store: context.store.clone(),
                   selection: s1
                });
                let s3 = p2.evaluate(EvaluationContext {
                   store: context.store.clone(),
                   selection: s2
                });
                let s4 = p3.evaluate(EvaluationContext {
                   store: context.store.clone(),
                   selection: s3
                });
                let s5 = p4.evaluate(EvaluationContext {
                   store: context.store.clone(),
                   selection: s4
                });
                let s6 = p5.evaluate(EvaluationContext {
                   store: context.store.clone(),
                   selection: s5
                });
                let s7 = p6.evaluate(EvaluationContext {
                   store: context.store,
                   selection: s6
                });
                for await each in s7 {
                    yield each?;
                }
            }),
            Join::Eight(p0, p1, p2, p3, p4, p5, p6, p7) => Box::pin(try_stream! {
                let s1 = p0.evaluate(EvaluationContext {
                   store: context.store.clone(),
                   selection: context.selection
                });
                let s2 = p1.evaluate(EvaluationContext {
                   store: context.store.clone(),
                   selection: s1
                });
                let s3 = p2.evaluate(EvaluationContext {
                   store: context.store.clone(),
                   selection: s2
                });
                let s4 = p3.evaluate(EvaluationContext {
                   store: context.store.clone(),
                   selection: s3
                });
                let s5 = p4.evaluate(EvaluationContext {
                   store: context.store.clone(),
                   selection: s4
                });
                let s6 = p5.evaluate(EvaluationContext {
                   store: context.store.clone(),
                   selection: s5
                });
                let s7 = p6.evaluate(EvaluationContext {
                   store: context.store.clone(),
                   selection: s6
                });
                let s8 = p7.evaluate(EvaluationContext {
                   store: context.store,
                   selection: s7
                });
                for await each in s8 {
                    yield each?;
                }
            }),
            Join::Nine(p0, p1, p2, p3, p4, p5, p6, p7, p8) => Box::pin(try_stream! {
                let s1 = p0.evaluate(EvaluationContext {
                   store: context.store.clone(),
                   selection: context.selection
                });
                let s2 = p1.evaluate(EvaluationContext {
                   store: context.store.clone(),
                   selection: s1
                });
                let s3 = p2.evaluate(EvaluationContext {
                   store: context.store.clone(),
                   selection: s2
                });
                let s4 = p3.evaluate(EvaluationContext {
                   store: context.store.clone(),
                   selection: s3
                });
                let s5 = p4.evaluate(EvaluationContext {
                   store: context.store.clone(),
                   selection: s4
                });
                let s6 = p5.evaluate(EvaluationContext {
                   store: context.store.clone(),
                   selection: s5
                });
                let s7 = p6.evaluate(EvaluationContext {
                   store: context.store.clone(),
                   selection: s6
                });
                let s8 = p7.evaluate(EvaluationContext {
                   store: context.store.clone(),
                   selection: s7
                });
                let s9 = p8.evaluate(EvaluationContext {
                   store: context.store,
                   selection: s8
                });
                for await each in s9 {
                    yield each?;
                }
            }),
            Join::Ten(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9) => Box::pin(try_stream! {
                let s1 = p0.evaluate(EvaluationContext {
                   store: context.store.clone(),
                   selection: context.selection
                });
                let s2 = p1.evaluate(EvaluationContext {
                   store: context.store.clone(),
                   selection: s1
                });
                let s3 = p2.evaluate(EvaluationContext {
                   store: context.store.clone(),
                   selection: s2
                });
                let s4 = p3.evaluate(EvaluationContext {
                   store: context.store.clone(),
                   selection: s3
                });
                let s5 = p4.evaluate(EvaluationContext {
                   store: context.store.clone(),
                   selection: s4
                });
                let s6 = p5.evaluate(EvaluationContext {
                   store: context.store.clone(),
                   selection: s5
                });
                let s7 = p6.evaluate(EvaluationContext {
                   store: context.store.clone(),
                   selection: s6
                });
                let s8 = p7.evaluate(EvaluationContext {
                   store: context.store.clone(),
                   selection: s7
                });
                let s9 = p8.evaluate(EvaluationContext {
                   store: context.store.clone(),
                   selection: s8
                });
                let s10 = p9.evaluate(EvaluationContext {
                   store: context.store,
                   selection: s9
                });
                for await each in s10 {
                    yield each?;
                }
            }),
            Join::More(left, right) => Box::pin(try_stream! {
                let store = context.store.clone();
                let selection = Self::execute(*left, context);
                let output = right.evaluate(EvaluationContext { selection, store });
                for await each in output {
                    yield each?;
                }
            }),
        }
    }
}
