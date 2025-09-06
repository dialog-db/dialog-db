use crate::artifact::Value;
use crate::plan::{EvaluationContext, EvaluationPlan};
use crate::query::Store;
use crate::stream::fork_stream;
use crate::syntax::VariableScope;
use crate::term::Term;
use crate::{FactSelectorPlan, Selection};
use async_stream::try_stream;
use futures_util::{
    stream::{once, TryStreamExt},
    stream_select,
};

#[derive(Debug, Clone, PartialEq)]
pub enum Combinator {
    Noop,
    Select(FactSelectorPlan<Value>),
    // FormulaApplication {
    //     name: String,
    //     terms: Vec<(String, Term<Value>)>,
    // },
    Deduce {
        name: String,
        terms: Vec<(String, Term<Value>)>,
    },
    And(Box<Combinator>, Box<Combinator>),
    Or(Box<Combinator>, Box<Combinator>),
    Not(Box<Combinator>),
}

impl EvaluationPlan for Combinator {
    fn cost(&self) -> usize {
        match self {
            Combinator::Noop => 0,
            Combinator::Select(plan) => plan.cost(),
            // Combinator::FormulaApplication { name, terms } => {
            //     terms.iter().map(|(_, term)| term.cost()).sum()
            // }
            Combinator::Deduce { name, terms } => {
                for (id, term) in terms {
                    if (term.is_named_variable()) {
                        // Handle named variable case
                    }
                }
                unimplemented!()
            }
            Combinator::And(left, right) => left.cost() + right.cost(),
            Combinator::Or(left, right) => left.cost() + right.cost(),
            Combinator::Not(child) => child.cost(),
        }
    }

    fn provides(&self) -> VariableScope {
        match self {
            Combinator::Noop => VariableScope::new(),
            Combinator::Select(plan) => plan.provides(),
            Combinator::And(left, right) => left.provides().union(right.provides()),
            Combinator::Or(left, right) => left.provides().intersection(right.provides()),
            Combinator::Not(_) => VariableScope::new(),
            Combinator::Deduce { name: _, terms: _ } => unimplemented!("Not implemented yet"),
        }
    }

    fn evaluate<S: Store, M: Selection>(&self, context: EvaluationContext<S, M>) -> impl Selection {
        let combinator = self.clone();
        try_stream! {
            match combinator {
                Combinator::Noop => {
                    for await frame in context.selection {
                        yield frame?;
                    }
                }
                Combinator::Select(plan) => {
                    let source = plan.evaluate(context);

                    for await frame in source {
                        yield frame?;
                    }
                }
                Combinator::And(left, right) => {
                    let store = context.store.clone();
                    let source = left.evaluate(context);
                    let selection = right.evaluate(EvaluationContext { selection: source, store });

                    for await frame in selection {
                        yield frame?;
                    }
                }
                Combinator::Or(left, right) => {
                    let (left_source, right_source) = fork_stream(context.selection);
                    let (left_store, right_store) = (context.store.clone(), context.store.clone());
                    let (left_selection, right_selection) = (
                        left.evaluate(EvaluationContext { selection: left_source, store: left_store }),
                        right.evaluate(EvaluationContext { selection: right_source, store: right_store }),
                    );

                    tokio::pin!(left_selection);
                    tokio::pin!(right_selection);

                    for await frame in stream_select!(left_selection, right_selection) {
                        yield frame?;
                    }
                }
                Combinator::Not(plan) => {
                    for await frame in context.selection {
                        let frame = frame?;
                        let except = frame.clone();
                        let source = EvaluationContext {
                            selection: once(async move { Ok(except) }),
                            store: context.store.clone()
                        };
                        let exclude = plan
                            .clone()
                            .evaluate(source);

                        tokio::pin!(exclude);

                        if let Ok(Some(_)) = exclude.try_next().await {
                            continue;
                        }

                        yield frame;
                    }
                }
                Combinator::Deduce { name: _, terms: _ } => unimplemented!("Not implemented yet"),
            }
        }
    }
}
