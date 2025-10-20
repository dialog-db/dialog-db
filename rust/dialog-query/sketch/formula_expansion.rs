use async_stream::try_stream;
use dialog_query::application::FormulaApplication;
use dialog_query::deductive_rule::{AnalyzerError, Cell};
use dialog_query::error::AnalyzerError;
use dialog_query::{EvaluationContext, Match, Paramateres, Selection, Store};

// #[formula]
/// Increment a number by one
fn inc(of: isize) -> isize {
    of + 1
}

pub struct Inc {
    pub operator: &'static str,
    pub operands: &'static IncCells,
    pub cost: &'static usize,
}

pub struct IncCells {
    pub of: Cell,
    pub is: Cell,
}

trait IntoIter {
    type Item;
    type IntoIter: Iterator<Item = Self::Item>;

    fn into_iter() -> Self::IntoIter;
}

impl IntoIter for IncCells {
    type Item = (&str, &Cell);
    type IntoIter = [Self::Item; constant];

    fn into_iter() -> Self::IntoIter {
        [
            (&INC.operands.of.name, &INC.operands.of),
            (&INC.operands.is.name, &INC.operands.is),
        ]
    }
}

pub static INC: Inc = Inc {
    cost: &1,
    operator: &"inc",
    operands: &IncCells {
        of: Cell {
            name: &"of",
            description: &"Source to increment",
            requirement: Requirement::Required,
            data_type: Type::SignedInteger,
        },
        is: Cell {
            name: &"is",
            description: &"Output of the increment",
            requirement: Requirement::Derived(0),
            data_type: Type::SignedInteger,
        },
    },
};

pub struct IncMatch {
    formula: &'static Increment,
    terms: Terms,
}
impl FormulaApplication for IncMatch {
    fn process(&self, source: Match) -> Vec<Match> {
        // Get constants for the input cells
        let of = match self.terms.get(INC.operands.of.name) {
            Some(of) => source.get(of).ok(),
            None => None,
        };

        // Get terms for the outputs cells
        let is = self.terms.get(INC.operands.is.name);

        match (of, is) {
            (Some(of), Some(is)) => {
                // Compute results
                let result = inc(of::into());
                // Write results into output cells
                let mut output = source.unify(is.into(), result.into());
                // If success we produce a frame otherwise we consume it
                // and produce no frames.
                if let Ok(frame) = output {
                    vec![frame]
                } else {
                    vec![]
                }
            }
            _ => vec![],
        }
    }
}

impl Formula for Inc {
    type Cells = IncCells;
    type Match = IncMatch;

    fn new() -> &'static Self {
        &INC
    }
}

trait Formula {
    type Cells: IntoIter<Item = (&str, Cell)>;
    type Match;

    fn new() -> &'static Self;
    fn apply(terms: Terms) -> Result<Self::Match, AnalyzerError> {
        let formula = Self::new();

        // Iterate over all the cells and ensure that we have terms for
        // all the required cells.
        for (name, cell) in Self::Cells::into_iter() {
            if (cell.requirement.is_required()) {
                terms.get(name).ok_or(PlanError::OmitsRequiredCell {
                    formula,
                    cell: name,
                })?;
            }
        }

        // now we can create a formula application.
        Ok(Self::Match { formula, terms })
    }
}

trait FormulaApplication {
    fn process(&self, source: Match) -> Vec<Match>;

    pub fn evaluate<S: Store, M: Selection>(
        &self,
        context: EvaluationContext<S, M>,
    ) -> impl Selection {
        try_stream! {
            for await input in context.selection {
                for output in self.process(input?) {
                    yield output;
                }
            }
        }
    }
}
