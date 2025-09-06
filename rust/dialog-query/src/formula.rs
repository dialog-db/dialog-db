use crate::{try_stream, EvaluationContext, QueryError, Selection, Store};
use crate::{Match, Term};

use std::fmt::Debug;
use thiserror::Error;

pub trait Formula: Sized + Clone + Debug {
    type Input;
    type Output;

    fn expand(&self, terms: Self::Input) -> Result<Vec<Self::Output>, FormulaEvaluationError>;

    fn new(self) -> FormulaApplication
    where
        Self: FormulaWrapper,
    {
        FormulaApplication {
            terms: Match::new(),
            formula: Box::new(self) as Box<dyn FormulaWrapper>,
        }
    }
}

#[derive(Error, Debug, Clone, PartialEq)]
pub enum FormulaEvaluationError {
    #[error("Required cell '{name}' has no value")]
    ReadError { name: String },
}

impl<T: ?Sized> Formula for Box<T>
where
    T: Formula,
{
    type Input = T::Input;
    type Output = T::Output;

    fn expand(&self, args: Self::Input) -> Result<Vec<Self::Output>, FormulaEvaluationError> {
        (**self).expand(args)
    }
}

pub trait FormulaWrapper: Send + Sync + Debug + 'static {
    fn apply(&self, terms: Match) -> Result<Vec<Match>, FormulaEvaluationError>;
    fn clone_box(&self) -> Box<dyn FormulaWrapper>;
}

impl<T, I, O> FormulaWrapper for T
where
    T: Formula<Input = I, Output = O> + Send + Sync + Clone + 'static,
    I: TryFrom<Match, Error = ()>,
    O: TryInto<Match, Error = ()>,
{
    fn apply(&self, source: Match) -> Result<Vec<Match>, FormulaEvaluationError> {
        let frame = <T::Input as TryFrom<Match>>::try_from(source).unwrap();

        let mut output: Vec<Match> = Vec::new();
        for frame in T::expand(self, frame)? {
            output.push(frame.try_into().unwrap());
        }

        Ok(output)
    }

    fn clone_box(&self) -> Box<dyn FormulaWrapper> {
        Box::new(self.clone())
    }
}

impl Clone for Box<dyn FormulaWrapper> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

#[derive(Clone, Debug)]
pub struct FormulaApplication {
    terms: Match,
    formula: Box<dyn FormulaWrapper>,
}

impl FormulaApplication {
    fn apply(&self, terms: Match) -> Result<Vec<Match>, FormulaEvaluationError> {
        self.formula.apply(terms)
    }

    fn evaluate<S: Store, M: Selection>(&self, context: EvaluationContext<S, M>) -> impl Selection {
        let formula = self.formula.clone();
        try_stream! {
            for await source in context.selection {
                let frame = source?;
                let output = formula.apply(frame).map_err(|e| match e {
                    FormulaEvaluationError::ReadError { name } => {
                        QueryError::UnboundVariable { variable_name: name }
                    }
                })?;

                for frame in output {
                    yield frame;
                }
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
struct Inc;

struct IncInput {
    pub of: i32,
}

impl TryFrom<Match> for IncInput {
    type Error = ();

    fn try_from(match_: Match) -> Result<Self, Self::Error> {
        let of = match_.get::<i32>(&Term::var("of")).map_err(|_| ())?;
        Ok(IncInput { of })
    }
}

struct IncOutput {
    pub is: i32,
}

impl TryInto<Match> for IncOutput {
    type Error = ();

    fn try_into(self) -> Result<Match, Self::Error> {
        Match::new()
            .set::<i32>(Term::var("is"), self.is)
            .map_err(|_| ())
    }
}

impl Formula for Inc {
    type Input = IncInput;
    type Output = IncOutput;

    fn expand(&self, input: Self::Input) -> Result<Vec<Self::Output>, FormulaEvaluationError> {
        Ok(vec![IncOutput { is: input.of + 1 }])
    }
}

#[test]
fn test_formula() {
    let inc = Inc.new();

    let input = Match::new().set::<i32>(Term::var("of"), 0).unwrap();
    let result = inc.apply(input).unwrap();
    let expected = Match::new().set::<i32>(Term::var("is"), 1).unwrap();
    assert_eq!(result, vec![expected]);
}
