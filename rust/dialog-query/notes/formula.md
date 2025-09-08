# Formulas

Define a formula like `Sum`

```rs
#[derive(Debug, Clone, Formula)]
pub struct Sum {
    of: usize,
    with: usize,
    // This is derived from the sum of `of` and `with`.
    #[derived]
    is: usize,
}

// You implement formula
impl Compute for Sum {
    fn compute(input: Self::Input) -> Vec<Self> {
        vec![Sum {
            of: input.of,
            with: input.with,
            is: input.of + input.with,
        }]
    }
}

trait Compute:Formula {
    fn compute(input: Self::Input) -> Vec<Self>;
}

trait Formula {
    type Input: TryFrom<Cursor, Error = FormulaEvaluationError>;
    type Match;

    fn write(&self, cursor: Cursor) -> Result<(), FormulaEvaluationError>;

    fn apply(terms: Match) -> FormulaApplication<Self> {
        FormulaApplication::new(terms, PhantomData)
    }
}


struct FormulaApplication<F> {
    terms: Match,
    _marker: PhantomData<F>,
}

impl<F: Formula> FormulaApplication {
    fn new(terms: Match, _marker: PhantomData<F>) -> Self {
        FormulaApplication { terms, _marker }
    }

    fn expand(source: Match) -> Result<Vec<Match>, FormulaEvaluationError>> {
        let cursor = Cursor::new(source, self.terms);
        let input = F::Input::try_from(cursor)?;
        let output = F::compute(input);
        let mut results = Vec::new();
        for output in output {
            let result = cursor.clone();
            output.write(cursor)?;

            vec.push(result.source);
        }

        Ok(results)
    }

    fn evaluate<S: Store, M: Selection>(&self, context: EvaluationContext<S, M>) -> impl Selection {
        try_stream! {
            for await frame in context.selection {
                for output in Self::expand(frame?)? {
                    yield output;
                }
            }
        }
    }
}


struct Cursor {
    source: Match,
    terms: Terms,
}

impl Cursor {
    fn new(source: Match, terms: Terms) -> Self {
        Cursor { source, terms }
    }

    fn read(&self, key: &str) -> Result<Term, FormulaEvaluationError> {
        let term = self.terms.get(key).ok_or(FormulaEvaluationError::MissingTerm(key.to_string()))?;
        self.source.get(term)
    }

    fn write(&mut self, key: &str, value: Term) -> Result<(), FormulaEvaluationError> {
        let term = self.terms.get(key).ok_or(FormulaEvaluationError::MissingTerm(key.to_string()))?;

        self.source = self.source.set(term, value)?;
        Ok(())
    }
}

// Derives expand into something like this

pub struct SumInput {
    of: usize,
    with: usize,
}

impl TryFrom<Cursor> for SumInput {
    type Error = FormulaEvaluationError;

    fn try_from(cursor: Cursor) -> Result<Self, Self::Error> {
        let of = cursor.read("of")?;
        let with = cursor.read("with")?;
        Ok(SumInput { of, with })
    }
}

pub struct SumMatch {
    of: Term<usize>,
    with: Term<usize>,
    is: Term<usize>
}

impl Formula for Sum {
    type Input = SumInput;
    type Match = SumMatch;

    fn compute(input: Self::Input) -> Vec<Self> {
        vec![Sum::compute(input)]
    }

    fn write(&self, cursor: &mut Cursor) -> Result<(), FormulaEvaluationError> {
        cursor.write('is'.into(), self.is.clone())
    }

}
```
