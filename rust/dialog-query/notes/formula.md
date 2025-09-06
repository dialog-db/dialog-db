# Formulas

Define a formula like increment

```rs
#[formula]
/// Increment a number by one
fn inc(of: isize) -> isize {
    of + 1
}
```

Which unpack to following definition

```rs
struct Increment {
    pub operator: &'static str,
    pub operands: &'static IncrementCells,
    pub cost: &'static usize,
};
struct IncrementInput {
    pub of: isize,
}
struct IncrementOutput {
    pub of: isize,
}

pub struct IncrementCells {
    pub of: Cell,
    pub is: Cell,
}



pub static INCREMENT = Increment {
    cost: 1,
    operator: "inc",
    operands: IncrementCells {
        of: Cell {
            name: &"of",
            description: &"Source to increment",
            requirement: Requirement::Required,
            data_type: ValueDataType::SignedInteger,
        },
        is: Cell {
            name: &"is",
            description: &"Output of the increment",
            requirement: Requirement::Derived(0),
            data_type: ValueDataType::SignedInteger,
        },
    },
};


impl Formula for Increment {
    type Input = IncrementInput;
    type Output = IncrementOutput;
    type Cells = IncrementCells;

    pub fn new() -> &'static Self {
        INCREMENT
    }

    pub fn apply(terms: Terms) -> Result<FormulaApplication, AnalyzerError> {
        let formula = Self::new();
        if !self.terms.contains(INCREMENT.of.name) {
            Err(AnalyzerError::RequiredCell {
                formula: self.formula.clone(),
                cell: name.into(),
            })
        }

        Ok(FormulaApplication {
            formula,
            terms,
        })
    }
}

struct IncrementPlan {
    pub fn plan(&self, scope: VariableScope) -> Result<Plan, PlanError> {

    }
}
```
