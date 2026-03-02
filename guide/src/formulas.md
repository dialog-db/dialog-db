# Formulas

Formulas are pure computations that you can use inside queries and rules. They take some bound input values and derive output values. Think of them as the equivalent of expressions in a SQL SELECT clause: `CONCAT(first_name, ' ', last_name)`, `price * quantity`, `LOWER(email)`.

## Built-in formulas

Dialog ships with formulas for common operations:

**Math**: `Sum`, `Difference`, `Product`, `Quotient`, `Modulo`

```rust
use dialog_query::formulas::math::Sum;

// Derive z = x + y
Match::<Sum> {
    of: Term::var("x"),
    with: Term::var("y"),
    is: Term::var("z"),
}
```

**Strings**: `Concatenate`, `Length`, `Uppercase`, `Lowercase`

```rust
use dialog_query::formulas::string::Concatenate;

// Derive full_name = first_name + " " + last_name
// (Concatenate joins two strings)
Match::<Concatenate> {
    first: Term::var("first_name"),
    second: Term::from(" ".to_string()),
    is: Term::var("with_space"),
}
```

**Logic**: `And`, `Or`, `Not`

**Conversions**: `ToString`, `ParseNumber`

## Using formulas in rules

Formulas are most useful inside rule premises where you need to compute derived values:

```rust
use dialog_query::formulas::math::Product;
use dialog_query::formulas::string::Concatenate;

fn recipe_summary(summary: Query<RecipeSummary>) -> impl When {
    (
        Query::<Recipe> {
            this: summary.this.clone(),
            name: Term::var("name"),
            servings: Term::var("servings"),
        },
        // Compute doubled servings
        Match::<Product> {
            of: Term::var("servings"),
            with: Term::from(2u32),
            is: summary.doubled_servings.clone(),
        },
        // Build a display string
        Match::<Concatenate> {
            first: Term::var("name"),
            second: Term::from(" (recipe)".to_string()),
            is: summary.display_name.clone(),
        },
    )
}
```

The planner knows that formula inputs need to be bound before the formula can run, so it will schedule the `Recipe` query first, then the `Product` and `Concatenate` computations.

## Defining custom formulas

If the built-in formulas aren't enough, you can define your own with the `Formula` derive macro:

```rust
use dialog_query::{Formula, Input};

#[derive(Debug, Clone, Formula)]
pub struct ScaleServings {
    pub original: u32,
    pub factor: u32,
    #[derived]
    pub scaled: u32,
}

impl ScaleServings {
    pub fn derive(input: Input<Self>) -> Vec<Self> {
        vec![ScaleServings {
            original: input.original,
            factor: input.factor,
            scaled: input.original * input.factor,
        }]
    }
}
```

The `#[derived]` annotation marks which fields are computed outputs. All other fields are inputs that must be bound before the formula can run. The `derive` method receives the bound inputs and returns the computed results.

A formula can return multiple results if the computation is one-to-many:

```rust
#[derive(Debug, Clone, Formula)]
pub struct SplitWords {
    pub text: String,
    #[derived]
    pub word: String,
}

impl SplitWords {
    pub fn derive(input: Input<Self>) -> Vec<Self> {
        input.text
            .split_whitespace()
            .map(|w| SplitWords {
                text: input.text.clone(),
                word: w.to_string(),
            })
            .collect()
    }
}
```

### Derived field costs

If a formula is expensive, you can annotate derived fields with a cost hint. The planner uses this when deciding execution order:

```rust
#[derive(Debug, Clone, Formula)]
pub struct ExpensiveComputation {
    pub input: String,
    #[derived(cost = 10)]
    pub result: String,
}
```

The default cost is 1. Higher costs make the planner prefer to bind more variables before resorting to this formula.

## Formulas vs. application code

You might wonder why you'd use a formula instead of just computing values in Rust code after the query returns. The key difference is that formulas participate in the query plan. This means:

- Formula outputs can be used in subsequent premises. A rule can compute a value with a formula and then match it against another pattern.
- The planner can optimize around formulas. It knows their input/output dependencies and can schedule them efficiently.
- Formulas are declarative. They describe *what* to compute, and the planner decides *when*.

For simple post-processing of query results, regular Rust code is fine. Formulas are most valuable when the computed value feeds back into the query itself.
