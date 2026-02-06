# Formulas

Formulas are pure computations integrated into the query planner. Given bound input fields, they derive output fields.

## Defining a Formula

```rs
#[derive(Debug, Clone, Formula)]
pub struct Sum {
    pub of: u32,
    pub with: u32,
    #[derived]
    pub is: u32,
}

impl Sum {
    pub fn derive(input: Input<Self>) -> Vec<Self> {
        vec![Sum {
            of: input.of,
            with: input.with,
            is: input.of + input.with,
        }]
    }
}
```

The `#[derive(Formula)]` macro generates:
- `SumInput` struct with only non-derived fields (`of`, `with`)
- `SumMatch` struct with all fields as `Term<T>` for pattern matching
- `impl Formula for Sum` with `operator()`, `cells()`, `cost()`, `derive()`
- `impl Output for Sum` with `write()` that writes all derived fields
- `impl formula::Match for SumMatch`
- `impl From<SumMatch> for Parameters`
- `impl TryFrom<&mut Cursor> for SumInput`
- `impl Quarriable for Sum` (so `Match::<Sum>` resolves to `SumMatch`)

You must manually implement:
- `impl Sum { pub fn derive(input: Input<Self>) -> Vec<Self> { ... } }` — the formula logic

## Using in Queries

```rs
Match::<Sum> {
    of: Term::var("x"),
    with: Term::var("y"),
    is: Term::var("z"),
}
```

## Derived Field Costs

You can specify a cost for each derived field:

```rs
#[derive(Debug, Clone, Formula)]
pub struct QuotientRemainder {
    pub dividend: u32,
    pub divisor: u32,
    #[derived(cost = 3)]
    pub quotient: u32,
    #[derived(cost = 2)]
    pub remainder: u32,
}
// Total formula cost = 3 + 2 = 5
```

If cost is omitted, it defaults to 1.

## Built-in Formulas

**Math** (`dialog_query::formulas::math`): `Sum`, `Difference`, `Product`, `Quotient`, `Modulo`

**Strings** (`dialog_query::formulas::strings`): `Concatenate`, `Length`, `Uppercase`, `Lowercase`, `Is`

**Logic** (`dialog_query::formulas::logic`): `And`, `Or`, `Not`

**Conversions** (`dialog_query::formulas::conversions`): `ToString`, `ParseNumber`
