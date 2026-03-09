# Formulas

Formulas are pure computations integrated into the query planner. Given bound input fields, they compute output fields.

## Defining a Formula

```rs
#[derive(Debug, Clone, Formula)]
pub struct Sum {
    pub of: u32,
    pub with: u32,
    #[output]
    pub is: u32,
}

impl Sum {
    pub fn compute(input: Input<Self>) -> Vec<Self> {
        vec![Sum {
            of: input.of,
            with: input.with,
            is: input.of + input.with,
        }]
    }
}
```

The `#[derive(Formula)]` macro generates all the boilerplate needed to use the formula in queries and rules. It also generates an `Input` struct (e.g. `SumInput`) containing only the non-output fields. The type alias `Input<Sum>` resolves to this struct, which is what the `compute` function receives.

You must manually implement the `compute` function that produces output fields from the input. Note that `compute` returns a `Vec`, so a single input can produce zero, one, or many outputs. Returning an empty vec filters out the match (acting as a guard), while returning multiple results expands a single input into many (e.g. splitting a string into parts). Most formulas return exactly one result.

## Using in Queries

```rs
Query::<Sum> {
    of: Term::var("x"),
    with: Term::var("y"),
    is: Term::var("z"),
}
```

## Output Field Costs

You can specify a cost for each output field:

```rs
#[derive(Debug, Clone, Formula)]
pub struct QuotientRemainder {
    pub dividend: u32,
    pub divisor: u32,
    #[output(cost = 3)]
    pub quotient: u32,
    #[output(cost = 2)]
    pub remainder: u32,
}
// Total formula cost = 3 + 2 = 5
```

If cost is omitted, it defaults to 1.

## Built-in Formulas

While `#[derive(Formula)]` can define new formula types, they must be registered in the `define_formulas!` macro in `dialog_query::formula::query` to be usable in the query engine. The current set of built-in formulas:

**Math** (`dialog_query::formula::math`):
- `Sum` ("math/sum"), `Difference` ("math/difference"), `Product` ("math/product"), `Quotient` ("math/quotient"), `Modulo` ("math/modulo")

**Strings** (`dialog_query::formula::string`):
- `Concatenate` ("text/concatenate"), `Length` ("text/length"), `Uppercase` ("text/uppercase"), `Lowercase` ("text/lowercase"), `Like` ("text/like")

**Logic** (`dialog_query::formula::logic`):
- `And` ("logic/and"), `Or` ("logic/or"), `Not` ("logic/not")

**Conversions** (`dialog_query::formula::conversions`):
- `ToString` ("text/from"), `ParseUnsignedInteger` ("unsigned-integer/parse"), `ParseSignedInteger` ("signed-integer/parse"), `ParseFloat` ("float/parse")
