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

The `#[derive(Formula)]` macro generates all the boilerplate needed to use the formula in queries and rules. It also generates an `Input` struct (e.g. `SumInput`) containing only the non-derived fields. The type alias `Input<Sum>` resolves to this struct, which is what the `derive` function receives.

You must manually implement the `derive` function that computes derived fields from the input. Note that `derive` returns a `Vec`, so a single input can produce zero, one, or many outputs. Returning an empty vec filters out the match (acting as a guard), while returning multiple results expands a single input into many (e.g. splitting a string into parts). Most formulas return exactly one result.

## Using in Queries

```rs
Query::<Sum> {
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

The system ships with a fixed set of built-in formulas. User-defined formulas are not supported. The complete set is registered in the `define_formulas!` macro in `dialog_query::formula::query`, which maps each formula to a `domain/name` identifier:

**Math** (`dialog_query::formula::math`):
- `Sum` ("math/sum"), `Difference` ("math/difference"), `Product` ("math/product"), `Quotient` ("math/quotient"), `Modulo` ("math/modulo")

**Strings** (`dialog_query::formula::string`):
- `Concatenate` ("text/concatenate"), `Length` ("text/length"), `Uppercase` ("text/uppercase"), `Lowercase` ("text/lowercase"), `Like` ("text/like")

**Logic** (`dialog_query::formula::logic`):
- `And` ("logic/and"), `Or` ("logic/or"), `Not` ("logic/not")

**Conversions** (`dialog_query::formula::conversions`):
- `ToString` ("text/from"), `ParseUnsignedInteger` ("unsigned-integer/parse"), `ParseSignedInteger` ("signed-integer/parse"), `ParseFloat` ("float/parse")
