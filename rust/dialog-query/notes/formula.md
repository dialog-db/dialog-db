# Formulas

Define a formula like `Sum`

```rs
#[derive(Debug, Clone, Formula)]
pub struct Sum {
    of: usize,
    with: usize,
    // This is derived from the sum of `of` and `with`.
    #[derived]  // Default cost is 1
    is: usize,
}

// You must manually implement the derive method with the formula logic
impl Sum {
    // The Formula trait's derive() method will call this
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
- `impl Formula for Sum` with operator(), cells(), cost(), dependencies(), derive()
- `impl Output for Sum` with auto-generated `write()` that writes all derived fields
- `impl Pattern for Sum`
- `impl formula::Match for SumMatch`
- `impl From<SumMatch> for Parameters`
- `impl TryFrom<&mut Cursor> for SumInput`

You must manually implement:
- `impl Sum { pub fn derive(input: Input<Self>) -> Vec<Self> { ... } }` - the formula business logic

Then use it like this in the rule bodies

```rs
SumMatch {
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
// Total formula cost will be 3 + 2 = 5
```

If you don't specify a cost, it defaults to 1
