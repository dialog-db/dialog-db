# Formulas and Negation

Formulas and negation are premise types that don't access the store directly.
Formulas perform pure computation; negation filters matches by checking that a
pattern does *not* match.

## Formulas

A `FormulaQuery` wraps a pure function that reads bound variables and writes
derived values:

```rust
#[derive(Formula, Debug, Clone)]
pub struct FullName {
    pub first: String,
    pub last: String,
    #[derived(cost = 2)]
    pub full: String,
}

impl FullName {
    fn derive(input: Input<Self>) -> Vec<Self> {
        vec![FullName {
            first: input.first.clone(),
            last: input.last.clone(),
            full: format!("{} {}", input.first, input.last),
        }]
    }
}
```

### Evaluation

For each incoming match: create `Bindings` from the match, call the compute
function (which reads inputs and writes outputs), and produce expanded matches
with `Factor::Derived` provenance. If the output conflicts with an existing
binding, the match is silently eliminated (unification failure).

### Built-in Formulas

| Formula | Inputs | Output | Operation |
|---------|--------|--------|-----------|
| `Sum` | `of`, `with` | `is` | Addition |
| `Difference` | `of`, `with` | `is` | Subtraction |
| `Product` | `of`, `with` | `is` | Multiplication |
| `Quotient` | `of`, `with` | `is` | Division |
| `Concatenate` | `of`, `with` | `is` | String join |
| `Length` | `of` | `is` | String length |
| `UpperCase` | `of` | `is` | To uppercase |
| `LowerCase` | `of` | `is` | To lowercase |
| `Like` | `of`, `pattern` | `is` | Glob match |
| `And` / `Or` / `Not` | `of`[, `with`] | `is` | Logic |

### Cost

Formula cost is the sum of `#[derived(cost = N)]` annotations. Since formulas
involve no I/O, they're always cheaper than attribute queries and schedule
after their inputs are bound.

## Negation

Negation implements **negation-as-failure**: if a pattern can be satisfied, the
match is eliminated; if not, it passes through.

```rust
Premise::Unless(Negation(proposition))
```

For each incoming match, the inner proposition is evaluated. If it produces
any result, the match is discarded. If not, the match passes through unchanged.

Key properties:

- Negation **never binds variables** — it only filters.
- **All variables** in the negated pattern must be bound (the planner blocks
  until they are). This ensures deterministic evaluation.
- Negated premises always schedule **at the end**, after all positive premises.

## Constraints

Constraints enforce equality between two terms without accessing the store:

```rust
let constraint = Equality::new(Term::var("x"), Term::var("y"));
```

A constraint requires **one** of its two operands to be bound. It supports
bidirectional inference: if one operand is bound, the other is inferred to have
the same value. If both are bound, it checks equality and filters mismatches.

Negated constraints (`!x.is(y)`) check that two variables are **not** equal.
