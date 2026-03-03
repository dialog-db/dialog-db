# Formulas and Negation

Formulas and negation are two premise types that don't access the store
directly. Formulas perform pure computation; negation filters answers by
checking that a pattern does *not* match.

## Formulas

A `FormulaQuery` wraps a pure function that reads bound variables, computes a
result, and writes it to output variables:

```rust
pub struct FormulaQuery {
    pub name: &'static str,
    pub cells: &'static Cells,       // parameter schema
    pub parameters: Parameters,       // term bindings
    pub cost: usize,
    pub compute: fn(&mut Bindings) -> Result<Vec<Answer>, EvaluationError>,
}
```

### Evaluation

For each incoming answer:

1. **Create bindings**: Wrap the answer in a `Bindings` struct that provides
   controlled read/write access:

   ```rust
   let mut bindings = Bindings::new(formula, answer, parameters);
   ```

2. **Call compute**: The formula's compute function reads inputs and writes
   outputs through the bindings API:

   ```rust
   // Inside a formula's compute function:
   let text: String = bindings.read("of")?;      // read input
   let len = text.len() as u32;
   bindings.write("is", &Value::from(len))?;      // write output
   ```

3. **Produce answers**: Each write creates a `Factor::Derived` with full
   provenance (which inputs were read, which formula computed the result).

### The Bindings API

`Bindings` provides a controlled interface to the answer:

```rust
pub struct Bindings {
    source: Answer,
    terms: Parameters,
    reads: HashMap<String, Factors>,
    formula: Arc<FormulaQuery>,
}
```

**`read<T>(name)`**: Look up a parameter by name, resolve its term in the
answer, and convert to the requested Rust type. Tracks which factors were
read for provenance.

**`write(name, value)`**: Bind an output parameter to a computed value. If
the parameter's term is a constant, verifies that the computed value matches
(otherwise it's a conflict and the answer is eliminated). Creates a
`Factor::Derived` that references the read factors.

### Conflict Handling

If a formula's output conflicts with an existing binding:

```rust
// In FormulaQuery::expand():
match expansion {
    Ok(output) => Ok(output),
    Err(EvaluationError::Conflict { .. }) => Ok(vec![]),  // filter out
    Err(e) => Err(e),                                      // propagate
}
```

Conflicts are treated as unification failures. The answer is silently
eliminated, same as when a relation query doesn't match.

### Provenance Chain

The `Factor::Derived` variant records a complete derivation chain:

```
Factor::Derived {
    value: 5,                        // computed result
    from: {
        "of": Factors(               // input factor
            Factor::Selected {
                selector: Is,
                fact: Claim(person/name, alice, "Alice"),
            }
        )
    },
    formula: Arc<Length>,            // which formula
}
```

This enables tracing any derived value back through the chain of formulas and
facts that produced it.

### Built-in Formulas

Dialog provides formulas for common operations. Each has input cells (required)
and output cells (derived):

| Formula | Inputs | Output | Operation |
|---------|--------|--------|-----------|
| `Sum` | `left`, `right` | `is` | Addition |
| `Difference` | `left`, `right` | `is` | Subtraction |
| `Product` | `left`, `right` | `is` | Multiplication |
| `Quotient` | `left`, `right` | `is` | Division |
| `Modulo` | `left`, `right` | `is` | Remainder |
| `Concatenate` | `left`, `right` | `is` | String concatenation |
| `Length` | `of` | `is` | String length |
| `UpperCase` | `of` | `is` | To uppercase |
| `LowerCase` | `of` | `is` | To lowercase |
| `Like` | `of`, `pattern` | `is` | Glob pattern match |
| `And` | `left`, `right` | `is` | Logical AND |
| `Or` | `left`, `right` | `is` | Logical OR |
| `Not` | `of` | `is` | Logical NOT |

### Cost

Formula cost is the sum of `#[derived(cost = N)]` annotations on output
fields. Since formulas involve no I/O, their cost is always lower than
relation queries, so they're scheduled after their inputs are bound.

## Negation

Negation implements **negation-as-failure**: if a pattern can be satisfied,
the answer is eliminated; if it cannot, the answer passes through.

### Structure

```rust
pub struct Negation(pub Proposition);

pub enum Premise {
    Assert(Proposition),
    Unless(Negation),
}
```

### Evaluation

```rust
// For each incoming answer:
let mut output = proposition.evaluate(answer.seed(), source);

if let Ok(Some(_)) = output.try_next().await {
    // Pattern matched, DISCARD this answer
    continue;
}
// Pattern did NOT match, KEEP this answer
yield answer;
```

Key properties:

- **Negation never binds variables.** It only filters existing answers. The
  planner marks negated premises as non-binding.

- **All variables in the negated pattern must be bound.** The planner blocks
  negated premises until their variables are available. This ensures the
  negation check is deterministic.

- **Safety.** Because negated premises require bound variables and don't
  produce bindings, they're always scheduled at the end of the plan, after
  all positive premises have run. This is the standard stratified negation
  approach from Datalog.

### Example

Find all people who are NOT retired:

```rust
let premises = vec![
    // Positive: find people with names
    Premise::Assert(Proposition::Relation(Box::new(
        RelationQuery::new(
            Term::from(the!("person/name")),
            Term::var("person"),
            Term::var("name"),
            Term::blank(),
            Some(Cardinality::One),
        ),
    ))),
    // Negative: exclude retired people
    Premise::Unless(Negation(Proposition::Relation(Box::new(
        RelationQuery::new(
            Term::from(the!("person/retired")),
            Term::var("person"),    // must be bound by premise 1
            Term::from(true),
            Term::blank(),
            Some(Cardinality::One),
        ),
    )))),
];
```

The planner ensures premise 1 runs first (it binds `?person`), then premise 2
filters out any person with `retired = true`.

## Constraints

Constraints enforce equality between two terms without accessing the store:

```rust
let x = Term::<String>::var("x");
let y = Term::<String>::var("y");
let constraint = x.is(y);  // ?x == ?y
```

Constraints are similar to formulas but simpler. They just check that two
already-bound variables have the same value. Like negation, they require their
operands to be bound and don't produce new bindings.

Negated constraints (`!x.is(y)`) check that two variables are **not** equal.
