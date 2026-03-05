# Building Queries

This chapter explains how query patterns are assembled and how they compose
into multi-premise queries.

## Terms

Every position in a query pattern is a `Term<T>`:

| Kind | Example | Behavior |
|------|---------|----------|
| **Named variable** | `Term::var("x")` | Captures a binding; joins across premises |
| **Blank variable** | `Term::blank()` | Matches anything; no join participation |
| **Constant** | `Term::from(42u32)` | Must match exactly |

When the same named variable appears in multiple premises, it must bind to the
same value everywhere. This is how Dialog implements joins — through shared
variable names rather than explicit join syntax.

## Attribute Queries

An `AttributeQuery` matches a single `(the, of, is, cause)` claim pattern:

```rust
let query = AttributeQuery::new(
    Term::from(the!("person/name")),  // the: constant
    Term::var("person"),               // of:  variable
    Term::var("name"),                 // is:  variable
    Term::blank(),                     // cause: don't care
    Some(Cardinality::One),
);
```

The fluent API produces the same thing:

```rust
employee::Name::of(Term::var("e")).is(Term::<String>::var("n"))
```

## Concept Queries

A concept query matches entities that have all of a concept's attributes:

```rust
let query = Query::<Employee> {
    this: Term::var("person"),
    name: Term::from("Alice".to_string()),
    role: Term::var("role"),
};
```

Internally this expands into multiple attribute queries (one per attribute).
The planner decides their order.

## Composing Premises

A query is a conjunction (AND) of premises. Each premise is:

- **`Premise::Assert(proposition)`** — must match
- **`Premise::Unless(negation)`** — must *not* match

A `Proposition` is one of:

| Variant | Purpose |
|---------|---------|
| `Attribute(AttributeQuery)` | Match a single claim pattern |
| `Concept(ConceptQuery)` | Match an entity against a concept |
| `Formula(FormulaQuery)` | Compute derived values |
| `Constraint(Constraint)` | Enforce equality between terms |

### Deductive Rules

A **deductive rule** derives a concept from alternative premises:

```rust
fn employee_from_person(employee: Query<Employee>) -> impl When {
    (
        Query::<Person> {
            this: employee.this.clone(),
            name: employee.name.clone(),
            title: employee.role.clone(),
        },
    )
}

session.install(employee_from_person)?;
```

Multiple rules for the same concept form a **disjunction** (OR): an entity
matches if it satisfies any rule.

## Parameters

Each premise advertises a `Parameters` map (`HashMap<String, Term<Any>>`)
naming each position:

| Premise type | Parameters |
|-------------|-----------|
| `AttributeQuery` | `the`, `of`, `is`, `cause` |
| `ConceptQuery` | `this`, plus one per attribute field |
| `FormulaQuery` | One per formula cell (input + output) |
| `Constraint` | `this`, `is` |

The planner inspects parameters to determine prerequisites (must be bound)
and products (will be bound).
