# Building Queries

This chapter explains how query patterns are assembled from attribute and
concept definitions, and how they compose into multi-premise queries.

## Terms: The Building Blocks

Every position in a query pattern is a `Term<T>`:

```rust
pub enum Term<T: Typed> {
    Variable {
        name: Option<String>,
        descriptor: <T as Typed>::Descriptor,
    },
    Constant(Value),
}
```

There are three kinds of terms:

| Kind | Example | Behavior |
|------|---------|----------|
| **Named variable** | `Term::var("x")` | Captures a binding; participates in joins across premises |
| **Blank variable** | `Term::blank()` | Matches anything; does **not** participate in joins |
| **Constant** | `Term::from(42u32)` | Must match exactly; constrains the query |

The crucial property: **when the same named variable appears in multiple
premises, it must bind to the same value in all of them**. This is how Dialog
implements joins — through shared variable names, not explicit join syntax.

## Relation Queries

A `RelationQuery` matches a single `(the, of, is, cause)` claim pattern:

```rust
let query = RelationQuery::new(
    Term::from(the!("person/name")),  // the: attribute (constant)
    Term::var("person"),               // of:  entity (variable)
    Term::var("name"),                 // is:  value (variable)
    Term::blank(),                     // cause: don't care
    Some(Cardinality::One),
);
```

This matches all claims where `the` is `person/name`, binding the entity to
`?person` and the value to `?name`.

### Using the Fluent API

The `AttributeExpression` builder provides a more ergonomic syntax:

```rust
// Query pattern (all persons with their names)
Name::of(Term::var("e")).matches(Term::var("n"))

// Assertion (set a specific value)
Name::of(alice).is("Alice")
```

Both forms produce the same underlying `RelationQuery`.

## Concept Queries

A concept query matches entities that have all of a concept's attributes:

```rust
let query = PersonMatch {
    this: Term::var("person"),
    name: Term::from("Alice".to_string()),
    role: Term::var("role"),
};
```

This finds all entities with `person/name = "Alice"` and a `person/role`
attribute, binding the entity to `?person` and the role to `?role`.

Internally, a `ConceptQuery` expands into multiple `RelationQuery` steps — one
per attribute. The planner decides their order.

## Composing Premises

A query is a conjunction (AND) of premises. Each premise is either:

- **Assert**: A pattern that must match (`Premise::Assert(proposition)`)
- **Unless**: A negated pattern that must *not* match
  (`Premise::Unless(negation)`)

A `Proposition` is one of:

| Variant | Purpose |
|---------|---------|
| `Relation(RelationQuery)` | Match a single claim pattern |
| `Concept(ConceptQuery)` | Match an entity against a concept |
| `Formula(FormulaQuery)` | Compute derived values |
| `Constraint(Constraint)` | Enforce equality between terms |

### Example: Multi-premise Query

Find all people who live in "New York" and their ages:

```rust
let premises = vec![
    // Premise 1: find name
    Premise::Assert(Proposition::Relation(Box::new(
        RelationQuery::new(
            Term::from(the!("person/name")),
            Term::var("person"),
            Term::var("name"),
            Term::blank(),
            Some(Cardinality::One),
        )
    ))),
    // Premise 2: same person lives in New York
    Premise::Assert(Proposition::Relation(Box::new(
        RelationQuery::new(
            Term::from(the!("person/city")),
            Term::var("person"),           // joins on ?person
            Term::from("New York".to_string()),
            Term::blank(),
            Some(Cardinality::One),
        )
    ))),
    // Premise 3: same person's age
    Premise::Assert(Proposition::Relation(Box::new(
        RelationQuery::new(
            Term::from(the!("person/age")),
            Term::var("person"),           // joins on ?person
            Term::var("age"),
            Term::blank(),
            Some(Cardinality::One),
        )
    ))),
];
```

The variable `?person` appears in all three premises — it must bind to the
same entity across all of them. The planner will choose which premise to
execute first based on cost.

### Deductive Rules

A **deductive rule** is a named conjunction that derives a concept:

```rust
session.rule::<Person>(
    PersonMatch::default(),
    vec![/* premises */],
);
```

Rules are registered with the session and evaluated lazily when the concept is
queried. Multiple rules for the same concept form a **disjunction** (OR) — an
entity matches the concept if it satisfies any one of the rules.

### JSON Notation for Queries

In the formal JSON notation, queries use `where` clauses with variable
references:

```json
{
  "match": { "name": { "?": "name" }, "age": { "?": "age" } },
  "where": [
    {
      "the": "person/name",
      "of": { "?": "person" },
      "is": { "?": "name" }
    },
    {
      "the": "person/city",
      "of": { "?": "person" },
      "is": "New York"
    },
    {
      "the": "person/age",
      "of": { "?": "person" },
      "is": { "?": "age" }
    }
  ]
}
```

Variables are represented as `{ "?": "variable_name" }` (or `{ "?": {} }` for
blanks). The abbreviated YAML notation uses inline addressing and punning for
conciseness.

## Parameters

Each premise advertises its parameters through a `Parameters` map — a
`HashMap<String, Term<Any>>` that names each position in the pattern:

| Premise type | Parameters |
|-------------|-----------|
| `RelationQuery` | `the`, `of`, `is`, `cause` |
| `ConceptQuery` | `this`, plus one per attribute field name |
| `FormulaQuery` | One per formula cell (input + output) |
| `Constraint` | `left`, `right` |

Parameters serve two purposes:

1. **Planning**: The planner inspects parameters against the schema to determine
   which variables are prerequisites (must be bound) and which are products
   (will be bound).

2. **Scoping**: For concept queries, parameters map between user variable names
   and internal parameter names, enabling nested evaluation with proper scoping.

## The Application Trait

The `Application` trait is the polymorphic interface for anything that can be
evaluated:

```rust
pub trait Application: Clone {
    type Conclusion: ConditionalSend;

    fn evaluate<S: Source, M: Answers>(
        self,
        answers: M,
        source: &S,
    ) -> impl Answers;

    fn realize(&self, input: Answer) -> Result<Self::Conclusion, EvaluationError>;
}
```

- `evaluate` takes a stream of incoming answers and produces expanded answers
- `realize` converts a raw `Answer` (variable bindings) into a typed result

Both `PersonMatch` and `FormulaMatch` implement `Application`, making them
directly executable as queries.
