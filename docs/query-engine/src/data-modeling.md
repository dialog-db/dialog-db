# Data Modeling

Dialog queries operate on claims — `(the, of, is, cause)` tuples — but users
define domain models using **attributes** and **concepts**, either through Rust
derive macros or JSON notation.

## Attributes

An **attribute** names a typed relation in `domain/name` format. It describes
one kind of association an entity can have.

### Rust: `#[derive(Attribute)]`

```rust
mod employee {
    use dialog_query::prelude::*;

    /// Person's given name
    #[derive(Attribute, Clone)]
    pub struct Name(pub String);   // → "employee/name"

    /// Skills associated with the employee
    #[derive(Attribute, Clone)]
    #[cardinality(many)]
    pub struct Skill(pub String);  // → "employee/skill"
}
```

The domain is derived from the enclosing module name (underscores become
hyphens), the name from the struct name (lowercased, kebab-cased). The macro
generates an `Attribute` trait impl, an `AttributeDescriptor`, and fluent
builder methods:

```rust
// Assert a value
employee::Name::of(alice).is("Alice")

// Query with a variable
employee::Name::of(Term::var("e")).is(Term::<String>::var("name"))
```

Both use `.is()` — the type system distinguishes statements (concrete values)
from query patterns (`Term` variables).

### The `the!()` Macro

The `the!()` macro validates `domain/name` format at compile time and produces
a `The` value. You can use it to build expressions without derive macros:

```rust
the!("employee/name").of(alice).is("Alice")         // assert
the!("employee/name").of(Term::var("e")).is(Term::<String>::var("n"))  // query
```

### JSON Notation

```json
{
  "the": "employee/name",
  "description": "Person's given name",
  "as": "Text",
  "cardinality": "one"
}
```

### Naming Rules

- **Domain**: lowercase ASCII, digits, hyphens, dots. Must start with a letter.
- **Name**: lowercase kebab-case. Must start with a letter.
- Combined length ≤ 64 bytes.

### Type Bridge

Dialog maps Rust types to runtime value types:

| Rust type | Runtime `Type` |
|-----------|----------------|
| `String`  | `Type::String` |
| `bool`    | `Type::Boolean`|
| `u32`     | `Type::UnsignedInt` |
| `i64`     | `Type::SignedInt` |
| `f64`     | `Type::Float`  |
| `Vec<u8>` | `Type::Bytes`  |
| `Entity`  | `Type::Entity` |

## Concepts

A **concept** groups attributes sharing an entity — like a type in a
programming language, realized through schema-on-read. An entity can satisfy
multiple concepts simultaneously if it has the right claims.

### Rust: `#[derive(Concept)]`

```rust
#[derive(Concept, Debug, Clone)]
pub struct Employee {
    pub this: Entity,
    pub name: employee::Name,
    pub role: employee::Role,
}
```

The macro generates:

- **`Query<Employee>`** — a match struct with `Term`-wrapped fields for querying
- **`Concept` impl** — bidirectional mapping: assert decomposes into claims,
  query composes claims into conclusions
- **`ConceptDescriptor`** — identity derived from the sorted set of attribute
  identities (two concepts with the same attributes are the same concept)

### JSON Notation

```json
{
  "description": "An employee in the system",
  "with": {
    "name":  { "the": "employee/name", "as": "Text" },
    "role":  { "the": "employee/role", "as": "Text" }
  }
}
```
