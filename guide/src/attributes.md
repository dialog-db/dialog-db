# Attributes

An attribute describes one specific thing you can say about an entity: its name, its color, its author, its price.

## Defining attributes

An attribute is a newtype struct with the `Attribute` derive macro:

```rust
mod recipe {
    use dialog_query::Attribute;

    #[derive(Attribute, Clone)]
    pub struct Name(pub String);
}
```

This gives Dialog:

- **Selector**: `"recipe/name"`, derived from the module (`recipe`) and struct name (`Name`), joined with `/` and converted to kebab-case.
- **Value type**: `String` maps to Dialog's `Text` type.
- **Cardinality**: `one` (the default) — an entity can have at most one name.

### Naming conventions

The module name becomes the **domain** and the struct name becomes the **name**. Both are converted to kebab-case:

```rust
mod meal_plan {
    #[derive(Attribute, Clone)]
    pub struct DayOfWeek(pub String);
    // Selector: "meal-plan/day-of-week"
}
```

If you want the domain to differ from the module name, use the `#[domain(...)]` attribute:

```rust
mod internal {
    use dialog_query::Attribute;

    #[derive(Attribute, Clone)]
    #[domain(recipe)]
    pub struct Name(pub String);
    // Selector: "recipe/name" (not "internal/name")
}
```

### Supported value types

| Rust type | Dialog type | Use for |
|---|---|---|
| `String` | Text | Names, descriptions, free-form text |
| `bool` | Boolean | Flags, toggles |
| `u32` | UnsignedInteger | Counts, quantities |
| `i32` | SignedInteger | Offsets, deltas |
| `f64` | Float | Measurements, ratings |
| `Vec<u8>` | Bytes | Binary data, images |
| `Entity` | Entity | References to other entities |

## Expressions

Attributes provide a natural-language-like API:

```rust
use dialog_query::Entity;

let pancakes = Entity::new()?;

// Read as: "the Name of pancakes is Pancakes"
recipe::Name::of(pancakes.clone()).is("Pancakes")
```

The `of(...).is(...)` chain creates an **expression**. An expression can always be used as a query. When all members are concrete values, it is also a **statement** that you can assert or retract.

### Asserting and retracting

```rust
let mut edit = session.edit();

// Assert a claim
edit.assert(recipe::Name::of(pancakes.clone()).is("Pancakes"));
edit.assert(recipe::Servings::of(pancakes.clone()).is(4u32));

// Retract a claim (negate with !)
edit.assert(!recipe::Name::of(pancakes.clone()).is("Pancakes"));

session.commit(edit).await?;
```

Retraction doesn't delete the claim from history. It marks it as retracted in the current state. The full history is preserved for sync and auditing.

### Querying with variables

Use `Term::var(...)` to introduce variables that the query engine binds:

```rust
use dialog_query::Term;

// "What is the name of pancakes?"
recipe::Name::of(pancakes.clone()).matches(Term::var("name"));

// "Which entities have a name?"
recipe::Name::of(Term::var("entity")).matches(Term::var("name"));
```

We'll cover querying in the [Querying chapter](./querying.md).

## Multiple attributes, same entity

Attributes are independent. You can attach as many as you want to an entity:

```rust
mod recipe {
    use dialog_query::Attribute;

    #[derive(Attribute, Clone)]
    pub struct Name(pub String);

    #[derive(Attribute, Clone)]
    pub struct Servings(pub u32);

    #[derive(Attribute, Clone)]
    pub struct PrepTime(pub u32);  // minutes

    #[derive(Attribute, Clone)]
    pub struct Vegetarian(pub bool);

    #[derive(Attribute, Clone)]
    pub struct Author(pub Entity);
}
```

Each is an independent claim. You can assert any subset for an entity; there's no requirement that an entity have all of them. This is what enables [schema-on-query](./concepts.md).
