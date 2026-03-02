# Attributes

An attribute is the fundamental unit of meaning in Dialog. It describes one specific thing you can say about an entity: its name, its color, its author, its price. Let's look at how to define and use them.

## Defining attributes

In Rust, an attribute is a newtype struct with the `Attribute` derive macro:

```rust
mod recipe {
    use dialog_query::Attribute;

    /// The name of a recipe
    #[derive(Attribute, Clone)]
    pub struct Name(pub String);
}
```

This single definition gives Dialog everything it needs:

- **Selector**: `"recipe/name"`, derived from the module (`recipe`) and struct name (`Name`), joined with `/` and converted to kebab-case.
- **Value type**: `String` maps to Dialog's `Text` type.
- **Cardinality**: `one` (the default), meaning an entity can have at most one name.

### Naming conventions

The module name becomes the **namespace** and the struct name becomes the **name**. Both are converted to kebab-case:

```rust
mod meal_plan {
    #[derive(Attribute, Clone)]
    pub struct DayOfWeek(pub String);
    // Selector: "meal-plan/day-of-week"
}
```

If you want the namespace to differ from the module name, use the `#[domain(...)]` attribute:

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

The inner type of your attribute determines what kind of values it can hold:

| Rust type | Dialog type | Use for |
|---|---|---|
| `String` | Text | Names, descriptions, free-form text |
| `bool` | Boolean | Flags, toggles |
| `u32` | UnsignedInteger | Counts, quantities |
| `i32` | SignedInteger | Offsets, deltas |
| `f64` | Float | Measurements, ratings |
| `Vec<u8>` | Bytes | Binary data, images |
| `Entity` | Entity | References to other entities |

## Using attributes

Once defined, attributes give you a natural-language-like API for building expressions:

### The expression syntax

```rust
use dialog_query::Entity;

let pancakes = Entity::new()?;

// Read this as: "the Name of pancakes is Pancakes"
recipe::Name::of(pancakes.clone()).is("Pancakes")
```

The `of(...).is(...)` chain creates an **expression**. Depending on what you pass in, this expression can be used for different things:

- **Both concrete** means it's a statement you can assert or retract
- **One or both variables** means it's a pattern you can query with

We'll see both uses in the coming chapters.

### Asserting claims

When both `of` and `is` are concrete values, the expression is a **statement**, something you can write to the database:

```rust
let mut session = Session::open(store);
let mut edit = session.edit();

edit.assert(recipe::Name::of(pancakes.clone()).is("Pancakes"));
edit.assert(recipe::Servings::of(pancakes.clone()).is(4u32));

session.commit(edit).await?;
```

### Retracting claims

To remove a claim, negate the statement with `!`:

```rust
let mut edit = session.edit();
edit.assert(!recipe::Name::of(pancakes.clone()).is("Pancakes"));
session.commit(edit).await?;
```

This doesn't delete the claim from history. It marks the claim as retracted in the current state. The full history is preserved for sync and auditing.

### Querying with patterns

When you use `Term::var(...)` instead of a concrete value, the expression becomes a **pattern** that the query engine will match against:

```rust
use dialog_query::Term;

// "What is the name of pancakes?"
let pattern = recipe::Name::of(pancakes.clone())
    .matches(Term::var("name"));

// "Which entities have a name?"
let pattern = recipe::Name::of(Term::var("entity"))
    .matches(Term::var("name"));
```

We'll cover querying in full detail in the [Querying chapter](./querying.md).

## Multiple attributes, same entity

Since attributes are independent, you can attach as many as you want to a single entity:

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

    /// Reference to the user who created this recipe
    #[derive(Attribute, Clone)]
    pub struct Author(pub Entity);
}
```

Each of these is an independent claim. You can assert any subset of them for an entity; there's no requirement that an entity have all of them. This flexibility is what enables [schema-on-query](./concepts.md), which we'll cover next.
