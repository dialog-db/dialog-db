# Core Concepts

Dialog organizes information around four simple ideas: **entities**, **attributes**, **values**, and **claims**. If you've used a relational database, you can think of this as a more flexible alternative to tables and rows. If you haven't, don't worry; we'll build up from first principles.

## Entities

An entity is a thing you want to track: a person, a recipe, a tag, a device. In Dialog, an entity is just a unique identifier. It doesn't have a type or a predefined set of fields. It's a blank canvas that you attach information to.

```rust
use dialog_query::Entity;

let alice = Entity::new()?;
let pancakes = Entity::new()?;
```

That's it. `alice` and `pancakes` are now entities. They don't "know" that one is a person and the other is a recipe. That meaning comes from the claims you assert about them.

## Values

A value is a piece of concrete data. Dialog supports these value types:

| Type | Rust type | Example |
|---|---|---|
| Text | `String` | `"Pancakes"` |
| Boolean | `bool` | `true` |
| Unsigned integer | `u32` | `4` |
| Signed integer | `i32` | `-10` |
| Float | `f64` | `3.14` |
| Bytes | `Vec<u8>` | binary data |
| Entity | `Entity` | a reference to another entity |
| Symbol | `Symbol` | an interned string |

Most of the time you'll work with `String`, numbers, `bool`, and `Entity` (for references between entities).

## Attributes

An attribute describes a **relationship** between an entity and a value. It's a named, typed slot: "the name of this entity is a String" or "the servings of this entity is a u32."

In Rust, you define an attribute as a newtype:

```rust
mod recipe {
    use dialog_query::Attribute;

    /// The name of a recipe
    #[derive(Attribute, Clone)]
    pub struct Name(pub String);

    /// How many people this recipe serves
    #[derive(Attribute, Clone)]
    pub struct Servings(pub u32);
}
```

The `Attribute` derive macro does a few things automatically:

- **Namespace**: Derived from the module name. `recipe::Name` becomes `"recipe/name"` in the database.
- **Type**: Inferred from the inner type. `String` maps to Dialog's `Text` type, `u32` to `UnsignedInteger`, etc.
- **Cardinality**: Defaults to "one," meaning an entity has at most one value for this attribute.

We'll cover cardinality in more detail in the [Cardinality chapter](./cardinality.md).

## Claims

A claim combines all three: it says something specific about an entity.

```text
the Name of <pancakes> is "Pancakes"
```

In Dialog's internal representation, a claim has four components:

| Field | Meaning | Example |
|---|---|---|
| `the` | The attribute (what we're saying) | `"recipe/name"` |
| `of` | The entity (who we're saying it about) | `<pancakes>` |
| `is` | The value (what we're asserting) | `"Pancakes"` |
| `cause` | Causal reference (provenance) | *(automatic)* |

The `cause` field is managed by Dialog; you don't set it directly. It records *when* and *by whom* the claim was asserted, which is essential for sync (covered in a [later chapter](./sync.md)).

### Claims are immutable

Once a claim exists, it never changes. If you want to update a recipe's name, you don't modify the existing claim. Instead you **retract** the old claim and **assert** a new one. Dialog keeps both in its history. This append-only model is what makes sync possible: since claims are never mutated, two peers can never have conflicting versions of the same claim.

## Putting it together

Here's how these concepts relate in practice:

```text
Entity: <recipe-123>
  ├── recipe/name    = "Pancakes"         (claim 1)
  ├── recipe/servings = 4                 (claim 2)
  └── recipe/author  = <user-456>         (claim 3)

Entity: <user-456>
  ├── user/name      = "Alice"            (claim 4)
  └── user/email     = "alice@example.com" (claim 5)
```

Notice that there's no "recipe table" or "user table." Entity `<recipe-123>` simply has claims with recipe-related attributes, and `<user-456>` has claims with user-related attributes. The same entity could have attributes from both domains. Dialog doesn't enforce boundaries.

This flexibility is fundamental. Your application gives claims *meaning* by choosing which attributes to query for. In the next section, we'll see how to define attributes and compose them into higher-level structures called concepts.
