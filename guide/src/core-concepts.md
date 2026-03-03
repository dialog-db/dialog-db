# Core Concepts

Dialog organizes information around four ideas: **entities**, **attributes**, **values**, and **claims**.

If you're coming from relational databases: an entity is like a primary key, an attribute is like a two-column table (primary key + value), and the value is the second column. The flexibility comes from every attribute sharing the same primary key space, so you can join across them freely.

## Entities

An entity is a unique identifier for a thing you want to track. It has no type, no predefined fields — just identity.

```rust
use dialog_query::Entity;

let alice = Entity::try_from("did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK")?;
let pancakes = Entity::new()?;
```

`alice` and `pancakes` are now entities. They don't "know" that one is a person and the other is a recipe. That meaning comes from the claims you assert about them.

## Values

Dialog supports these value types:

| Type | Rust type | Example |
|---|---|---|
| Text | `String` | `"Pancakes"` |
| Boolean | `bool` | `true` |
| Unsigned integer | `u32` | `4` |
| Signed integer | `i32` | `-10` |
| Float | `f64` | `3.14` |
| Bytes | `Vec<u8>` | binary data |
| Entity | `Entity` | a reference to another entity |
| Symbol | `Symbol` | an attribute identifier, e.g. `the!("diy.cook/recipe")` |

## Attributes

An attribute describes a relationship between an entity and a value: "the name of this entity is a String" or "the servings of this entity is a u32."

```rust
mod recipe {
    use dialog_query::Attribute;

    #[derive(Attribute, Clone)]
    pub struct Name(pub String);

    #[derive(Attribute, Clone)]
    pub struct Servings(pub u32);
}
```

The `Attribute` derive macro gives you:

- **Domain**: Derived from the module name. `recipe::Name` becomes `"recipe/name"` in the database.
- **Type**: Inferred from the inner type. `String` maps to `Text`, `u32` to `UnsignedInteger`, etc.
- **Cardinality**: Defaults to "one" — an entity has at most one value for this attribute.

We'll cover cardinality in the [Cardinality chapter](./cardinality.md).

## Claims

A claim combines all three: it says something specific about an entity.

```text
the Name of <pancakes> is "Pancakes"
```

Internally, a claim has four components:

| Field | Meaning | Example |
|---|---|---|
| `the` | The attribute | `"recipe/name"` |
| `of` | The entity | `<pancakes>` |
| `is` | The value | `"Pancakes"` |
| `cause` | Causal reference (provenance) | *(automatic)* |

The `cause` is managed by Dialog. It records *when* and *by whom* the claim was asserted, which is essential for [sync](./sync.md).

### Claims are immutable

Once a claim exists, it never changes. To update a recipe's name, you **retract** the old claim and **assert** a new one. Dialog keeps both in its history. Since claims are never mutated, two peers can never have conflicting versions of the same claim — which is what makes sync possible.

## Putting it together

```text
Entity: <recipe-123>
  ├── recipe/name    = "Pancakes"         (claim 1)
  ├── recipe/servings = 4                 (claim 2)
  └── recipe/author  = <user-456>         (claim 3)

Entity: <user-456>
  ├── user/name      = "Alice"            (claim 4)
  └── user/email     = "alice@example.com" (claim 5)
```

There's no "recipe table" or "user table." Entity `<recipe-123>` has claims with recipe-related attributes; `<user-456>` has claims with user-related attributes. The same entity could have attributes from both domains.

Your application gives claims *meaning* by choosing which attributes to query for. Next, we'll see how to define attributes and compose them into higher-level structures called concepts.
