# Concepts

Attributes are fine-grained — each says one thing about an entity. A **concept** groups attributes you care about together.

```rust
use dialog_query::{Concept, Entity};

#[derive(Concept, Debug, Clone)]
pub struct Recipe {
    this: Entity,
    name: recipe::Name,
    servings: recipe::Servings,
}
```

A `Recipe` is any entity that has both a `recipe::Name` and a `recipe::Servings`. The `this` field holds the entity itself.

## Schema-on-query

Concepts apply structure at query time, not write time. You can define multiple concepts over the same entities:

```rust
#[derive(Concept, Debug, Clone)]
pub struct RecipeSummary {
    this: Entity,
    name: recipe::Name,
}

#[derive(Concept, Debug, Clone)]
pub struct RecipeDetail {
    this: Entity,
    name: recipe::Name,
    servings: recipe::Servings,
    prep_time: recipe::PrepTime,
    author: recipe::Author,
}
```

An entity with all five attributes matches both. One with only name and servings matches `RecipeSummary` but not `RecipeDetail`. No migration needed — different parts of your application define different views.

## Asserting concepts

When you assert a concept, Dialog stores one claim per attribute:

```rust
let pancakes = Entity::new()?;

let mut edit = session.edit();
edit.assert(Recipe {
    this: pancakes.clone(),
    name: recipe::Name("Pancakes".to_string()),
    servings: recipe::Servings(4),
});
session.commit(edit).await?;
```

This is equivalent to asserting each attribute individually:

```rust
edit.assert(recipe::Name::of(pancakes.clone()).is("Pancakes"));
edit.assert(recipe::Servings::of(pancakes.clone()).is(4u32));
```

The concept form is a convenience. Under the hood, the same individual claims are stored.

## Retracting concepts

Similarly, retracting a concept retracts each attribute:

```rust
let mut edit = session.edit();
edit.retract(Recipe {
    this: pancakes.clone(),
    name: recipe::Name("Pancakes".to_string()),
    servings: recipe::Servings(4),
});
session.commit(edit).await?;
```

## Querying concepts

A concept query is a conjunction: it matches entities that have *all* the listed attributes. We'll cover this in detail in the [Querying chapter](./querying.md), but here's a preview:

```rust
use dialog_query::Term;

let results = Query::<Recipe> {
    this: Term::var("entity"),
    name: Term::from("Pancakes".to_string()),
    role: Term::var("servings"),
}.perform(&session).try_vec().await?;
```

This finds all entities whose `recipe/name` is "Pancakes" and returns whatever their `recipe/servings` value is.

## Concepts vs. tables

| SQL | Dialog |
|---|---|
| Table defines columns at creation time | Concept selects attributes at query time |
| Every row has the same columns | Every entity can have any set of attributes |
| Adding a column requires ALTER TABLE | Adding an attribute requires nothing |
| Foreign key references another table | Entity attribute references another entity |
| One canonical schema | Multiple concepts over the same entities |

The tradeoff: Dialog doesn't enforce completeness. If you forget to assert `servings` for a recipe, it won't match concept queries that require it. Your application is responsible for asserting the attributes it cares about.
