# Concepts

Attributes are fine-grained. Each one says one thing about an entity. But when you're building an application, you usually care about groups of attributes together: "give me entities that have a name *and* servings *and* an author." That's what concepts are for.

## What is a concept?

A concept is a named group of attributes that you query for together. If an entity has all the attributes in a concept, it matches. If it's missing any of them, it doesn't.

Think of it like a lens you hold up to the database. You're not changing the data, you're choosing which combination of attributes you want to see.

```rust
use dialog_query::{Concept, Entity};

#[derive(Concept, Debug, Clone)]
pub struct Recipe {
    this: Entity,
    name: recipe::Name,
    servings: recipe::Servings,
}
```

This says: a `Recipe` is any entity that has both a `recipe::Name` and a `recipe::Servings`. The `this` field holds the entity itself.

## Schema-on-query

In a traditional database, you define a table's columns up front and every row must conform. Concepts work differently: they apply structure at query time rather than write time.

This has a practical consequence. You can define multiple concepts over the same entities:

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

An entity with all five attributes will match both `RecipeSummary` and `RecipeDetail`. An entity with only a name and servings will match `RecipeSummary` but not `RecipeDetail` (since it's missing `prep_time` and `author`).

No migration is needed. No schema conflict arises. Different parts of your application can define different views of the same data.

## Asserting concepts

When you assert a concept, Dialog stores one fact per attribute:

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

The concept form is a convenience. Under the hood, the same individual facts are stored.

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

If you're coming from SQL, here's a comparison:

| SQL | Dialog |
|---|---|
| Table defines columns at creation time | Concept selects attributes at query time |
| Every row has the same columns | Every entity can have any set of attributes |
| Adding a column requires ALTER TABLE | Adding an attribute requires nothing |
| Foreign key references another table | Entity attribute references another entity |
| One canonical schema | Multiple concepts over the same entities |

The tradeoff is that Dialog doesn't enforce completeness. If you forget to assert a `servings` attribute for a recipe, it simply won't match concept queries that require `servings`. There's no "NOT NULL constraint" to catch the omission. Your application is responsible for asserting the attributes it cares about.
