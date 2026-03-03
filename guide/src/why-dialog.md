# Why Dialog?

You're building a collaborative app — say a recipe book that a household shares. You need to store recipes, let people edit from their own devices, keep everything in sync offline, and derive views like "all vegetarian recipes."

Traditionally, each of those is a separate system: database, REST API, WebSockets, conflict resolution, sync protocol. Dialog collapses these into one.

## Claims, not rows

A relational database needs a `recipes` table with fixed columns. Adding a field means a migration. Schema disagreements mean trouble.

Dialog stores data as individual **claims**:

```text
the name of <recipe-123> is "Pancakes"
the servings of <recipe-123> is 4
the author of <recipe-123> is <user-456>
```

Each claim stands on its own. No table to alter — just assert another claim.

## Query-driven structure

Dialog applies structure at query time, not write time. A **concept** selects the attributes you care about:

```rust
#[derive(Concept)]
struct Recipe {
    this: Entity,
    name: recipe::Name,
    servings: recipe::Servings,
}
```

Entities that have both `name` and `servings` match. Missing one? No match. Have a hundred extra attributes? Still matches — they're just not part of this view.

## Built-in sync

Every claim gets an immutable, content-addressed identity and a **causal reference**. Two peers compare causal histories and exchange exactly the claims they're each missing. No central server, no sync protocol to design.

**You don't build sync on top of Dialog; sync is part of what Dialog is.**

## Derived data

Rules derive new claims from existing ones, inspired by Datalog:

```rust
fn vegetarian_recipe(recipe: Query<VegetarianRecipe>) -> impl When {
    (
        Query::<Recipe> {
            this: recipe.this.clone(),
            name: recipe.name.clone(),
            servings: recipe.servings.clone(),
        },
        !Query::<HasMeatIngredient> {
            this: recipe.this.clone(),
        },
    )
}
```

No "vegetarian" flag to maintain — the classification is derived whenever the underlying claims change.

## Summary

| Concern | Traditional stack | Dialog |
|---|---|---|
| Storage | SQL tables with fixed schema | Immutable claims, schema-on-query |
| Sync | Bolt-on protocol (CRDTs, WebSockets, etc.) | Built-in via content-addressed claims |
| Derived data | Application code or materialized views | Declarative rules |
| Offline support | Complex sync queue | Claims merge causally |
| Schema evolution | Migrations | Additive: assert new attributes |
