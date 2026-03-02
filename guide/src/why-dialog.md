# Why Dialog?

Let's start with a familiar scenario. You're building a collaborative app, say a recipe book that a household shares. You need to:

- Store recipes with their ingredients and instructions
- Let family members add and edit recipes from their own devices
- Keep everything in sync, even when someone edits offline
- Derive useful views like "all vegetarian recipes" or "recipes I can make with what's in the fridge"

With a traditional stack, you'd pick a database, build a REST API, add WebSocket notifications for real-time updates, bolt on a conflict resolution strategy, and set up a sync protocol. Each of these is a separate concern with its own complexity.

Dialog collapses these layers. Let's see how.

## Facts, not rows

In a relational database, you'd create a `recipes` table with columns for `name`, `servings`, `author`, etc. If you later want to add a `cuisine` field, you need a migration. If two systems disagree about the schema, you have a problem.

Dialog stores data as individual **facts**. A fact is a simple statement:

```text
the name of <recipe-123> is "Pancakes"
the servings of <recipe-123> is 4
the author of <recipe-123> is <user-456>
```

Each fact stands on its own. There's no table to alter when you want to say something new about a recipe; you just assert another fact. Two applications can say different things about the same entity without conflicting.

## Query-driven structure

Instead of enforcing a schema at write time ("every row in this table must have these columns"), Dialog applies structure at query time. You define a **concept**, a group of attributes you're interested in, and query for entities that have all of them:

```rust
#[derive(Concept)]
struct Recipe {
    this: Entity,
    name: recipe::Name,
    servings: recipe::Servings,
}
```

This says: "Find me entities that have both a `name` and a `servings`." If an entity only has a name, it won't match this query. If it has name, servings, and a hundred other attributes, it still matches. The extra attributes are simply not part of this view.

This means different parts of your application can define different concepts over the same entities, and they'll all work without stepping on each other.

## Built-in sync

When you assert a fact in Dialog, it gets an immutable, content-addressed identity and a **causal reference** that establishes "this fact was asserted knowing about these prior facts."

This gives Dialog a built-in sync protocol. Two peers can compare their causal histories and exchange exactly the facts they're each missing. There's no central server required, no conflict resolution protocol to design. The data structure itself enables synchronization.

We'll cover sync in detail in a [later chapter](./sync.md). For now, the key insight is: **you don't build sync on top of Dialog; sync is part of what Dialog is.**

## Derived data

Dialog includes a rule system inspired by Datalog. You can define rules that derive new facts from existing ones:

```rust
fn vegetarian_recipe(recipe: Query<VegetarianRecipe>) -> impl When {
    (
        Query::<Recipe> {
            this: recipe.this.clone(),
            name: recipe.name.clone(),
            servings: recipe.servings.clone(),
        },
        // No meat ingredients
        !Query::<HasMeatIngredient> {
            this: recipe.this.clone(),
        },
    )
}
```

This rule says: "A `VegetarianRecipe` is any `Recipe` that doesn't have a meat ingredient." You don't need to maintain a separate "vegetarian" flag; the classification is derived whenever the underlying facts change.

## Summary

| Concern | Traditional stack | Dialog |
|---|---|---|
| Storage | SQL tables with fixed schema | Immutable facts, schema-on-query |
| Sync | Bolt-on protocol (CRDTs, WebSockets, etc.) | Built-in via content-addressed facts |
| Derived data | Application code or materialized views | Declarative rules |
| Offline support | Complex sync queue | Facts merge causally |
| Schema evolution | Migrations | Additive: assert new attributes |

In the next chapter, we'll look at the core concepts that make this work.
