# Rules

So far, every query has matched directly against stored claims. Rules let you derive new data from existing data, without storing it. If a concept query is a conjunction (AND), rules add disjunction (OR): multiple alternative ways to derive the same concept.

## A motivating example

Suppose you have two different sources of recipe data. Some recipes were entered by users (as `UserRecipe` concepts), and others were imported from a cookbook (as `ImportedRecipe` concepts). You want to query all recipes uniformly, regardless of their source.

Without rules, you'd have to query both sources separately and merge the results in your application code. With rules, you define how each source maps to a common `Recipe` concept, and Dialog handles the rest.

## Defining a rule

A rule is a function that takes a query pattern for the *conclusion* (what you want to derive) and returns the *premises* (what must be true for the conclusion to hold).

```rust
use dialog_query::{Query, When};

fn recipe_from_user_recipe(recipe: Query<Recipe>) -> impl When {
    (
        Query::<UserRecipe> {
            this: recipe.this.clone(),
            name: recipe.name.clone(),
            servings: recipe.servings.clone(),
        },
    )
}
```

This says: "A `Recipe` can be derived from any `UserRecipe` by mapping their shared attributes."

And another rule for imported recipes:

```rust
fn recipe_from_import(recipe: Query<Recipe>) -> impl When {
    (
        Query::<ImportedRecipe> {
            this: recipe.this.clone(),
            title: recipe.name.clone(),    // different attribute name, same variable
            yield_count: recipe.servings.clone(),
        },
    )
}
```

Notice that the imported recipe uses `title` instead of `name` and `yield_count` instead of `servings`. The rule maps them to the corresponding `Recipe` fields through shared variables.

## Installing rules

Rules are installed on a session:

```rust
let session = Session::open(store)
    .install(recipe_from_user_recipe)?
    .install(recipe_from_import)?;
```

Now any `Query::<Recipe>` against this session will find results from both sources. The rules are evaluated at query time, not stored in the database.

## Multiple premises

A rule can have multiple premises that must all be satisfied. They form a conjunction:

```rust
fn authored_recipe(recipe: Query<AuthoredRecipe>) -> impl When {
    (
        Query::<Recipe> {
            this: recipe.this.clone(),
            name: recipe.name.clone(),
            servings: Term::var("_servings"),
        },
        Query::<User> {
            this: Term::var("author_entity"),
            name: recipe.author_name.clone(),
        },
        // The recipe must reference this user as its author
        recipe::Author::of(recipe.this.clone())
            .matches(Term::var("author_entity")),
    )
}
```

This derives `AuthoredRecipe` only when a recipe exists, a user exists, and the recipe's `author` attribute points to that user. All three premises must hold.

## Disjunction through multiple rules

Each individual rule's premises are ANDed together. But installing multiple rules for the same concept creates OR behavior:

```rust
// A recipe comes from user input OR from an import
let session = Session::open(store)
    .install(recipe_from_user_recipe)?    // OR
    .install(recipe_from_import)?;        // OR
```

A `Query::<Recipe>` returns results matching *any* of the installed rules. This is Dialog's form of logical disjunction.

## Negation

Sometimes you want to derive a concept only when something is *not* true. Use `!` to negate a pattern:

```rust
fn meatless_recipe(recipe: Query<MeatlessRecipe>) -> impl When {
    (
        Query::<Recipe> {
            this: recipe.this.clone(),
            name: recipe.name.clone(),
            servings: recipe.servings.clone(),
        },
        // This entity must NOT have a meat-ingredient claim
        !Query::<MeatIngredient> {
            this: recipe.this.clone(),
            ingredient: Term::var("_"),
        },
    )
}
```

The negated pattern (`!Query::<MeatIngredient>`) succeeds when *no* matching claim exists. This is known as negation-as-failure: the absence of a match counts as success.

There's an important constraint with negation: the variables used in a negated pattern should already be bound by preceding positive premises. In the example above, `recipe.this` is bound by the `Query::<Recipe>` pattern before the negation uses it. If you put the negation first, the engine wouldn't know which entities to check.

## Rules are composable

Rules can reference concepts that are themselves derived by other rules. If you have a rule that derives `VegetarianRecipe` from `MeatlessRecipe`, and another that derives `MeatlessRecipe` from `Recipe`, Dialog will chain them automatically.

This composability is what makes rules powerful for larger applications. You can build up layers of derived data, each layer using the one below it, and the query planner handles the execution.

## When to use rules

Rules are useful when:

- You have multiple sources of data that should be queryable through a common interface
- You want to derive classifications or groupings (vegetarian, quick, easy) from raw attributes
- You need to join data across concepts in a reusable way
- You want to keep derived logic declarative rather than imperative

Rules are evaluated at query time, so they always reflect the current state of the database. There is no need to update derived data when the underlying claims change.
