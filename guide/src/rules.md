# Rules

Rules derive new data from existing data without storing it. If concept queries are conjunction (AND), rules add disjunction (OR): multiple ways to derive the same concept.

**Example**: Some recipes come from users (`UserRecipe`), others from imports (`ImportedRecipe`). Rules let you query both through a common `Recipe` concept.

## Defining a rule

A rule takes a query pattern for the *conclusion* and returns *premises* (what must hold).

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

The imported recipe uses `title` and `yield_count` instead of `name` and `servings`. The rule maps them through shared variables.

## Installing rules

```rust
let session = Session::open(source)
    .install(recipe_from_user_recipe)?
    .install(recipe_from_import)?;
```

Now `Query::<Recipe>` finds results from both sources. Rules are evaluated at query time, not stored.

## Multiple premises

Premises form a conjunction — all must hold:

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

## Disjunction through multiple rules

Each rule's premises are ANDed. Installing multiple rules for the same concept creates OR:

```rust
let session = Session::open(source)
    .install(recipe_from_user_recipe)?    // OR
    .install(recipe_from_import)?;        // OR
```

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

`!Query::<MeatIngredient>` succeeds when *no* matching claim exists (negation-as-failure).

**Constraint**: variables in negated patterns must already be bound by preceding positive premises. Above, `recipe.this` is bound by `Query::<Recipe>` before the negation uses it.

## Composability

Rules can reference concepts derived by other rules. Dialog chains them automatically — you can build layers of derived data and the query planner handles execution.

Rules are evaluated at query time, so they always reflect the current state. No need to update derived data when underlying claims change.
