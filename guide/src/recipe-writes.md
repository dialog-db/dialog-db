# Adding Recipes

With our domain defined, let's write some data. We'll create users, recipes, ingredients, and link them together.

## Setting up a session

First, open a session with a store:

```rust
use dialog_query::{Entity, Session};

let mut session = Session::open(store);
```

## Creating a user

```rust
let alice = Entity::new()?;

let mut edit = session.edit();
edit.assert(User {
    this: alice.clone(),
    name: user::Name("Alice".to_string()),
});
session.commit(edit).await?;
```

## Creating ingredients

```rust
let flour = Entity::new()?;
let eggs = Entity::new()?;
let milk = Entity::new()?;
let sugar = Entity::new()?;

let mut edit = session.edit();
edit.assert(ingredient::Name::of(flour.clone()).is("Flour"));
edit.assert(ingredient::Name::of(eggs.clone()).is("Eggs"));
edit.assert(ingredient::Name::of(milk.clone()).is("Milk"));
edit.assert(ingredient::Name::of(sugar.clone()).is("Sugar"));
session.commit(edit).await?;
```

Here we used the attribute expression syntax instead of concepts, since `ingredient::Name` is the only attribute we're setting. Both approaches work.

## Creating a recipe

```rust
let pancakes = Entity::new()?;

let mut edit = session.edit();

// Core recipe data
edit.assert(Recipe {
    this: pancakes.clone(),
    name: recipe::Name("Pancakes".to_string()),
    servings: recipe::Servings(4),
    prep_time: recipe::PrepTime(20),
    author: recipe::Author(alice.clone()),
});

// Tags (cardinality many, so these accumulate)
edit.assert(recipe::Tag::of(pancakes.clone()).is("breakfast"));
edit.assert(recipe::Tag::of(pancakes.clone()).is("sweet"));
edit.assert(recipe::Tag::of(pancakes.clone()).is("quick"));

session.commit(edit).await?;
```

The `Recipe` concept assertion stores four claims (name, servings, prep_time, author). The tags are separate cardinality-many claims.

## Linking ingredients to the recipe

For each ingredient, we create a recipe-ingredient entry entity:

```rust
let mut edit = session.edit();

// Flour: 2 cups
let entry1 = Entity::new()?;
edit.assert(RecipeIngredientEntry {
    this: entry1,
    recipe: recipe_ingredient::Recipe(pancakes.clone()),
    ingredient: recipe_ingredient::Ingredient(flour.clone()),
    quantity: recipe_ingredient::Quantity("2".to_string()),
    unit: recipe_ingredient::Unit("cups".to_string()),
});

// Eggs: 2 whole
let entry2 = Entity::new()?;
edit.assert(RecipeIngredientEntry {
    this: entry2,
    recipe: recipe_ingredient::Recipe(pancakes.clone()),
    ingredient: recipe_ingredient::Ingredient(eggs.clone()),
    quantity: recipe_ingredient::Quantity("2".to_string()),
    unit: recipe_ingredient::Unit("whole".to_string()),
});

// Milk: 1.5 cups
let entry3 = Entity::new()?;
edit.assert(RecipeIngredientEntry {
    this: entry3,
    recipe: recipe_ingredient::Recipe(pancakes.clone()),
    ingredient: recipe_ingredient::Ingredient(milk.clone()),
    quantity: recipe_ingredient::Quantity("1.5".to_string()),
    unit: recipe_ingredient::Unit("cups".to_string()),
});

session.commit(edit).await?;
```

Each entry is its own entity with references to both the recipe and the ingredient. This is the join table pattern mentioned in the domain chapter.

## Updating a recipe

To change a recipe's name, assert the new value. Since `recipe::Name` has cardinality one, the old value is retracted:

```rust
let mut edit = session.edit();
edit.assert(recipe::Name::of(pancakes.clone()).is("Fluffy Pancakes"));
session.commit(edit).await?;
```

After this, the recipe's name is "Fluffy Pancakes". The old "Pancakes" claim is still in the history but no longer part of the current state.

## Removing a tag

Tags have cardinality many, so asserting a new tag doesn't remove existing ones. To remove a tag, retract it explicitly:

```rust
let mut edit = session.edit();
edit.assert(!recipe::Tag::of(pancakes.clone()).is("sweet"));
session.commit(edit).await?;
```

The recipe now has tags "breakfast" and "quick" but not "sweet."

## Adding another recipe

Let's add a second recipe to make the querying examples more interesting:

```rust
let salad = Entity::new()?;

let mut edit = session.edit();
edit.assert(Recipe {
    this: salad.clone(),
    name: recipe::Name("Garden Salad".to_string()),
    servings: recipe::Servings(2),
    prep_time: recipe::PrepTime(10),
    author: recipe::Author(alice.clone()),
});
edit.assert(recipe::Tag::of(salad.clone()).is("healthy"));
edit.assert(recipe::Tag::of(salad.clone()).is("quick"));
edit.assert(recipe::Tag::of(salad.clone()).is("vegetarian"));
session.commit(edit).await?;
```

We now have two recipes, four ingredients, ingredient entries linking them, tags, and a user. In the next section, we'll query this data.
