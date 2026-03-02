# Defining the Domain

Let's start by defining the attributes and concepts for our recipe book.

## Recipe attributes

A recipe has a name, a serving count, a prep time (in minutes), and an author (a reference to a user entity):

```rust
mod recipe {
    use dialog_query::{Attribute, Entity};

    /// The display name of a recipe
    #[derive(Attribute, Clone)]
    pub struct Name(pub String);

    /// Number of servings this recipe makes
    #[derive(Attribute, Clone)]
    pub struct Servings(pub u32);

    /// Preparation time in minutes
    #[derive(Attribute, Clone)]
    pub struct PrepTime(pub u32);

    /// The user who created this recipe
    #[derive(Attribute, Clone)]
    pub struct Author(pub Entity);

    /// A freeform tag for categorization
    #[derive(Attribute, Clone)]
    #[cardinality(many)]
    pub struct Tag(pub String);
}
```

Notice that `Tag` has `#[cardinality(many)]` because a recipe can have multiple tags. All the other attributes have the default cardinality of one.

## Ingredient attributes

An ingredient is its own entity. Each ingredient has a name:

```rust
mod ingredient {
    use dialog_query::Attribute;

    /// The name of an ingredient (e.g., "flour", "eggs")
    #[derive(Attribute, Clone)]
    pub struct Name(pub String);
}
```

## Linking recipes to ingredients

To say "recipe X uses ingredient Y," we need a relationship between two entities. There are a few ways to model this in Dialog.

One approach is to use a cardinality-many entity attribute on the recipe:

```rust
mod recipe {
    /// An ingredient used in this recipe
    #[derive(Attribute, Clone)]
    #[cardinality(many)]
    pub struct Ingredient(pub Entity);
}
```

This says "a recipe can reference many ingredient entities." It doesn't capture the quantity or unit for each ingredient, though. For that, we'd need a richer model.

### Ingredient entries as entities

A more flexible approach is to create a separate entity for each "recipe uses ingredient" relationship:

```rust
mod recipe_ingredient {
    use dialog_query::{Attribute, Entity};

    /// The recipe this entry belongs to
    #[derive(Attribute, Clone)]
    pub struct Recipe(pub Entity);

    /// The ingredient being used
    #[derive(Attribute, Clone)]
    pub struct Ingredient(pub Entity);

    /// How much of the ingredient (e.g., "2", "0.5")
    #[derive(Attribute, Clone)]
    pub struct Quantity(pub String);

    /// The unit of measurement (e.g., "cups", "tsp", "whole")
    #[derive(Attribute, Clone)]
    pub struct Unit(pub String);
}
```

Each "recipe-ingredient entry" is its own entity with attributes pointing to the recipe, the ingredient, and the amount. This pattern is equivalent to a join table in a relational database.

## User attributes

Users are simple for now:

```rust
mod user {
    use dialog_query::Attribute;

    /// A user's display name
    #[derive(Attribute, Clone)]
    pub struct Name(pub String);
}
```

## Concepts

Now we can define concepts that group these attributes:

```rust
use dialog_query::{Concept, Entity};

/// A recipe with its core information
#[derive(Concept, Debug, Clone)]
pub struct Recipe {
    pub this: Entity,
    pub name: recipe::Name,
    pub servings: recipe::Servings,
    pub prep_time: recipe::PrepTime,
    pub author: recipe::Author,
}

/// A minimal recipe view (just name)
#[derive(Concept, Debug, Clone)]
pub struct RecipeListItem {
    pub this: Entity,
    pub name: recipe::Name,
}

/// An ingredient entry linking a recipe to an ingredient with quantity
#[derive(Concept, Debug, Clone)]
pub struct RecipeIngredientEntry {
    pub this: Entity,
    pub recipe: recipe_ingredient::Recipe,
    pub ingredient: recipe_ingredient::Ingredient,
    pub quantity: recipe_ingredient::Quantity,
    pub unit: recipe_ingredient::Unit,
}

/// A user
#[derive(Concept, Debug, Clone)]
pub struct User {
    pub this: Entity,
    pub name: user::Name,
}
```

Note how `RecipeListItem` is a subset of `Recipe`. Any entity that matches `Recipe` will also match `RecipeListItem`, since having all five attributes implies having name. The reverse isn't true: an entity could have just a name and no other recipe attributes.

## What we have so far

We've defined:
- 10 attributes across 4 modules (`recipe`, `ingredient`, `recipe_ingredient`, `user`)
- 4 concepts providing different views
- Entity references connecting recipes to ingredients and users

None of this has touched the database yet. These are type-level definitions that tell Dialog how to interpret claims. In the next section, we'll use them to write data.
