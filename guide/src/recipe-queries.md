# Searching and Filtering

Now that we have some data, let's query it. We'll start with simple lookups and build up to joins across entities.

## List all recipes

The simplest concept query: find all entities that match `Recipe`.

```rust
use dialog_query::{Query, Term};

let all_recipes = Query::<Recipe> {
    this: Term::var("entity"),
    name: Term::var("name"),
    servings: Term::var("servings"),
    prep_time: Term::var("prep_time"),
    author: Term::var("author"),
}.perform(&session).try_vec().await?;
```

This returns every entity that has all four recipe attributes (name, servings, prep_time, author). Each result binds all five variables.

If you only need names, use the lighter concept:

```rust
let names = Query::<RecipeListItem> {
    this: Term::var("entity"),
    name: Term::var("name"),
}.perform(&session).try_vec().await?;
```

## Find a specific recipe

Fix the `name` field to a constant:

```rust
let result = Query::<Recipe> {
    this: Term::var("entity"),
    name: Term::from("Fluffy Pancakes".to_string()),
    servings: Term::var("servings"),
    prep_time: Term::var("prep_time"),
    author: Term::var("author"),
}.perform(&session).try_vec().await?;
```

## Find recipes by tag

Tags are cardinality-many attributes, not part of the `Recipe` concept. To find recipes with a specific tag, combine a concept query with an attribute query using shared variables in a rule:

```rust
#[derive(Concept, Debug, Clone)]
pub struct TaggedRecipe {
    pub this: Entity,
    pub name: recipe::Name,
    pub tag: recipe::Tag,
}

fn tagged_recipe_rule(result: Query<TaggedRecipe>) -> impl When {
    (
        Query::<RecipeListItem> {
            this: result.this.clone(),
            name: result.name.clone(),
        },
        recipe::Tag::of(result.this.clone())
            .matches(result.tag.clone()),
    )
}

let session = session.install(tagged_recipe_rule)?;

// All recipes tagged "quick"
let quick_recipes = Query::<TaggedRecipe> {
    this: Term::var("entity"),
    name: Term::var("name"),
    tag: Term::from("quick".to_string()),
}.perform(&session).try_vec().await?;
```

This returns both "Fluffy Pancakes" and "Garden Salad," since both are tagged "quick."

## Find ingredients for a recipe

This requires joining across three types of entities: the recipe, the ingredient entries, and the ingredients themselves.

```rust
#[derive(Concept, Debug, Clone)]
pub struct RecipeIngredientView {
    pub this: Entity,  // the entry entity
    pub recipe_name: recipe::Name,
    pub ingredient_name: ingredient::Name,
    pub quantity: recipe_ingredient::Quantity,
    pub unit: recipe_ingredient::Unit,
}
```

We need a rule to wire this up, since the data spans three entity types:

```rust
fn recipe_ingredient_view(view: Query<RecipeIngredientView>) -> impl When {
    (
        // The recipe-ingredient entry
        Query::<RecipeIngredientEntry> {
            this: view.this.clone(),
            recipe: Term::var("recipe_entity"),
            ingredient: Term::var("ingredient_entity"),
            quantity: view.quantity.clone(),
            unit: view.unit.clone(),
        },
        // The recipe's name
        recipe::Name::of(Term::var("recipe_entity"))
            .matches(view.recipe_name.clone()),
        // The ingredient's name
        ingredient::Name::of(Term::var("ingredient_entity"))
            .matches(view.ingredient_name.clone()),
    )
}

let session = session.install(recipe_ingredient_view)?;

// All ingredients for "Fluffy Pancakes"
let ingredients = Query::<RecipeIngredientView> {
    this: Term::var("entry"),
    recipe_name: Term::from("Fluffy Pancakes".to_string()),
    ingredient_name: Term::var("ingredient"),
    quantity: Term::var("qty"),
    unit: Term::var("unit"),
}.perform(&session).try_vec().await?;
```

The shared variables `"recipe_entity"` and `"ingredient_entity"` join the three entity types together. The query planner will pick an efficient execution order.

## Find recipes by author name

Similarly, to search by author name instead of author entity, join through the user:

```rust
#[derive(Concept, Debug, Clone)]
pub struct RecipeByAuthor {
    pub this: Entity,
    pub name: recipe::Name,
    pub author_name: user::Name,
}

fn recipe_by_author_rule(result: Query<RecipeByAuthor>) -> impl When {
    (
        Query::<RecipeListItem> {
            this: result.this.clone(),
            name: result.name.clone(),
        },
        recipe::Author::of(result.this.clone())
            .matches(Term::var("author_entity")),
        user::Name::of(Term::var("author_entity"))
            .matches(result.author_name.clone()),
    )
}

let session = session.install(recipe_by_author_rule)?;

// All recipes by Alice
let alice_recipes = Query::<RecipeByAuthor> {
    this: Term::var("entity"),
    name: Term::var("name"),
    author_name: Term::from("Alice".to_string()),
}.perform(&session).try_vec().await?;
```

## Pattern: reusable views through rules

Notice the pattern here. Each query that joins across entity types is expressed as:

1. A concept defining the shape of the result
2. A rule describing how to derive it from existing data
3. A query against the concept with some fields fixed

This pattern scales well. As your domain grows, you build up a library of views (concepts + rules) that can be combined and reused. The query planner handles the execution details.
