# Derived Data with Rules

In the previous chapter, we used rules to join data across entity types. Rules can also derive entirely new classifications and computed properties. Let's add some to our recipe book.

## Quick recipes

Let's define a concept for recipes that take 15 minutes or less to prepare. We'll use a formula to compare the prep time:

```rust
mod recipe {
    /// Whether the recipe qualifies as "quick"
    #[derive(Attribute, Clone)]
    pub struct Quick(pub bool);
}

#[derive(Concept, Debug, Clone)]
pub struct QuickRecipe {
    pub this: Entity,
    pub name: recipe::Name,
    pub prep_time: recipe::PrepTime,
}
```

There's a subtlety here. We want to express "prep_time <= 15" but Dialog's formulas work with equality, not comparison operators. One approach is to define a custom formula:

```rust
use dialog_query::{Formula, Input};

#[derive(Debug, Clone, Formula)]
pub struct LessThanOrEqual {
    pub value: u32,
    pub threshold: u32,
    #[derived]
    pub result: bool,
}

impl LessThanOrEqual {
    pub fn derive(input: Input<Self>) -> Vec<Self> {
        vec![LessThanOrEqual {
            value: input.value,
            threshold: input.threshold,
            result: input.value <= input.threshold,
        }]
    }
}
```

Now the rule:

```rust
fn quick_recipe_rule(recipe: Query<QuickRecipe>) -> impl When {
    (
        Query::<Recipe> {
            this: recipe.this.clone(),
            name: recipe.name.clone(),
            servings: Term::var("_servings"),
            prep_time: recipe.prep_time.clone(),
            author: Term::var("_author"),
        },
        Match::<LessThanOrEqual> {
            value: recipe.prep_time.clone().into(),
            threshold: Term::from(15u32),
            result: Term::from(true),
        },
    )
}
```

The `Match::<LessThanOrEqual>` formula computes whether the prep time is <= 15. By fixing `result` to `true`, we filter out recipes where it isn't.

```rust
let session = session.install(quick_recipe_rule)?;

let quick = Query::<QuickRecipe> {
    this: Term::var("entity"),
    name: Term::var("name"),
    prep_time: Term::var("time"),
}.perform(&session).try_vec().await?;
// Returns "Garden Salad" (10 min) but not "Fluffy Pancakes" (20 min)
```

## Recipes by ingredient

Let's derive which recipes use a specific ingredient, producing a simple view:

```rust
#[derive(Concept, Debug, Clone)]
pub struct RecipeUsingIngredient {
    pub this: Entity,
    pub name: recipe::Name,
    pub ingredient_name: ingredient::Name,
}

fn recipe_using_ingredient(view: Query<RecipeUsingIngredient>) -> impl When {
    (
        Query::<RecipeListItem> {
            this: view.this.clone(),
            name: view.name.clone(),
        },
        Query::<RecipeIngredientEntry> {
            this: Term::var("_entry"),
            recipe: Term::var("recipe_ref"),
            ingredient: Term::var("ingredient_entity"),
            quantity: Term::var("_qty"),
            unit: Term::var("_unit"),
        },
        // The entry's recipe reference must match our recipe
        view.this.clone().is(Term::var("recipe_ref")),
        // Get the ingredient's name
        ingredient::Name::of(Term::var("ingredient_entity"))
            .matches(view.ingredient_name.clone()),
    )
}
```

Now you can ask "which recipes use Flour?":

```rust
let session = session.install(recipe_using_ingredient)?;

let with_flour = Query::<RecipeUsingIngredient> {
    this: Term::var("entity"),
    name: Term::var("recipe_name"),
    ingredient_name: Term::from("Flour".to_string()),
}.perform(&session).try_vec().await?;
```

## Composing rules

Rules can build on other rules. If `QuickRecipe` is defined by a rule and `RecipeUsingIngredient` is defined by another rule, you can write a third rule that combines them:

```rust
#[derive(Concept, Debug, Clone)]
pub struct QuickRecipeWith {
    pub this: Entity,
    pub name: recipe::Name,
    pub ingredient_name: ingredient::Name,
}

fn quick_recipe_with(view: Query<QuickRecipeWith>) -> impl When {
    (
        Query::<QuickRecipe> {
            this: view.this.clone(),
            name: view.name.clone(),
            prep_time: Term::var("_time"),
        },
        Query::<RecipeUsingIngredient> {
            this: view.this.clone(),
            name: Term::var("_name2"),
            ingredient_name: view.ingredient_name.clone(),
        },
    )
}
```

This derives "quick recipes that use a given ingredient" by composing the two earlier rules. The query planner handles the execution. You don't need to think about how the intermediate results are joined.

## Summary of our recipe book

At this point, our recipe book supports:

| Capability | How |
|---|---|
| Store recipes with structured data | Attributes + concepts |
| Multiple tags per recipe | Cardinality-many attribute |
| Link recipes to ingredients with quantities | Join entities (recipe-ingredient entries) |
| Look up recipes by name, tag, author, ingredient | Concept queries with constants |
| Classify recipes as "quick" | Rule with formula |
| Compose classifications | Rules that reference other rules |
| Sync across devices | Built-in (no extra code) |

All of the query logic is declarative. You describe *what* you want, and Dialog figures out *how* to get it.
