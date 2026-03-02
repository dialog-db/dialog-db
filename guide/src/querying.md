# Querying

Querying in Dialog works through pattern matching. You describe a pattern with variables and constants, and the query engine finds all claims that match.

## Terms: variables and constants

The building block of patterns is `Term<T>`. A term is either:

- A **variable**: a named placeholder that the engine will try to fill in
- A **constant**: a concrete value that the engine must match exactly

```rust
use dialog_query::Term;

// A variable named "name" - the engine will bind matching values to it
let name_var: Term<String> = Term::var("name");

// A constant - the engine will only match claims with this exact value
let name_const: Term<String> = Term::from("Pancakes".to_string());
```

Variables with the same name are **unified**: if two patterns both use `Term::var("x")`, the engine will only return results where both positions have the same value. This is how you join data across multiple patterns.

## Querying a single attribute

The simplest query asks about one attribute:

```rust
// "What is the name of this entity?"
let query = Query::<recipe::Name> {
    of: Term::from(pancakes.clone()),
    is: Term::var("name"),
};

let results = query.perform(&session).try_vec().await?;
```

The `of` field specifies which entity, and `is` specifies the value. Here we know the entity and want to find the value, but you can flip it around:

```rust
// "Which entities are named 'Pancakes'?"
let query = Query::<recipe::Name> {
    of: Term::var("entity"),
    is: Term::from("Pancakes".to_string()),
};
```

Or leave both open:

```rust
// "Which entities have a name, and what is it?"
let query = Query::<recipe::Name> {
    of: Term::var("entity"),
    is: Term::var("name"),
};
```

### The expression syntax

There's also an expression-based syntax that reads more naturally:

```rust
// "What name does pancakes have?"
let pattern = recipe::Name::of(pancakes.clone())
    .matches(Term::var("name"));

// "Which entities have name 'Pancakes'?"
let pattern = recipe::Name::of(Term::var("entity"))
    .matches(Term::from("Pancakes".to_string()));
```

Both syntaxes produce the same query.

## Querying a concept

When you query a concept, you're asking for entities that have *all* the concept's attributes. This is a logical conjunction (AND):

```rust
let results = Query::<Recipe> {
    this: Term::var("entity"),
    name: Term::var("name"),
    servings: Term::var("servings"),
}.perform(&session).try_vec().await?;
```

This returns all entities that have both a `recipe/name` and a `recipe/servings`. Each result binds all three variables.

### Fixing some fields

You can make any field a constant to filter results:

```rust
// All recipes named "Pancakes"
let results = Query::<Recipe> {
    this: Term::var("entity"),
    name: Term::from("Pancakes".to_string()),
    servings: Term::var("servings"),
}.perform(&session).try_vec().await?;
```

This is similar to a SQL `WHERE name = 'Pancakes'`. The `name` field is fixed, and the engine only returns entities whose name matches.

### Joining across concepts

Variable unification lets you join data across different concepts. If two query patterns share a variable name, they must match the same value:

```rust
// Find recipes and their authors' names
// (assuming recipe has an Author attribute referencing a User entity)
fn recipe_with_author(result: Query<RecipeWithAuthor>) -> impl When {
    (
        Query::<Recipe> {
            this: result.this.clone(),
            name: result.recipe_name.clone(),
            author: Term::var("author_entity"),
        },
        Query::<User> {
            this: Term::var("author_entity"),
            name: result.author_name.clone(),
        },
    )
}
```

The variable `"author_entity"` appears in both patterns. The engine will only return results where the recipe's `author` attribute points to an entity that also has a `user/name`. This is a join, expressed through shared variables rather than SQL's JOIN syntax.

## How the query planner works

When you run a query, Dialog's planner converts your pattern into an execution plan. It considers:

1. **Which variables are already bound?** Constants and variables bound by earlier patterns constrain the search.
2. **Which indexes are available?** Dialog maintains EAV (entity-attribute-value) and AVE (attribute-value-entity) indexes, so lookups by entity or by value are both efficient.
3. **What ordering minimizes work?** The planner picks the order that produces the fewest intermediate results.

You don't need to think about this in most cases. The planner handles optimization automatically. But it's useful to know that Dialog isn't doing a brute-force scan. Patterns with constants are generally cheaper than patterns with all variables, because constants let the engine use indexes directly.

## Async iteration

Query results are returned as async streams. The simplest way to consume them is `try_vec()`, which collects everything into a `Vec`:

```rust
let results = query.perform(&session).try_vec().await?;
```

For large result sets, you can iterate with the stream directly:

```rust
use futures::TryStreamExt;

let mut stream = query.perform(&session);
while let Some(result) = stream.try_next().await? {
    // process each result
}
```

In the next chapter, we'll see how rules extend querying with derived data.
