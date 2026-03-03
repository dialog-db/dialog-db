# Sessions and Transactions

The entry point for all interactions with Dialog is a **session**.

## Opening a session

A session wraps a source (the backing storage). You open one with `Session::open`:

```rust
use dialog_query::Session;

let session = Session::open(source);
```

The `source` parameter implements the `Source` trait. A session gives you two capabilities:

1. **Querying** — reading data, including through rules
2. **Editing** — writing data through transactions

## Transactions

You create a transaction with `session.edit()`, make changes, and commit:

```rust
let mut session = Session::open(source);
let mut edit = session.edit();

edit.assert(recipe::Name::of(pancakes.clone()).is("Pancakes"));
edit.assert(recipe::Servings::of(pancakes.clone()).is(4u32));

session.commit(edit).await?;
```

All assertions in a single transaction are committed atomically.

### Asserting and retracting

**Assert** adds a claim:

```rust
edit.assert(recipe::Name::of(pancakes.clone()).is("Pancakes"));
```

**Retract** removes a claim (negate with `!`):

```rust
edit.assert(!recipe::Name::of(pancakes.clone()).is("Pancakes"));
```

Retraction doesn't delete the claim from history — it marks it as no longer part of the current state.

### Working with concepts

You can assert or retract a concept to handle all its attributes at once. This lets you work at the semantic layer — thinking in terms of your data model — without worrying about the individual claims a concept is comprised of:

```rust
let mut edit = session.edit();
edit.assert(Recipe {
    this: pancakes.clone(),
    name: recipe::Name("Pancakes".to_string()),
    servings: recipe::Servings(4),
});
session.commit(edit).await?;
```

## Session lifecycle

A session is a lightweight handle. You can clone it, pass it around, and create multiple transactions over its lifetime. Each `commit` advances the session's view of the database.

```rust
let mut session = Session::open(source);

let mut edit = session.edit();
edit.assert(recipe::Name::of(pancakes.clone()).is("Pancakes"));
session.commit(edit).await?;

// Second transaction sees the first transaction's changes
let mut edit = session.edit();
edit.assert(recipe::Servings::of(pancakes.clone()).is(4u32));
session.commit(edit).await?;
```

In the next chapter we'll look at the other half of working with data: querying.
