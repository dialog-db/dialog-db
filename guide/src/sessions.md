# Sessions and Transactions

So far we've seen how to define attributes and concepts. Now let's look at how to actually read and write data. The entry point for all interactions with Dialog is a **session**.

## Opening a session

A session wraps a store (the backing storage) and an optional set of rules. You open a session with `Session::open`:

```rust
use dialog_query::Session;

let session = Session::open(store);
```

The `store` parameter is anything that implements the `Store` trait. In practice this will be a Dialog artifact store. The session gives you two capabilities:

1. **Querying** - reading data out, including through rules
2. **Editing** - writing data through transactions

## Transactions

Writes happen through transactions. You create a transaction with `session.edit()`, make your changes, and then commit:

```rust
let mut session = Session::open(store);

// Start a transaction
let mut edit = session.edit();

// Assert some claims
edit.assert(recipe::Name::of(pancakes.clone()).is("Pancakes"));
edit.assert(recipe::Servings::of(pancakes.clone()).is(4u32));

// Commit the transaction
session.commit(edit).await?;
```

All assertions in a single transaction are committed atomically. Either they all succeed or none of them do.

### Asserting and retracting

A transaction supports two operations:

**Assert** adds a claim to the current state:

```rust
edit.assert(recipe::Name::of(pancakes.clone()).is("Pancakes"));
```

**Retract** removes a claim from the current state. You can retract by negating the expression with `!`:

```rust
edit.assert(!recipe::Name::of(pancakes.clone()).is("Pancakes"));
```

Or when working with concepts, use the `retract` method:

```rust
edit.retract(Recipe {
    this: pancakes.clone(),
    name: recipe::Name("Pancakes".to_string()),
    servings: recipe::Servings(4),
});
```

Remember that retraction doesn't delete the claim from history. It marks the claim as no longer part of the current state. The original assertion is preserved in the immutable log.

### Asserting concepts

You can assert a concept to write all its attributes at once:

```rust
let mut edit = session.edit();
edit.assert(Recipe {
    this: pancakes.clone(),
    name: recipe::Name("Pancakes".to_string()),
    servings: recipe::Servings(4),
});
session.commit(edit).await?;
```

This is equivalent to asserting each attribute individually, but more convenient when you have all the values at hand.

## Installing rules

Sessions also hold deductive rules. You install rules before querying:

```rust
let session = Session::open(store)
    .install(vegetarian_recipe_rule)?
    .install(quick_recipe_rule)?;
```

Rules are covered in the [Rules chapter](./rules.md). The key point here is that rules are registered on the session, and all queries through that session will consider the installed rules.

## Session lifecycle

A session is a lightweight handle. You can clone it, pass it around, and create multiple transactions over its lifetime. Each `commit` persists the transaction's changes and advances the session's view of the database.

```rust
let mut session = Session::open(store);

// First transaction
let mut edit = session.edit();
edit.assert(recipe::Name::of(pancakes.clone()).is("Pancakes"));
session.commit(edit).await?;

// Second transaction - session now sees the first transaction's changes
let mut edit = session.edit();
edit.assert(recipe::Servings::of(pancakes.clone()).is(4u32));
session.commit(edit).await?;
```

In the next chapter we'll look at the other half of working with data: querying.
