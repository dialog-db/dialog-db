# Cardinality

When you assert a new value for an attribute that already has one, what should happen? Should the new value replace the old one, or should both coexist? This is what cardinality controls.

## Cardinality one

By default, attributes have cardinality **one**. An entity can have at most one value for the attribute at a time. If you assert a new value, the previous one is retracted:

```rust
mod recipe {
    #[derive(Attribute, Clone)]
    pub struct Name(pub String);  // cardinality one (default)
}

// Assert "Pancakes"
edit.assert(recipe::Name::of(pancakes.clone()).is("Pancakes"));
session.commit(edit).await?;

// Later, assert "Fluffy Pancakes" - this retracts "Pancakes"
let mut edit = session.edit();
edit.assert(recipe::Name::of(pancakes.clone()).is("Fluffy Pancakes"));
session.commit(edit).await?;
```

After the second assertion, the name of `pancakes` is "Fluffy Pancakes". The fact asserting "Pancakes" has been retracted. (It still exists in the history, but it is no longer part of the current state.)

This is the common case. A recipe has one name, one serving count, one author.

## Cardinality many

Some attributes naturally have multiple values. A recipe can have several tags. A recipe has multiple ingredients. For these, use `#[cardinality(many)]`:

```rust
mod recipe {
    #[derive(Attribute, Clone)]
    #[cardinality(many)]
    pub struct Tag(pub String);

    #[derive(Attribute, Clone)]
    #[cardinality(many)]
    pub struct Ingredient(pub Entity);
}
```

With cardinality many, asserting a new value does *not* retract existing values. Values accumulate:

```rust
edit.assert(recipe::Tag::of(pancakes.clone()).is("breakfast"));
edit.assert(recipe::Tag::of(pancakes.clone()).is("sweet"));
```

Now `pancakes` has two tags: "breakfast" and "sweet". To remove one, you explicitly retract it:

```rust
edit.assert(!recipe::Tag::of(pancakes.clone()).is("sweet"));
```

## Cardinality and sync

Cardinality becomes especially relevant when multiple peers are editing the same entity concurrently. Consider this scenario:

1. Peer A sets the name of a recipe to "Pancakes"
2. Peer B, concurrently, sets the name to "Fluffy Pancakes"
3. They sync

With cardinality one, Dialog's transactor can use the causal references on each fact to determine what happened. Each assertion carries information about which prior facts it was aware of. The transactor uses this to resolve the situation, typically keeping both values temporarily and letting the application decide, or applying last-writer-wins semantics.

With cardinality many, concurrent assertions simply accumulate. Both values are kept because that's the intended semantics of the attribute.

> **Note**: The precise conflict resolution behavior for concurrent cardinality-one writes is an active area of development. The current model uses causal references (the `cause` on each fact) and value-based comparison rather than requiring applications to pass provenance tokens. See the [Sync chapter](./sync.md) for more details.

## Choosing the right cardinality

A reasonable rule of thumb:

- If the question is "what is the X of this entity?" use cardinality one. (What is the name? What is the serving count?)
- If the question is "what are the Xs of this entity?" use cardinality many. (What are the tags? What are the ingredients?)

Sometimes the answer isn't obvious. Is an "assignee" singular or plural? Different applications might model it differently. In Dialog, this is a feature rather than a problem: two modules can define the same semantic relation with different cardinalities and both will work against the same underlying facts. The [cardinality model](https://gozala.io/dialog/modeling-cardinality) document discusses this in more detail.
