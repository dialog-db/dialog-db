# The Dialog Notation

While the Rust API uses derive macros and Rust types to define attributes, concepts, and rules, Dialog also has a platform-agnostic notation for describing domain models. This notation is designed for use outside of Rust, in configuration files, tooling, and language bindings.

This chapter covers the notation system. If you're only using Dialog from Rust, you can skip this chapter, but it may still be useful for understanding how Dialog models data at a structural level.

## Selectors

Every attribute in Dialog has a **selector**: a string in `domain/name` format that identifies it. In Rust, this is derived automatically from the module and struct name:

```rust
mod recipe {
    #[derive(Attribute, Clone)]
    pub struct PrepTime(pub u32);
    // Selector: "recipe/prep-time"
}
```

In the notation, selectors are written directly:

```yaml
recipe/prep-time:
  type: UnsignedInteger
  cardinality: one
```

Domains use kebab-case and follow a reversed-domain-name convention to avoid collisions in shared environments. For example, attributes in a personal project might use `io.gozala.recipe/prep-time`, while a shared standard might use `org.open-recipes/prep-time`.

The combined selector cannot exceed 64 bytes.

## Structural identity

An important property of the notation: **identity is structural, not nominal**. Two attributes with the same selector, type, and cardinality are the same attribute, regardless of what you call them in your code. Similarly, two concepts with the same set of attributes are the same concept.

This means that different applications can define the same attribute independently (using the same selector, type, and cardinality) and their data will be interoperable.

## Attribute definitions

An attribute in formal notation specifies:

```yaml
recipe/name:
  type: Text
  cardinality: one
  description: "The name of a recipe"
```

The fields:
- `type`: One of `Bytes`, `Entity`, `Boolean`, `Text`, `UnsignedInteger`, `SignedInteger`, `Float`, `Symbol`
- `cardinality`: `one` or `many` (defaults to `one`)
- `description`: Optional human-readable description (does not affect identity)

## Concept definitions

A concept in formal notation lists its constituent attributes:

```yaml
Recipe:
  domain: recipe
  attributes:
    name:
      the: recipe/name
      type: Text
    servings:
      the: recipe/servings
      type: UnsignedInteger
    prep-time:
      the: recipe/prep-time
      type: UnsignedInteger
```

Since a concept's identity is derived from its sorted set of attributes, two concepts with the same attributes are equivalent even if they have different names.

## Abbreviated notation

For convenience, there's an abbreviated form that infers domains and selectors from structure:

```yaml
recipe:
  Name: Text
  Servings: UnsignedInteger
  PrepTime: UnsignedInteger
  Tag:
    type: Text
    cardinality: many
```

This expands to the full form automatically:
- Labels become attribute names (kebab-cased)
- The enclosing key becomes the domain
- Simple `Type` values imply cardinality one
- Object values allow specifying cardinality and other properties

## Rules in notation

Rules can also be expressed in notation, describing how to derive concepts from premises:

```yaml
VegetarianRecipe:
  domain: recipe
  rules:
    - when:
        - Recipe:
            this: $this
            name: $name
            servings: $servings
      unless:
        - recipe/meat-ingredient:
            of: $this
            is: _
      then:
        this: $this
        name: $name
        servings: $servings
```

Variables are prefixed with `$`. The blank wildcard `_` matches any value without binding it. The `when` clause lists premises that must all match. The `unless` clause lists patterns that must *not* match. The `then` clause maps the bound variables to the derived concept's fields.

## Formulas in notation

Built-in formulas are referenced by name:

```yaml
- formula/sum:
    of: $servings
    with: $extra
    is: $total
```

## When to use the notation

The notation is useful when:

- You want to define domain models in a language-agnostic way, so they can be shared between Rust, TypeScript, and other language bindings
- You're building tooling that generates Dialog models
- You want a human-readable representation of your schema for documentation or review

For Rust development, the derive macros are generally more ergonomic. The notation and the Rust types describe the same thing, just in different formats.
