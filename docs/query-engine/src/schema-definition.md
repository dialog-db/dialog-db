# Defining the Schema

Dialog queries operate on claims, `(the, of, is, cause)` tuples, but users
don't typically write raw tuples. Instead they define domain models using
**attributes**, **concepts**, and **formulas**, either through Rust derive
macros or through a JSON notation. This chapter explains both paths.

## Attributes

An **attribute** names a typed relation in `domain/name` format. It describes
one property an entity can have.

### Rust: `#[derive(Attribute)]`

```rust
mod employee {
    use dialog_query::prelude::*;

    /// The employee's display name.
    #[derive(Attribute, Clone)]
    pub struct Name(pub String);

    /// Tags associated with the employee.
    #[derive(Attribute, Clone)]
    #[cardinality(many)]
    pub struct Tag(pub String);
}
```

The macro generates:

1. **An `Attribute` trait impl** that exposes the inner type:
   ```rust
   impl Attribute for Name {
       type Type = String; // extracted from the tuple field
       fn value(&self) -> &String { &self.0 }
   }
   ```

2. **An `AttributeDescriptor`** built once via `OnceLock`:
   ```rust
   AttributeDescriptor::new(
       "employee/name".parse::<The>().unwrap(),
       "The employee's display name.",
       Cardinality::One,
       Some(Type::String),
   )
   ```
   The domain (`employee`) is derived from the enclosing Rust module name. The
   name (`name`) is the struct name, lowercased and kebab-cased.

3. **Fluent builder methods** for queries and assertions:
   ```rust
   Name::of(alice).is("Alice")          // assertion
   Name::of(Term::var("e")).matches(v)   // query pattern
   ```

### JSON Notation (Formal)

```json
{
  "the": "employee/name",
  "description": "The employee's display name.",
  "as": "Text",
  "cardinality": "one"
}
```

### JSON Notation (Abbreviated)

In abbreviated YAML notation, the domain and name are inferred from context:

```yaml
Employee:
  name:
    as: Text
    description: The employee's display name.
  tags:
    as: Text
    cardinality: many
```

### Naming Rules

The `the` selector follows strict formatting:

- **Domain**: lowercase ASCII, digits, hyphens, dots. Must start with a letter,
  no trailing punctuation. Examples: `person`, `diy.cook`, `io.gozala.person`.
- **Name**: lowercase kebab-case. Must start with a letter, no trailing hyphen.
  Examples: `name`, `ingredient-name`.
- Combined length must not exceed 64 bytes.

The `the!()` macro validates these rules at compile time:

```rust
let attr = the!("employee/name");     // compiles
// let bad = the!("Employee/Name");   // compile error
```

### The Type Bridge

Dialog bridges Rust's type system to its runtime value types through a trait
hierarchy:

```
Typed          Maps a Rust type to its TypeDescriptor
TypeDescriptor Associates a descriptor with a runtime Type tag
Scalar         A Typed value that converts to/from Value
```

Concrete Rust types map to zero-sized type (ZST) descriptors:

| Rust type | Descriptor (ZST) | Runtime `Type` |
|-----------|-------------------|----------------|
| `String`  | `Text`            | `Type::String` |
| `bool`    | `Boolean`         | `Type::Boolean`|
| `u32`     | `UnsignedInteger` | `Type::UnsignedInt` |
| `i64`     | `SignedInteger`    | `Type::SignedInt` |
| `f64`     | `Float`           | `Type::Float`  |
| `Vec<u8>` | `Bytes`           | `Type::Bytes`  |
| `Entity`  | `EntityType`      | `Type::Entity` |

The `Any(Option<Type>)` descriptor carries an optional runtime type
tag, allowing dynamic typing. All typed terms can be widened into `Term<Any>`
while preserving their type information.

## Concepts

A **concept** groups attributes that share an entity. Think of it like a table
in a relational database, but defined at query time rather than write time.

### Rust: `#[derive(Concept)]`

```rust
/// A person in the system.
#[derive(Concept, Debug, Clone)]
pub struct Person {
    pub this: Entity,
    pub name: employee::Name,
    pub role: employee::Role,
}
```

The macro generates:

1. **A match struct** for query patterns:
   ```rust
   pub struct PersonMatch {
       pub this: Term<Entity>,
       pub name: Term<String>,
       pub role: Term<String>,
   }
   ```
   All variables default to named variables (`Term::var("this")`,
   `Term::var("name")`, etc.).

2. **A terms struct** for concise variable access:
   ```rust
   pub struct PersonTerms;
   impl PersonTerms {
       pub fn this() -> Term<Entity> { Term::var("this") }
       pub fn name() -> Term<String> { Term::var("name") }
   }
   ```

3. **A `ConceptDescriptor`** whose identity is the sorted hash of its
   constituent `AttributeDescriptor`s. Two concepts with the same attributes
   produce the same descriptor regardless of field names or declaration order.

4. **`Application` impl** that evaluates the concept query and converts
   answers back into typed `Person` instances.

5. **`Statement` impl** that decomposes a `Person` into individual attribute
   claims for write transactions.

### JSON Notation

```json
{
  "description": "A person in the system",
  "with": {
    "name":  { "the": "employee/name", "as": "Text" },
    "role":  { "the": "employee/role", "as": "Text" }
  }
}
```

### Structural Identity

A concept's identity is derived from the **sorted set of its attribute
identities** (each attribute identity being `(the, type, cardinality)`). This
means:

- Two concepts with the same attributes are the same concept
- Field names are local labels, not part of identity
- Attribute order does not matter

The hash is computed via CBOR serialization of sorted attribute URIs and then
Blake3 hashing. The resulting hash becomes the concept's entity ID:
`concept:<base58(hash)>`.

## Formulas

A **formula** is a pure computation that reads bound variables and writes
derived values without accessing the store.

### Rust: `#[derive(Formula)]`

```rust
#[derive(Formula, Debug, Clone)]
pub struct FullName {
    pub first: String,
    pub last: String,

    #[derived(cost = 2)]
    pub full: String,
}

impl FullName {
    fn derive(input: FullNameInput) -> Vec<Self> {
        vec![FullName {
            first: input.first.clone(),
            last: input.last.clone(),
            full: format!("{} {}", input.first, input.last),
        }]
    }
}
```

The macro generates:

1. **An input struct** with only the non-`#[derived]` fields.
2. **A match struct** with all fields as `Term`s.
3. **A `Formula` trait impl** including `cells()` (parameter schema), `derive()`
   (computation), and `write()` (output binding).
4. **Conversion to `FormulaQuery`**, a type-erased representation that the
   planner and evaluator work with.

### Built-in Formulas

Dialog provides built-in formulas for common operations:

- **Math**: `Sum`, `Difference`, `Product`, `Quotient`, `Modulo`
- **Text**: `Concatenate`, `Length`, `UpperCase`, `LowerCase`, `Like`
- **Logic**: `And`, `Or`, `Not`

## From Definition to Query

The pipeline from schema definition to executable query:

```
Rust macros / JSON notation
       |
       v
AttributeDescriptor / ConceptDescriptor / FormulaQuery
       |
       v
Premise (Assert or Unless)
       |
       v
Proposition (Relation, Concept, Formula, or Constraint)
       |
       v
Planner -> Conjunction (ordered execution plan)
       |
       v
Evaluation -> Stream<Answer>
```

The next chapter covers how query patterns are assembled from these
definitions.
