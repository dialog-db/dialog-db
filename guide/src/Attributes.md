# Attributes

An **attribute** defines a relation you can express in your database. It describes what can be said about an entity.

## Your First Attribute

Let's define an employee name attribute:

```rust
mod employee {
    use dialog_query::Attribute;

    /// Name of the employee
    #[derive(Attribute)]
    pub struct Name(pub String);
}
```

That's it! The `#[derive(Attribute)]` macro generates everything you need.

## What Gets Generated

When you derive `Attribute`, Dialog automatically creates:

- **Namespace**: `employee` (from the module name)
- **Attribute name**: `name` (from the struct name, converted to lowercase)
- **Full selector**: `employee/name`
- **Value type**: `String` (from the field type)
- **Cardinality**: `One` (default, unless specified otherwise)
- **Description**: "Name of the employee" (from the doc comment)

## Anatomy of an Attribute

```rust
mod employee {
//  ^^^^^^^^ The module name becomes the namespace
    use dialog_query::Attribute;

    /// Name of the employee
    //  ^^^^^^^^^^^^^^^^^^^^ Doc comment becomes description
    #[derive(Attribute)]
    //       └─ This macro does the magic
    pub struct Name(pub String);
    //         ^^^^     ^^^^^^
    //         |        └─ The value type
    //         └─ Converted to lowercase: "name"
}
```

## Namespaces

Attributes are organized into namespaces. The namespace comes from the module:

```rust
mod employee {
    use dialog_query::Attribute;

    /// Name of the employee
    #[derive(Attribute)]
    pub struct Name(pub String);
    // Selector: employee/name

    /// Salary of the employee
    #[derive(Attribute)]
    pub struct Salary(pub u32);
    // Selector: employee/salary
}
```

### Why Namespaces?

1. **Avoid conflicts**: `employee::Name` vs `customer::Name`
2. **Data locality**: Attributes in the same namespace are stored together, making queries faster
3. **Organization**: Group related attributes

### Namespace Recommendations

Use reverse domain notation for global uniqueness:

```rust
mod io_example_employee {
    // Selector: io-example-employee/name
}
```

For local development, simple names work fine:

```rust
mod employee {
    // Selector: employee/name
}
```

## Value Types

Attributes can hold different value types:

```rust
use dialog_query::{Attribute, Entity};

mod employee {
    use super::*;

    /// Name - a string
    #[derive(Attribute)]
    pub struct Name(pub String);

    /// Age - a number
    #[derive(Attribute)]
    pub struct Age(pub u32);

    /// Active status - a boolean
    #[derive(Attribute)]
    pub struct Active(pub bool);

    /// Manager - a reference to another entity
    #[derive(Attribute)]
    pub struct Manager(pub Entity);
}
```

## Cardinality

Attributes have a **cardinality** that determines how many values an entity can have for that attribute.

### Cardinality::One

An entity can have at most **one** value for this attribute. If you assert a new value, it supersedes the previous assertion.

```rust
mod employee {
    use dialog_query::Attribute;

    /// Employee's current salary (only one at a time)
    #[derive(Attribute)]
    pub struct Salary(pub u32);
}

// First assertion
let mut edit = session.edit();
edit.assert(With { this: alice.clone(), has: Salary(50000) });
session.commit(edit).await?;

// Second assertion supersedes the first
let mut edit = session.edit();
edit.assert(With { this: alice.clone(), has: Salary(60000) });
session.commit(edit).await?;

// Query returns: 60000 (only the latest)
```

This is the default cardinality for attributes.

### Cardinality::Many

An entity can have **multiple** values for this attribute. Each assertion adds a new value without superseding previous ones.

```rust
mod employee {
    use dialog_query::Attribute;

    /// Employee's skills (can have multiple)
    #[derive(Attribute)]
    #[cardinality(many)]  // Specify many cardinality
    pub struct Skill(pub String);
}

// Each assertion adds a skill
let mut edit = session.edit();
edit.assert(With { this: alice.clone(), has: Skill("Rust".into()) });
session.commit(edit).await?;

let mut edit = session.edit();
edit.assert(With { this: alice.clone(), has: Skill("Python".into()) });
session.commit(edit).await?;

// Query returns both: ["Rust", "Python"]
```

Use `many` cardinality for:
- Tags or labels
- Multiple relationships (e.g., team members, skills)
- Collections of values

## Multiple Attributes

Group related attributes in a module:

```rust
mod employee {
    use dialog_query::{Attribute, Entity};

    /// Name of the employee
    #[derive(Attribute)]
    pub struct Name(pub String);

    /// Job title
    #[derive(Attribute)]
    pub struct Job(pub String);

    /// Salary in dollars
    #[derive(Attribute)]
    pub struct Salary(pub u32);

    /// The employee's manager
    #[derive(Attribute)]
    pub struct Manager(pub Entity);
}
```

## Accessing Metadata

Attributes expose metadata at compile time:

```rust
use employee::Name;

// Get the namespace
assert_eq!(Name::namespace(), "employee");

// Get the attribute name
assert_eq!(Name::name(), "name");

// Get the full selector
assert_eq!(Name::selector().to_string(), "employee/name");

// Get the description
assert_eq!(Name::description(), "Name of the employee");
```

## Next Steps

Now that you can define attributes, let's learn how to group them into concepts to model domain entities.
