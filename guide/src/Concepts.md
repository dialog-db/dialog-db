# Concepts

A **concept** describes an entity in terms of the attributes it has. Concepts apply meaning by specifying which attributes an entity should have, without constraining what gets stored.

Concepts are **expectations, not constraints**. They don't enforce structure at write time - they provide interpretation at query time.

## Defining Concepts

Define a concept by grouping related attributes:

```rust
mod employee {
    use dialog_query::{Attribute, Entity};

    /// Full name of the employee
    #[derive(Attribute)]
    pub struct Name(pub String);

    /// Annual salary in dollars
    #[derive(Attribute)]
    pub struct Salary(pub u32);

    /// Job title
    #[derive(Attribute)]
    pub struct Job(pub String);
}

use dialog_query::Concept;

/// An employee in the system
#[derive(Concept)]
struct Employee {
    this: Entity,
    name: employee::Name,
    salary: employee::Salary,
    job: employee::Job,
}
```

The `#[derive(Concept)]` macro generates everything needed to work with this concept.

**Doc Comments Help Discovery**: Adding doc comments to your attributes and concepts makes your schema self-documenting. These descriptions are captured in the generated code and become part of the schema metadata - attribute descriptions are accessible via `Attribute::description()` and concept descriptions via the `Concept` type. This enables tools, IDEs, and runtime introspection to help users discover and understand your data model without needing to read the source code.

## What Concepts Provide

Concepts give you:

**Type Safety**: The compiler knows what attributes belong together

```rust
use dialog_query::Attribute;

// Employee must have name, salary, and job
let employee = Employee {
    this: alice,
    name: employee::Name::from("Alice"),
    salary: employee::Salary::from(60000),
    job: employee::Job::from("Engineer"),
};
```

**Reusable Queries**: Define once, query many times

```rust
// Query for all employees
let employees = Employee::query()
    .query(&session)
    .try_collect::<Vec<_>>()
    .await?;
```

**Documentation**: Make your data model explicit

```rust
/// An employee in the system
///
/// Required attributes:
/// - name: Employee's full name
/// - salary: Current salary in dollars
/// - job: Job title
#[derive(Concept)]
struct Employee { ... }
```

## Querying Concepts

Query for all entities that match a concept:

```rust
use dialog_query::Concept; // Required to use the query() shortcut

// Simple syntax: query all employees
let results = Employee::query(session.clone())
    .try_collect::<Vec<_>>()
    .await?;

for employee in results {
    println!("{} - {} - ${}",
        employee.name.value(),
        employee.job.value(),
        employee.salary.value()
    );
}
```

The `Employee::query()` method is a shortcut for creating a `Match` with all fields as variables. For more control, use the explicit `Match` syntax:

```rust
use dialog_query::{Attribute, Match, Term};

// Explicit syntax with all fields as variables
let all_employees = Match::<Employee> {
    this: Term::var("this"),
    name: Term::var("name"),
    salary: Term::var("salary"),
    job: Term::var("job"),
};

let results = all_employees.query(session).try_collect::<Vec<_>>().await?;

// Or filter by specific values
let engineers = Match::<Employee> {
    this: Term::var("this"),
    name: Term::var("name"),
    salary: Term::var("salary"),
    job: employee::Job::from("Engineer").into(),
};

let results = engineers.query(session).try_collect::<Vec<_>>().await?;
```

## Transacting Concepts

Assert entities using concepts:

```rust
use dialog_query::Attribute;

let alice = Entity::new()?;

let employee = Employee {
    this: alice,
    name: employee::Name::from("Alice"),
    salary: employee::Salary::from(60000),
    job: employee::Job::from("Engineer"),
};

let mut edit = session.edit();
edit.assert(employee);
session.commit(edit).await?;
```

This asserts all the attributes in one transaction.

## Concepts vs Relations

Important distinction:

- **Relations** (`{ the, of, is }`) - the actual facts in the database
- **Concepts** - interpretations of groups of relations

The same relations can satisfy multiple concepts:

```rust
// alice has these relations:
// - employee/name: "Alice"
// - employee/salary: 60000
// - employee/direct_reports: [bob]

// alice satisfies these concepts:
// - Employee (has name, salary)
// - Manager (has direct_reports)
// - Person (has name)
```

## With<A>: Ad-Hoc Concepts

For convenience, Dialog provides `With<A>` - an ad-hoc concept for working with individual attributes without defining a formal concept.

```rust
use dialog_query::{Attribute, Concept}; // Required to use the query() shortcut
use dialog_query::attribute::With;

// Instead of defining a concept, use With directly
let mut edit = session.edit();
edit.assert(With { this: alice.clone(), has: employee::Name::from("Alice") })
    .assert(With { this: alice.clone(), has: employee::Salary::from(60000) });
session.commit(edit).await?;

// Query individual attributes using the shortcut syntax
let names = With::<employee::Name>::query(session.clone())
    .try_collect::<Vec<_>>()
    .await?;

for with_name in names {
    println!("{}", with_name.has.value());
}
```

`With<A>` is useful when:
- You're working with individual attributes
- You don't need the structure of a full concept
- You're doing quick operations or prototyping

Think of `With<A>` as saying: "this entity has this attribute value"

## When to Use What

**Use Concepts when**:
- You have groups of related attributes
- You want type safety and reusability
- You're modeling domain entities

**Use `With<A>` when**:
- Working with individual attributes
- Building direct, simple queries
- Prototyping or exploring data

## Documentation and Discovery

Doc comments on attributes and concepts are more than just code documentation - they become part of your schema:

```rust
mod employee {
    use dialog_query::Attribute;

    /// Full legal name of the employee
    ///
    /// This should match the name on official documents.
    #[derive(Attribute)]
    pub struct Name(pub String);

    /// Annual base salary in US dollars
    ///
    /// Does not include bonuses or stock compensation.
    #[derive(Attribute)]
    pub struct Salary(pub u32);
}

/// An employee in the organization
///
/// Represents a person employed by the company with their
/// basic information including name, position, and compensation.
#[derive(Concept)]
struct Employee {
    this: Entity,
    name: employee::Name,
    salary: employee::Salary,
    job: employee::Job,
}
```

Benefits of documenting your schema:

**IDE Support**: Your editor can show attribute and concept descriptions while you code.

**Runtime Introspection**: Attribute descriptions are accessible at runtime via `employee::Name::description()`, concept descriptions via the `Concept` type. Tools can query these to generate documentation, build admin UIs, create forms, or provide contextual help.

**Discovery**: When exploring an unfamiliar schema, users can call `description()` on any attribute to understand what it represents without finding the source code.

**Team Communication**: New team members can understand the data model by reading the descriptions, whether in code or at runtime.

**Self-Documenting**: The schema itself explains what each piece of data means and how it should be used, making the codebase more maintainable.

## From Concepts to Rules

Concepts can be combined with rules (covered in a later chapter) to:
- Derive one concept from others
- Create views and computed data
- Bridge different data models
- Handle schema evolution
