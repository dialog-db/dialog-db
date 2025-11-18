# Transacting

Transactions are how you modify the database - asserting new facts or retracting existing ones. All changes happen atomically: either all succeed together, or none do.

## Sessions

Before you can transact, you need a **session**:

```rust
use dialog_query::Session;
use dialog_query::artifact::Artifacts;
use dialog_storage::MemoryStorageBackend;

let backend = MemoryStorageBackend::default();
let store = Artifacts::anonymous(backend).await?;

let mut session = Session::open(store);
```

A session provides the context for both queries and transactions.

## Asserting Concepts

The primary way to transact is by asserting concept instances:

```rust
use dialog_query::{Attribute, Concept, Entity};

mod employee {
    use dialog_query::{Attribute, Entity};

    #[derive(Attribute)]
    pub struct Name(pub String);

    #[derive(Attribute)]
    pub struct Salary(pub u32);

    #[derive(Attribute)]
    pub struct Job(pub String);
}

#[derive(Concept)]
struct Employee {
    this: Entity,
    name: employee::Name,
    salary: employee::Salary,
    job: employee::Job,
}

let alice = Entity::new()?;

let employee = Employee {
    this: alice,
    name: employee::Name::from("Alice"),
    salary: employee::Salary::from(75000),
    job: employee::Job::from("Engineer"),
};

let mut edit = session.edit();
edit.assert(employee);
session.commit(edit).await?;
```

This creates assertions for all attributes of the concept in one transaction.

### Multiple Entities in One Transaction

Transactions can include multiple concept instances:

```rust
use dialog_query::Attribute;

let alice = Entity::new()?;
let bob = Entity::new()?;

let mut edit = session.edit();
edit.assert(Employee {
    this: alice,
    name: employee::Name::from("Alice"),
    salary: employee::Salary::from(75000),
    job: employee::Job::from("Engineer"),
}).assert(Employee {
    this: bob,
    name: employee::Name::from("Bob"),
    salary: employee::Salary::from(60000),
    job: employee::Job::from("Designer"),
});
session.commit(edit).await?;
```

Either all facts are asserted, or none are - transactions are atomic.

## Updating with Cardinality::One

For attributes with `Cardinality::One`, asserting a new value automatically supersedes the old one:

```rust
mod employee {
    use dialog_query::Attribute;

    /// Salary has Cardinality::One by default
    #[derive(Attribute)]
    pub struct Salary(pub u32);
}

// First assertion
let mut edit = session.edit();
edit.assert(With { this: alice.clone(), has: employee::Salary::from(60000) });
session.commit(edit).await?;

// Second assertion supersedes the first
let mut edit = session.edit();
edit.assert(With { this: alice.clone(), has: employee::Salary::from(75000) });
session.commit(edit).await?;

// Query returns only: 75000
```

No explicit retraction needed for `Cardinality::One` attributes.

## Retracting Facts

Use `.retract()` to explicitly remove facts:

```rust
use dialog_query::{Attribute, attribute::With};

let mut edit = session.edit();
edit.retract(With { this: alice.clone(), has: employee::Name::from("Alice") });
session.commit(edit).await?;
```

This creates a **retraction** - a fact saying "alice no longer has this name" at this causal point.

## Transaction Lifecycle

```rust
// 1. Open session
let mut session = Session::open(store.clone());

// 2. Create an edit
let mut edit = session.edit();

// 3. Add changes to the edit
edit.assert(Employee {
    this: alice,
    name: employee::Name::from("Alice"),
    salary: employee::Salary::from(75000),
    job: employee::Job::from("Engineer"),
});

// 4. Commit the edit
session.commit(edit).await?;

// 5. Changes are now visible
// Session now sees the new data

// 6. Query to verify
let query = Match::<Employee> {
    this: Term::value(alice),
    name: Term::var("name"),
    salary: Term::var("salary"),
    job: Term::var("job"),
};
let results: Vec<_> = query.query(&session).try_collect().await?;
```

## Atomicity Guarantees

Transactions ensure **atomicity**:

```rust
// This transaction will either:
// - Assert all facts from both concepts, OR
// - Assert none of them (if there's an error)
let mut edit = session.edit();
edit.assert(Employee {
    this: alice.clone(),
    name: employee::Name::from("Alice"),
    salary: employee::Salary::from(75000),
    job: employee::Job::from("Engineer"),
}).assert(Employee {
    this: bob.clone(),
    name: employee::Name::from("Bob"),
    salary: employee::Salary::from(60000),
    job: employee::Job::from("Designer"),
});
session.commit(edit).await?;
```

You never get partial updates - either the whole transaction succeeds or it fails.

## Revisions

Each successful transaction creates a new **revision** of the database:

```rust
// Revision 1: Empty database
let mut session1 = Session::open(store.clone());

// Revision 2: Added alice
let mut edit = session1.edit();
edit.assert(Employee {
    this: alice.clone(),
    name: employee::Name::from("Alice"),
    salary: employee::Salary::from(75000),
    job: employee::Job::from("Engineer"),
});
session1.commit(edit).await?;

// Revision 3: Added bob
let mut edit = session1.edit();
edit.assert(Employee {
    this: bob.clone(),
    name: employee::Name::from("Bob"),
    salary: employee::Salary::from(60000),
    job: employee::Job::from("Designer"),
});
session1.commit(edit).await?;
```

Revisions enable:
- **Time-travel**: Query the database as it existed at any revision
- **Audit trails**: See what changed and when
- **Synchronization**: Efficiently sync changes between peers

## Transacting Individual Attributes with `With<A>`

For working with individual attributes without concepts, use `With<A>`:

```rust
use dialog_query::attribute::With;

let alice = Entity::new()?;

let mut edit = session.edit();
edit.assert(With { this: alice.clone(), has: employee::Name::from("Alice") })
    .assert(With { this: alice.clone(), has: employee::Salary::from(75000) });
session.commit(edit).await?;
```

This is useful when:
- Working with individual attributes
- Building quick prototypes
- Attributes span multiple namespaces/concepts

## Entity References

Attributes can reference other entities to create relationships:

```rust
mod employee {
    use dialog_query::{Attribute, Entity};

    #[derive(Attribute)]
    pub struct Name(pub String);

    #[derive(Attribute)]
    pub struct Salary(pub u32);

    #[derive(Attribute)]
    pub struct Job(pub String);

    #[derive(Attribute)]
    pub struct Manager(pub Entity);
}

#[derive(Concept)]
struct Employee {
    this: Entity,
    name: employee::Name,
    salary: employee::Salary,
    job: employee::Job,
    manager: employee::Manager,
}

let alice = Entity::new()?;
let bob = Entity::new()?;

let mut edit = session.edit();
edit.assert(Employee {
    this: alice.clone(),
    name: employee::Name::from("Alice"),
    salary: employee::Salary::from(100000),
    job: employee::Job::from("Director"),
    manager: employee::Manager::from(Entity::nil()), // No manager
}).assert(Employee {
    this: bob,
    name: employee::Name::from("Bob"),
    salary: employee::Salary::from(60000),
    job: employee::Job::from("Engineer"),
    manager: employee::Manager::from(alice), // Bob reports to Alice
});
session.commit(edit).await?;
```

## Error Handling

Transactions can fail:

```rust
let mut edit = session.edit();
edit.assert(Employee {
    this: alice,
    name: employee::Name::from("Alice"),
    salary: employee::Salary::from(75000),
    job: employee::Job::from("Engineer"),
});

match session.commit(edit).await {
    Ok(_) => println!("Transaction succeeded"),
    Err(e) => eprintln!("Transaction failed: {}", e),
}
```

When a transaction fails, no changes are applied.

## Complete Example

```rust
use dialog_query::{Attribute, Entity, Session, Concept, Match, Term};
use dialog_query::artifact::Artifacts;
use dialog_storage::MemoryStorageBackend;
use futures_util::TryStreamExt;

mod employee {
    use dialog_query::{Attribute, Entity};

    #[derive(Attribute)]
    pub struct Name(pub String);

    #[derive(Attribute)]
    pub struct Salary(pub u32);

    #[derive(Attribute)]
    pub struct Job(pub String);
}

#[derive(Concept)]
struct Employee {
    this: Entity,
    name: employee::Name,
    salary: employee::Salary,
    job: employee::Job,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let backend = MemoryStorageBackend::default();
    let store = Artifacts::anonymous(backend).await?;
    let mut session = Session::open(store.clone());

    // Create entities
    let alice = Entity::new()?;
    let bob = Entity::new()?;

    // Transaction 1: Add alice
    let mut edit = session.edit();
    edit.assert(Employee {
        this: alice.clone(),
        name: employee::Name::from("Alice"),
        salary: employee::Salary::from(75000),
        job: employee::Job::from("Engineer"),
    });
    session.commit(edit).await?;
    println!("Added Alice");

    // Transaction 2: Add bob
    let mut edit = session.edit();
    edit.assert(Employee {
        this: bob.clone(),
        name: employee::Name::from("Bob"),
        salary: employee::Salary::from(50000),
        job: employee::Job::from("Designer"),
    });
    session.commit(edit).await?;
    println!("Added Bob");

    // Transaction 3: Give bob a raise (Cardinality::One supersedes)
    let mut edit = session.edit();
    edit.assert(Employee {
        this: bob.clone(),
        name: employee::Name::from("Bob"),
        salary: employee::Salary::from(60000),  // This supersedes the old salary
        job: employee::Job::from("Designer"),
    });
    session.commit(edit).await?;
    println!("Gave Bob a raise");

    // Query to verify
    let query = Match::<Employee> {
        this: Term::value(bob),
        name: Term::var("name"),
        salary: Term::var("salary"),
        job: Term::var("job"),
    };
    let results: Vec<_> = query.query(&session).try_collect().await?;

    if let Some(emp) = results.first() {
        println!("Bob's new salary: ${}", emp.salary.value());
    }

    Ok(())
}
```

## Best Practices

**Use concepts for structured data**: Concepts provide type safety and group related attributes.

**Use `With<A>` for individual attributes**: When working with single attributes or prototyping.

**Leverage Cardinality::One**: For single-valued attributes, new assertions automatically supersede old ones - no need to retract.

**Entity references create structure**: Use entity references to build relationships and graphs.

**Keep transactions focused**: Smaller transactions are easier to reason about and have less chance of conflicts.

Next, let's explore rules - how to derive new knowledge from existing facts.
