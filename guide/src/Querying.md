# Querying

Querying in Dialog is built on pattern matching. You describe what you're looking for, and Dialog finds all the matching facts.

## Querying Concepts

The primary way to query is using concepts:

```rust
use dialog_query::{Attribute, Concept, Entity};

mod employee {
    use dialog_query::Attribute;

    #[derive(Attribute, Clone)]
    pub struct Name(pub String);

    #[derive(Attribute, Clone)]
    pub struct Salary(pub u32);

    #[derive(Attribute, Clone)]
    pub struct Job(pub String);
}

#[derive(Concept, Debug, Clone)]
pub struct Employee {
    pub this: Entity,
    pub name: employee::Name,
    pub salary: employee::Salary,
    pub job: employee::Job,
}
```

### Find All Matching a Concept

Use the shortcut syntax to query all instances:

```rust
// Find all employees
let results: Vec<Employee> = Employee::query(session.clone())
    .try_collect()
    .await?;

for employee in results {
    println!("{} - {} - ${}",
        employee.name.value(),
        employee.job.value(),
        employee.salary.value()
    );
}
```

### Find with Specific Values

```rust
use dialog_query::Attribute;

// Find all engineers
let engineers_pattern = Match::<Employee> {
    this: Term::var("this"),
    name: Term::var("name"),
    salary: Term::var("salary"),
    job: employee::Job::from("Engineer").into(),
};

let engineers: Vec<Employee> = engineers_pattern.query(&session).try_collect().await?;
```

### Query a Specific Entity

```rust
// Get alice's employee data
let query = Match::<Employee> {
    this: Term::value(alice),
    name: Term::var("name"),
    salary: Term::var("salary"),
    job: Term::var("job"),
};

let results: Vec<Employee> = query.query(&session).try_collect().await?;

match results.first() {
    Some(emp) => println!("{} works as {}", emp.name.value(), emp.job.value()),
    None => println!("Not found"),
}
```

## Processing Results

Queries return a stream of results that you can process:

```rust
use futures_util::TryStreamExt;

let query = Match::<Employee> {
    this: Term::var("this"),
    name: Term::var("name"),
    salary: Term::var("salary"),
    job: Term::var("job"),
};

// Collect all results
let all_employees: Vec<Employee> = query.query(&session).try_collect().await?;

// Or process as a stream
query.query(&session)
    .try_for_each(|employee| async move {
        println!("{} - ${}", employee.name.value(), employee.salary.value());
        Ok(())
    })
    .await?;
```

## Working with Results

Query results are concept instances:

```rust
let employee: Employee = /* ... */;

// Access the entity
let entity: Entity = employee.this;

// Access attribute values
let name: &String = employee.name.value();
let salary: u32 = *employee.salary.value();
let job: &String = employee.job.value();
```

## Querying Individual Attributes with `With<A>`

For working with individual attributes without defining a full concept, use `With<A>`:

```rust
use dialog_query::Concept; // Required to use the query() shortcut
use dialog_query::attribute::With;

// Find all entities that have a name using the shortcut syntax
let results: Vec<With<employee::Name>> = With::<employee::Name>::query(session.clone())
    .try_collect()
    .await?;

for result in results {
    println!("Entity: {}, Name: {}", result.this, result.has.value());
}
```

### Query Patterns with `With<A>`

```rust
use dialog_query::Attribute;

// Specific entity's attribute
let pattern = With {
    this: alice.clone(),
    has: employee::Salary::variable()
};

// Reverse lookup: who has this value?
let pattern = With {
    this: Entity::variable(),
    has: employee::Name::from("Alice")
};

// Entity references: who has alice as manager?
let pattern = With {
    this: Entity::variable(),
    has: employee::Manager::from(alice.clone())
};
```

## Index Selection

Dialog automatically uses the right index for your query. Whether you're querying concepts or individual attributes, the query planner selects the optimal index based on your pattern.

No manual query planning needed - every pattern is efficient!

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

    // Add some data
    let alice = Entity::new()?;
    let bob = Entity::new()?;

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

    // Query: Find all employees
    println!("All employees:");
    let all_employees = Match::<Employee> {
        this: Term::var("this"),
        name: Term::var("name"),
        salary: Term::var("salary"),
        job: Term::var("job"),
    };
    let employees: Vec<_> = all_employees.query(&session).try_collect().await?;
    for emp in employees {
        println!("  - {} ({}) - ${}",
            emp.name.value(),
            emp.job.value(),
            emp.salary.value()
        );
    }

    // Query: Find all engineers
    println!("\nEngineers:");
    let engineers_pattern = Match::<Employee> {
        this: Term::var("this"),
        name: Term::var("name"),
        salary: Term::var("salary"),
        job: employee::Job::from("Engineer").into(),
    };
    let engineers: Vec<_> = engineers_pattern.query(&session).try_collect().await?;
    for eng in engineers {
        println!("  - {}", eng.name.value());
    }

    // Query: Get alice's data
    println!("\nAlice's details:");
    let alice_pattern = Match::<Employee> {
        this: Term::value(alice),
        name: Term::var("name"),
        salary: Term::var("salary"),
        job: Term::var("job"),
    };
    let results: Vec<_> = alice_pattern.query(&session).try_collect().await?;
    if let Some(emp) = results.first() {
        println!("  Name: {}", emp.name.value());
        println!("  Job: {}", emp.job.value());
        println!("  Salary: ${}", emp.salary.value());
    }

    Ok(())
}
```

Next, let's explore transactions - how to assert and retract facts atomically.
