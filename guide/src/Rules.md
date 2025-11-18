# Rules

Rules are Dialog's equivalent of **views** in relational databases. They define how concepts can be derived from existing facts through logical inference.

## What Are Rules?

A rule says: "If these premises are true, then this conclusion follows."

```
If:
  - alice has employee/name = "Alice"
  - alice has employee/direct_reports = bob
Then:
  - alice is a Manager
```

Rules let you:
- **Derive concepts from facts**: Define "Manager" based on having direct reports
- **Bridge data models**: Adapt between different schemas
- **Handle schema evolution**: Support old and new models simultaneously
- **Create computed views**: Generate derived data on the fly

## Defining Rules

Rules are functions that take a `Match<T>` pattern and return premises:

```rust
use dialog_query::{Concept, Entity, Match, Term, When};

mod employee {
    use dialog_query::Attribute;

    #[derive(Attribute)]
    pub struct Name(pub String);

    #[derive(Attribute)]
    pub struct DirectReports(pub u32);
}

mod manager {
    use dialog_query::Attribute;

    #[derive(Attribute)]
    pub struct Name(pub String);
}

#[derive(Concept)]
struct Employee {
    this: Entity,
    name: employee::Name,
    direct_reports: employee::DirectReports,
}

#[derive(Concept)]
struct Manager {
    this: Entity,
    name: manager::Name,
}

// A Manager is an Employee who has direct reports > 0
fn manager_rule(manager: Match<Manager>) -> impl When {
    (
        Match::<Employee> {
            this: manager.this,
            name: manager.name,
            direct_reports: Term::var("count"),
        },
    )
}
```

The rule function:
1. Takes a `Match<Manager>` representing what we're trying to prove
2. Returns premises that must be true (here, an `Employee` with matching attributes)
3. Returns `impl When` - premises can be tuples, arrays, or vecs

## Installing Rules

Install rules on a session to make them active:

```rust
let mut session = Session::open(store).install(manager_rule)?;
```

Once installed, querying for `Manager` will derive results from `Employee` facts.

## Schema Bridging

Rules enable different applications to use different models for the same data:

```rust
mod employee_v1 {
    use dialog_query::Attribute;

    #[derive(Attribute)]
    pub struct Name(pub String);

    #[derive(Attribute)]
    pub struct Job(pub String);
}

mod stuff {
    use dialog_query::Attribute;

    #[derive(Attribute)]
    pub struct Name(pub String);

    #[derive(Attribute)]
    pub struct Role(pub String);
}

#[derive(Concept)]
struct Employee {
    this: Entity,
    name: employee_v1::Name,
    job: employee_v1::Job,
}

#[derive(Concept)]
struct Stuff {
    this: Entity,
    name: stuff::Name,
    role: stuff::Role,
}

// Bridge: Employee can be derived from Stuff
fn employee_from_stuff(employee: Match<Employee>) -> impl When {
    (
        Match::<Stuff> {
            this: employee.this,
            name: employee.name,
            role: employee.job,
        },
    )
}
```

Now queries for `Employee` work even if data is stored as `Stuff`.

## Schema Evolution

Rules make schema migration painless:

```rust
mod note {
    use dialog_query::Attribute;

    #[derive(Attribute)]
    pub struct Title(pub String);
}

mod note_v2 {
    use dialog_query::Attribute;

    #[derive(Attribute)]
    pub struct Name(pub String);
}

#[derive(Concept)]
struct Note {
    this: Entity,
    title: note::Title,
}

#[derive(Concept)]
struct NoteV2 {
    this: Entity,
    name: note_v2::Name,
}

// Migrate Note to NoteV2
fn note_v2_migration(note_v2: Match<NoteV2>) -> impl When {
    (
        Match::<Note> {
            this: note_v2.this,
            title: note_v2.name,
        },
    )
}
```

Old code querying `Note` continues working. New code can query `NoteV2` and get the same data.

## Multiple Premises

Rules can have multiple premises as a tuple:

```rust
mod person {
    use dialog_query::{Attribute, Entity};

    #[derive(Attribute)]
    pub struct Name(pub String);

    #[derive(Attribute)]
    pub struct Age(pub u32);

    #[derive(Attribute)]
    pub struct Manager(pub Entity);
}

#[derive(Concept)]
struct Person {
    this: Entity,
    name: person::Name,
    age: person::Age,
}

#[derive(Concept)]
struct Employee {
    this: Entity,
    name: person::Name,
    manager: person::Manager,
}

#[derive(Concept)]
struct SeniorEmployee {
    this: Entity,
    name: person::Name,
}

// A SeniorEmployee is a Person over 50 who is an Employee
fn senior_employee_rule(senior: Match<SeniorEmployee>) -> impl When {
    (
        Match::<Person> {
            this: senior.this.clone(),
            name: senior.name.clone(),
            age: Term::var("age"),
        },
        Match::<Employee> {
            this: senior.this,
            name: senior.name,
            manager: Term::var("manager"),
        },
    )
}
```

Both premises must be satisfied for the rule to derive a `SeniorEmployee`.

## Negation

Rules can use negation with the `!` operator:

```rust
use dialog_query::attribute::With;

#[derive(Concept)]
struct ActiveEmployee {
    this: Entity,
    name: employee::Name,
}

mod termination {
    use dialog_query::{Attribute, Entity};

    #[derive(Attribute)]
    pub struct Date(pub String);
}

// An ActiveEmployee is an Employee without a termination date
fn active_employee_rule(active: Match<ActiveEmployee>) -> impl When {
    (
        Match::<Employee> {
            this: active.this.clone(),
            name: active.name,
            direct_reports: Term::var("reports"),
        },
        !Match::<With<termination::Date>> {
            this: active.this,
            has: Term::var("date"),
        },
    )
}
```

The `!` negates the premise - the rule only matches when the entity does NOT have a termination date.

## Complete Example

```rust
use dialog_query::{Attribute, Concept, Entity, Match, Session, Term, When};
use dialog_query::artifact::Artifacts;
use dialog_storage::MemoryStorageBackend;
use futures_util::TryStreamExt;

mod employee {
    use dialog_query::Attribute;

    #[derive(Attribute)]
    pub struct Name(pub String);

    #[derive(Attribute)]
    pub struct Job(pub String);
}

mod stuff {
    use dialog_query::Attribute;

    #[derive(Attribute)]
    pub struct Name(pub String);

    #[derive(Attribute)]
    pub struct Role(pub String);
}

#[derive(Concept)]
struct Employee {
    this: Entity,
    name: employee::Name,
    job: employee::Job,
}

#[derive(Concept)]
struct Stuff {
    this: Entity,
    name: stuff::Name,
    role: stuff::Role,
}

// Rule: Employees can be derived from Stuff
fn employee_from_stuff(employee: Match<Employee>) -> impl When {
    (
        Match::<Stuff> {
            this: employee.this,
            name: employee.name,
            role: employee.job,
        },
    )
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let backend = MemoryStorageBackend::default();
    let store = Artifacts::anonymous(backend).await?;

    // Install the rule
    let mut session = Session::open(store).install(employee_from_stuff)?;

    // Insert data as Stuff
    let alice = Entity::new()?;
    let bob = Entity::new()?;

    let mut edit = session.edit();
    edit.assert(Stuff {
        this: alice.clone(),
        name: stuff::Name("Alice".into()),
        role: stuff::Role("Engineer".into()),
    }).assert(Stuff {
        this: bob.clone(),
        name: stuff::Name("Bob".into()),
        role: stuff::Role("Designer".into()),
    });
    session.commit(edit).await?;

    // Query for Employees - rule derives them from Stuff
    let query = Match::<Employee> {
        this: Term::var("employee"),
        name: Term::var("name"),
        job: Term::var("job"),
    };

    let employees: Vec<Employee> = query
        .query(&session)
        .try_collect()
        .await?;

    for emp in employees {
        println!("{} - {}", emp.name.value(), emp.job.value());
    }
    // Output:
    // Alice - Engineer
    // Bob - Designer

    Ok(())
}
```

## Terms

Terms are the building blocks of queries and rules. A `Term<T>` can be:

### Variables

Variables match any value and bind the result:

```rust
use dialog_query::Term;

// Match any entity
let entity_var = Term::var("person");

// Match any name
let name_var: Term<String> = Term::var("name");

// Match any number
let age_var: Term<u32> = Term::var("age");
```

### Constants

Constants match specific values:

```rust
// Match specific entity
let alice = Entity::new()?;
let alice_term = Term::value(alice);

// Match specific string
let name_term = Term::value("Alice".to_string());

// Match specific number
let age_term = Term::value(42u32);
```

You can also use `Term::from()` for convenience:

```rust
let name: Term<String> = Term::from("Alice".to_string());
let age: Term<u32> = Term::from(25u32);
```

## Formulas

Formulas are computational predicates that derive output values from input values. They enable calculations within rules.

### Built-in Formulas

Dialog provides formulas for math and strings:

```rust
use dialog_query::formulas::*;

// Math operations
Sum { of: 10, with: 5, is: ? }          // Addition
Difference { of: 20, subtract: 8, is: ? }  // Subtraction
Product { of: 6, times: 7, is: ? }      // Multiplication
Quotient { of: 42, by: 6, is: ? }      // Division

// String operations
Concatenate { first: "Hello", second: " World", is: ? }
Length { of: "test", is: ? }
Uppercase { of: "hello", is: ? }
Lowercase { of: "HELLO", is: ? }
```

### Using Formulas in Rules

Formulas can be used as premises in rules to perform computations:

```rust
mod person {
    use dialog_query::Attribute;

    #[derive(Attribute)]
    pub struct FirstName(pub String);

    #[derive(Attribute)]
    pub struct LastName(pub String);

    #[derive(Attribute)]
    pub struct FullName(pub String);
}

#[derive(Concept)]
struct Person {
    this: Entity,
    first_name: person::FirstName,
    last_name: person::LastName,
}

#[derive(Concept)]
struct PersonWithFullName {
    this: Entity,
    full_name: person::FullName,
}

use dialog_query::formulas::Concatenate;

// Derive full name from first and last names
fn full_name_rule(person: Match<PersonWithFullName>) -> impl When {
    (
        Match::<Person> {
            this: person.this.clone(),
            first_name: Term::var("first"),
            last_name: Term::var("last"),
        },
        Concatenate {
            first: Term::var("first"),
            second: Term::var("last"),
            is: person.full_name,
        },
    )
}
```

The `Concatenate` formula computes the full name from the first and last name variables.

## Conjunctions (AND)

A **conjunction** means all premises must be satisfied. In Dialog, conjunctions are expressed as tuples of premises.

```rust
// This rule requires BOTH conditions to be true
fn senior_manager_rule(senior: Match<SeniorManager>) -> impl When {
    (
        // Must be a manager
        Match::<Manager> {
            this: senior.this.clone(),
            name: senior.name.clone(),
        },
        // AND must be over 50
        Match::<Person> {
            this: senior.this,
            name: senior.name,
            age: Term::var("age"),  // age must exist and be bound
        },
    )
}
```

Both the `Manager` match AND the `Person` match must succeed for the rule to derive a `SeniorManager`.

### Multiple Conjunctions

You can have many premises in a conjunction:

```rust
fn complex_rule(target: Match<Target>) -> impl When {
    (
        Match::<ConceptA> { /* ... */ },
        Match::<ConceptB> { /* ... */ },
        Match::<ConceptC> { /* ... */ },
        Concatenate { /* compute something */ },
        Sum { /* calculate something */ },
    )
}
```

All five premises must be satisfied for the rule to succeed.

## Disjunctions (OR)

A **disjunction** means at least one alternative must be satisfied. In Dialog, disjunctions are expressed by defining multiple rules for the same concept.

```rust
#[derive(Concept)]
struct VIPCustomer {
    this: Entity,
    name: customer::Name,
}

// Rule 1: VIP if high spending
fn vip_by_spending(vip: Match<VIPCustomer>) -> impl When {
    (
        Match::<Customer> {
            this: vip.this,
            name: vip.name,
            total_spent: Term::var("spent"),
        },
        // Some formula checking spent > 10000
    )
}

// Rule 2: VIP if premium member
fn vip_by_membership(vip: Match<VIPCustomer>) -> impl When {
    (
        Match::<Customer> {
            this: vip.this,
            name: vip.name,
            membership: Term::value("Premium".to_string()),
        },
    )
}

// Install both rules
let session = Session::open(store)
    .install(vip_by_spending)?
    .install(vip_by_membership)?;
```

Now querying for `VIPCustomer` returns entities that satisfy **either** rule - customers with high spending **OR** premium membership.

### Disjunction Semantics

When multiple rules define the same concept:

- The concept matches if **any** rule matches
- Results from all matching rules are combined
- Duplicates are naturally handled (same entity appears once)

This is how Dialog implements **OR** logic:

```
VIPCustomer(?customer) :-
    HighSpending(?customer).      // Rule 1

VIPCustomer(?customer) :-
    PremiumMember(?customer).     // Rule 2
```

A customer is a VIP if they match Rule 1 **OR** Rule 2 **OR** both.

## Conjunctions within Disjunctions

You can combine AND and OR logic by using multiple rules with multiple premises:

```rust
// Executive: (Director AND HighSalary) OR (VP)
fn executive_via_director(exec: Match<Executive>) -> impl When {
    (
        Match::<Director> { /* ... */ },  // AND
        Match::<HighSalary> { /* ... */ },
    )
}

fn executive_via_vp(exec: Match<Executive>) -> impl When {
    (
        Match::<VP> { /* ... */ },
    )
}
```

An `Executive` is derived if:
- (They are a `Director` AND have `HighSalary`) OR
- (They are a `VP`)

## How Rules Work

Rules are evaluated at **query time**, not write time:

1. You query for a concept (e.g., "all Employees")
2. Dialog applies installed rules to find matching facts
3. For each match, Dialog derives an instance of the concept
4. You get back the derived instances

This is **lazy evaluation** - rules only run when you query for them.

## Schema-on-Read

Rules implement **schema-on-read**:

- Write facts once
- Read them through multiple lenses (rules)
- Different applications see different views
- No data duplication
- No migration needed

This is the foundation of Dialog's flexibility.

## When to Use Rules

**Use rules when**:
- Deriving one concept from others
- Supporting multiple schemas
- Creating computed views
- Adapting data for different clients

**Use direct queries when**:
- Finding specific facts
- Simple attribute lookups
- Building transactions

## What's Next?

You now understand Dialog's core concepts:

- **Relations** and **facts** - the building blocks
- **Entities** - persistent identities
- **Attributes** - defining what can be said
- **Concepts** - structured models
- **Queries** - finding facts
- **Transactions** - modifying the database
- **Rules** - deriving knowledge

For more details, check the [Glossary](./Glossary.md) for comprehensive definitions of all terms.
