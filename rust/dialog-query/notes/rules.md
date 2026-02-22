# Rules

Deductive rules derive new concepts from existing data (Datalog-style inference).

## Defining Attributes and Concepts

```rs
mod employee {
    #[derive(Attribute, Clone, PartialEq)]
    pub struct Name(pub String);

    #[derive(Attribute, Clone, PartialEq)]
    pub struct Role(pub String);
}

#[derive(Concept, Debug, Clone, PartialEq)]
pub struct Employee {
    this: Entity,
    name: employee::Name,
    role: employee::Role,
}
```

## Defining Rules

A rule is a function that takes a `Match<T>` pattern for the conclusion and returns
an `impl When` describing the premises. The premises are a tuple of patterns that
must all hold for the conclusion to be derived.

```rs
fn employee_from_stuff(employee: Match<Employee>) -> impl When {
    (
        Match::<Stuff> {
            this: employee.this.clone(),
            name: employee.name.clone(),
            role: employee.role.clone(),
        },
    )
}
```

## Installing Rules

Rules are installed into a `Session` with `.install()`:

```rs
let session = Session::open(store).install(employee_from_stuff)?;

// Now querying Employee will also find matches derived from Stuff
let employees = Match::<Employee> {
    this: Term::var("this"),
    name: Term::var("name"),
    role: Term::var("role"),
}.query(session).try_vec().await?;
```

## Using Formulas in Rules

```rs
fn full_name(person: Match<Person>) -> impl When {
    (
        Match::<Employee> {
            this: person.this.clone(),
            name: Term::var("first"),
            role: Term::var("_"),
        },
        Match::<Concatenate> {
            first: Term::var("first"),
            second: " Smith".to_string().into(),
            is: person.name,
        },
    )
}
```

## Negation

Use `!` to negate a pattern — the rule matches only when the negated pattern does *not* hold:

```rs
fn employee_without_role(employee: Match<Employee>) -> impl When {
    (
        Fact::<String>::select()
            .the("person/name")
            .of(employee.this.clone())
            .is(employee.name.clone().as_unknown())
            .compile().unwrap(),
        // Entity must NOT have a role fact
        !Fact::<String>::select()
            .the("person/role")
            .of(employee.this.clone())
            .is(Term::blank())
            .compile().unwrap(),
        employee.role.is(employee::Role("unknown".into())),
    )
}
```

## Writing Data

Use transactions to assert and retract facts:

```rs
let mut session = Session::open(store);

// Assert
let mut tx = session.edit();
tx.assert(Employee {
    this: alice.clone(),
    name: employee::Name("Alice".to_string()),
    role: employee::Role("cryptographer".to_string()),
});
session.commit(tx).await?;

// Retract
let mut tx = session.edit();
tx.retract(Employee {
    this: alice,
    name: employee::Name("Alice".to_string()),
    role: employee::Role("cryptographer".to_string()),
});
session.commit(tx).await?;
```
