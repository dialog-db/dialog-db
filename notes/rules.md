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
    pub this: Entity,
    pub name: employee::Name,
    pub role: employee::Role,
}
```

## Defining Rules

A rule is a function that takes a `Query<T>` pattern for the conclusion and returns an `impl When` describing the premises. The premises are a tuple of patterns that must all hold for the conclusion to be derived.

```rs
fn employee_from_contractor(employee: Query<Employee>) -> impl When {
    (
        Query::<Contractor> {
            this: employee.this.clone(),
            name: employee.name.clone(),
            position: employee.role.clone(),
        },
    )
}
```

### Grounding

Following Datalog's grounding requirement, every variable used in the conclusion must be bound by at least one positive premise, and every variable in a negated premise must also appear in a positive premise. This ensures that derived facts are always grounded in existing data. If these conditions are not met, `.install()` will return an error as rule validation will fail.

### Premise Ordering

The order of premises in a rule body does not matter. The query planner reorders them during compilation for optimal execution.

## Installing Rules

Rules are installed into a `Session` with `.install()`, which compiles and validates the rule:

```rs
let session = Session::open(artifacts)
    .install(employee_from_contractor)?;

// Now querying Employee will also find conclusions derived from Contractor
let employees = Query::<Employee>::default()
    .perform(&session)
    .try_vec()
    .await?;
```

## Using Formulas in Rules

Formulas can be used as premises to compute derived values from bound variables:

```rs
fn greeting_rule(greeting: Query<Greeting>) -> impl When {
    (
        Query::<Employee> {
            this: greeting.this.clone(),
            name: Term::var("name"),
            role: Term::blank(),
        },
        Query::<Concatenate> {
            first: "Hello, ".to_string().into(),
            second: Term::var("name"),
            is: greeting.message,
        },
    )
}
```

## Using Attribute Expressions in Rules

Attribute expressions can be used directly as premises, allowing rules to work with the associative model without defining a concept:

```rs
fn employee_from_relations(employee: Query<Employee>) -> impl When {
    (
        the!("person/name")
            .of(employee.this.clone())
            .is(employee.name.clone()),
        the!("person/role")
            .of(employee.this.clone())
            .is(employee.role.clone()),
    )
}
```

## Negation

Use `!` to negate a premise. The rule matches only when the negated pattern does *not* hold:

```rs
fn employee_without_role(employee: Query<Employee>) -> impl When {
    (
        the!("person/name")
            .of(employee.this.clone())
            .is(employee.name.clone()),
        // Entity must NOT have a role claim
        !the!("person/role")
            .of(employee.this.clone())
            .is(Term::<String>::blank()),
        employee.role.is(employee::Role("unknown".into())),
    )
}
```

Concept queries can also be negated:

```rs
fn non_manager(employee: Query<Employee>) -> impl When {
    (
        Query::<Employee> {
            this: employee.this.clone(),
            name: employee.name.clone(),
            role: employee.role.clone(),
        },
        !Query::<Manager> {
            this: employee.this.clone(),
            ..Default::default()
        },
    )
}
```

## Writing Data

Use transactions to assert and retract facts:

```rs
let mut tx = session.edit();
tx.assert(Employee {
    this: alice.clone(),
    name: employee::Name("Alice".into()),
    role: employee::Role("cryptographer".into()),
});
session.commit(tx).await?;

// Retract
let mut tx = session.edit();
tx.retract(Employee {
    this: alice,
    name: employee::Name("Alice".into()),
    role: employee::Role("cryptographer".into()),
});
session.commit(tx).await?;
```
