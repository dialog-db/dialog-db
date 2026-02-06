# dialog-query

Datalog-inspired query engine for Dialog-DB. Operates over an Entity-Attribute-Value fact store with typed pattern matching, deductive rules, and built-in formulas.

## Information Model

### Facts

All data in Dialog-DB is represented as atomic, immutable facts — equivalent to semantic triples in [RDF] and [datoms][datom] in Datomic. A fact takes the form `{the, of, is, cause}`, corresponding to natural language: _the_ **role** _of_ **alice** _is_ **"cryptographer"**.

```
{ the: "employee/role", of: alice, is: "cryptographer", cause }
```

- **Entity** (`of`) — the subject, represented as a URI
- **Attribute** (`the`) — something that can be said about an entity, a `/`-delimited name like `employee/role`
- **Value** (`is`) — a concrete value (string, number, boolean, bytes, etc.)
- **Cause** — causal reference establishing partial order between facts

Facts are immutable and content-addressed. Asserting and retracting facts produces new database revisions rather than mutating existing state. An entity's state is the set of all facts about it — there is no schema enforced at the storage layer.

### Attributes

An attribute describes a relation between an entity and a value. Attributes are often referenced in `namespace/name` format (e.g. `employee/name`), where the first component is the **namespace** and the rest is the **name** — but in practice the value type and cardinality are also part of the identity. `employee/name` typed as `String` and `employee/name` typed as `Bytes` are distinct attributes that can coexist without conflict.

An attribute is defined as a newtype wrapping a value type. The **namespace** is derived from the enclosing module name (underscores become hyphens), and the **name** from the struct name (converted to kebab-case). The namespace can be overridden with `#[namespace(...)]`.

Doc comments on the struct are captured as the attribute's description.

```rs
mod employee {
    /// Person's given name
    #[derive(Attribute, Clone, PartialEq)]
    pub struct Name(pub String);   // -> "employee/name"

    /// Job title or function
    #[derive(Attribute, Clone, PartialEq)]
    pub struct Role(pub String);   // -> "employee/role"
}
```

By default an attribute has **cardinality one** — an entity has at most one value for it. Use `#[cardinality(many)]` when an entity can have multiple values:

```rs
mod employee {
    /// Skills associated with the employee
    #[derive(Attribute, Clone, PartialEq)]
    #[cardinality(many)]
    pub struct Skill(pub String);  // -> "employee/skill" (many)
}
```

> Note: cardinality affects whether an existing value is retracted when a new one is asserted — cardinality one implies replacement, cardinality many accumulates. This is not yet fully implemented.

### Concepts

A concept groups related attributes into a struct, providing a higher-level view of an entity — similar to a relation in relational databases, but applied at query time rather than write time (schema-on-query). Any entity can have attributes from multiple concepts without migration.

Asserting a concept stores one fact per attribute:

```rs
#[derive(Concept, Debug, Clone, PartialEq)]
pub struct Employee {
    this: Entity,
    name: employee::Name,
    role: employee::Role,
}

// Asserting an Employee stores two facts:
//   { the: "employee/name", of: alice, is: "Alice" }
//   { the: "employee/role", of: alice, is: "cryptographer" }
```

[RDF]: https://en.wikipedia.org/wiki/Resource_Description_Framework
[datom]: https://docs.datomic.com/glossary.html#datom

## Querying Information

Query patterns use `Term<T>` — either a variable (`Term::var("x")`) or a constant (`Term::from(value)`). Variables are bound by the query engine; constants constrain the search.

### Attributes with `With<A>`

`With<A>` queries a single attribute relation — one fact per match:

```rs
// All entities that have a Name
let named = Match::<With<employee::Name>> {
    this: Term::var("entity"),
    has: Term::var("name"),
}.query(&session).try_vec().await?;

// A specific entity's name
let alice_name = Match::<With<employee::Name>> {
    this: Term::from(alice.clone()),
    has: Term::var("name"),
}.query(&session).try_vec().await?;
```

### Concepts

Querying a concept is a logical conjunction (AND) — an entity matches only when _all_ of the concept's attributes are present:

```rs
// All employees named Alice (must have both name AND role)
let pattern = Match::<Employee> {
    this: Term::var("person"),
    name: Term::from("Alice".to_string()),
    role: Term::var("role"),
};
let results = pattern.query(&session).try_vec().await?;
```

## Accreting Information

### Attributes with `With<A>`

`With<A>` maps directly to a single fact assertion or retraction:

```rs
let mut session = Session::open(artifacts);

let mut tx = session.edit();
tx.assert(With { this: alice.clone(), has: employee::Name("Alice".to_string()) });
// stores the fact: { the: "employee/name", of: alice, is: "Alice" }
tx.assert(With { this: alice.clone(), has: employee::Role("cryptographer".to_string()) });
// stores the fact: { the: "employee/role", of: alice, is: "cryptographer" }
session.commit(tx).await?;

// Retract a single attribute
let mut tx = session.edit();
tx.retract(With { this: alice, has: employee::Name("Alice".to_string()) });
session.commit(tx).await?;
```

### Concepts

Asserting a concept asserts all its attributes at once. Retracting works the same way.

```rs
let mut tx = session.edit();
tx.assert(Employee {
    this: alice.clone(),
    name: employee::Name("Alice".to_string()),
    role: employee::Role("cryptographer".to_string()),
});
session.commit(tx).await?;

let mut tx = session.edit();
tx.retract(Employee {
    this: alice,
    name: employee::Name("Alice".to_string()),
    role: employee::Role("cryptographer".to_string()),
});
session.commit(tx).await?;
```

## Deductive Rules

Rules provide logical disjunction (OR) — they derive a concept from alternative sets of premises. Where a concept query requires all attributes to match, installing multiple rules for the same concept means _any_ rule can produce a match.

```rs
// An Employee can be derived from a Person
fn employee_from_person(employee: Match<Employee>) -> impl When {
    (
        Match::<Person> {
            this: employee.this.clone(),
            name: employee.name.clone(),
            title: employee.role.clone(),
        },
    )
}

// ...or from a Contractor
fn employee_from_contractor(employee: Match<Employee>) -> impl When {
    (
        Match::<Contractor> {
            this: employee.this.clone(),
            name: employee.name.clone(),
            position: employee.role.clone(),
        },
    )
}

// Installing both rules means querying Employee finds matches from either source
let session = Session::open(store)
    .install(employee_from_person)?
    .install(employee_from_contractor)?;
```

## Formulas

Pure computations integrated into the query planner — roughly equivalent to built-in functions in relational databases (like `CONCAT`, `SUM`, `LOWER`). Given bound input fields, a formula derives output fields.

```rs
#[derive(Debug, Clone, Formula)]
pub struct Sum {
    pub of: u32,
    pub with: u32,
    #[derived]
    pub is: u32,
}

impl Sum {
    pub fn derive(input: Input<Self>) -> Vec<Self> {
        vec![Sum {
          of: input.of,
          with: input.with,
          is: input.of + input.with
        }]
    }
}
```

Built-in formulas: `Sum`, `Difference`, `Product`, `Quotient`, `Modulo`, `Concatenate`, `Length`, `Uppercase`, `Lowercase`, `ToString`, `ParseNumber`, `And`, `Or`, `Not`.
