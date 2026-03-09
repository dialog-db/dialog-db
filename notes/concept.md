# Concepts

Dialog stores information as immutable claims in the form `{the, of, is, cause}`, where `the` identifies the relation, `of` the entity, `is` the value, and `cause` the provenance. At the associative layer there is no schema; claims are just associations.

The semantic layer introduces **attributes** and **concepts** on top of this. An attribute elevates a raw relation with domain-specific invariants (value type and cardinality). A concept composes multiple attributes sharing an entity, describing the shape of a thing much like a type or class in a programming language, but realized through schema-on-read rather than schema-on-write. An entity is not limited to a single concept: the same entity can simultaneously satisfy `Employee`, `Manager`, and `Person` if it has the right claims.

## Defining Attributes

An attribute is a newtype wrapping a value type. Its identity is the triple `(the, type, cardinality)` where `the` is a nominal identifier in `domain/name` format. The `the` carries semantic meaning beyond structure: `employee/name` and `employee/role` may both wrap `String` with cardinality one, yet they remain distinct because their `the` denotes different relations.

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

The **domain** is derived from the enclosing module name (underscores become hyphens), and the **name** from the struct name (converted to kebab-case). You can override the domain with `#[domain(...)]`:

```rs
mod model {
    /// Person's given name
    #[derive(Attribute, Clone, PartialEq)]
    #[domain("employee")]
    pub struct Name(pub String);       // -> "employee/name" (not "model/name")

    /// Account identifier
    #[derive(Attribute, Clone, PartialEq)]
    #[domain("io.gozala")]
    pub struct AccountId(pub String);  // -> "io.gozala/account-id"
}
```

By default attributes have **cardinality one** (at most one value per entity). Use `#[cardinality(many)]` for multi-valued attributes:

```rs
mod employee {
    #[derive(Attribute, Clone, PartialEq)]
    #[cardinality(many)]
    pub struct Skill(pub String);  // -> "employee/skill" (many)
}
```

## Defining a Concept

A concept struct groups attributes together with a required `this: Entity` field. Every field except `this` must be an `Attribute` type. A concept's identity derives from the sorted set of its constituent attribute identities, not from its Rust struct name. Two concepts with the same attribute set are structurally equivalent regardless of naming.

```rs
#[derive(Concept, Debug, Clone, PartialEq)]
pub struct Employee {
    pub this: Entity,
    pub name: employee::Name,
    pub role: employee::Role,
}
```

The `#[derive(Concept)]` macro generates all the boilerplate needed to query and transact at this granularity.

## Bidirectional Mapping

A concept acts as a bidirectional mapping between the semantic and associative layers:

- **Writing** (semantic -> associative): asserting a concept decomposes it into individual `{the, of, is}` assertions, one per attribute.
- **Reading** (associative -> semantic): querying a concept composes matching claims into **conclusions**, which are realized concept instances with typed fields.

## Asserting Data

Asserting a concept decomposes it into individual attribute claims:

```rs
let mut tx = session.edit();
tx.assert(Employee {
    this: alice.clone(),
    name: employee::Name("Alice".into()),
    role: employee::Role("cryptographer".into()),
});
session.commit(tx).await?;

// Equivalent to:
// tx.assert(employee::Name::of(alice.clone()).is("Alice"));
// tx.assert(employee::Role::of(alice.clone()).is("cryptographer"));
```

## Retracting Data

```rs
let mut tx = session.edit();
tx.retract(Employee {
    this: alice.clone(),
    name: employee::Name("Alice".into()),
    role: employee::Role("cryptographer".into()),
});
session.commit(tx).await?;
```

## Querying

Querying a concept is a logical conjunction: an entity matches only when _all_ attributes are present. Use `Query::<T>` (an alias for the generated query struct):

```rs
// Find all employees named Alice
let results = Query::<Employee> {
    this: Term::var("person"),
    name: Term::from("Alice"),
    role: Term::var("role"),
}.perform(&session).try_vec().await?;
```

`Default` fills every field with a named variable, useful when you want all matches:

```rs
let all_employees = Query::<Employee>::default()
    .perform(&session)
    .try_vec()
    .await?;
```

## Single-Attribute Queries

Individual attributes can be queried without defining a concept:

```rs
let query = Query::<employee::Name> {
    of: Term::var("entity"),
    is: Term::var("name"),
};
let results = query.perform(&session).try_vec().await?;
```

## Concepts in Rules

Concepts are the conclusion type for deductive rules. A rule derives a concept from premises:

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

let session = Session::open(artifacts)
    .install(employee_from_contractor)?;
```

Multiple rules for the same concept provide logical disjunction (OR). Any rule can produce a conclusion. See [rules.md](rules.md) for details.

## Schema-on-Read

A concept doesn't define storage layout. It's a lens over the claim store. The same underlying claims can satisfy multiple concepts simultaneously:

```rs
#[derive(Concept, Debug, Clone)]
pub struct Named {
    pub this: Entity,
    pub name: employee::Name,
}

// Alice satisfies both Employee and Named, because she has
// both employee/name and employee/role claims.
```

An entity only matches a concept when it has claims for _every_ attribute in the concept.
