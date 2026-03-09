# dialog-query

Datalog-inspired query engine for Dialog. Operates over the associative model's claim store, providing typed pattern matching, deductive rules, and built-in formulas.

## Information Model

### Associative

Stores and replicates information as an immutable, append-only history of claims. There is no schema enforced at this level.

#### Claims

A statement is a set of `{the, of, is}` associations. A concept conclusion is a statement that decomposes into the attribute statements it is comprised of, each corresponding to a single association. When statements are asserted, new associations are stored as claims with an added `cause` logical timestamp. When retracted, matching claims are evicted.

A claim takes the form `{the, of, is, cause}`, corresponding to natural language: _the_ **role** _of_ **alice** _is_ **"cryptographer"**.

```
{ the: "employee/role", of: alice, is: "cryptographer", cause }
```

- **Entity** (`of`) - the subject of the claim
- **Relation** (`the`) - categorizes the claim by the kind of association being established, in `domain/name` format (e.g. `employee/role`)
- **Value** (`is`) - the value being linked through the relation (string, number, boolean, bytes, entity, etc.)
- **Cause** - provenance describing who produced the claim and when, establishing causal order

Claims are immutable and content-addressed. An entity's state is the set of all claims about it.

#### Relations

Relations categorize claims by the kind of association being established. A relation is comprised of a **domain** (scoping it to a specific problem area) and a **name** (identifying the specific association within that domain), denoted as `domain/name`.

The `the!` macro produces a relation from a `"domain/name"` string literal, validated at compile time. You can construct statements from relations directly, skipping the semantic model:

```rs
use dialog_query::the;

let alice = Entity::new()?;

// Assert statements using relations directly
let mut edit = session.edit();
edit.assert(
    the!("employee/name")
        .of(alice.clone())
        .is("Alice")
);
edit.assert(
    the!("employee/role")
        .of(alice.clone())
        .is("cryptographer")
);
session.commit(edit).await?;
```

Dynamic expressions support both concrete values and `Term` variables for querying:

```rs
// Query with a variable value
let premise: Premise = the!("employee/name")
    .of(alice.clone())
    .is(Term::<String>::var("name"))
    .into();

// Query with a variable entity
let premise: Premise = the!("employee/name")
    .of(Term::var("entity"))
    .is("Alice".to_string())
    .into();
```

It is possible to use `Term::<The>` to discover all relations for an entity:

```rs
// Find all relations between alice and bob
let premise: Premise = Term::<The>::var("relation")
    .of(alice.clone())
    .is(bob.clone())
    .into();
```

### Semantic

Provides modeling primitives for describing a domain: attributes, concepts, rules, and formulas. Statements made here are decomposed into claims in the associative model.

#### Attributes

An attribute is a relation elevated with domain-specific invariants. It extends the `domain/name` identifier with a value type and cardinality, specifying what kind of values the association admits and how many.

An attribute is defined as a newtype wrapping a value type. The **domain** is derived from the enclosing module name (underscores become hyphens), and the **name** from the struct name (converted to kebab-case):

```rs
mod employee {
    /// Person's given name
    #[derive(Attribute, Clone)]
    pub struct Name(pub String);   // -> "employee/name"

    /// Job title or function
    #[derive(Attribute, Clone)]
    pub struct Role(pub String);   // -> "employee/role"
}
```

The domain can be overridden with `#[domain(...)]` when the module name doesn't match the desired domain:

```rs
mod model {
    /// Person's given name
    #[derive(Attribute, Clone)]
    #[domain("employee")]
    pub struct Name(pub String);       // -> "employee/name" (not "model/name")

    /// Account identifier
    #[derive(Attribute, Clone)]
    #[domain("io.gozala")]
    pub struct AccountId(pub String);  // -> "io.gozala/account-id"
}
```

By default an attribute has **cardinality one**, an entity has at most one value for it. Use `#[cardinality(many)]` when an entity can have multiple values:

```rs
mod employee {
    /// Skills associated with the employee
    #[derive(Attribute, Clone)]
    #[cardinality(many)]
    pub struct Skill(pub String);  // -> "employee/skill" (many)
}
```

> Note: cardinality affects whether an existing claim is retracted when a new one is asserted. Cardinality one implies replacement, cardinality many accumulates.

`Attribute::of(...).is(...)` constructs a statement that can be asserted or retracted:

```rs
let mut session = Session::open(artifacts);
let mut edit = session.edit();

// Assert single attributes
edit.assert(employee::Name::of(alice.clone()).is("Alice"));
edit.assert(employee::Role::of(alice.clone()).is("cryptographer"));

session.commit(edit).await?;

// Retract a single attribute
let mut edit = session.edit();
employee::Name::of(alice).is("Alice").retract(&mut edit);
session.commit(edit).await?;
```

#### Concepts

A concept is a composition of attributes sharing an entity, much like a type in a programming language. It is the primary unit of domain modeling, realized through schema-on-read rather than schema-on-write. An entity is not limited to a single concept: the same entity can simultaneously satisfy `Employee`, `Manager`, and `Person` if it has the right claims.

A concept acts as a bidirectional mapping into the associative model. In one direction, querying a concept composes matching claims into **conclusions** (realized concept instances, analogous to instances of a type). In the other direction, asserting a concept decomposes it into individual attribute statements.

```rs
#[derive(Concept, Debug, Clone, PartialEq)]
pub struct Employee {
    this: Entity,
    name: employee::Name,
    role: employee::Role,
}
```

Asserting a concept decomposes it into individual attribute statements:

```rs
let mut tx = session.edit();
tx.assert(Employee {
    this: alice.clone(),
    name: employee::Name("Alice".to_string()),
    role: employee::Role("cryptographer".to_string()),
});
session.commit(tx).await?;

// Equivalent to:
// tx.assert(the!("employee/name").of(alice.clone()).is("Alice"));
// tx.assert(the!("employee/role").of(alice.clone()).is("cryptographer"));
```

Querying a concept is a logical conjunction (AND). An entity matches only when _all_ of the concept's attributes are present. The result is a set of conclusions:

```rs
let pattern = Query::<Employee> {
    this: Term::var("person"),
    name: Term::from("Alice".to_string()),
    role: Term::var("role"),
};
let conclusions = pattern.perform(&session).try_vec().await?;
```

You can also query by a single attribute:

```rs
let query = Query::<employee::Name> {
    of: Term::var("entity"),
    is: Term::var("name"),
};
let results = query.perform(&session).try_vec().await?;
```

Retracting works the same way as asserting:

```rs
let mut tx = session.edit();
tx.retract(Employee {
    this: alice.clone(),
    name: employee::Name("Alice".to_string()),
    role: employee::Role("cryptographer".to_string()),
});
session.commit(tx).await?;

// Equivalent to:
// tx.retract(the!("employee/name").of(alice.clone()).is("Alice"));
// tx.retract(the!("employee/role").of(alice.clone()).is("cryptographer"));
```

#### Deductive Rules

Rules provide logical disjunction (OR). They derive a concept from alternative sets of premises. Where a concept query requires all attributes to match, installing multiple rules for the same concept means _any_ rule can produce a conclusion.

A rule's body is a set of premises with `Term` variables acting as join points across them.

```rs
// An Employee can be derived from a Person
fn employee_from_person(employee: Query<Employee>) -> impl When {
    (
        Query::<Person> {
            this: employee.this.clone(),
            name: employee.name.clone(),
            title: employee.role.clone(),
        },
    )
}

// ...or from a Contractor
fn employee_from_contractor(employee: Query<Employee>) -> impl When {
    (
        Query::<Contractor> {
            this: employee.this.clone(),
            name: employee.name.clone(),
            position: employee.role.clone(),
        },
    )
}

// Installing both rules means querying Employee finds conclusions from either source
let session = Session::open(store)
    .install(employee_from_person)?
    .install(employee_from_contractor)?;
```

Relation expressions can also be used as premises, allowing rules to work directly with the associative model. Use `Term::<The>::var` to query arbitrary relations:

```rs
// Derive Employee from ad-hoc relations
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

#### Formulas

Pure computations integrated into the query planner. Given bound input fields, a formula computes output fields.

```rs
#[derive(Debug, Clone, Formula)]
pub struct Sum {
    pub of: u32,
    pub with: u32,
    #[output]
    pub is: u32,
}

impl Sum {
    pub fn compute(input: Input<Self>) -> Vec<Self> {
        vec![Sum {
          of: input.of,
          with: input.with,
          is: input.of + input.with
        }]
    }
}
```

Formulas are used as premises in rules, computing derived values from bound variables:

```rs
fn total_compensation(result: Query<TotalComp>) -> impl When {
    (
        Query::<Salary> {
            of: result.this.clone(),
            is: Term::var("salary"),
        },
        Query::<Bonus> {
            of: result.this.clone(),
            is: Term::var("bonus"),
        },
        Query::<Sum> {
            of: Term::var("salary"),
            with: Term::var("bonus"),
            is: result.total.clone(),
        },
    )
}
```

Built-in formulas: `Sum`, `Difference`, `Product`, `Quotient`, `Modulo`, `Concatenate`, `Length`, `Uppercase`, `Lowercase`, `ToString`, `ParseNumber`, `And`, `Or`, `Not`.
