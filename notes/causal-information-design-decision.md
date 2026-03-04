# Surfacing Causal Information in Dialog's Query API

## The Problem

When a tool queries Dialog and gets results, it probably wants to capture the `Cause` of the records it read. That way, when it submits an edit, it can communicate its assumptions to the transactor, and the transactor can verify consistency. This is basically compare-and-swap: if you change a counter from 3 to 5 but the current value is 4, the transactor can tell your assumption is stale.

The challenge is that the current derive macros produce clean domain structs with no place to stick a `Cause`:

```rs
mod employee {
    #[derive(Attribute)]
    pub struct Name(String);

    #[derive(Attribute)]
    pub struct Role(String);

    #[derive(Concept)]
    pub struct Employee {
        pub name: Name,
        pub role: Role,
    }
}
```

## Context: Dialog's Causal Model

> ⚠️ This is not what is currently implemented, but it is the current plan.

Every claim in the associative layer carries a `Provenance`:

```rs
pub struct Provenance {
    pub origin: Did,   // unique site identifier
    pub period: usize, // coordinated time (last sync cycle)
    pub moment: usize, // uncoordinated local counter
}
```

> ℹ️ This plays a similar structural role to logical timestamps in Automerge. The `origin` is direct equivalent of **actor ID**. The `(period, moment)` pair is similar to Lamport counter, except it captures whether changes happened within a single offline session or across multiple sessions with syncs between them which Lamport counter conflates.

Causal assertions (described in [Modeling Cardinality](https://gozala.io/dialog/modeling-cardinality)) let a writer express intent: "I mean to succeed *this specific prior claim*." The transactor uses this, along with the current claim set, to resolve cardinality across tools that may disagree about whether an attribute holds one value or many.

## Options Considered

### Option 1: Custom Primitive Types

Replace built-in types with Dialog-specific wrappers that carry `Cause`:

```rs
pub struct Text(String, Cause);
pub struct UnsignedInteger(u128, Cause);

#[derive(Attribute)]
pub struct Name(Text);
```

This leaks infrastructure into the domain model. Every attribute carries provenance concerns even when the author just wants to model their problem domain.

This would also impose a closed set of supported types for modeling, which would have been a concern if it was not already the case.

The real concern is that carrying a 32-byte `Cause` hash for every single scalar feels excessive when most tools won't need it.

### Option 2: Proof Wrapper Type

Query returns `Proof<T>` instead of `T` directly, where the macro generates parallel value and provenance structures:

```rs
pub struct Proof<M: Model> {
    value: M::Type,
    provenance: M::Provenance,
}
```

For attributes: `Type = String`, `Provenance = Cause`.
For concepts: `Type = Employee`, `Provenance = EmployeeProvenance` (a generated struct with per-field causes).

This gives clean separation. The derive macros stay domain-focused, and `Proof` is a query-time concern. It composes well with cardinality-many too (an iterator of `Proof<Assignee>`, each with its own cause).

The downside is that every tool author pays the cognitive cost of understanding `Proof<T>` even if they never need causal information.

### Option 3: Weakened Consistency (Value-Based CAS)

Don't surface causal information by default. Instead of telling the transaction the `cause` a change is based on, tell it the `value` it's based on.

This works for the cardinality cases described in the modeling note. The transactor inspects the claim set at write time and can apply sole-claim vs multi-claim rules without the submitter providing cause.

It does not allow the transactor to differentiate when two different origins assert the *same value* at different times for different reasons. In practice this window is extremely narrow in the primary deployment scenario (see below). Even when it occurs, the application submitting the change has two options: silently retry, or surface an error to the user. If the application retries silently because the value didn't actually change, we have just introduced incidental complexity for no practical benefit. If it raises an error to the user, it is hard to imagine reasonable UX around where the user is able to make an informed decisions to act upon.

It weakens what's communicated, but the question is whether the stronger signal is actually actionable.

#### Primary Deployment Scenario

In the near-term, the primary deployment target is a service worker acting as the local transactor, with the main thread (UI) submitting queries and edits over a message channel. This is a single-writer setup where the transactor enforces consistency across local views. The window for a remote sync to land between a UI read and the subsequent edit submission is extremely narrow. Eventual consistency across replicas is a separate concern, reconciled at sync time based on what revision changes were made against.

## How Other Systems Handle This

### Automerge

Automerge sidesteps CAS entirely. Changes compose via CRDT merge rules with no rejection and no staleness check. Concurrent writes to the same scalar key get resolved deterministically, one value wins, and the API never asks the caller to provide a causal reference.

Automerge supports cross-thread communication (repo in a worker, UI in main thread via [`MessageChannelNetworkAdapter`](https://automerge.org/docs/reference/repositories/networking/#messagechannel), but the design assumes all changes eventually merge. There is no concept of rejecting a stale write.

Dialog's causal assertions express *intent* (which prior claim to succeed). This is what enables the cross-tool cardinality resolution described in the modeling note, where tools with different cardinality assumptions need to cooperate on shared data without a shared schema.

### Datomic

Datomic has a built-in [`db/cas`](https://docs.datomic.com/transactions/transaction-functions.html) that is **value-based**: `[:db/cas entity attribute old-value new-value]`. It checks whether the entity currently has the expected old value, and if so, expands to retract + assert. Otherwise the transaction aborts. The [best practices doc](https://docs.datomic.com/reference/best.html) shows it used for account balance deposits, and the [Jepsen analysis](https://jepsen.io/analyses/datomic-pro-1.0.7075) digs into the semantics.

Datomic could have done version-based or tx-id-based CAS since it has full history and transaction IDs, but chose value-based as the built-in. The reasoning: the value is what application logic actually depends on.

An important architectural similarity: Datomic has a single serialized transactor, giving it linearized transactions. Dialog's local transactor (say in a service worker) works the same way as a single writer enforcing consistency across local views. Eventual consistency is across replicas, where changes are reconciled based on what revision they were made against, similar to Automerge's sync model.

## Decision

**Causal information is a querying concern, not a modeling concern.**

The default query path returns plain domain types. Tools that need causal information opt into a richer query mode. The `Conclusion<T>` type still gets generated by the macro, but it is not the default return type.

```rs
fn demo(session: Session) -> Result<(), Error> {
    // Default: just give me the data
    let query = Query::<Employee> { 
        this: Term::var("alice"),
        name: Term::Constant("Alice"),
        role: Term::var("Intern"),
    };
    
    let [alice] = query
        .perform(&session)
        try_vec()
        .await?;

    let mut tx = session.edit();
    tx.assert(Role::of(alice).is("Cryptographer"));

    session.commit(tx).await?;
}
```

If we find that some use cases genuinely need provenance information, we can extend the interface to support `Proof<T>` (inspired by [Option 2](#option-2-proof-wrapper-type)) without imposing costs on all use cases:

```rs
let query = Query::<Proof<Employee>> { 
    this: Term::var("alice"),
    name: Term::Constant("Alice"),
    role: Term::var("Intern"),
};

let [proof] = query
    .perform(&session)
    try_vec()
    .await?;

// prints provenance for the name
println!("{:?}", proof.name().cause());
```

### Rationale

1. **Most tools likely don't need it.** We assume that the vast majority of authors would want to read values and write updates. Forcing everyone through `Proof<T>` would impose cognitive overhead for a capability most would never use.

2. **Transactor can do the right thing without it.** The cardinality resolution rules (sole claim vs multiple claims, promotion, etc.) depend on the current claim set at transaction time, which the transactor always has. This can work just as well if writer provides observed value instead of `cause`-al reference of observed claim.

3. **Value-based CAS should cover the common staleness case.** Following Datomic's lead, checking "is the value still what I read?" likely catches the meaningful conflicts. The edge case where value matches but cause differs seems both rare in practice and not clearly actionable. As discussed above, the application's options (silent retry or user-facing error) don't produce meaningful benefit in this scenario.

4. **Provenance-aware queries can be introduced if the need arises.** If we find use cases that genuinely require causal information, we can extend the query interface with `Proof<T>` as shown above, without imposing costs on all use cases.

5. **Cardinality resolution and staleness detection appear to be separate concerns.** The transactor handles cardinality by inspecting claims at write time. Staleness detection is a CAS concern, value-based by default. We believe bundling them behind a universal wrapper would conflate two things that are better kept apart.
