# Paradigm

Before we start writing code, let's understand what makes Dialog fundamentally different.

## Places vs Information

Most databases are organized around **places** - mutable containers that change over time. Tables, documents, and objects are all abstractions for mutable memory locations. When you update them, you lose what was there before and destroy history.

Dialog takes a different approach: **information accumulates as immutable facts**.

```rust
// Traditional: A place that changes
table.update({ id: "alice", salary: 60000 });  // Old salary gone
table.update({ id: "alice", salary: 65000 });  // 60000 lost forever

// Dialog: Information that accumulates
Assertion { employee/salary of alice is 50000, at t1 }
Assertion { employee/salary of alice is 65000, at t2 }
// Both facts exist - true at different points in time
```

This paradigm shift provides:
- **Time is queryable**: Query "what was true then?" without special infrastructure
- **Identity is separate from state**: An entity is just an ID; facts about it evolve independently
- **No coordination needed**: Facts are immutable values that merge deterministically
- **Multiple interpretations**: The same facts can satisfy different query patterns

## Building Blocks

### Entities: Identity Without State

An **entity** is pure identity - a stable identifier with no inherent state:

```rust
let alice = Entity::new()?;
// Creates: did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK
```

**Key insight**: An entity is not a container. It has no fields, no properties, no data. It's just an ID.

All information about alice exists as **separate facts**:

```rust
// alice doesn't "contain" a name
// Rather: there exists a fact "alice has name 'Alice'"
Relation {
    the: employee::Name,
    of: alice,
    is: "Alice"
}
```

This separation means:
- **Identity is stable**: alice is always alice, regardless of what's true about her
- **State evolves**: Facts about alice can be asserted and retracted without affecting alice's identity
- **No coupling**: Different facts about alice can change independently

Entities are globally unique (using [did:key](https://w3c-ccg.github.io/did-key-spec/) URIs), so they work across databases without coordination.

### Relations: The Shape of Information

A **relation** is a triple expressing information about an entity:

```
{ the, of, is }
```

- **the** - which attribute (e.g., employee::Name)
- **of** - which entity (e.g., alice)
- **is** - what value (e.g., "Alice", 60000, or another entity)

This reads naturally: **the** name **of** alice **is** "Alice".

```rust
// Scalar values
Relation { the: employee::Name, of: alice, is: "Alice" }
Relation { the: employee::Salary, of: alice, is: 60000 }

// Entity references (forming graphs)
Relation { the: employee::Manager, of: bob, is: alice }
```

**Key insight**: Relations are uniform. Every piece of information has the same shape, regardless of type. This uniformity makes querying, replication, and reasoning consistent across all data.

Attributes use namespace/name format (`employee/name`, `person/birthday`) to organize information without constraining what facts can exist:

```rust
// alice can have facts from any namespace
alice has employee/name "Alice"
alice has employee/salary 60000
alice has person/birthday "1990-01-15"
alice has account/username "alice123"
```

No schema enforcement - facts accumulate freely.

### Facts: Information + Time

A **fact** is a relation placed in time - information about when something became (or stopped being) true:

```rust
// Assertion: makes a relation true at a causal point
Fact::Assertion {
    the: employee::Name,
    of: alice,
    is: "Alice",
    cause: t1
}
// "At t1, alice's name became 'Alice'"

// Retraction: makes a relation false at a causal point
Fact::Retraction {
    the: employee::Salary,
    of: alice,
    is: 50000,
    cause: t2
}
// "At t2, alice's salary of 50000 stopped being true"
```

**Key insight**: Facts are immutable values. Once asserted, a fact never changes. To "update" data, you assert new facts and retract old ones.

Example - giving Alice a raise:

```rust
// t1: Hire Alice at 50k
Assertion { the: employee::Salary, of: alice, is: 50000, cause: t1 }

// t2: Give Alice a raise to 60k
Retraction { the: employee::Salary, of: alice, is: 50000, cause: t2 }
Assertion { the: employee::Salary, of: alice, is: 60000, cause: t2 }

// Query at t3: 60000 (current)
// Query at t1.5: 50000 (time-travel!)
```

Nothing was destroyed. Both "Alice earns 50000" and "Alice earns 60000" are true - just at different times.

### Cardinality: Single vs Multiple Values

Attributes declare cardinality to control how facts relate to each other over time:

**Cardinality::One** - Only one value is true at a time. Newer facts supersede older ones:

```rust
// employee::Salary has Cardinality::One
Assertion { the: employee::Salary, of: alice, is: 50000, cause: t1 }
Assertion { the: employee::Salary, of: alice, is: 60000, cause: t2 }

// Query at t1.5: returns 50000 (what was true then)
// Query at t3: returns 60000 (what is true now)

// The fact "salary was 50000 at t1" remains eternally true
// The fact "salary is 60000 from t2 onward" is also true
// Both facts exist - they're just true at different points in time
```

**Cardinality::Many** - Multiple values can be true simultaneously:

```rust
// employee::Skill has Cardinality::Many
Assertion { the: employee::Skill, of: alice, is: "Rust", cause: t1 }
Assertion { the: employee::Skill, of: alice, is: "Python", cause: t2 }

// Query at t1.5: returns ["Rust"]
// Query at t3: returns ["Rust", "Python"]

// Both facts are true at the same time from t2 onward
```

### Time: Logical, Not Physical

Dialog doesn't use wall-clock time. Instead, facts exist in **causal order** using logical timestamps:

```rust
Fact::Assertion {
    the: employee::Name,
    of: alice,
    is: "Alice",
    cause: logical_timestamp  // Causal position, not wall-clock time
}
```

**Why logical timestamps?**

- **No coordination needed**: Distributed peers don't need synchronized clocks
- **Concurrent edits work**: Multiple peers can create facts simultaneously - their logical timestamps establish partial order
- **Deterministic merging**: When peers sync, facts merge based on causality, not wall-clock time

Example - two peers edit simultaneously:

```rust
// Peer 1 at 3:00pm
Assertion { the: employee::Salary, of: alice, is: 65000, cause: peer1_t5 }

// Peer 2 at 3:00pm (same wall-clock time!)
Assertion { the: employee::Salary, of: alice, is: 70000, cause: peer2_t3 }

// When peers sync:
// - Both facts are preserved (no data loss)
// - Logical timestamps establish order
// - Conflict resolution applies (last-write-wins, max, etc.)
// - History contains both values
```

**You don't manage causality**. Dialog handles logical timestamps automatically when you commit transactions.

---

Now that we understand how Dialog models information as immutable facts, let's start building with attributes and concepts.
