# Glossary

This glossary provides comprehensive definitions for Dialog concepts. Terms are organized by category for easy reference.

---

## Core Concepts

### Entity

A persistent subject of knowledge. An entity serves as the anchor for facts and descriptions over time, across changing roles and perspectives. It is not bound to a single concept or structure — the same entity may participate in multiple conceptual models.

**Entity Identifier (`of`)**: Entities are uniquely identified by URIs. In practice, these are Ed25519 [did:key](https://w3c-ccg.github.io/did-key-spec/) identifiers derived from the hash of what makes the entity unique within the system.

### Attribute

Attributes define the space of expressible relations in the system — they describe what can be said about an entity and how. An attribute is a description consisting of:

- A namespace and name (e.g., `person/name`)
- A value type (e.g., `string`, `boolean`, ...)
- A cardinality (`One` or `Many`)
- A description of the semantics of the relation

**Attribute Identifier (`the`)**: Attributes are referred to in relations via a unique identifier, denoted by the `the` field. They are often encoded as string representations like `person/name`.

**Namespace**: The first component of an attribute name. Namespaces serve a role similar to table names in a relational store but without imposing constraints. An entity can have attributes from multiple namespaces. It is generally recommended to use [reverse domain name notation](https://en.wikipedia.org/wiki/Reverse_domain_name_notation) (e.g., `io.gozala.note`).

**Cardinality**: Determines how many values an entity can have for an attribute. `Cardinality::One` means at most one value (new assertions supersede previous ones). `Cardinality::Many` means multiple values (each assertion adds a new value).

### Value (`is`)

An immutable scalar representing data such as `42`, `"John"`, `true`, or an entity reference. Values are the concrete data attached to attributes in a relation. Supported types include: `boolean`, `integer`, `float`, `string`, `bytes`, `symbol`, `entity`.

### Relation

A statement in the form of `{ the, of, is }` describing a possible connection between an entity, an attribute, and a value. A relation is not a fact itself — it's a unit of knowledge that can be asserted or retracted.

> Relations are inspired by natural language structure: _the_ **color** _of_ **sky** _is_ **blue**.

### Attribution

An attribution describes a potential relation between an attribute and a value, independently of any entity.

### Fact

Atomic, immutable unit of knowledge which is either an assertion or a retraction of a relation at specific causal point in time.

**Assertion**: An atomic fact in the system, associating an entity with a particular value of an attribute at specific causal point in time. Opposite of retraction.

**Retraction**: An atomic fact in the system, dissociating an entity from particular value of an attribute at specific causal point in time. Opposite of assertion.

### Causal Reference (cause)

Causal references ground facts in time and establish partial order between them. They are represented as logical timestamps that enable causal reasoning without requiring global clock synchronization.

### Concept

A concept describes a set of attributes that an entity may have — similar to a table in a relational database or a schema in a document store. Concepts define expected relations, giving structure and semantic meaning to facts.

Concepts in Dialog are composable and applied at query time. Unlike rigid schemas, they do not enforce structure at write time, allowing for flexible, evolving knowledge.

### Realization

A realization is an instance of a concept — a set of facts describing an entity according to the concept's definition. It is similar to a document in a document store or a row in a relational table, but grounded in relations.

### Rule

Rules are equivalent of views in relational databases. Rules define how concepts can be derived from existing facts through set of premises that must be true. They enable logical inference by deriving new conclusions from existing facts.

Unlike materialized views, rules do not have predetermined evaluation time and can be evaluated lazily at query time or eagerly at assertion time.

---

## Database Operations

### Claim

A claim represents a proposed change to the system - either assertion or retraction of a relation. Claims are not facts until they are accepted at specific causal point in time.

### Transaction

Atomic operation describing set of assertions and retractions in the database. Transactions ensure atomicity, that is all assertions & retractions are applied together or none are, maintaining database consistency. Each transaction results in a new revision.

### Commit

Act of applying a transaction to the database, resulting in a new revision.

### Instruction

Instruction is a way to refer to a component of the transaction without specifying whether it is an assertion or a retraction.

### Session

Database connection providing query and transaction capabilities. Sessions manage the context for interacting with the database, including caching and transaction boundaries.

### Revision

Immutable snapshot of the database state at a point in time, represented as a content hash. Each commit creates a new revision, enabling time-travel queries and audit trails.

---

## Querying

### Datalog

The declarative query language used by DialogDB, well-suited for graph-structured facts. Datalog allows expressing complex graph traversals and pattern matching through logical rules.

### Variable

Query placeholder that gets bound to values during evaluation, denoted with `?` prefix (e.g., `?person`, `?name`). Variables act as unknowns that the query engine fills in by pattern matching against facts in the database.

### Term

Either a concrete scalar value or a variable in a query. Terms are the building blocks of query patterns - concrete terms match exact values while variable terms match any value and bind it for use elsewhere in the query.

### Selector

Basic filter for querying facts, specifying patterns for the `the`, `of`, and/or `is` components. Selectors are the simplest form of query, matching facts directly without complex logic.

### Premise

Query component that can be a formula application, rule application or a negation. Predicates extend basic pattern matching with computational logic, enabling derived values and complex conditions.

### Formula

Computational predicate that derives output values from input values. Formulas perform calculations within queries, such as string manipulation, arithmetic, or data transformation.

### Negation

Query constraint that matches when a pattern is NOT present. Negation enables queries like "find all people without email addresses" by matching the absence of facts.

### Query Planner

Component that reorders query conjuncts to minimize search space and detect cycles. The planner optimizes query execution by choosing the most selective patterns first and identifying infinite loops.

---

## Data Architecture

### Schema-on-Read

DialogDB's approach where schema is applied at query time rather than write time. Unlike traditional databases that enforce schema constraints during data insertion, DialogDB allows any valid fact to be stored and applies interpretation during queries.

### Local-First

Core principle where all queries run against local database instances with background synchronization. This architecture ensures applications remain responsive and functional even without network connectivity.

### Causal Temporal Model

DialogDB's approach to time where facts exist in causal timelines rather than a universal timeline. This model allows distributed nodes to operate independently and merge their timelines later.

---

## Indexing & Storage

### EAV Index (Entity-Attribute-Value)

Primary index optimized for retrieving all attributes of a given entity. Efficiently answers questions like "What do we know about entity X?"

### AEV Index (Attribute-Entity-Value)

Index optimized for retrieving all entities with a specific attribute. Efficiently answers questions like "Which entities have a 'name' attribute?"

### VAE Index (Value-Attribute-Entity)

Index optimized for finding entities with specific attribute values. Efficiently answers questions like "Which entities have the name 'Alice'?"

### Probabilistic Search Tree

Deterministic, content-addressed tree structure ensuring same data produces same tree regardless of insertion order. They use content-based splitting decisions rather than child count, making them optimal for replication.

### Content-Addressed Storage

Storage system where data is addressed by its cryptographic hash rather than location. This approach ensures data integrity, enables deduplication, and allows efficient caching.

### Blob Store

Hash-addressed storage system for immutable, content-addressed blobs. DialogDB is agnostic to the specific blob store implementation - any system supporting get/put operations by hash can serve as a blob store.

---

## Distributed Systems

### CRDT (Conflict-free Replicated Data Type)

DialogDB implements Merkle-CRDT properties for convergent replication. CRDTs ensure that distributed replicas can be updated independently and will converge to the same state when they exchange updates.

### Merkle-CRDT

Conflict-free replicated data type using merkle trees, forming the basis of DialogDB's synchronization. The merkle tree structure allows efficient detection of differences between replicas.

### Mutable Pointer

Cryptographically signed reference to the current root hash, identified by DID:Key. The mutable pointer serves as the "HEAD" of the database, allowing the immutable content-addressed structure to have a stable, updatable reference point.

### DID (Decentralized Identifier)

Identifier format used for databases, formatted as `did:method:identifier`. DialogDB currently supports `did:key` method where the identifier is derived from a public key.

### Eventual Consistency

Property where all replicas converge to the same state when they have the same facts. DialogDB's CRDT-based design ensures that regardless of the order in which updates are applied, all replicas will eventually reach identical states.

---

For the complete glossary with all terms, see the [comprehensive glossary](../../notes/glossary.md).
