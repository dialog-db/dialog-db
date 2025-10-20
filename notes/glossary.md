# Glossary

## Core Concepts

#### Entity

A persistent subject of knowledge. An entity serves as the anchor for [fact][]s and descriptions over time, across changing roles and perspectives.
It is not bound to a single [concept][] or structure — the same entity may participate in multiple conceptual models.

##### Entity Identifier (`of`)

Entities are uniquely identified by URIs. In practice, these are Ed25519 [did:key][] identifiers derived from the hash of what makes the entity unique within the system.
This identifier is referred to as `of` in a [relation][].


#### Attribute

Attributes define the space of expressible [relation][]s in the system — they describe what can be said about an [entity][] and how. An attribute is a description consisting of:

- A [namespace][] and `name` (e.g., `person/name`)
- A value type (e.g., `string`, `boolean`, ...)
- A cardinality (e.g., one or many)
- A description of the semantics of the relation

##### Attribute Identifier (`the`)

Attributes are referred to in [relation][]s via a unique identifier, denoted by the `the` field. They are often encoded as string representations like `person/name`.

> ℹ️ Attribute identifiers do not carry type information themselves; this information is present in the [attribution][] and [relation][] where the value appears and its type can be inferred.

##### Namespace

The first component of an attribute name is the namespace. Namespaces serve a role similar to table names in a relational store but without imposing constraints — an entity can have attributes from multiple namespaces.

Namespaces help organize attributes into categories and, importantly, provide data locality within the system, ensuring that attributes sharing a namespace are collocated, making querying more efficient.

Namespaces are intended to make attributes globally unique. It is generally **RECOMMENDED** to use [reverse domain name notation](https://en.wikipedia.org/wiki/Reverse_domain_name_notation) (e.g., `io.gozala.note`).

### Value (`is`)

An immutable scalar representing data such as `42`, `"John"`, `true`, or an entity reference.

Values are the concrete data attached to [attribute][]s in a [relation][]. They are denoted via the `is` field.
Supported types include: `boolean`, `integer`, `float`, `string`, `bytes`, `symbol`, `entity`.

#### Causal Reference (cause)

Causal references ground facts in time and establish partial order between them. At the moment they are represented as hash reference to preceding [fact], but alternative approaches throug logical clocks are being actively explored.

### Relation

A statement in the form of `{ the, of, is }` describing a possible connection between an [entity][], an [attribute][], and a [value][].

A relation is not a [fact][] itself — it's a unit of knowledge that can be **asserted** or **retracted**, typically forming part of a larger [claim][] or [conceptual model][].

> ℹ️ Relations are inspired by natural language structure: _the_ **color** _of_ **sky** _is_ **blue**.

You're absolutely right — let's fix that now.

Here's a new **section for `Attribution`**, written in the same style and tone as the rest of your glossary. It includes:

* A clear definition of what an attribution is.
* Its relation to `relation`, `attribute`, and `value`.
* A note about its role in groupings or conceptual models.

---

### Attribution

An attribution describes a potential relation between an [attribute][] and a [value][], independently of any [entity][].

#### Fact

Atomic, immutable unit of knowledge which is either [assertion] or a [retraction] a [relation] at specific [cause]-al point in time.


##### Assertion

An atomic [fact] in the system, associating an [entity] with a particular [value] of an [attribute] at specific [cause]-al point in time. Opposite of [retraction].

##### Retraction

An atomic [fact] in the system, dissociating an [entity] from particular [value] of an [attribute] at specific [cause]-al point in time. Opposite of [assertion].

### Claim

A claim represents a proposed change to the system - either assertion or retraction of a [relation][]. Claims are not [fact]s until they are accepted at specific [cause][]-al point in time.

### Concept

A concept describes a set of [attribute][]s that an [entity][] may have — similar to a table in a relational database or a schema in a document store. Concepts define **expected relations**, giving structure and semantic meaning to facts.

Concepts in Dialog are composable and applied at query time. Unlike rigid schemas, they do not enforce structure at write time, allowing for flexible, evolving knowledge.

```ts
const Employee = concept({
  role: String,
  salary: Number,
  department: Object
})
```

Concepts enable type safety and reusable queries. An entity may match multiple concepts simultaneously, allowing multiple views on the same underlying data.

### Realization

A realization is an instance of a [concept][] — a set of [fact][]s describing an [entity][] according to the concept’s definition. It is similar to a document in a document store or a row in a relational table, but grounded in [relation][]s.

```ts
Employee.assert({
  name: 'Bitdiddle Ben',
  role: 'Computer wizard',
  salary: 60_000
})
```

Realizations are typically used for querying or asserting facts through higher-level conceptual models.

### Rule

Rules are equivalent of views in relational databases. Rules define how concepts can be derived from existing facts through set of premises that must be true. They enable logical inference by deriving new conclusions from existing facts.

```typescript
const Manager = concept({
  subordinate: Object
}).where(manager => [
  Employee.match({ this: manager.subordinate }),
  Manager.relation({ this: manager.this, is: manager.subordinate }),
])
```

Rules can be recursive, enabling complex queries like transitive relationships.
Unlike materialized views, rules do not have predetermined evaluation time and can be evaluated lazily at query time or eagerly at assertion time.

### Fact Store

Storage system for [fact]s. Dialog implements a fact store that indexes facts in multiple ways to support every possible access pattern without linear scans.

## Database Operations

### Assertion

An atomic [fact] in the database, associating an [entity], [attribute], [value], and a [cause]. Opposite of a [retraction].

Assertions are the primary way data enters the system - they create new facts without modifying existing ones, maintaining the immutable, append-only nature of the database.

### Retraction

An atomic [fact] in the database, dissociating an [entity] from particular [value] of an [attribute]. Opposite of an [assertion].

Rather than removing information, retractions add new information to indicate that facts is no longer true.

### Transaction

Atomic operation describing set of [assertion]s and [retraction]s in the database. Transactions ensure atomicity, that is all assertions & retractions are applied together or none are, maintaining database consistency. Each transaction results in a new [revision].

### Commit

Act of applying a transaction to the database, resulting in a new [revision].

### Instruction

Instruction is a way to refer to a component of the transaction without specifying whether it is an [assertion] or a [retraction].

### Session

Database connection providing query and transaction capabilities. Sessions manage the context for interacting with the database, including caching and transaction boundaries.

### Revision

Immutable snapshot of the database state at a point in time, represented as a content hash. Each [commit] creates a new revision, enabling time-travel queries and audit trails.

## Querying

### Datalog

The declarative query language used by DialogDB, well-suited for graph-structured facts. Datalog allows expressing complex graph traversals and pattern matching through logical rules, making it good fit for querying interconnected data without explicit joins.

### Variable

Query placeholder that gets bound to values during evaluation, denoted with `?` prefix (e.g., `?person`, `?name`). Variables act as unknowns that the query engine fills in by pattern matching against facts in the database.

### Term

Either a concrete scalar value or a variable in a query. Terms are the building blocks of query patterns - concrete terms match exact values while variable terms match any value and bind it for use elsewhere in the query.

### Selector

Basic filter for querying facts, specifying patterns for the `the`, `of`, and/or `is` components. Selectors are the simplest form of query, matching facts directly without complex logic.

### Premise

Query component that can be a [formula] application, [rule] application or a [negation]. Predicates extend basic pattern matching with computational logic, enabling derived values and complex conditions.

### Formula

Computational predicate that derives output values from input values. Formulas perform calculations within queries, such as string manipulation, arithmetic, or data transformation.

### Negation

Query constraint that matches when a pattern is NOT present. Negation enables queries like "find all people without email addresses" by matching the absence of facts.

### Query Planner

Component that reorders query conjuncts to minimize search space and detect cycles. The planner optimizes query execution by choosing the most selective patterns first and identifying infinite loops.

## Data Architecture

### Schema-on-Query

DialogDB's approach where schema is applied at query time rather than write time. Unlike traditional databases that enforce schema constraints during data insertion, DialogDB allows any valid fact to be stored and applies interpretation during queries. This enables schema evolution without migrations and allows different applications to interpret the same data differently.

### Local-First

Core principle where all queries run against local database instances with background synchronization. This architecture ensures applications remain responsive and functional even without network connectivity, with changes synchronized opportunistically when connections are available.

### Causal Temporal Model

DialogDB's approach to time where facts exist in causal timelines rather than a universal timeline. This model, inspired by physics' B-theory of time, allows distributed nodes to operate independently and merge their timelines later, avoiding the need for global clock synchronization.

## Indexing & Storage

### EAV Index (Entity-Attribute-Value)

Primary index optimized for retrieving all attributes of a given entity. This index efficiently answers questions like "What do we know about entity X?" by organizing facts with entity as the primary sort key.

### AEV Index (Attribute-Entity-Value)

One of three core indexes optimized for retrieving all entities with a specific attribute. This index efficiently answers questions like "Which entities have a 'name' attribute?" by organizing facts with attribute as the primary sort key.

### VAE Index (Value-Attribute-Entity)

Index optimized for finding entities with specific attribute values. This index efficiently answers questions like "Which entities have the name 'Alice'?" by organizing facts with value as the primary sort key, enabling reverse lookups.

### Index

Generic term for Probabilistic Search Tree structure maintaining sorted access to facts. Dialog maintains three indexes (EAV, AEV, VAE) simultaneously, ensuring all common query patterns have optimal access paths without requiring query planning or index selection.

### Probabilistic Search Tree

Deterministic, content-addressed tree structure ensuring same data produces same tree regardless of insertion order. They use content-based splitting decisions rather than child count, making them more optimal for replication.

### Index Node

Internal node in the Probabilistic Search Tree that contains sorted keys and references to child nodes. Index nodes don't contain facts directly but instead guide traversal through the tree structure.

### Segment Node

Node in the Probabilistic Search Tree that contains inlined leaf entries for optimization. Rather than having separate leaf nodes, segment nodes directly contain arrays of key-value pairs where keys are EAV, AEV, or VAE tuples and values are the corresponding facts. This inlining optimization reduces the number of network requests needed during tree traversal by bundling multiple logical leaf entries into a single physical node.

### Segment

Base storage unit - content-addressed, immutable, serialized, and compressed data chunk. Segments represent the serialized form of segment nodes and are what actually gets stored in and retrieved from the blob store. Each segment is identified by its content hash, enabling deduplication and efficient caching.

### Content-Addressed Storage

Storage system where data is addressed by its cryptographic hash rather than location. This approach ensures data integrity (tampering is detectable), enables deduplication, and allows efficient caching since content never changes for a given address.

### Blob Store

Hash-addressed storage system for immutable, content-addressed blobs. DialogDB is agnostic to the specific blob store implementation - any system supporting get/put operations by hash (S3, R2, IPFS, filesystem, etc.) can serve as a blob store. The blob store has no knowledge of DialogDB's structure; it simply stores and retrieves opaque binary data.

## Distributed Systems & Synchronization

### CRDT (Conflict-free Replicated Data Type)

DialogDB implements Merkle-CRDT properties for convergent replication. CRDTs ensure that distributed replicas can be updated independently and will converge to the same state when they exchange updates, without requiring coordination or consensus protocols.

### Merkle-CRDT

Conflict-free replicated data type using merkle trees, forming the basis of DialogDB's synchronization. The merkle tree structure allows efficient detection of differences between replicas and transmission of only the changed portions, similar to how Git synchronizes repositories.

### Mutable Pointer

Cryptographically signed reference to the current root hash, identified by DID:Key. The mutable pointer serves as the "HEAD" of the database, allowing the immutable content-addressed structure to have a stable, updatable reference point. Updates must be signed with the corresponding private key.

### DID (Decentralized Identifier)

Identifier format used for databases, formatted as `did:method:identifier`. DialogDB currently supports `did:key` method where the identifier is derived from a public key. DIDs provide a decentralized way to identify and authenticate database instances without central authorities.

### Compare-and-Swap (CAS)

Optimistic concurrency control mechanism used for updating the mutable pointer. CAS operations include the expected current value and only succeed if that expectation matches reality, preventing lost updates in concurrent scenarios. Failed CAS operations indicate concurrent changes that need to be merged.

### Eventual Consistency

Property where all replicas converge to the same state when they have the same facts. DialogDB's CRDT-based design ensures that regardless of the order in which updates are applied, all replicas will eventually reach identical states once they've exchanged all updates.

### Pull

Operation to retrieve the differential of facts from a specific revision to the current state. Pull operations efficiently synchronize databases by fetching only the facts that have changed since a known revision, similar to Git's pull operation.

### Partial Replication

Ability to replicate only needed subtrees rather than entire database. This feature enables privacy-preserving synchronization where nodes only fetch the portions of the database they have access to, and allows efficient operation on devices with limited storage.

## Time & Causality


## Advanced Concepts

### Incremental View Maintenance

DBSP-based approach to efficiently update query results when facts change. Instead of re-running complex queries after each transaction, incremental view maintenance computes only the delta (change) to the result set, dramatically improving performance for standing queries and subscriptions.

### Top-Down Evaluation

Current query evaluation strategy that selectively loads data. This approach starts from query goals and works backwards to find supporting facts, loading only the portions of the database needed to answer the query, rather than scanning entire indexes.

## Implementation Details

### Artifact

The Rust implementation's term for a fact - a semantic triple that may be stored in or retrieved from the database. This terminology distinction helps differentiate between the abstract concept of facts and their concrete representation in code.

### Scalar

The value component of a fact - can be null, boolean, number, string, bytes, attribute, or entity. Scalars represent the concrete data types that can be stored as values in facts, providing a rich type system while maintaining simplicity.

### Branch Factor

Configuration parameter for the Probabilistic Search Tree structure. This constant determines how many children each internal node can have, affecting the tree's depth and performance characteristics. Typical values range from 16 to 32.

### Genesis

The empty database revision, represented as an IPLD Link for empty byte array. This serves as the starting point for all databases, providing a well-known initial state from which all other states can be derived.

[RDF]:https://en.wikipedia.org/wiki/Resource_Description_Framework
[datom]:https://docs.datomic.com/glossary.html#datom


[entity]:#entity_(of)
[attribute]:#attribute_(of)
[value]:#value_(is)
[namespace]:#namespace
[cause]:#Causal_Reference_(cause)
[relation]:#Relation
[assertion]:#Assertion
[retraction]:#Retraction
[revision]:#Revision
[transaction]:#Transaction
[commit]:#Commit
[concept]:#Concept

[did:key]:https://w3c-ccg.github.io/did-key-spec/
