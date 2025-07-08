# Glossary

## Core Concepts

### Fact

Atomic, immutable unit of knowledge. It is equivalent to semantic triples in [RDF] and [datom]s in Datomic.

In dialog facts are present in `{the, of, is, cause}` form that is meant to correspond to how the fact is expressed in natural language: _the_ **color** _of_ **sky** _is_ **blue**.

> The `cause` field establishes a causal relationship see [cause] for more details.

#### Entity (of)

Entity is the subject denoted via `of` field. Entities are represented as an arbitrary URI (e.g., `uuid:...`, `did:...`).

#### Attribute (the)

Something that can be asserted about an [entity]. Attribute has a name denoted via `the` field. Attribute names `/` delimited UTF-8 strings e.g. `person/name`, `db.type/uint32`. First component of the attribute name is a [namespace](#namespace).

##### Namespace

First component of the attribute is a namespace. Namespaces serve a similar function to table names in a relational store, without imposing any obligations or limitations, e.g. an entity can have attributes from more than one namespace.

Namespaces help organize attributes into categories, more importantly, they provide data locality inside the database meaning that attributes sharing namespace will end up collocated making querying them more efficient than if they were scattered across the database.

Namespaces are meant to make attributes globally unique and it is generally RECOMMENDED to use [reverse domain name](https://en.wikipedia.org/wiki/Reverse_domain_name_notation) notation (e.g., `io.gozala.note`).

#### Value (is)

Something that does not change e.g. `42`, `"John"`, `true` . Value is denoted via `is` field. A [fact] relates an [entity] to a [value] through an [attribute].

Values can be in handful of data types (bytes, entity, boolean, string, integers, floats, records, symbols).

#### Causal Reference (cause)

Causal references ground facts in time and establish partial order between them. At the moment they are represented as hash reference to preceding [fact], but alternative approaches are being actively explored.

### Relation

DialogDB's equivalent of a table in relational databases or a document schema in document databases. Relation describes set of attributes that entities can have, establishing relationships across facts. Any entity can have any attribute, relation simply define groups that have semantic meaning. Unlike rigid schemas, relations in DialogDB are composable and applied at query time.

```typescript
const Employee = relation({
  role: String,
  salary: Number,
  department: Object
})
```

Relations provide type safety and structure while maintaining the flexibility of the underlying fact store. Multiple relations can describe the same entity, enabling different views of the data without migration.

### Evidence

DialogDB's equivalent of a table row in relational databases or a document in document databases. Evidence represents a set of facts about an entity that prove a particular relation. When querying the database, you're searching for evidence that supports claimed relations.

```typescript
// Query for evidence of employees with specific role
Employee({ role: 'Project Manager' }).query({ from: db })

// Create evidence by asserting a relation
Employee.assert({
  name: 'Bitdiddle Ben',
  role: 'Computer wizard',
  salary: 60_000
})
```

When evidence is added to the database through assertions, the corresponding facts are derived and stored.

### Rule

DialogDB's equivalent of views in relational databases. Rules define derived relations by specifying predicates that must be true for the relation to hold. They enable logical inference by creating new relations from existing facts and relations.

```typescript
const Manager = relation({
  subordinate: Object
}).where(manager => [
  Employee.match({ this: manager.subordinate }),
  Manager.relation({ this: manager.this, is: manager.subordinate }),
])
```

Rules can be recursive, enabling complex queries like transitive relationships. Unlike materialized views, rules are evaluated at query time, though future versions may support incremental view maintenance for performance.

### Fact Store

Storage system for facts (semantic triples with causal references). DialogDB implements a fact store that efficiently indexes facts in multiple ways to support diverse query patterns.

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

### Predicate

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

Generic term for Probabilistic B-Tree structures maintaining sorted access to facts. DialogDB maintains three indexes (EAV, AEV, VAE) simultaneously, ensuring all common query patterns have optimal access paths without requiring query planning or index selection.

### Probabilistic B-Tree (Prolly Tree)

Deterministic, content-addressed tree structure ensuring same data produces same tree regardless of insertion order. Prolly trees use content-based splitting decisions rather than child count, making them more optimal for replication.

### Index Node

Internal node in the Probabilistic B-Tree that contains sorted keys and references to child nodes. Index nodes don't contain facts directly but instead guide traversal through the tree structure.

### Segment Node

Node in the Probabilistic B-Tree that contains inlined leaf entries for optimization. Rather than having separate leaf nodes, segment nodes directly contain arrays of key-value pairs where keys are EAV, AEV, or VAE tuples and values are the corresponding facts. This inlining optimization reduces the number of network requests needed during tree traversal by bundling multiple logical leaf entries into a single physical node.

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

Configuration parameter for the Probabilistic B-Tree structure. This constant determines how many children each internal node can have, affecting the tree's depth and performance characteristics. Typical values range from 16 to 32.

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
