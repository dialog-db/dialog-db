# Glossary

## Core Concepts

### Claim (fact)

Atomic, immutable unit of knowledge, equivalent to a semantic triple in [RDF] and a [datom] in Datomic. "Fact" and "claim" name the same thing; the Rust code calls one an `Artifact`.

A claim has the form `{the, of, is, cause}`, read the way it is said: _the_ **color** _of_ **sky** _is_ **blue**.

> The `cause` field establishes a causal relationship, see [cause] for more details.

#### Entity (of)

The subject a claim is about, denoted by `of`. An entity is an arbitrary URI (`uuid:...`, `did:...`, `concept:...`).

#### Relation (the)

The named set of claims a claim belongs to, denoted by `the`: `person/name` is the relation every claim of a person's name belongs to, and _the_ **person/name** _of_ **alice** _is_ **"Alice"** is one member. A relation is a name and nothing more: it says nothing about the type of its values or how many an entity has. Relation names are `domain/name`, at most 64 bytes, in lowercase kebab-case.

In Rust a relation is `dialog_artifacts::Relation`, and `dialog_query::Relation` is the same name validated (the `the!` macro builds one at compile time).

##### Domain

The part of a relation name before the `/`. Domains play the role table names play in a relational store, without imposing anything: an entity can be in relations of any number of domains.

Relations of one domain are stored next to each other, so querying them together is cheaper than querying relations scattered across the database. Domains are meant to make relations globally unique, and it is RECOMMENDED to spell them as [reverse domain names](https://en.wikipedia.org/wiki/Reverse_domain_name_notation) (`io.gozala.note`).

#### Value (is)

What a claim relates its entity to, denoted by `is`: `42`, `"John"`, `true`. Values do not change.

#### Type

What kind of value a value is. A type is named by an entity:

| Type       | Values                         |
| ---------- | ------------------------------ |
| `text:`    | UTF-8 strings                  |
| `integer:` | signed 128-bit integers        |
| `natural:` | unsigned 128-bit integers      |
| `float:`   | 64-bit floating point numbers  |
| `boolean:` | `true`, `false`                |
| `bytes:`   | byte buffers                   |
| `entity:`  | entity URIs                    |
| `symbol:`  | symbols                        |
| `record:`  | structured records             |

The names an earlier release used (`Text`, `SignedInteger`, `UnsignedInteger`, ...) are still read; they are never written.

#### Causal Reference (cause)

Causal references ground claims in time and establish partial order between them. At the moment they are a hash reference to the preceding [claim], but alternative approaches are being actively explored.

### Attribute

A relation qualified by a value type and a pick: how a relation is read. `person/name` read as `text:` under `last` is an attribute. Two attributes over one relation, read as different types or under different picks, are two attributes, each with its own identity.

```json
{ "the": "person/name", "as": "text:" }
{ "the": "person/email", "as": "text:", "pick": "all" }
{ "the": "job/status", "as": ["case:suspended", "case:active"] }
```

An attribute's identity is a hash of the relations it reads, its type, its pick and the values its pick ranks.

#### Pick

Which of the claims a relation holds for an entity an attribute reads, spelled `pick`:

- `last` (the default): the newest claim;
- `all`: every claim, as a set;
- `top`: the best ranked of listed values (`as: [..]`) or relations (`the: [..]`), and a list implies `top`;
- `max`, `min`: the greatest or least value of an ordered type.

A pick governs writes too: writing through an attribute under any pick but `all` succeeds the claim a read under that pick returns, and `all` adds a claim beside the others. In Rust a pick is `dialog_artifacts::Pick`.

### Concept

DialogDB's equivalent of a table in a relational database or a document schema in a document database: a set of fields an entity can be described by. Any entity can be in any relation; a concept names a group of attributes that has meaning together. Concepts are applied at query time, so several concepts can describe one entity without a migration.

#### Field

A slot of a concept, named locally and holding an attribute: the `name` field of a `person` concept holds the attribute `person/name` read as `text:`. A field is required unless it is marked optional.

### Conclusion

The claims that show an entity is an instance of a concept: one per required field, and those present of the optional ones. Querying a concept searches for conclusions; asserting a concept instance writes the claims its conclusion is made of.

### Rule

DialogDB's equivalent of a view in a relational database. A rule concludes a concept from premises: when its body holds, the conclusion holds too. A deductive rule's conclusions are derived when read and never stored; an inductive rule asserts (or retracts) its conclusion when its premises become true.

Rules can be recursive, enabling queries like transitive relationships.

### Fact Store

Storage for claims and their causal references. DialogDB indexes claims in several orders to support diverse query patterns.

## Database Operations

### Assertion

An atomic [claim] in the database, associating an [entity] with a [value] in a [relation], with a [cause]. Opposite of a [retraction].

Assertions are the primary way data enters the system - they create new facts without modifying existing ones, maintaining the immutable, append-only nature of the database.

### Retraction

An atomic [claim] in the database, dissociating an [entity] from a particular [value] in a [relation]. Opposite of an [assertion].

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

The value component of a claim: a value of any [type](#type). Scalars represent the concrete data types that can be stored as values in facts, providing a rich type system while maintaining simplicity.

### Branch Factor

Configuration parameter for the Probabilistic B-Tree structure. This constant determines how many children each internal node can have, affecting the tree's depth and performance characteristics. Typical values range from 16 to 32.

### Genesis

The empty database revision, represented as an IPLD Link for empty byte array. This serves as the starting point for all databases, providing a well-known initial state from which all other states can be derived.

[RDF]:https://en.wikipedia.org/wiki/Resource_Description_Framework
[datom]:https://docs.datomic.com/glossary.html#datom


[claim]:#claim-fact
[fact]:#claim-fact
[entity]:#entity-of
[relation]:#relation-the
[attribute]:#attribute
[value]:#value-is
[domain]:#domain
[cause]:#causal-reference-cause
[assertion]:#Assertion
[retraction]:#Retraction
[revision]:#Revision
[transaction]:#Transaction
[commit]:#Commit
