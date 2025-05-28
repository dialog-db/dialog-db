# DBSP Exploration

## Context

We have a working Datalog query engine with a data store of facts represented as semantic quads. Our current system uses a **top-down evaluation strategy** with the following key components:

1. **Query Planner**: Capable of re-ordering conjuncts to minimize search space
2. **Cycle Analyzer**: Detects cycles in queries during planning phase and rejects non-evaluable queries  
3. **Selective Data Loading**: Only loads relevant subset of the data store during queries (can be a tiny slice of huge fact graphs)

**Storage Architecture**: Similar to Datomic but using **probabilistic b-trees (prolly trees)** instead of traditional b-trees. The store is implemented as:

- **Hash-addressed blobs** stored in commodity blob storage (e.g., S3)
- **Partial replication** at query time with hierarchical caching:
  - Local LRU cache (fastest)
  - Persisted partial replica cache (fallback)
  - Remote blob store (final fallback)
- **Mutable pointer** to latest store tree root (our revision concept)
- **EAV, AEV, VAE indexes** with embedded values for efficient access patterns

**Key Insight**: Incremental view maintenance can leverage prolly tree properties - when the root pointer changes, we can selectively replicate only the changed subtrees that are relevant to our queries, achieving partial replication of just the data needed for incremental updates.

The data store has the following interaction interface

```ts
/**
 * The fact embodies a datum - a semantic triple - that may be stored in or
 * retrieved from data source. It may have forth optional `cause` component
 * that forms a causal reference to establish a partial order.
 */
interface Fact {
  the: Attribute
  of: Entity
  is?: Scalar
  cause?: Reference<Fact>
}

/**
 * The predicate of a semantic triple; it must be namespaced.
 */
type Attribute = `${string}/${string}`;

/**
 * Arbitrary URI.
 */
type URI = `${string}:${string}`

/**
 * The subject of a semantic triple; it can be an arbitrary URI.
 */
type Entity = URI;


/**
 * The object of a semantic triple. It is a semantic triple of sorts.
 */
type Scalar = null | boolean | number | bigint | string | Uint8Array | Attribute | Entity;

/**
 * A causal reference is a hash reference; Usually it is a hash to previous fact. It is represented usally as a fixed size (usually 32) byte array.
 */
interface Cause<T> extends Uint8Array {
  // It is just a byte array but we capture a type.
  valueOf(): Cause<T>
}


/**
 * A basic filter that can be used to query the datastore for facts.
 */
export interface Selector {
  the?: Attribute
  of?: Entity
  is?: Scalar
}

/**
 * Reveision is hash of the data source at specific point in time.
 */
type Revision = Uint8Array;

/**
 * A triple store that can be used to store and retrieve set of semantic triples
 * that match the given selector.
 */
interface Source {
  /**
   * Retrieves a differential of the facts in the datastore from the provided revision.
   */
  pull(selector: Selector, revision?: Revision): Promise<Differential>
}


type Change =
  | { assert: Fact, retract?: void }
  | { retract: Fact, assert?: void }


/**
 * Differential of facts matching a fact selector from some revision of the
 * datastore.
 * set of facts hence outer async iterable
 */
interface Differential {
  /**
   * Last revision of the data source.
   */
  revision: Revision

  /**
   * Differential is a effectively a zset of facts.
   */
  weights: Map<Fact, number>
}
```

## Goal

We want to adopt DBSP for **incremental view maintenance** while preserving the benefits of our existing top-down evaluation strategy. Our objectives are:

1. **Primary Goal**: Use DBSP for incremental view maintenance without having to pass all new facts through the system - instead, exploit triple store properties to fetch only relevant facts for updates

2. **Secondary Goal**: If successful with incremental maintenance, apply the same strategy to initial query evaluation to unify our approach and avoid maintaining two separate evaluation strategies (top-down + DBSP)

3. **Preserve Existing Benefits**: Maintain our current advantages of selective data loading and query optimization while gaining DBSP's incremental processing capabilities

## Hypothesis

**Primary Hypothesis**: We can exploit properties of our prolly tree-based triple store and existing query planning capabilities to enable DBSP-based incremental view maintenance that only pulls facts relevant to specific queries, rather than processing all fact changes.

**Secondary Hypothesis**: If we can successfully implement selective fact pulling for incremental maintenance, the same mechanism should work for initial query evaluation, allowing us to replace our top-down evaluation with a unified DBSP approach that maintains the same efficiency characteristics.

**Key Insight**: Our existing query planner's ability to reorder conjuncts and detect cycles, combined with the prolly tree's EAV/AEV/VAE indexes and partial replication capabilities, should allow us to:

1. **Detect relevant changes** by comparing root pointers (revisions) 
2. **Selectively replicate** only changed subtrees that contain facts relevant to our queries
3. **Leverage partial replication** to maintain minimal data transfer and storage overhead
4. **Exploit index structure** (EAV, AEV, VAE) to efficiently identify which subtrees need replication
5. **Benefit from caching hierarchy** to minimize remote blob store access during incremental updates

This approach transforms incremental view maintenance from "pull all changes and filter" to "replicate only relevant subtrees based on query analysis."

## Query Syntax

Our datalog queries are represent as `Query` per definition below

```ts
type Term<T extends Scalar = Scalar> = T | Variable<T>

interface Variable<T extends Scalar = Scalar> {
  "?": string
  // Capturing a type of the variable
  valueOf(): Variable<T>
}

type Select = {
  match: {
    the: Term<Attribute>
    of: Term<Entity>
    is: Term<Scalar>
  }
  fact: {}
  rule?: void
  not?: void
}

export type Conjunct = Constraint | Negation
export type Constraint = Select | Predicate

export type Disjuncts = Record<string, Conjuncts>
export type Conjuncts = Conjunct[]
export type Negation {
  not: Constraint
  match?: void
  rule?: void
  fact?: void
}

interface DeductiveRule<Conclusion extends Record<string, Scalar> = Record<string, Scalar>> {
  claim: {[Key in keyof Conclusion]: Term<Conclusion[Key]>}
  when: Disjuncts
}

type RuleApplication<Conclusion extends Record<string, Scalar>> = {
  match: {[Key in keyof Conclusion]?: Term<Conclusion[Key]>}
  rule: DeductiveRule<Conclusion>
  formula?: void
  fact?: void
  not?: void
}

interface Formula<Input extends Record<string, Scalar>, Output extends Record<string, Scalar>> {
  derive(input: Input) => Output
}

type FormulaApplication<Input extends Record<string, Scalar>, Output extends Record<string, Scalar>> = {
  match: {[Key in keyof Input]: Term<Conclusion[Key]>} & {[Key in keyof Output]?: Term<Conclusion[Key]>}
  formula: Formula<Input, Output>
  rule?: void
  fact?: void
  not?: void
}

type Predicate<Conclusion extends Record<string, Scalar> = Record<string, Scalar>> =
  | FormulaApplication<Partial<Conclusion>, Partial<Conclusion>>
  | RuleApplication<Conclusion>

type Query<Conclusion extends Record<string, Scalar> = Record<string, Scalar>> =
  | RuleApplication<Conclusion>
```


## Exploration

Given our existing query planning and cycle detection capabilities, let's explore how the above design could work for both incremental view maintenance and potentially unified query evaluation. We'll use this example query:

```ts
{
  match: {
    person: { "?": "person" },
    name: {'?': "name" },
    address: {'?': 'address'}
  }
  when: {
    where: [
      {
        match: { the: "person/name", of: { "?": "person" }, is: {'?': "name" } },
        fact: {}
      }
      {
        match: { the: "person/address", of: { "?": "person" }, is: {'?': "address" } },
        fact: {}
      }
    ]
  }
}
```

The exploration should validate whether we can:

1. **For Incremental Maintenance**: When the store root pointer changes (indicating new facts), use our query planner's analysis to:
   - Determine which prolly tree subtrees could contain facts affecting existing materialized views
   - Leverage EAV/AEV/VAE index structure to identify relevant subtree ranges
   - Replicate only those subtrees through our partial replication system
   - Process the replicated fact changes through DBSP operators
   - Benefit from caching hierarchy to minimize redundant blob store access

2. **For Initial Evaluation**: Apply the same selective replication mechanism during initial query evaluation:
   - Use conjunct analysis to determine required index ranges (EAV/AEV/VAE patterns)
   - Replicate only subtrees containing facts matching our query patterns
   - Leverage existing conjunct reordering and cycle detection for optimal access patterns
   - Achieve same efficiency as current top-down approach through selective subtree replication

Specifically, we want to compute matches like `{ person: Entity, name: string, address: string }` and handle differentials in both scenarios by:

- Using our **existing query planner** to determine optimal conjunct ordering and identify required index access patterns
- Leveraging **cycle detection** to ensure queries are evaluable 
- Exploiting **prolly tree structure** and **EAV/AEV/VAE indexes** to determine which subtrees need replication
- Using **query structure knowledge** (attributes, entities) to identify specific subtree ranges in the indexes
- After evaluating conjuncts, using **variable bindings** to further constrain subsequent subtree replication
- Leveraging **partial replication and caching hierarchy** to minimize data transfer and storage overhead
- Achieving the **same selective loading** benefits through selective subtree replication instead of selective fact loading

The key question is whether this approach can provide a **unified evaluation strategy** that works for both initial queries and incremental maintenance while:
- Preserving current performance characteristics through selective subtree replication
- Leveraging prolly tree properties for efficient change detection and partial replication
- Minimizing blob store access through intelligent caching and replication strategies
