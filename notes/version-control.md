# Version Control

## Context

The [divergence clock] design encodes causal position as `{ since, at, drift }` where `since` increments at synchronization points, enabling cheap concurrency detection: two changes with the same `since` and different `at` values are concurrent by inspection, no traversal required.

This works well for a single collaborative repository. The limitation surfaces when two repositories with independent histories need to interact. The `since` counter is local to a repo's synchronization history. There is no meaningful way to compare `since: 3` from repo A with `since: 3` from repo B. They count different sync events. A cross-repo merge would require either renumbering one history (losing provenance) or accepting that the clock values are simply incommensurable.

A desired property of Dialog's collaboration model is that forks and follows work as first-class operations across independent repositories. This document describes a causal encoding grounded in the revision DAG that aims to preserve the divergence clock's fast concurrency detection while composing correctly across repo boundaries.

## Idea

Instead of deriving causal position from a logical counter, derive it from the structure of the revision DAG directly. Every revision has a natural position: the count of revisions in the causal chain leading to it. This is its `Edition`. An author increments their edition with each local revision and advances it to `max(seen) + 1` on sync. This is isomorphic to a Lamport timestamp, which gives it a useful property: a higher edition has seen at least as much causal history as any lower one, regardless of which repository it came from.

`Edition` alone is not enough to identify a revision globally, because two authors could independently reach the same edition count. It is therefore paired with `Origin`, a repository-scoped identity derived as `Blake3(issuer + subject)`. Deriving origin from both the signing key and the repository DID ensures that the same principal acting on two different repositories produces two distinct origins. Without this, a principal whose histories in two separate repositories later merge could produce colliding identifiers.

Together, `Origin` and `Edition` form a `Version`: a compact revision identifier that sorts naturally by causal depth and uniquely addresses any revision across all repositories.

## Core Types

```rust
/// Root of the search tree at a given revision
#[derive(Attribute, Debug, Clone)]
#[domain("dialog.revision")]
pub struct Tree(Blake3Hash);

/// Count of revisions in the causal chain leading to this one.
/// Increments locally on each revision; advances to max(seen) + 1 on sync.
/// Isomorphic to a Lamport timestamp.
#[derive(Attribute, Debug, Clone)]
#[domain("dialog.revision")]
pub struct Edition(u64);

/// Repository membership identifier derived as Blake3(issuer + subject).
/// Deriving from both signing key and repository DID ensures that the same
/// principal acting on two different repositories produces two distinct origins,
/// preventing collisions when independent repositories later merge.
#[derive(Attribute, Debug, Clone)]
#[domain("dialog.revision")]
pub struct Origin(Blake3Hash);

/// Ed25519 authority responsible for the revision
#[derive(Attribute, Debug, Clone)]
#[domain("dialog.revision")]
pub struct Authority([u8; 32]);

/// Ed25519 principal committing the revision
#[derive(Attribute, Debug, Clone)]
#[domain("dialog.revision")]
pub struct Issuer([u8; 32]);

/// Cryptographic signature for the revision
#[derive(Attribute, Debug, Clone)]
#[domain("dialog.revision")]
pub struct Signature(Vec<u8>);

/// Uniquely identifies a specific revision by a specific origin.
/// Sorts naturally by causal depth via edition.
/// Two versions with the same edition but different origins are concurrent.
pub struct Version {
    pub origin:  Origin,
    pub edition: Edition,
}

/// A set of versions identifying prior claims superseded by this one.
pub struct Cause(Vec<Version>);
```

## Revision

A revision is a Dialog concept stored as a claim in the EAV index under the repository DID as the entity, making revision history queryable via Datalog like any other data.

```rust
#[derive(Concept, Debug, Clone)]
pub struct Revision {
    pub this:      Entity,     // content-addressed hash of this Revision object
    pub tree:      Tree,
    pub edition:   Edition,
    pub subject:   Did,        // DID of the repository
    pub issuer:    Issuer,
    pub authority: Authority,
    pub signature: Signature,
    pub cause:     Cause,      // parent revision versions (one normally, multiple on merge)
}

impl Revision {
    /// Derives the origin from issuer and repository DID.
    /// Stored nowhere; always computed on demand.
    pub fn origin(&self) -> Origin {
        Origin(blake3::hash(&[self.issuer.0.as_ref(), self.subject.as_bytes()].concat()))
    }

    /// Returns the Version identifying this revision.
    pub fn version(&self) -> Version {
        Version {
            origin:  self.origin(),
            edition: self.edition.clone(),
        }
    }
}
```

A revision is content-addressed: `this` is the hash of the Revision object itself, serving as its stable entity identifier. When stored as claims in the EAV index, `of` is the repository DID and `is` is the revision's entity hash rather than the full Revision object:

```
Claim {
    the:   "dialog.db/revision",
    of:    repository_did,
    is:    version_hash,  // hash(edition + origin), the Version identifying this revision
    cause: Cause(vec![...]),      // parent revision version(s)
}
```

The repository DID as `of` means querying the current revision is a simple lookup, and the full revision history is all claims on that entity ordered by edition. The `Version` hash serves as a stable, content-addressed identifier for the revision. Multiple repositories each maintain their own independent lineage under their respective DIDs.

**Edition rule:** on each local revision, `edition = last_edition + 1`. On sync, `edition = max(local_edition, received_edition) + 1`. A higher edition has seen more causal history regardless of which repository it came from.

**Offline construction:** a new revision requires only the previous revision. No fetches needed. The new version is derived from the previous revision's edition and origin, and `cause` points to the previous revision's version.

**Merge revisions:** when incorporating changes from another revision lineage, `cause` lists the versions of all parent revisions. This is the only case where `cause` contains more than one entry.

## Claim Structure

Claims carry a `cause` field identifying the prior claims on the same `(entity, attribute)` that this claim supersedes. This is analogous to how a git commit records which commits it builds on, but scoped to individual fact lineages rather than the full repository state.

```rust
pub struct Claim {
    pub the:   The,
    pub of:    Entity,
    pub is:    Value,
    pub cause: Cause,
}
```

`cause` is empty on first write to an attribute. It contains one entry in the normal sequential case. It contains multiple entries when explicitly resolving concurrent claims from different authors, recording that the author saw and deliberately superseded all of them.

This enables conflict detection scoped to individual attribute lineages rather than requiring full revision DAG traversal.

## History Index

Revisions and claims share a unified history index:

```
/edition/origin/entity/attribute/value_hash -> Claim
```

This key structure serves two purposes.

**Revision DAG traversal:** revision claims are stored under `entity = repository_did`. Scanning by edition gives revision history in causal order. Finding a common ancestor between two revision lineages is done by following `cause` pointers backward from each head.

**Claim conflict resolution:** when two conflicting claims on the same `(entity, attribute)` are encountered, the index allows efficient traversal of the attribute's causal lineage. Given a conflicting claim's `Version`, the key `/edition/origin/entity/attribute` locates it directly and its `cause` chain can be followed backward.

## Conflict Detection

When two claims A and B conflict on the same `(entity, attribute)`, resolution proceeds in tiers.

**Tier 1: Direct cause check, O(1):**
- If B's version is in A's `cause`, A supersedes B
- If A's version is in B's `cause`, B supersedes A
- If neither, proceed to tier 2

**Tier 2: Cause chain traversal, O(k):**

Follow the higher-edition claim's `cause` chain backward through the history index, looking for the lower-edition claim's version. Edition comparison bounds and guides the traversal:

- Found the other claim's version in the chain: superseded
- Reached a claim whose edition is less than the other claim's edition: concurrent, stop
- Chain exhausted: concurrent

The traversal is bounded by k, the number of writes to that specific `(entity, attribute)`, not the total revision history. In practice k is small since most attributes are written infrequently.

**Incomplete replication:**

If the cause chain is incomplete due to missing claims, causal ordering cannot be determined locally. Resolution blocks until the missing claims have been replicated. This is expected behavior: a partial replica does not have enough information to resolve conflicts it has not fully received yet.

### Conflict Detection Illustrated

Two authors work independently from the same revision then sync. Alice makes two revisions, Bob makes one, then Bob pulls Alice's work before committing again:

```mermaid
%%{init: { 'gitGraph': {'showBranches': true, 'showCommitLabel':true,'mainBranchName': 'shared', 'parallelCommits': true}} }%%
gitGraph TB:
   commit id: "genesis" tag: "edition:0"
   branch alice
   branch bob
   checkout alice
   commit id: "A:1"
   commit id: "A:2"
   checkout bob
   commit id: "B:1"
   checkout shared
   merge alice tag: "edition:2"
   checkout bob
   merge shared
   commit id: "B:3"
   checkout shared
   merge bob tag: "edition:3"
```

| Author | Action | Edition |
|--------|--------|---------|
| Alice | commit | 1 |
| Alice | commit | 2 |
| Bob | commit | 1 |
| Alice | push | 2 |
| Bob | pull | max(2, 1) + 1 = 3, counter advances |
| Bob | commit | 3 |
| Bob | push | 3 |

When Alice's `A:2` and Bob's `B:1` meet during sync, neither version appears in the other's `cause`. Following Tier 2, we traverse `A:2`'s cause chain backward. We check the higher edition because it may have seen the lower one, while the lower edition cannot have seen something with a higher edition. The chain contains only `A:1` at edition 1, which matches `B:1`'s edition but has a different origin. Neither is an ancestor of the other: concurrent.

After Bob pulls and commits `B:3`, his `cause` contains `A:2`'s version. Any subsequent claim from Bob on an attribute Alice also wrote supersedes hers via Tier 1 direct check, O(1).

```mermaid
stateDiagram-v2
    shared: edition 0 shared base
    alice: A edition 2 writes name
    bob: B edition 1 writes name
    concurrent: concurrent neither in others cause
    bobafter: B edition 3 after pulling A
    resolved: B edition 3 supersedes both

    shared --> alice
    shared --> bob
    alice --> concurrent
    bob --> concurrent
    alice --> bobafter
    bob --> bobafter
    bobafter --> resolved
```

## Cross-Repo Merges and Forks

Each repository maintains its own revision lineage identified by its DID. Because `Edition` is a Lamport timestamp, editions from different repositories are directly comparable: a higher edition has seen more causal history regardless of which repository produced it.

**Forking:** Alice creates her own repository DID and writes her first revision with `cause` pointing to Bob's current head version. Her edition takes `max(Bob's edition) + 1`. All subsequent edition comparisons are meaningful relative to Bob's history.

**Merging upstream:** Alice finds the common ancestor by traversing both revision lineages via `cause` pointers. Conflicting claims are resolved using the two-tier conflict detection above.

**Collaborator joining with no prior history:** Carol initializes a fresh repository (edition 0, no claims), accepts Bob's invite, and pulls his history. Her counter advances to `max(Bob's edition) + 1` on pull. She then makes her first commit at that edition.

```mermaid
%%{init: { 'gitGraph': {'showBranches': true, 'showCommitLabel':true,'mainBranchName': 'bob', 'parallelCommits': true}} }%%
gitGraph TB:
   commit id: "B:1"
   commit id: "B:2"
   branch carol
   checkout carol
   commit id: "C:3"
```

Carol has no prior claims. She pulls Bob's history, her counter advances to 3 (max(2, 0) + 1), and her first commit lands at edition 3. No reconciliation needed.

**Collaborator joining with prior history:** Carol has been working independently and has reached edition 2 with her own claims. She accepts Bob's invite and pulls his history. Her editions were incommensurable with Bob's since both reached edition 2 via independent histories. On pulling, her counter advances to `max(Bob's edition, Carol's edition) + 1`. Her prior claims are preserved in the revision DAG and her cause chain re-anchors from the merge point.

```mermaid
%%{init: { 'gitGraph': {'showBranches': true, 'showCommitLabel':true,'mainBranchName': 'bob', 'parallelCommits': true}} }%%
gitGraph TB:
   branch carol
   checkout carol
   commit id: "C:1"
   commit id: "C:2"
   checkout bob
   commit id: "B:1"
   commit id: "B:2"
   checkout carol
   merge bob
   commit id: "C:3"
```

Carol's pre-join claims at C:1 and C:2 are preserved with their original editions. Her first new revision after joining is edition 3 (max(2, 2) + 1).

### Fork and Merge Illustrated

Alice forks Bob's repository at edition 2. Both continue writing independently, then Alice merges Bob's updates:

```mermaid
%%{init: { 'gitGraph': {'showBranches': true, 'showCommitLabel':true,'mainBranchName': 'bob', 'parallelCommits': true}} }%%
gitGraph TB:
   commit id: "genesis" tag: "edition:0"
   commit id: "B:1"
   commit id: "B:2"
   branch alice
   checkout alice
   commit id: "A:3"
   commit id: "A:4"
   checkout bob
   commit id: "B:3"
   commit id: "B:4"
   checkout alice
   merge bob
   commit id: "A:5"
```

Alice forks at Bob's `edition: 2`. Her first revision is `edition: 3` (max(2) + 1). When she merges Bob's `B:3` and `B:4`, the common ancestor at `edition: 2` is found by traversing the revision DAG via `cause` pointers. Conflicting claims are resolved using the two-tier conflict detection. After the merge, Alice's `A:5` has `edition: 5` and her `cause` references both lineages.

## Concurrent Claim Resolution

When two claims on the same `(entity, attribute)` are genuinely concurrent (neither appears in the other's cause chain), both are valid. A last-write-wins query resolves this deterministically by sorting on claim hash, producing a stable winner without requiring user intervention. Applications that need to surface the conflict explicitly can do so by requesting all concurrent values.

## Cross-Repository Collaboration

The [divergence clock] `since` counter is local to a repository's synchronization history. Two independent repositories that later merge have incommensurable counters: `since: 3` from repo A and `since: 3` from repo B count different sync events. Reconciling them requires either renumbering one history, losing provenance, or treating the merge as a special case outside the normal conflict detection machinery.

This design addresses that limitation directly. Because `Edition` is a Lamport timestamp and `Origin` is derived from `Blake3(issuer + subject)`, both are meaningful across repository boundaries without coordination. Two revisions from independent repositories can be compared, their common ancestor can be found by following `cause` pointers, and conflicting claims can be resolved using the same two-tier detection that works within a single repository. Forks, merges, and collaborators joining with prior history all follow naturally from the same primitives.

[divergence clock]:./divergence-clock.md
