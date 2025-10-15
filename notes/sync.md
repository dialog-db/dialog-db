# Sync Implementation Proposal

## Goals

- Fetch and integrate changes from a remote tree.
- Reconcile all changes into a deterministic, convergent structure.
- Push the resulting changes back to the remote.

---

## Architecture Overview

A **Mutable Pointer** represents the canonical shared root of a tree, while the **Archive** stores immutable, hash-addressed blobs representing tree nodes.

```mermaid
graph TD
    subgraph Remote
        A[Mutable Pointer]
        B[Archive Store S3/IPFS/R2]
    end

    subgraph Local
        C[Local Tree Replica]
    end

    C -- Query root --> A
    C -- Fetch blobs --> B
    C -- Upload new blobs --> B
    C -- Update root --> A
```

The synchronization process follows this sequence:

```mermaid
sequenceDiagram
    participant Archive
    participant Local
    participant Remote Pointer
    Local->>Remote Pointer: HEAD /did (query root)
    Remote Pointer-->>Local: 200 ETag=abc123
    Local->>Archive: GET blobs for root abc123 (Fetch)
    Archive-->>Local: 200
		
    Local->>Local: merge(local, remote)
    Local->>Archive: PUT new blobs (Put)
    Local->>Remote Pointer: PATCH /did (Push)
    Remote Pointer-->>Local: 200 OK or 412 Precondition Failed
```

---

## Mutable Pointer

A **Mutable Pointer** represents the shared state of the tree and serves as a single authoritative reference — conceptually similar to a Git remote reference. It stores only the root hash and enforces access and consistency constraints.

### Query Mutable Pointer

#### Authorization

A `HEAD` request is used to query the latest known root of the tree. 

Requests **MUST** include a valid `Authorization` header:

```javascript
Authorization: Basic ${principal.sign(blake3(payload))}
```

The payload has the following structure:

```javascript
{
  "iss": tree.did(),
  "aud": tree.did(),
  "sub": tree.did(),
  "cmd": "/state/query",
  "args": {}
}
```

The signer **MUST** match `payload.iss`, `payload.sub`, and the `did:key` being queried. 
If authorization is invalid, the server **MUST** return `401 Unauthorized`.

> ℹ️ In future versions, [UCAN][]s or other decentralized auth mechanisms may replace this simple signature-based scheme.

#### Response

If authorized, the response **MUST** include an `ETag` header with the current tree root:

```http
HEAD /did:key:z6Mkk89bC3JrVqKie71YEcc5M1SMVxuCgNx6zLZ8SYJsxALi HTTP/1.1
Authorization: Basic f2178157fe0a1e0993b2e42ed315e8a955013783121e6b4ef24e6b9f9a8781d9
```

```http
HTTP/1.1 200 Ok
last-modified: Tue, 23 Sep 2025 05:41:40 GMT
ETag: af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262
```

> ℹ️ In the future we may add `GET` request support to read latest payload that was published. 

---

### Update Mutable Pointer

Updating a mutable pointer follows [Compare-and-Swap](https://en.wikipedia.org/wiki/Compare-and-swap) semantics:
the remote root is updated from `A → B`, where

- `A` is the tree root on which local changes are based, and
- `B` is the new local tree root.

#### Authorization

Requests **MUST** include a valid `Authorization` header:

```javascript
Authorization: Basic ${tree.principal.sign(blake3(payload)}
```

The `payload` has the following structure:

```js
{
  "iss": tree.did(),
  "aud": tree.did(),
  "sub": tree.did(),
  "cmd": "/state/assert",
  "args": {
    "revision": tree.revision()
  }
}
```

If authorization is invalid, return `401 Unauthorized`.

> Future implementations may use [UCAN][]s or similar capability tokens instead of direct signatures.

#### Invariant Check

The implementation **MUST** return `412 Precondition Failed` if the `If-Match` header does not match the current root of the pointer (i.e. another writer has already updated it). Response **MUST** have an `ETag` header set to current  of the pointer.

```http
HTTP/1.1 412 Precondition Failed
last-modified: Tue, 23 Sep 2025 05:41:40 GMT
ETag: af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262
Content-Type: application/json
{
  error: {
     expected: "b20ab0a020a48d349e0c64d109c441f87c9bc43d49fc701c4a5f6f1b16aa4e32"
     actual: "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262"
  }
}
```

#### Example

```http
PUT /did:key:z6Mkk89bC3JrVqKie71YEcc5M1SMVxuCgNx6zLZ8SYJsxALi HTTP/1.1
If-Match: af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262
Authorization: Basic f2178157fe0a1e0993b2e42ed315e8a955013783121e6b4ef24e6b9f9a8781d9
Content-Type: application/json
{
  "iss": "did:key:z6Mkk89bC3JrVqKie71YEcc5M1SMVxuCgNx6zLZ8SYJsxALi",
  "sub": "did:key:z6Mkk89bC3JrVqKie71YEcc5M1SMVxuCgNx6zLZ8SYJsxALi",
  "cmd": "/state/assert",
  "args": {
    "revision": "b20ab0a020a48d349e0c64d109c441f87c9bc43d49fc701c4a5f6f1b16aa4e32"
  }
}
```

```http
HTTP/1.1 200 Ok
last-modified: Tue, 23 Sep 2025 05:41:40 GMT
ETag: af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262
Content-Type: application/json
{
  "iss": "did:key:z6Mkk89bC3JrVqKie71YEcc5M1SMVxuCgNx6zLZ8SYJsxALi",
  "sub": "did:key:z6Mkk89bC3JrVqKie71YEcc5M1SMVxuCgNx6zLZ8SYJsxALi",
  "cmd": "/state/assert",
  "args": {
    "revision": "b20ab0a020a48d349e0c64d109c441f87c9bc43d49fc701c4a5f6f1b16aa4e32"
  }
}
```



---

## Archive

The **Archive** is a shared data store over a commodity backend such as S3, R2, or IPFS. 
It stores **hash-addressed blobs** representing encoded tree index and segment nodes.

Access control (read/write) is managed out of band and tied directly to the backend’s authentication.

> **Note:** The Mutable Pointer is fully decoupled from the archive — it SHOULD NOT have access to it, nor verify that uploaded roots are archived.

More advanced implementations may require proof-based updates (e.g., Merkle inclusion proofs or signed archive commitments) to enforce stronger consistency invariants.

---

## Sync

## Pull: Fetch + Merge

Pulling changes is composed of two sequential phases: **Fetch** and **Merge**.

### Fetch

The **Fetch** phase discovers the latest remote state and materializes a **partial** replica of it from the discovered root.

Steps:

1. Determine the last known remote root (`ETag`).
2. Query the mutable pointer for the current remote root.
3. Load tree from the root, which will get root node from archive unless it's available in local cache.

Fetch create replica of the tree that in the next phase can be merged into a local replica.

Failure handling:

| Code               | Meaning               | Client Action                  |
| ------------------ | --------------------- | ------------------------------ |
| `401 Unauthorized` | Signature invalid     | Re-sign and retry query        |
| `404 Not Found`    | Remote uninitialized  | Treat as empty tree            |
| `5xx`              | Network/backend error | Retry with exponential backoff |

### Reconciliation

In order to merge local and remote trees we need to identify what changes have occured where. For clarity let's consider scenario where local tree has some node (`P17@b`) that does not exist in the remote tree

```yaml
  local@x                     remote@z
  ├─ P11@a                       ├─ P11@a
  │  ├─ P08@f                    │  ├─ P08@f
  │  │  └─ ...                   │  │  └─ ...
  │  └─ P11@g                    │  └─ P11@g
  │     └─ ...                   │     └─ ...
► └─ P17@b                       └─ P88@w
     ├─ ...                         └─ ...    
     └─ P17@k
        └─ ...
```

From the the two trees alone it is not clear if node (`P17@b`) was inserted into a local tree or if it was removed frome the remote tree since last sync point. This information is critical for making a decision about whether `P17@b` should remain in the merged tree or not.

We can compute differential between current tree and one it **diverged from**, which is a tree we have last pushed (succesfully). This will give us local changes that we can then **integrate** into remote tree we have fetched.

> ✨ Integration will naturally only replicate only subtrees from the remote where we have overlapping changes. Subtrees that have not changed locally will not be accessed so they will not be fetched and subtrees that changed locally but not remotely will be in cache (unless evicted) because we would have fetched them to change.

Merge logic could be described as follows

```rs
#[derive(Debug, Clone)]
pub struct Replica {
  current: Tree
  checkpoint: Tree
  upstream: MutablePointer
}

impl Replica {
  /// Computed changes that were made since last push
  fn changes(&self) -> impl Differntial {
     Differntial::from((self.current, self.checkpoint))
  }
  
  /// Computes `remote` tree with local changes. Expects
  /// remote to be diverged from remote
  async fn merge(&mut self, remote: &Tree) -> Result<&mut Self> {
    // if remote has not changed from last chekpoint
    // replica represents current state
    if remote.root() != self.checkpoint.root() {
      // inegrate changes into remote tree to derive
      // merged tree & produce new replica with it
      // and old checkpoint
      self.current = integrate(remote, self.changes())?;
    }
    
    Ok(self)
  }
  
  async fn pull(&mut self) -> Result<&mut Self> {
    let root = self.upstream.query().await?;
    if root != self.checkpoint.root() {
       self.merge(Tree::load(root)).await;
    } 
    
    
    Ok(self)
  }
  
  async fn push(&self) -> Result<&mut Self> {
     loop {
        let checkpoint = self.current.clone()
        if let Ok(_) = upstream.assert(checkpoint.root()).await {
          self.checkpoint = checkpoint
        } else {
          self.pull().await?;
        }
     }
  }
}
```

### Differentiation

It is best to think of our search tree as a sorted set of entries. Differential between last **checkpoint** (tree that was succesfully pushed) and **current** working tree can be represented as a set of entries added or removed.


```rust
trait Differential: TryStream<Change> {
  fn from(source: (Tree, Tree)) -> Self
}

pub enum Change {
  Remove(Entry)
  Add(Entry),
}
```

<details>
<summary>ℹ️ It is worth observing that we end with something similar to ZSet here from DBSP</summary>
ZSet weight can be out of -1/+1 range but my understanding is that for collections it would stay in that range and our tree is effectively a collection and Remove/Add is -1/+1
</details>

> ✨ Computing differential should not require replicating subtrees that have not changed because those can be skipped over. It is also highly likely that subtrees that have changed will already be in cache because to change them we had to replicate first


### Integration

Integration entails applying differential to the target tree.

> ℹ️ It is assumed that target tree has diverged from the **checkpoint** tree otherwise integration process will simply apply changes to some other unrelated tree.

Integration process simple needs to iterate over changes and either:

1. add entry to the target tree
2. remove entry from the target tree

> ✨ Integration process naturally will only replicate subtrees from the target that have changed locally. Furthermore, if locally changed subtrees have not changed in the target tree we'll already have them in cache (unless evicted) because we would not have being able make changes without replicating them first. Even if they were evicted from cache we'd still need them locally in order to be able to push, that is to say we'll end up only replicating what's strictly necessary.

#### Adding an Entry

Target tree may have a conflicting entry in the tree which would imply concurrent change. In such case we simply compare hashes, if existing hash is greater replace it with entry being added otherwise keep existing entry.

If tree does not contain connflicting entry we simply insert an entry in the right location of the tree.

#### Removing an Entry

Target tree may not have have an entry under the same key and same hash, this would imply that entry being removed was either removed or replaced therefor we do nothing.

Target tree may not conttain an entry for the same key in which implies it was already removed. In such case we do nothing.

Target tree may contain an entry for the same key with a same hash, in that cas we remove it from the tree.




## Consistency Model

This protocol provides **eventual consistency** with **deterministic convergence**.  
Each peer maintains an immutable local tree (partial replica) and coordinates via a single mutable pointer that serves as the canonical root reference.  
Given identical merge strategies and histories, all replicas converge to the same state.
