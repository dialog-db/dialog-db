# Sync Implementation Proposal

## Goals

- Pull changes from the remote
- Reconcile changes to a determinitstic tree
- Push change to the remote

## Mutable Pointer

Mutable pointer represents shared state of the tree and is centralized by nature. Conceptually it is similar to git remote, you can figure out what the current root of the tree is and you can update it to a new root.

### Query Mutable Pointer

#### Mutable Pointer Query Authorization

Dialog can query mutable pointer via HTTEP HEAD request to find out latest root of the search tree. Mutable pointer implementation MUST respond respond `401 Unauthorized` unless request has `Authorization: Basic ${principal.sign(blake3(payload)}` where payload matches following structure 

```json
{
  "iss": "did:key:z6Mkk89bC3JrVqKie71YEcc5M1SMVxuCgNx6zLZ8SYJsxALi",
  "sub": "did:key:z6Mkk89bC3JrVqKie71YEcc5M1SMVxuCgNx6zLZ8SYJsxALi"
  "cmd": "/state/query",
  "args": {} 
}
```

> Note: In the future we're likely use UCANs or something along those lines but this is a simple solution for now

Signer of the payload MUST be same as `payload.iss`, `payload.sub` and `did:key` of the mutable being updated.

#### Mutabel Pointer Query Result

If request has a valid authorization response it MUST contain an `ETag` header where value is set to the search tree root known to the pointer.


```http
HEAD /did:key:z6Mkk89bC3JrVqKie71YEcc5M1SMVxuCgNx6zLZ8SYJsxALi HTTP/1.1
Authorization: Basic f2178157fe0a1e0993b2e42ed315e8a955013783121e6b4ef24e6b9f9a8781d9
```

Server MUST repsond with `ETag` header

```curl
HTTP/2 200
last-modified: Tue, 23 Sep 2025 05:41:40 GMT
ETag: af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262
```

### Update Mutable Pointer

Update has a [Compare and Swap](https://en.wikipedia.org/wiki/Compare-and-swap) semantics and instructs remote to update root of the tree from from `A -> B` where:

1. `A` reperesents search tree root on top of which local changes have being made.
2. `B` represents my local search tree root.

#### Mutable Pointer Update Auhtorization

Implementation MUST return `401 Unauthorized` if request does not have a valid `Authorization` header. Valid authorization header MUST match following template `Authorization: Basic ${tree.principal.sign(blake3({ cmd: "/root/put", sub: tree.did() }))}` payload is signed by the `did:key` of the tree.

> Note: In the future we're likely use UCANs or something along those lines but this is a simple solution for now

#### Mutable Pointer Invariant Check

Implementation MUST return `412 Precondition Failed` if expected root specified via `If-Match` header does not match latest root of the mutipble pointer.


### Mutable Update Pointer Example

```http
PATCH /did:key:z6Mkk89bC3JrVqKie71YEcc5M1SMVxuCgNx6zLZ8SYJsxALi HTTP/1.1
If-Match: af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262
Authorization: Basic f2178157fe0a1e0993b2e42ed315e8a955013783121e6b4ef24e6b9f9a8781d9
Content-Type: application/json
{
  "iss": "did:key:z6Mkk89bC3JrVqKie71YEcc5M1SMVxuCgNx6zLZ8SYJsxALi",
  "cmd": "/state/assert",
  "sub": "did:key:z6Mkk89bC3JrVqKie71YEcc5M1SMVxuCgNx6zLZ8SYJsxALi"
  "args": {
     "revision": "b20ab0a020a48d349e0c64d109c441f87c9bc43d49fc701c4a5f6f1b16aa4e32"
  }
}
```

## Archive

Synchronization protocol assumes shared data archive over some commodity store like S3/R2/IPFS. Archive stores hash addressed blobs which represent encoded tree index and segment nodes.

Read and write authorization is managed out of bound and directly tied to the backend.

**Note:** Mutable Poiner is completely decoupled from the data archive. In fact it SHOULD not have access to the archive and consequently have no ability or responsibility to check if new root has being archived.

> More advanced mutable poiners could be introduced in the future that perform additional checks, e.g. they could require signed commitment proofs from the archive along with merkle proofs that demonstrate that new root and all of the tree is archived. It is reasonable to assume that different mutable pointers may impose different update policies to ensure desired application invariants.


## Pulling Changes

Pulling changes implies:

1. Reference to last known root of the remote. If root is unknown empty tree can be assumed.
2. Querying mutable pointer to find out latest root.

At the end of the pull we shoud end up with two partially replicated trees one correponding remote from which local diverged and second representing remote local wishes to converge with. If both remotes have a same root no changes have occurred and we're up to date.

If remotes are different we can start integrating remote changes into local tree. General idea here is that we can look at the branches that are local and compare which ones are different on the remote and perform following steps:

1. If local and remote are the same go to next sibling branch.
1. If local is different from the remote and we don't have local branch in cache that implies we have not changed to so we update branch with remote one.
1. If local is different from the remote we're convering with, but remote we've diverged from is the same as the one wer'e converging with than we only have local changes so we keep local branch and go to the next sibling branch.
1. If local is different from the remote we're converging with and it's also different from the one we've diverde from there had being conflicting changes we descend (implicitly fetching remote branch from archive) and repeat the process until we arrive to segment nodes.
  1. If local and remote segment nodes are different they get merged as a sorted set union. Please note that merged set may overflow the segment (when we encounter boundary) causing a split of segment that propagates upwards the tree which needs to be done before considering next sibling).
  2. If segments contain conflicting facts same `{the, of}` but different `is` or/and `cause` winner could be chosen via provided merge strategy.

### Merge Strategy

```rust
trait Merge {
  fn merge(into: Artifact, from: Artifact): Artifact
}
```

And provide muliptle implementations like

- Into wins
- From wins
- Lower hash wins


> Please note: We don't necessarily need to deal with 3 way merge and could simplify things by only comparing local and remote trees, the net result should be the same we may just end up wasting some work

> [Okra implementation goes into more sync details](https://docs.canvas.xyz/blog/2023-05-04-merklizing-the-key-value-store.html#merging-state-based-crdt-values) which could be worth a look.

## Pushing Changes

Pushing changes implies:

1. Uploading all the updated tree nodes into the archive.
2. Updating mutable pointer to the latest tree.

Given that updating pointer will not succeed if the root has changed it is RECOMMENDED to perform a pull first integrating upstream changes into the tree. After pull is complete push can compare local tree with pulled remote identifying nodes that are different and publishing them to an archive (alternatively this bookeeping could be handled completely separately by representing partial view of the archive).

Once all the nodes had being archived attempt to update mutable pointer SHOULD be made. If pointer responds with `412 Precondition Failed`, push can be retried by performing another pull.
