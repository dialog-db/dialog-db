# Peers and sessions

Status: design, agreed in discussion; implementation in progress. Supersedes
the profile / operator split described in `repository.md` and
`space-and-storage.md`, which this note treats as the "today" column.

## Why

Four objects each own a slice of "who am I, what do I hold, where is it,
and what may I do", and every product layer adds one more:

- `Profile` is a persisted signer, opened at a `Location` through a
  bootstrap effect (`storage::Load`) that nothing else uses.
- `Operator` bundles the acting signer, the in-memory session grants, the
  storage pool, the base directory, the network dispatch table, the
  access-branch handle, the chain cache, the walk's remote reach, the
  hydration scheduler and the preload queue. Only the first two are an
  authorization session; the rest is runtime.
- Remotes are per-repository configuration (`remote/{name}/address` cells)
  naming a transport address plus a subject, and a branch tracks its
  upstream by remote name.
- Three name tables answer "which spaces do I hold and where": the
  in-memory `Loader` mount table, the on-disk directory layout, and tonk's
  JSON registry (plus its meta-branch mirror of the remotes).

Symptoms in the field: tonk wraps the operator (`AccountBoundOperator`) to
splice a second authorization session in by decoration; the worker must
rebuild its operator over the same storage pool on every key rotation or
cached handles drift; the CLI retains a fresh expiring session grant into
the synced access branch on every invocation; and the operator's build-time
branch handle races every other handle on the same head.

## Vocabulary

- A **peer** is a site identified by a public key that holds replicas of
  repositories. A replica is per peer: `Replica.this = hash(peer, subject)`,
  and a device may host several peers (the browser registry does).
- A **session** is what acts: a signer, in-memory grants, and the layers it
  proves from. A session contributes only the **origin**
  (`hash(branch, session key)`) to what it commits; the replica is the
  peer's. A session is a constrained peer.
- A **remote peer** is a peer whose key you do not hold. You connect to it;
  you cannot open it. `did:web:network.tonk` is one; an iroh node is one
  (its node id is already a `did:key`).
- The **registry** is a role, not a type: the branch of the peer's own
  repository where its `Peer`, `PeerAddress` and `Replica` facts live. It
  doubles as the default proof source.
- A **layer** is anything with a head and a readable store (a branch, a
  snapshot, an ephemeral store); a **stack** composes layers with
  placement deciding which layer a fact lands in. See `state-layers.md`
  (#519). The registry is a stack whose device-audience layer holds what
  must not replicate (addresses, mounts, cache snapshots).

Everything found by convention reduces to one rule: the peer's credential
and its own repository share one space at one `Location`. Everything else
is located by facts in that space.

## Surface

```rust
let alice = Peer::new()
    .network(net)                          // optional, Network::default()
    .storage(disk)                         // Storage<S>; volatile for tests
    .branch("main")                        // registry + default proof source
    .open(Location::profile("alice"))
    .await?;

let tonk = alice.connect(did).await?;      // address book lookup; NoAddress otherwise

let job = alice.session(b"refactor")       // derived key, deterministic per context
    .credential(signer)                    // or a supplied SignerCredential
    .allow(scope).expires(t)               // grants minted at build
    .allow(certificate)                    // or pre-minted, when the audience was known
    .using(branch)                         // extra proof layers, repeatable
    .build()
    .await?;

Repository::open("notes").perform(&job).await?;            // registry lookup, mounts
let there = job.at(&tonk);                                 // bound env
let head = branch.revision().resolve().perform(&there).await?;   // same effect, remote
```

Rules the surface encodes:

- `Peer` is the unconstrained env: `perform(&alice)` acts with the peer's
  own key and proves from its branch. `alice.session(..)` narrows it and
  can add proof layers, never swap storage or network.
- Every session starts from a key that exists. `open` for a root (load or
  generate and persist), `derive` for a child. There is no session that
  starts from a delegation, because a delegation names its audience.
- `derive` is deterministic per `(peer, context)` so that a grant issued
  to a derived DID is reusable across runs. A caller that wants a
  disposable key passes a random context (the worker does).
- `connect` reads the registry and never resolves or records on its own.
  Introducing a peer is asserting `Peer { did, address }` facts. Resolution
  (did:web documents, Pkarr/DNS for did:key) is a separate operation that
  produces the same facts, and once recorded they are pinned.
- A `Peer` fact's addresses are `NetworkAddress` values (the `#[derive(Site)]`
  enum). `S3` and `Fs` are addresses of the local peer, places it holds
  credentials for. `Ucan` (and later `Iroh`) is a remote peer's. S3 is never
  a peer: it has no key, cannot be an audience, cannot sign.

## Capabilities

Authorization is verified, not assumed, and the check happens once per
binding rather than per effect.

- **On a repository subject:** `/use/get/archive/block` and the rest of the
  chain-derived vocabulary (#524). Proven to whoever serves the read: the
  session's own env locally, the remote peer over the wire.
- **On the peer's own subject:** system operations, `space::Load` is
  already shaped this way. New: `Connect { peer }`. Proven at `session.at(peer)`
  and ridden by every effect through the binding. A session without it
  cannot bind a remote at all, and a local miss during hydration is a
  plain not-found instead of a fetch. This is what makes "read what is
  here" grantable without granting the network.
- **Commits sign, they do not prove.** A revision record carries issuer,
  authority and signature; the verifier is downstream (whoever pulls it)
  and walks its own proof sources. Open question below: whether the record
  should reference the proof CIDs so a peer that lacks the issuer's chain
  can verify it.
- **The walk's reach:** the access walk fetches delegation blocks only if
  the session holds connect authority, and the bound env a proof runs
  under is never itself bound (this is the recursion bound `WalkReach`
  implements with closures today). Behavior change from today, where the
  walk fetches with no check.
- A per-effect invocation (validity = now) buys instant revocation; a
  session-scoped permit buys amortization and costs a revocation window of
  the session TTL. The worker already accepted that window for signing;
  the binding is where it is decided for reads.

## Replication

Sync is composition, not machinery. `pull` is: read the head `there`, walk
what is missing, write it here. `push` is the mirror. Hydration fallback is
a list of bound envs in priority order.

```rust
let Upstream::Remote { peer, subject, branch: name, tree } = upstream;
let there  = env.at(&env.peer().connect(peer).await?);
let remote = Repository::from(subject).branch(name);
let head   = remote.revision().resolve().perform(&there).await?;
head.verify()?;
// merge over a store that reads `env` first and `there` on a miss; commit via `env`
```

Gone: `RemoteRepository`, `RemoteBranch`, `RemoteAddress`, the
`remote/{name}/*` cells, `Fork<RemoteSite, _>` as something callers name.
Changed: `Upstream::Remote` carries `peer` and `subject` DIDs; `Hydrate`
carries a peer. The last-seen remote head moves from a cell keyed by remote
name to one keyed by `(peer, subject, branch)`, later a device-layer fact.

Remote `space::Create` (provisioning a replica on the tonk service) is the
same effect performed against `there`; the set of effects a bound remote
serves is the remote peer's capability set, refused at runtime, not fixed
by type.

## State, caches, secrets

- Per-branch caches (`Branch` holds ten) move to the env, keyed by
  `(subject, branch)`. Per-handle cached heads are what caused the
  retain-after-another-handle-moved race; one head per env removes the
  class.
- Persist only what is expensive to recompute and cheap to validate:
  resolved proof chains (keyed by access-branch epoch), rule discovery and
  plan cache (head + content hash), causality/context/record memos
  (immutable per version). Not node/spill caches (the local archive already
  is one), not the scheduler or preload queue (in-flight state). A snapshot
  lives in the registry's device layer, so it gets CAS for free.
- The credential store shrinks to the one `self` slot per space. Every
  other secret (S3 credentials, local-root handoff, account session state)
  becomes a sealed fact (`dialog-credentials::secret`) in the device layer.
  Before that lands: extend the AAD to bind entity and attribute (today it
  binds only the recipient DID), use envelope encryption for account-wide
  secrets (one content key sealed per device), and say plainly that
  retracting a sealed fact is not rotation, since history is append-only.
- Retaining a delegation is `branch.delegations().retain(chain)`, nothing
  else. `Profile::save`, `Access::save`, `Provider<Retain<Ucan>> for Operator`
  and its refresh-and-retry loop go; `MigrateAccess` takes a target branch.

## What each existing piece becomes

| today | becomes |
| --- | --- |
| `Profile::open(name).at(dir)` | `Peer::new().storage(s).open(Location)` |
| `profile.derive(ctx).allow(..).network(n).build(storage)` | `peer.session(ctx).allow(..).build()` |
| `Operator<S>` | `Session<S>` (alias kept until tonk migrates) |
| `Authority { profile, operator, account }` | `(peer, session)`; account is a link in the proof chain |
| `OperatorBuilder::access_branch(name)` (#526) | `.using(BranchReference)` on the session; the peer's `.branch(..)` is the default |
| `Directory::Current` base + `space::Load` by name | registry lookup: name → subject → address; physical placement keyed by subject DID |
| `remote/{name}/address`, `RemoteAddress`, `Upstream::Remote { remote }` | `Peer`/`PeerAddress`/`Replica` facts; `Upstream::Remote { peer, subject }` |
| `Network` as composite site | address dispatch under a peer; invocation audience = peer DID |
| `credential().site(id).save(secret)` | sealed facts in the device layer |
| `profile.save(chain)` / operator `Retain` | `branch.delegations().retain(chain)` |
| `AccountBoundOperator` | a session `.using(account_branch)` |
| `WalkReach` closures | "the env a proof runs under is never bound" |
| `Storage`'s `Loader` mounts table | registry facts; the pool stays as a handle cache |
| `Space<A, M, C, D, B>` | `Space<A, M, B>` once the certificate and secret slots go |

## Open decisions

- Should a revision record reference the proof CIDs it was made under, so
  a peer without the issuer's chain can verify it offline? Delegations are
  already blobs, so the reference is cheap.
- A session with no `.using()` and no state layer: error, or a session that
  proves only self-issued authority? Proposed: the latter.
- Whether a registry can live in a repository other than the peer's own.
  The model allows it (the self-repo can point anywhere); do not build the
  pointer until something needs it.
- `Identify` returns a fixed `(subject, profile, operator)` chain today.
  Proposed: return the `(peer, session)` pair, with the chain available
  through proving.
- Per-effect versus per-binding authorization for reads against the tonk
  service: proposed per-binding, bounded by the session TTL, with the
  access-branch epoch as early exit.

## Order

Each step is one PR and leaves tonk compiling.

1. Retype #526's `access_branch(name)` to take a `BranchReference` and
   merge it.
2. Split `Operator` into `Peer` and `Session` inside `dialog-operator`,
   keeping `Operator<S>` as an alias for `Session<S>` and the old builder
   working on top. Storage, network, scheduler and queue move to the peer;
   signer, grants, proof layers, chain cache stay on the session. The
   worker's rotation becomes replacing the session; the CLI's
   per-invocation retained grant disappears.
3. Move the per-branch caches into the env keyed by `(subject, branch)`.
4. Registry facts (`Peer`, `PeerAddress`, `Replica`) in the peer's branch;
   `connect`; `Upstream::Remote { peer, subject }`; replace the remote
   cells and tonk's registry and meta-branch mirror.
5. `session.at(peer)` as a bound env; invocation audience = peer DID;
   `Connect` capability on the peer subject; pull/push/hydrate rewritten
   as effects against two envs.
6. Retire the `Secret` effects for sealed facts.
7. Rename `Profile` to `Peer` in the public API and drop the alias.

#519 (stacks) supplies the layer types steps 3 and 4 want for device-local
state and rebases after step 2, since it touches `branch.rs` in the same
places.

## Pitfalls already met

- A delegation names its audience, so a builder cannot accept "a delegation
  to the session" before the session key exists. Derived keys are known
  only at build (a non-extractable browser key derives by signing, which is
  async); supplied keys are known before. `allow` takes a scope and mints
  at build, or a certificate whose audience the caller already knew.
- Deriving from a context alone (no peer key) is a key anyone can compute.
  A throwaway peer is `open` at a temp location over volatile storage.
- `Storage::default()` creates a fresh pool each call; two pools over one
  database is how tonk ended up remounting profiles. One peer holds the
  one storage, sessions share it.
- The mounts table is keyed by the `Debug` rendering of a `Location`.
