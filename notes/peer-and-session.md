# Peers and sessions

Status: design, agreed in discussion; step 2 of the order below is
implemented (the `dialog-peer` crate). Supersedes the profile / operator
split described in `repository.md` and `space-and-storage.md`, which this
note treats as the "today" column. The migration guide at the end is what a
dependent (tonk) follows.

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

Implemented today (`dialog-peer`):

```rust
let alice = Peer::new()
    .storage(disk)                         // Storage<S>; volatile for tests
    .network(net)                          // optional, Network::default()
    .base(Directory::Current)              // optional; where space names resolve, until the registry
    .branch("main")                        // optional; registry + default proof source
    .open(Location::profile("alice"))      // or .load(..), or .attach(credential)
    .await?;

let key = alice.derive(b"refactor").await?;      // deterministic per (peer, context)
let job = alice.session(key)                     // or any other SignerCredential
    .allow(Subject::any())                       // unbounded, minted at build
    .allow(alice.access().claim(cap).expires(t)) // bounded: the claim carries the window
    .grant(certificate)                          // pre-minted to the session's key
    .build()
    .await?;

alice.space("notes").open().perform(&job).await?;   // named space under the peer
branch.revision().resolve().perform(&alice).await?; // the peer is the unconstrained env
```

The session key is always a `SignerCredential`, known before build:
`derive` is the deterministic path, any other signer is the supplied one,
and `SessionBuilder::did` names the audience a certificate for `grant`
must carry. A bare capability in `allow` is an unbounded claim by the
peer; a `Claim` made through `peer.access()` carries its window, and a
claim by any other issuer is refused at build.

Planned (steps 4 and 5 below):

```rust
let job = alice.session(alice.derive(b"refactor").await?)
    .using(branch)                         // extra proof layers, repeatable
    .build().await?;
Repository::open("notes").perform(&job).await?;            // registry lookup, mounts
let there = job.connect(did).await?;                       // registry lookup; NoAddress otherwise
let head = branch.revision().resolve().perform(&there).await?;   // same effect, remote
```

Rules the surface encodes:

- `Peer` is the unconstrained env: `perform(&alice)` acts with the peer's
  own key and proves from its branch. `alice.session(key)` narrows it and
  can add proof layers, never swap storage or network.
- Every session starts from a key that exists. `open` for a root (load or
  generate and persist), `derive` for a child. There is no session that
  starts from a delegation, because a delegation names its audience.
- `derive` is deterministic per `(peer, context)` so that a grant issued
  to a derived DID is reusable across runs. A caller that wants a
  disposable key passes a random context (the worker does).
- `connect` is on the session: it reads the registry, proves `Connect`
  once, and returns the env bound to that peer. A remote peer is never
  derived or opened, only bound. `connect` never resolves or records on
  its own.
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
  already shaped this way. New: `Connect { peer }`. Proven at `session.connect(peer)`
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
let there  = env.connect(peer).await?;
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
| `Profile::open(name).at(dir)` | `Peer::new().storage(storage).open(Location)` |
| `profile.derive(ctx).allow(..).network(n).build(storage)` | `peer.session(peer.derive(ctx).await?).allow(..).build()` |
| `Operator<S>` | `Session<S>`; the crate is `dialog-peer` |
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
- Names or subjects and locations. `peer.space(name).open()` resolves the
  name against the base directory (`Location::new(base, name)`) and loads
  the credential found there; the registry plan keeps names as a lookup
  (name, subject, address). Review on #527 proposes dropping names from
  the peer API, `peer.repository(did).open().at(location).perform(&job)`,
  subject up front and placement explicit, which removes `base` as well.
  What it touches: `SpaceHandle` and `RepositoryExt`, `space::Load` and
  `space::Create` by name, 25 test sites here, and tonk's registry, which
  is name-keyed. To settle first: what a caller names when creating a
  repository (no DID exists until the key does), and whether a name index
  is then a product-layer fact rather than a peer API.
- One type or two. With the session key always a credential, a session is
  a peer plus `(authority, grants)`, and `Peer::as_session` is the
  embedding with empty grants. The review proposes an enum (`Root`,
  `Delegate`, and after step 5 `Bound { remote }`) so a session is a
  constrained peer, not a second type. What the split buys is a
  type-level "unconstrained" (`perform(&peer)`) that no dependent asks
  for by type: `Peer<_>` appears in their signatures only as the thing to
  derive from or read a DID off. What it costs is `session.peer()` at 41
  sites and two builders. Proposed: fold in its own PR after this one,
  since it touches every `Provider` impl in the crate.

## Order

Each step is one PR and leaves tonk compiling.

1. Close #526: its knob landed as `PeerBuilder::branch(name)` in step 2,
   with the same "main holds nothing" test. Retyping it to a
   `BranchReference` waits for step 4, when the registry can name a
   branch of another repository.
2. **Done.** `dialog-operator` is `dialog-peer`, and `Operator` is split
   into `Peer` and `Session` with no compatibility alias: storage,
   network, base directory, registry branch, scheduler, queue and the
   chain cache are the peer's; the acting signer, the grants and the
   walk's reach are the session's. `OperatorBuilder::access_branch` (#526)
   is `PeerBuilder::branch`. The worker's rotation becomes replacing the
   session; the CLI's per-invocation retained grant disappears.
3. Move the per-branch caches into the env keyed by `(subject, branch)`.
4. Registry facts (`Peer`, `PeerAddress`, `Replica`) in the peer's branch;
   `connect`; `Upstream::Remote { peer, subject }`; replace the remote
   cells and tonk's registry and meta-branch mirror.
5. `session.connect(peer)` returning a bound env; invocation audience = peer DID;
   `Connect` capability on the peer subject; pull/push/hydrate rewritten
   as effects against two envs.
6. Retire the `Secret` effects for sealed facts.
7. Rename `Profile` to `Peer` in the public API and drop the alias.

#519 (stacks) supplies the layer types steps 3 and 4 want for device-local
state and rebases after step 2, since it touches `branch.rs` in the same
places.

## Pitfalls already met

- A delegation names its audience, so the session key must exist before
  its grants are minted or passed in. Derivation is async (it runs a key
  agreement, see `operator-derivation.md`), so `derive` is its own awaited
  step and `session` takes the credential; the builder's `did` is then
  known before build. The derivation label is still the operator's, so a
  session is the operator the same context derived before the rename.
- Deriving from a context alone (no peer key) is a key anyone can compute.
  A throwaway peer is `open` at a temp location over volatile storage.
- `Storage::default()` creates a fresh pool each call; two pools over one
  database is how tonk ended up remounting profiles. One peer holds the
  one storage, sessions share it.
- The mounts table is keyed by the `Debug` rendering of a `Location`.

## Migration guide (dialog-operator to dialog-peer)

The crate is `dialog-peer`; the module path is `dialog_peer`. There is no
`Operator`, `OperatorBuilder` or `DeriveOperator`. `Profile` still exists
in `dialog-identity` and is re-exported, but nothing needs it to build a
session any more.

Opening the identity and building the environment:

```rust
// before
let storage = Storage::<NativeSpace>::default();
let profile = Profile::open(name).at(Directory::Profile).perform(&storage).await?;
let operator = profile
    .derive(b"app")
    .allow(Subject::any())
    .base(Directory::At(root))
    .network(Network::default())
    .build(storage)
    .await?;

// after
let peer = Peer::new()
    .storage(Storage::<NativeSpace>::default())
    .base(Directory::At(root))             // was OperatorBuilder::base
    .network(Network::default())           // was OperatorBuilder::network
    .branch("main")                        // was OperatorBuilder::access_branch
    .open(Location::new(Directory::Profile, name))
    .await?;
let session = peer
    .session(peer.derive(b"app").await?)   // was profile.derive(b"app")
    .allow(Subject::any())
    .build()
    .await?;
```

`Peer::open` mounts the credential and opens the registry branch; there is
no separate "mount the profile then derive" step, and a peer cannot be
built from an unmounted handle. `Peer::load` fails when the credential is
absent; `Peer::new().storage(storage).attach(credential)` builds over a credential
mounted some other way.

| before | after |
| --- | --- |
| `Operator<S>` | `Session<S>` |
| `OperatorError` | `PeerError` |
| `.allow_until(cap, t)` | `.allow(peer.access().claim(cap).expires(t))` |
| a session over a supplied signer | `peer.session(signer)` |
| `operator.did()` | `session.did()` |
| `operator.profile_did()` | `session.peer().did()` |
| `operator.hydration()` | `session.hydration()` or `session.peer().hydration()` |
| `profile.did()` | `peer.did()` |
| `profile.signer()` | `peer.credential()` |
| `profile.access()` | `peer.access()` |
| `profile.repository(name)` | `peer.space(name)` |
| `profile.save(chain)` / `profile.access().save(chain)` | `peer.access().save(chain)` (goes away in step 4; use `branch.delegations().retain(chain)`) |
| `profile.credential().site(id)` | `peer.profile().credential().site(id)` (goes away in step 6) |
| `Repository::from(&profile)` | `Repository::from(&peer)` or `Repository::from(peer.credential().clone())` |
| `Storage::default()` per call site | one `Storage` per process, owned by the peer; sessions share it |
| `dialog_operator::helpers::test_operator()` | `dialog_peer::helpers::test_session()` |
| `dialog_operator::helpers::test_operator_with_profile()` | `dialog_peer::helpers::test_session_with_peer()`, returns `(Session, Peer)` |
| `test_repo(&operator, &profile)` | `test_repo(&session, &peer)` |

Things the split makes possible that the old shape did not:

- `perform(&peer)`: the peer is the unconstrained environment, acting with
  its own key. Where tonk built a "full" operator with `Subject::any()`
  and a fixed context only to act as the profile, use the peer.
- `peer.session(signer)`: a supplied session key. Where
  tonk wrapped the operator to sign as another principal, build a session
  over that principal's credential and `.grant(certificate)` what it holds.
- Key rotation without rebuilding the runtime: the worker's
  `session::rotate` becomes a session over `peer.derive(random)` with a
  bounded claim, on the peer it already holds. The scheduler and the preload queue
  survive the rotation.

Tonk-specific notes:

- `AccountBoundOperator` forwards every effect to the inner operator and
  overrides `Authorize` to splice the account chain in. Under the split,
  the inner operator is a `Session`; the wrapper keeps working unchanged
  until step 4 lets a session prove from the account branch directly.
- `account_state::operator_with_profile` remounts the profile by name to
  derive a second operator because a derived operator needed a mounted
  profile in its own storage. A `Peer` already holds its storage; derive
  the second session from the same peer instead.
- `site.rs` retains a fresh expiring session grant on every open
  (`profile.access().save(session)`). Delete it: the session's grant is in
  memory, and the retained copies were the accumulation the in-memory
  session was introduced to stop.
