# Peers and sessions

Status: design, agreed in discussion; steps 2, 7 and 8 of the order below
are implemented (the `dialog-peer` crate, one `Peer` type).

**Current surface, which supersedes the names used further down.** The
note was written while a derived peer was called a *worker* and built with
`Peer::open(home).credential(..)`. That landed as:

- `Peer::new(credential)` builds the peer acting with its own key, a
  `Peer<S, Local>`.
- `peer.session(context)` builds a *session* of it, a `Peer<S, Session>`:
  the same peer acting with a key derived for `context`, under what the
  peer grants it. `Peer::session_of(did).operator(key)` builds one when
  only the peer's DID is at hand, from pre-minted certificates; they must
  be issued to the session's key and not have expired.
- A session is never handed a key: the peer's key is withheld from it,
  a repository it loads comes without its signing key, and the peer's raw
  storage is not reachable through it. What it may do elsewhere is proven
  from its grants, the grant covering the claim's subject.
- A session is not yet confined to its grants for local reads and writes:
  it shares the peer's storage, and local effects are served without a
  proof. Authorizing them is planned separately.

Everywhere below, read *worker* as *session*, `Peer::open(home)` as
`Peer::new` or `Peer::session_of`, and `peer.worker(ctx)` as
`peer.session(ctx)`. Supersedes the
profile / operator split described in `repository.md` and
`space-and-storage.md`, which this note treats as the "today" column. The
migration guide at the end is what a dependent (tonk) follows.

## Why

Four objects each own a slice of "who am I, what do I hold, where is it,
and what may I do", and every product layer adds one more:

- `Profile` was a persisted signer, opened at a `Location` through a
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
- A **worker** is a peer built over a derived (or supplied) key with
  grants from another peer: a signer, in-memory grants, and the branch it
  proves from. It contributes only the **origin** (`hash(branch, key)`) to
  what it commits; the replica is its **home**'s. A worker is a peer, not
  a second type; what used to be called a session is one.
- A peer's **home** is the repository holding its own state: where it
  finds delegations, retains them, and keeps its registry facts. A root
  peer's home is the repository its own key names; a worker's is usually
  its parent's. The home DID is the replica identity every entity the
  peer writes derives from.
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
// The credential is opened apart from the peer; here from the storage.
let credential = OpenCredential::open("alice").perform(&storage).await?;

let alice = Peer::new(credential)             // the acting key, whose repository holds own state
    .storage(storage)                         // Storage<S>; volatile for tests
    .at(Location::profile("alice"))           // optional; mounts the home space if not yet
    .network(net)                             // optional, Network::default()
    .runtime(runtime)                         // optional; share a scheduler with other peers
    .base(Directory::Current)                 // optional; where space names resolve, until the registry
    .branch("main")                           // optional; the state branch, default main
    .await?;

let job = Peer::session_of(credential.did())  // a session: no handle on the peer needed
    .operator(credential.derive(b"refactor").await?)   // deterministic per (credential, context)
    .storage(storage)
    .allow(Subject::any().claim(&credential))          // unbounded, minted at open, deliberate
    .grant(cap.claim(&credential).expires(t))          // bounded: the claim carries the window
    .grant(certificate)                                // pre-minted to the worker's key
    .ephemeral()                                       // optional: no state branch, memory only
    .await?;

let job = alice.session(b"refactor")          // the same, pre-filled from alice
    .allow(Subject::any())                    // bare capabilities are claimed by alice
    .await?;

alice.space("notes").open().perform(&job).await?;   // named space under the home
branch.revision().resolve().perform(&alice).await?; // the root is the unconstrained env
```

The key is resolved at open: `operator(..)` takes a credential or bare
signer, and a [`PeerKey::Derived`] (what `session` builds) derives it then.
`grant` refuses a claim without an expiration; `allow` is the unbounded
form under its own name. A bare capability needs an issuer to claim it,
which `session` supplies and `issuer(..)` sets otherwise.

Planned (steps 4, 5, 9 and 10 below):

```rust
let job = alice.worker(b"refactor")
    .using(branch)                         // extra proof layers, repeatable
    .await?;
Repository::open(did).perform(&job).await?;               // registry lookup, mounts
let there = job.connect(did).await?;                       // registry lookup; NoAddress otherwise
let head = branch.revision().resolve().perform(&there).await?;   // same effect, remote
```

Rules the surface encodes:

- One type. A root peer holds no grants and proves self-issued authority
  from its branch; a worker holds grants and proves through its issuer's
  authority; an ephemeral worker holds no branch. `perform(&peer)` works
  for all three.
- Every peer starts from a key that exists: `OpenCredential` for a root
  (load or generate and persist), `credential.derive(ctx)` for a worker.
  There is no peer that starts from a delegation, because a delegation
  names its audience.
- `derive` is deterministic per `(credential, context)` so that a grant
  issued to a derived DID is reusable across runs. A caller that wants a
  disposable key passes a random context (the worker does), and should
  make that peer ephemeral: a persistent branch under a rotating key
  accumulates dead grants.
- The proof for a worker is `walk(issuer, claim) ++ grant`: the walk
  proves the grant's issuer, read from the grant itself, so a worker
  needs no handle on its parent and may hold grants from several.
- A principal's authority over its own subject is self-issued: the empty
  chain, resolved without a branch, so an ephemeral worker and a peer
  still opening its branch prove it alike.
- `connect` reads the registry, proves `Connect` once, and returns the env
  bound to that peer. A remote peer is never derived or opened, only
  bound. `connect` never resolves or records on its own. Introducing a
  peer is asserting `Peer { did, address }` facts. Resolution (did:web
  documents, Pkarr/DNS for did:key) is a separate operation that produces
  the same facts, and once recorded they are pinned.
- A `Peer` fact's addresses are `NetworkAddress` values (the `#[derive(Site)]`
  enum). `S3` and `Fs` are addresses of the local peer, places it holds
  credentials for. `Ucan` (and later `Iroh`) is a remote peer's. S3 is never
  a peer: it has no key, cannot be an audience, cannot sign.

## Placement and owners

Agreed, not yet built (steps 9 and 10):

- **Pointers as facts, no mount set.** Creating a repository takes a
  location and records `(subject, address)` in the home branch; opening
  by DID looks the pointer up, so local and remote replicas are one kind
  of fact (`Replica` with an address). First open by explicit location
  records too, so a space handed over out of band is discovered next
  time. The one location that cannot be discovered is the home's, which
  is what `at` is for. Stored locations use the symbolic `Directory`
  variants, so the branch stays valid if it ever replicates.
- **Providers have owners.** `Storage::new(owner)`, `Fs::new(owner)` and
  the client side of the network take the DID whose delegations direct
  them. The guarded operations are the ones that attach a resource to the
  peer's world: `storage/load`, `storage/create` and `mount` at a
  location, and `dial` to an address. Everything after attachment is
  authorized against the subject, as today. A self-issued `storage::Load`
  proves nothing about the storage; one rooted at the owner does. The
  root peer is the owner, so it pays nothing; a worker needs a grant:

  ```rust
  Subject::from(owner).storage().create().at(Directory::At(root))
  Subject::from(owner).storage().load().at(Location::profile("tonk"))
  Subject::from(owner).dial().via("https").to("tonk.xyz")
  ```

  Read versus write is the command (`load` versus `create`); which
  locations or addresses is policy on the argument, matched as structured
  values (the `Directory` variant first, the name second), never as path
  strings. Enforcement runs in the provider, which means these local
  effects become invocations carrying proofs, so the same providers work
  behind a sandbox. A `UcanSite`'s DID is its identity, not an owner.
- **Two connects.** `Dial` is the local owner's "you may reach that
  address through my network"; `Connect` is the remote's "you may connect
  to me", checked there as the invocation's audience. A worker with one
  and not the other is refused by whichever side it lacks.

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
| `Profile::open(name).at(dir)` | `OpenCredential::open(name).at(dir).perform(&storage)`, then `Peer::new(c).storage(storage)` |
| `profile.derive(ctx).allow(..).network(n).build(storage)` | `peer.session(ctx).allow(..)`, or `Peer::session_of(home).operator(c.derive(ctx).await?)..` |
| `Operator<S>` | `Peer<S>`; the crate is `dialog-peer` |
| `Authority { profile, operator, account }` | `(home, key)`; account is a link in the proof chain |
| `OperatorBuilder::access_branch(name)` (#526) | `PeerBuilder::branch(name)`; `.using(BranchReference)` adds layers later |
| `Directory::Current` base + `space::Load` by name | registry lookup: name → subject → address; physical placement keyed by subject DID |
| `remote/{name}/address`, `RemoteAddress`, `Upstream::Remote { remote }` | `Peer`/`PeerAddress`/`Replica` facts; `Upstream::Remote { peer, subject }` |
| `Network` as composite site | address dispatch under a peer; invocation audience = peer DID |
| `credential().site(id).save(secret)` | sealed facts in the device layer |
| `profile.save(chain)` / operator `Retain` | `branch.delegations().retain(chain)` |
| `AccountBoundOperator` | a worker `.using(account_branch)` |
| `WalkReach` closures | "the env a proof runs under is never bound" |
| `Storage`'s `Loader` mounts table | registry facts; the pool stays as a handle cache |
| `Space<A, M, C, D, B>` | `Space<A, M, B>` once the certificate and secret slots go |

## Open decisions

- Should a revision record reference the proof CIDs it was made under, so
  a peer without the issuer's chain can verify it offline? Delegations are
  already blobs, so the reference is cheap.
- **Settled:** a peer with no state branch (`ephemeral`) proves its
  in-memory grants and self-issued authority and retains nothing.
- **Settled:** the registry is the branch of the peer's home, named on
  the builder; a worker's home is usually its parent's. Whether a worker's
  branch forks `main` (sees the parent's delegations as of the fork) or
  starts empty (needs `using(main)`) is the open half.
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
- **Settled:** one type. `Session` folded into `Peer`; a worker is a
  peer with grants, an ephemeral one a peer without a branch.
- Who writes the pointer fact for a space created by another tool, and
  whether creation records one at all or scanning the home directory is
  the fallback. Proposed: creation and first open both record.
- Whether locations get their own DIDs (shareable across owners, at the
  cost of a key per location) or stay policy on the owner's subject.
  Proposed: policy; `owner.derive(location)` can mint a key later without
  a new DID method if a location must outlive its owner.
- Whether the network client's owner guards forks by destination, by
  subject, or both. `Dial` guards the destination; the subject stays the
  second dimension.

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
7. **Done.** `Profile` is gone. `dialog-identity` keeps the credential
   loader as `OpenCredential`, the access API, the site-secret handle and
   `SpaceHandle`; `Peer` fronts all of them (`access`, `secrets`, `space`).
   `Repository: From<Profile>` and `Peer::profile` went with it.
8. **Done.** `Session` folded into `Peer`. The builder is
   `Peer::open(home).credential(..).storage(..)`, awaited; the credential
   is opened apart from the peer (`OpenCredential`), and derives its own
   workers (`SignerCredential::derive`). `Authority` takes the home DID
   rather than a second signer. Grants carry their issuer, and the walk
   proves that issuer, so a worker needs no handle on its parent. The
   scheduler and preload queue moved to a `Runtime` handle the builder
   shares; the chain cache stays per peer. `grant` is bounded, `allow`
   unbounded, `ephemeral` drops the branch.
9. Pointers as facts: creation and first open record `(subject,
   address)` in the home branch; `Repository::open(did)` resolves it; the
   `Loader` mounts table and `base` go. Names leave the peer API; tonk's
   name index is a product fact.
10. Owners on providers and guarded placement: `Storage::new(owner)`,
    `storage/load`, `storage/create` and `mount` as owner-rooted
    capabilities with location policy; `Dial` on the network client.

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
`Operator`, `OperatorBuilder`, `DeriveOperator`, `Profile` or `Session`.
One type, `Peer`, is the identity and the environment: what a profile did,
a peer does; what an operator or session did, a worker peer does.

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
let storage = Storage::<NativeSpace>::default();
let credential = OpenCredential::open(name)         // the credential, apart from the peer
    .at(Directory::Profile)
    .perform(&storage)
    .await?;
let peer = Peer::new(credential)                    // home: the repository the key names
    .storage(storage)
    .base(Directory::At(root))                      // was OperatorBuilder::base
    .network(Network::default())                    // was OperatorBuilder::network
    .branch("main")                                 // was OperatorBuilder::access_branch
    .await?;
let session = peer
    .session(b"app")                                // was profile.derive(b"app")
    .allow(Subject::any())                          // claimed by peer
    .await?;
```

`OpenCredential` mounts the credential's space; `Peer::new(..)` opens the
state branch. `OpenCredential::load` fails when the credential is absent,
`create` when it is present. A session needs no handle on its peer:
`Peer::session_of(home).operator(credential.derive(ctx).await?).storage(storage)
.allow(Subject::any().claim(&credential))` is `peer.session(ctx).allow(..)`
spelled out.

| before | after |
| --- | --- |
| `Operator<S>`, `Session<S>` | `Peer<S>` |
| `OperatorError` | `PeerError` |
| `.allow_until(cap, t)` | `.grant(peer.access().claim(cap).expires(t))` |
| `.allow(cap)` on a session builder | `.allow(cap)` on `peer.session(ctx)`, or `.allow(cap.claim(&credential))` |
| a session over a supplied signer | `Peer::session_of(home).operator(signer)..allow(cap.claim(&parent))` |
| `operator.did()` | `session.did()` |
| `operator.profile_did()`, `session.peer().did()` | `session.home().clone()` |
| `operator.hydration()` | `session.hydration()`, shared through `Runtime` |
| `profile.did()` | `peer.did()` |
| `profile.signer()` | `peer.credential()` |
| `profile.access()` | `peer.access()` |
| `profile.repository(name)` | `peer.space(name)` |
| `profile.save(chain)` / `profile.access().save(chain)` | `peer.access().save(chain)` (goes away in step 4; use `branch.delegations().retain(chain)`) |
| `profile.credential().site(id)` | `peer.secrets().site(id)` (goes away in step 6) |
| `Profile::open(name).at(dir).perform(&storage)` | `OpenCredential::open(name).at(dir).perform(&storage)`, then `Peer::new(c).storage(storage)`; `load` and `create` likewise |
| `Peer::new().storage(s).attach(credential)` | `Peer::new(credential).storage(s)` |
| `Reactor::new(profile)` and other holders of a `Profile` | hold the `Peer`, or the `SignerCredential` from `peer.credential()` when only signing is needed |
| `Repository::from(&profile)` | `peer.repository()` (the home, by DID) or `Repository::from(peer.credential().clone())` |
| `Storage::default()` per call site | one `Storage` per process; every peer over it shares it |
| `dialog_operator::helpers::test_operator()` | `dialog_peer::helpers::test_session()` |
| `dialog_operator::helpers::test_operator_with_profile()` | `dialog_peer::helpers::test_session_with_peer()`, returns `(session, peer)` |
| `test_repo(&operator, &profile)` | `test_repo(&session, &peer)` |

Things the fold makes possible that the old shape did not:

- `perform(&peer)`: the root peer is the unconstrained environment, acting
  with its own key. Where tonk built a "full" operator with
  `Subject::any()` and a fixed context only to act as the profile, use
  the peer.
- A worker from a credential alone. Where tonk wrapped the operator to
  sign as another principal, build a peer over that principal's
  credential and `.grant(certificate)` what it holds.
- Key rotation without rebuilding the runtime: the worker's
  `session::rotate` becomes `peer.worker(random).grant(bounded claim)`,
  sharing the parent's `Runtime`. Make it `ephemeral` once nothing it
  retains needs to outlive it: a persistent branch under a rotating key
  accumulates dead grants.

Tonk-specific notes:

- `AccountBoundOperator` forwards every effect to the inner operator and
  overrides `Authorize` to splice the account chain in. Under the fold,
  the inner operator is a worker `Peer`; the wrapper keeps working
  unchanged until step 4 lets a peer prove from the account branch
  directly.
- `account_state::operator_with_profile` remounts the profile by name to
  derive a second operator because a derived operator needed a mounted
  profile in its own storage. A `Peer` already holds its storage; build
  the second worker from the same peer instead.
- `site.rs` retains a fresh expiring session grant on every open
  (`profile.access().save(session)`). Delete it: the worker's grant is in
  memory, and the retained copies were the accumulation the in-memory
  session was introduced to stop.
