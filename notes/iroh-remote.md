# Iroh Remote & Gossip Block Swarm

Design and approach for `dialog-iroh-remote`: a [`Site`] implementation that
lets an [iroh](https://www.iroh.computer/) endpoint be added as a remote to
synchronize with, plus a gossip swarm — built from all of a space's iroh
remotes — that lets blocks be fetched from *any* peer in the network instead
of a single remote block store.

## Goals

1. **Iroh remotes.** `repo.remote("laptop").create(iroh_address)` should work
   exactly like adding an S3, UCAN, or FS remote today: the address is stored
   in the `remote/{name}/address` cell, and `push` / `pull` / `fetch` flow
   through the existing fork machinery unchanged.
2. **Peer-to-peer, capability-secured sync.** A peer that holds a replica of a
   space serves it directly over QUIC. Authorization is the same UCAN
   invocation chain the UCAN-S3 access service verifies — no new trust model,
   the transport changes but the proof does not.
3. **Gossip block network.** Every space maps to a deterministic gossip topic.
   Peers that replicate the space join the topic; a block read that misses on
   the addressed remote can be satisfied by *any* peer in the swarm that has
   the block. One remote being offline no longer blocks replication.

## Background: how remotes work today

A remote transport is a [`Site`](../rust/dialog-capability/src/site.rs) — a
marker type with three associated types:

| Associated type | Meaning |
|---|---|
| `Address` | Serializable connection info, stored in the repository's remote cell |
| `Authorization` | Material produced by `SiteFork::authorize` (proof, credentials, handles) |
| `Fork<Fx>` | Site-owned wrapper implementing `SiteFork::authorize` |

Execution flows as:

```text
capability.fork(&address).perform(&operator)
  └── Operator: Provider<Fork<Site, Fx>>
        ├── SiteFork::authorize(env)      → ForkInvocation { capability, address, authorization }
        └── Network: Provider<ForkInvocation<Site, Fx>>
              └── site provider executes the effect at the address
```

`dialog_network::Network` composes all transports with `#[derive(Site)]`;
adding a field `iroh: Iroh` to that struct is all the integration the
repository layer needs — `NetworkAddress` gains an `Iroh(IrohAddress)`
variant, and `RemoteAddress` (site address + subject DID) stores it like any
other transport.

The effect vocabulary a remote must serve is the same set the FS remote
delegates to `dialog_storage`:

- `archive`: `Get`, `Put`, `Import` — content-addressed blocks in catalogs
- `memory`: `Resolve`, `Publish`, `Retract` — CAS cells (branch heads)
- `blob`: `Read`, `Import` — streaming, hash-addressed byte objects

## The `Iroh` site

### Address

An iroh peer is identified by its **endpoint id** (the ed25519 public key of
its endpoint; iroh ≥ 1.0 renamed `NodeId` → `EndpointId`). Reaching it may
additionally use a relay URL and/or direct socket addresses; with a discovery
service configured, the id alone suffices.

```rust
pub struct IrohAddress {
    /// z-base-32 endpoint id (the peer's public key), e.g. as printed by iroh.
    endpoint: String,
    /// Optional home relay URL for hole punching/relaying.
    relay_url: Option<String>,
    /// Optional direct socket addresses.
    direct_addresses: BTreeSet<String>,
}
```

The address is **pure data** (strings), deliberately independent of iroh
types: it serializes into the remote cell, hashes into a stable
`SiteId` (`iroh:{endpoint_id}`), and compiles on every target — including
wasm, where the iroh dependency itself is absent. On native it converts
to/from `iroh::EndpointAddr`. `FromStr` accepts a bare endpoint id, so
`repo.remote("laptop").create("<endpoint-id>".parse()?)` is enough when
discovery can resolve the peer.

### Authorization: UCAN invocations over the wire

This is the part worth getting right. The UCAN-S3 site already produces a
**signed, serializable invocation chain** (`UcanInvocation`, the
[UCAN container](https://github.com/ucan-wg/container) format) that it POSTs
to an HTTP access service, and `dialog-remote-ucan-s3`'s server side already
**verifies** such containers (`InvocationChain::try_from` + `verify`:
signature checks, delegation-chain walk, command-prefix and policy checks).

The iroh remote reuses both halves verbatim:

- `IrohFork::authorize` is the same code path as `UcanFork::authorize`:
  `authority::Identify` → `AuthorizeEffect::<Ucan>` → signed `UcanInvocation`.
  `IrohAuthorization` wraps that invocation.
- The serving peer verifies the container exactly as the S3 access service
  does — but instead of redeeming it for a presigned URL, it **performs the
  effect directly** against its local storage provider and returns the result.

In other words: *the peer is its own access service and its own store*. The
delegation chain roots in the space's subject key, so an invocation that
verifies is authorized by the space owner regardless of which peer receives
it — a property the gossip swarm depends on (see below). The QUIC connection
itself is mutually authenticated (both sides prove their endpoint keys) and
end-to-end encrypted, courtesy of iroh.

### Wire protocol

ALPN: **`dialog-db/remote/0`**.

One request per bidirectional stream; a connection carries many streams and
is pooled per peer. Frames are length-prefixed (u32 BE) DAG-CBOR:

```text
client → server   RequestEnvelope { invocation: bytes, body: bytes? }
                  (blob/import only: chunk frames … zero-length terminator)
server → client   response frame (per-effect Result)
                  (blob/read only: header, then chunk frames … zero-length terminator)
```

The **invocation** carries subject, command path, and the attenuated
arguments (digests, checksums, catalog/space/cell, CAS preconditions) — the
things that were *signed*. The **body** carries the payload bytes that never
travel inside a capability (block content, publish content, import blocks):

| Command | body | response |
|---|---|---|
| `/archive/get` | – | `Option<bytes>` |
| `/archive/put` | block bytes | `()` |
| `/archive/import` | CBOR `[bytes]` | `()` |
| `/memory/resolve` | – | `Option<Edition>` |
| `/memory/publish` | content bytes | `Version` |
| `/memory/retract` | – | `()` |
| `/archive/blob/read` | – | chunk stream |
| `/archive/blob/import` | chunk stream | digest |

Server pipeline per request:

1. Parse + verify the UCAN container (signatures, delegation chain, command
   prefixes, policies) — reusing `dialog-ucan-core`.
2. Check the invocation's **subject** is a space this host serves.
3. Reconstruct the effect from the verified arguments plus the body, and
   **cross-check** the body against the signed content bindings (block digest
   must equal the signed `digest`, publish content must hash to the signed
   `checksum`, import blocks must match the signed per-block checksums). A
   payload that doesn't match what was signed is rejected — a delegation
   scoped to specific content stays scoped.
4. Perform the effect against the host's storage environment (any
   `Provider<Fx>` — `FileSystem`, `Volatile`, …) and encode the result.
   Memory CAS failures travel structurally (`VersionMismatch { expected,
   actual }`), so push conflict detection works across the wire.

Errors are marked **denied / rejected / execution / version-mismatch** so
the client can map them into the effect's error type the same way other
sites do.

### Serving: `IrohNode` + `SpaceHost`

```rust
let node = IrohNode::builder()
    .host(subject.clone(), env)   // env: storage provider for the replica
    .spawn()
    .await?;
node.join_swarm(&subject, bootstrap).await?;
println!("serving at {}", node.address());
```

`IrohNode` owns the endpoint, its protocol router (our ALPN plus the gossip
ALPN), a connection pool, and the joined swarms. Each `.host(subject, env)`
adds a `SpaceHost<Env>` — generic over the storage environment, so a host
can serve a filesystem-backed replica (the same directory layout
`dialog-remote-fs` targets), an in-memory one in tests, or anything else
that provides the effect vocabulary. A `HostRegistry` implements iroh's
`ProtocolHandler` and routes each verified request to the host serving its
subject.

The host is *symmetric* with the client: a device that both replicates and
serves a space runs a `Repository` over the env **and** a `SpaceHost` over
the same env.

## Direct device-to-device sync

The payoff of that symmetry: start dialog on one device, start dialog on
another, and pull/push **directly between them** — no server, no
intermediate replica.

The trick is that there is no separate "server state" at all. A remote
branch's upstream cell — `memory/branch/{name}/revision` at the subject —
is the *very cell* a local repository maintains as its branch head. So a
device that serves the same storage its repository runs on
(`Storage` clones share state; `Operator::storage()` hands one back) is
serving its **live** branch head:

```rust
// Device A: a normal dialog instance that also serves its space.
let node = IrohNode::builder()
    .host(repo.did().clone(), operator.storage())
    .spawn()
    .await?;
share_with_other_device(node.address());   // endpoint id (+ hints)

// Device B: track A directly.
repo.remote("device-a")
    .create(SiteAddress::Iroh(address))
    .subject(shared_subject)
    .perform(&operator)
    .await?;
branch.pull().perform(&operator).await?;   // A's live head, no A-side push
```

- **Pull** resolves A's live head and reads missing blocks out of A's
  archive over QUIC. A never has to "publish" anything first — committing
  locally *is* publishing to peers.
- **Push** uploads B's novel blocks into A's archive and compare-and-swaps
  A's branch head. A sees the new revision as soon as it re-resolves the
  branch. If A committed concurrently, the CAS fails and B's push errors
  `NonFastForward` — B pulls (three-way merge) and retries, exactly like
  git. The CAS is what makes pushing *into a live device* safe.
- **Authorization** is unchanged: B's invocations carry a delegation chain
  rooted in the space subject (repo → B's profile → B's operator), verified
  by A before any effect touches its storage.

Both devices can do this to each other simultaneously (each hosts, each
tracks the other), and the gossip swarm generalizes it to N devices. What's
still missing for a fully live experience is *reactivity*: A learns about
B's push when it next resolves the branch. The `Announce` swarm message is
reserved exactly for this — a subscription layer can turn it into an
automatic fetch/merge.

`tests/device_to_device.rs` exercises the whole story end to end: B pulls
A's live head (A never pushes), B pushes back, A observes; then both
commit concurrently, B's push is rejected non-fast-forward, B merges and
retries, and both devices converge.

## Gossip block swarm

### Topic

Every space derives a deterministic 32-byte gossip topic:

```text
TopicId = blake3("dialog-db/gossip/v0:" ++ subject DID)
```

Anyone who knows the space's DID can compute the topic; joining it requires
knowing at least one peer (bootstrap). The natural bootstrap set is **the
iroh remotes already configured for the space** — that is the "gossip network
from all the remotes". iroh-gossip's HyParView membership then grows the
partial view beyond the bootstrap set, so peers discover each other
transitively: adding one iroh remote connects you to the whole swarm.

### Messages

CBOR-encoded, versioned enum:

```rust
enum SwarmMessage {
    /// "Who has this block?" — broadcast by a peer that missed locally
    /// and on its addressed remote.
    Want { catalog: String, digest: Blake3Hash },
    /// "I do." — includes the responder's dialable address info so the
    /// requester can connect without a discovery round-trip.
    Have { catalog: String, digest: Blake3Hash, provider: PeerInfo },
    /// New head revision published (advisory; drives reactive pull).
    Announce { space: String, cell: String, version: Version },
}
```

Gossip messages are **advisory only** — no bytes and no authority travel over
gossip. Block *transfer* always happens over the direct `dialog-db/remote/0`
protocol, where the regular UCAN invocation is presented and verified. This
is why invocation reuse matters: an invocation for `/archive/get` is rooted
in the *subject*, not addressed to a specific peer, so the same authorization
that was built for the configured remote can be redeemed at whichever swarm
peer answers `Have` first.

### Fetch flow

```text
archive.get(digest).fork(iroh_remote)                 (or NetworkedIndex read miss)
  1. direct request to the addressed peer ── hit? ──► done
  2. miss/unreachable + swarm joined for subject:
       broadcast Want { catalog, digest }
       await Have (bounded timeout)
       direct /archive/get to the answering peer (same invocation)
  3. still nothing ──► None / original error
```

Serving peers run a responder loop on the topic: on `Want`, check local
storage; if present, broadcast `Have` with their own address info.

### Head updates: reacting instead of polling

dialog's query subscriptions are deliberately poll-driven; the swarm
provides the wake-up signal that a poll (or a `pull`) will find something.
A joined [`SwarmHandle::updates`] is a broadcast stream of `HeadUpdate
{ space, cell, version, origin }` that fires when:

- **a peer pushes into this device** (`origin: Pushed`) — the host's
  publish path notifies local subscribers directly and announces the move
  on the topic, and
- **a peer announces a publish elsewhere in the swarm**
  (`origin: Announced`) — the responder loop surfaces incoming `Announce`
  messages.

So in the device-to-device flow, A doesn't re-resolve its branch on a
timer: B's push wakes A up, and A re-opens/pulls exactly then. Updates
are advisory signals, not a log — a subscriber that lags and drops
updates just re-resolves the head and has lost nothing, and the carried
version is only good for change detection (all real state flows through
the verified remote protocol).

A device whose *local* commits should wake peers without a push can call
`swarm.announce("branch/{name}", "revision", version)` after committing;
hooking that into `commit` itself is repository-layer future work.

### Why gossip + direct fetch (and not gossip'd blocks or iroh-blobs)

- Broadcasting block bytes over the topic would push every block to every
  peer (plumtree broadcast), spend the swarm's bandwidth on data only one
  peer asked for, and bypass capability checks. Want/Have is two tiny
  messages; the transfer is point-to-point, encrypted, and authorized.
- `iroh-blobs` ships its own store and BLAKE3-verified-streaming protocol;
  adopting it would mean a second block store beside `dialog-storage` and a
  bypass of the capability layer. dialog blocks are already blake3-addressed
  and small; the archive protocol above is sufficient and stays inside the
  effect system. Revisit if/when large-blob replication needs verified
  streaming and range requests at scale.

## Runtime: endpoints, lazily

`Network` is a zero-state dispatch table (`Copy`, `Default`) constructed in
the operator builder, and `#[derive(Site)]` cannot cfg-gate fields. So the
`Iroh` site stays a unit marker and the live state lives in one
process-global `IrohNode`, mirroring how the FS site resolves platform
directories globally:

- `install(node)` — hosts (and anyone who wants a configured identity:
  secret key, relay mode, pre-built endpoint) build a node explicitly and
  install it as the process global, exactly once.
- `node()` — the providers' accessor: returns the installed node, lazily
  binding a default one (n0 discovery + relays) if none was installed, so a
  pure client needs zero setup.
- A `MemoryLookup` address book seeded from every `IrohAddress` dialed and
  every `Have` received, so gossip and re-dials can resolve bare endpoint
  ids without external discovery.
- `node.swarm(subject)` — handle registry the `Get` provider consults for
  the gossip fallback; populated by `node.join_swarm(..)` on hosts and
  clients alike.

The crate tracks iroh 1.x (and iroh-gossip 0.101). Getting there required
bumping `s3s` to 0.14: iroh 1.0's `ed25519-dalek 3.0.0-rc` wants `sha2
0.11` final, which conflicted with the `sha2 =0.11.0-rc.5` pin `s3s 0.13`
carried.

## wasm strategy

The workspace (and `dialog-network`) must keep compiling on
`wasm32-unknown-unknown`. `dialog-iroh-remote` therefore splits:

- **Target-independent** (compiles everywhere): `IrohAddress`,
  `IrohAuthorization`, the `Iroh` site marker, `SiteFork::authorize` (it only
  builds a UCAN invocation — the same thing the UCAN site does on wasm), and
  the wire protocol *types*.
- **Native-only** (`#[cfg(not(target_arch = "wasm32"))]`): the iroh
  dependency, transport, host, and swarm. On wasm the effect providers return
  an "iroh transport is not supported on this target yet" execution error.

iroh does have experimental browser support; when it stabilizes for this
workspace's wasm targets, the cfg boundary is the only thing that moves.

## Trust model

- **Transport**: QUIC with both peers' endpoint keys verified by iroh —
  connections are mutually authenticated and E2E encrypted. Adding an iroh
  remote pins the peer you talk to by public key, not by network location.
- **Authorization**: every request carries a UCAN invocation chain rooted in
  the space subject; hosts verify it before touching storage. Read vs write
  gating falls out of command-prefix matching on delegations, exactly as with
  the UCAN access service.
- **Integrity**: content addressing end-to-end — block payloads are checked
  against the *signed* digests/checksums on the host, and `archive::Put`
  re-derives digests on deserialize; a lying peer can withhold data but not
  substitute it.
- **Gossip**: an unauthenticated overlay by design — messages are hints.
  A malicious swarm member can advertise blocks it doesn't have (wasted
  dial; the transfer still verifies) or observe which digests peers want
  (metadata leak comparable to any shared replica). Payloads and authority
  never ride the overlay.

## Testing

- Address parsing / serde / `SiteId` stability (all targets).
- Wire round-trips over real in-process endpoints (relay-less, direct
  addresses): archive put/get/import, memory publish/resolve/retract
  including CAS `VersionMismatch` propagation, blob import/read streaming.
- Authorization: a request signed by a key with no delegation is denied; a
  read-only delegation cannot write; wrong-subject requests are rejected.
- Swarm: host A serves a block, peer B joins the topic via A and fetches the
  block it does not have via Want/Have + direct get.
- Full-stack: an `Operator` env forking effects at an `IrohAddress` through
  the `Network` dispatch (the same path `repo.push()` exercises).

## Future work

- **Auto-pull on head updates**: the `HeadUpdate` stream exists; wiring it
  into the repository so tracked branches `pull` (and standing query
  subscriptions re-poll) automatically on updates — and announcing local
  commits from `commit` itself — is the remaining repository-layer piece.
- **Replica hints**: let a host advertise the set of subjects it serves so
  peers can bootstrap swarms for spaces they learn about laterally.
- **Verified streaming for large blobs** (iroh-blobs interop or range-based
  blob reads over the existing protocol).
- **Browser transport** once iroh's wasm support fits the workspace targets.
- **Revocation / delegation freshness** on long-lived host processes
  (re-verify cached chains against a revocation feed).

[`Site`]: ../rust/dialog-capability/src/site.rs
