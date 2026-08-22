# End-to-End Encryption

## Status

Proposed. This note turns the access-level sketch in [`privacy.md`](./privacy.md)
into an implementable design, grounded in what the code actually does today.

Where this note and `privacy.md` disagree, this note wins: `privacy.md` proposes
nested per-tier encryption *inside* each node, and the analysis below argues
against that shape. The tiered access goals survive; the onion does not.

## Goal

Storage infrastructure — S3, R2, the UCAN access service, the filesystem
provider — holds nothing but ciphertext. A replica holding the space's read key
can materialize and query the database exactly as it does today. Nothing about
tree shape, query planning, or the sync protocol changes.

Non-goals for the first cut: hiding the DAG shape, hiding block sizes, hiding
which blocks changed in a commit, and server-side computation over ciphertext.
Those are discussed under [What still leaks](#what-still-leaks).

## Background reading

Two inputs:

- **BeeKEM** (Yen, Fábrega, Da, Kleppmann, Mumm, Park, Zelenka; eprint
  2026/1434) — a decentralized continuous group key agreement (DCGKA) scheme
  with `O(log n)` update cost in the common case, degrading to `O(n)` under
  concurrency. It is the first DCGKA with security proofs at that cost, and it
  is a CRDT: members converge on the same key-agreement tree regardless of the
  order operations arrive.
- **`privacy.md`** — the existing four-tier access model (L0 opaque blobs, L1
  connectivity, L2 key ranges, L3 content).

BeeKEM answers *"how does a group of peers agree on a shared secret without a
server, across partitions?"* It does not answer *"how does a prolly tree get
encrypted?"* Those are separate layers and this note keeps them separate.

### Why BeeKEM fits Dialog specifically

BeeKEM assumes *authenticated causal broadcast* and constructs it from a
reliable broadcast protocol plus a hash DAG of signed operations (§4.2). Dialog
already has exactly that: signed [`Revision`](../rust/dialog-artifacts/src/revision.rs)
heads over a content-addressed block store, with causal ordering carried by the
revision DAG and per-origin watermarks. BeeKEM's operation graph can be stored
as ordinary blocks in the archive and synced by the existing push/pull path. We
do not need to build the transport BeeKEM assumes — we have it.

The paper's implementation lives at
`github.com/inkandswitch/keyhive/tree/main/beekem` (Rust, MPL-compatible
licensing to be confirmed). Evaluating it as a dependency should precede
reimplementation.

#### Operation history is the cost that grows

Table 1 puts sender cost at `O(log n)` primitives and existing-recipient cost
at `O(1)`, but a *new* member pays `O(h_B)` — they replay the whole operation
history. The paper quantifies it: **each `Update` grows the welcome message by
2.5 kB and adds 40 µs to processing it.**

For a chat group that is fine. A Dialog space lives far longer than a
conversation, so a few thousand membership operations means a multi-megabyte
welcome and a visible join delay. The paper waves at checkpointing as an
optimization (§4.3.3, footnote 16 — "in practice we find replaying the full
history does not cause performance issues"); for us it is likely to be
required, not optional. Budget for it in Phase 4 rather than discovering it
later.

The other cost to plan for is concurrency. §6.2 partitions a 64-member group
four ways and finds recovery cost grows with the *fraction of distinct members*
who updated while partitioned, then plateaus once every member has updated
once — further updates add little. Absolute numbers are small (~30 ms CPU,
~150 KiB to resolve every conflict node), so this is a shape to be aware of
rather than a risk.

#### Post-quantum is a primitive swap, not a redesign

`BeeKEM_PQ` (§7) replaces the NIKE with a KEM: each node additionally stores a
ciphertext from encapsulating the sibling's public key, and everything else
stays as it is. That means PQ migration is contained — *provided* the group
protocol carries its own version independent of the block-sealing suite byte.
Version the two separately from the start; they will not move together.

### Where Dialog's requirements differ from messaging

BeeKEM is parameterized by a key-retention parameter `κ`: members keep their
`κ` most recent personal secrets so they can recover group secrets established
on concurrent branches. Larger `κ` means better correctness-under-concurrency,
worse forward secrecy.

Messaging tolerates small `κ`: an unrecoverable group secret costs you some
messages you could have read. **A database does not have that luxury** — an
unrecoverable group secret means blocks you can never decrypt, which is data
loss. Dialog therefore wants large `κ`, and the paper's `BeeKEM_FS` variant
(§7), which trades correctness-under-concurrency for full forward secrecy, is
the wrong variant for us.

The key-wrapping layer described below softens this considerably: data is not
encrypted under the group secret directly, so a member who cannot derive one
particular epoch secret has lost only a key-wrap, and any peer who still holds
the seal key can republish it. See [Recovery](#recovery-from-a-lost-epoch).

## What exists today

Worth stating plainly, because it sets the size of the job: **there is no
encryption anywhere in the repository.** Every grep for `encrypt`, `cipher`,
`aead`, `x25519`, `chacha20poly1305`, or `hkdf` comes back either empty or
pointing at `rand_chacha` used as a deterministic test PRNG.

What we *do* have is most of the identity and authorization half:

| Need | Status |
| --- | --- |
| Ed25519 signing/verification, native and WebCrypto | Complete — `dialog-credentials` |
| `did:key` identity and resolution | Complete — `dialog-varsig` |
| Capability model, delegation, chain verification | Complete — `dialog-capability` + `dialog-ucan` (UCAN 1.0.0-rc.1) |
| Credential persistence (fs / IndexedDB / volatile) | Complete, but stored **in the clear** |
| KDF | Ad-hoc `blake3::derive_key` in one place (`dialog-operator`); no `hkdf` direct dependency |
| CSPRNG | `getrandom` wired for both targets, though two major versions coexist |
| Symmetric AEAD | Nothing |
| Key agreement (X25519) | Nothing — `curve25519-dalek` is present only transitively via `ed25519-dalek` |
| Key wrapping / group keys | Nothing |

So the PKI BeeKEM assumes as a black box is already built. What is missing is
every symmetric primitive and the group layer itself.

### Three constraints the platform imposes

**Sealing must be synchronous.** `TransientNode::persist` is a synchronous
function deep inside the tree's write path. Making it async to accommodate a
promise-based crypto API would ripple through the entire tree, so **WebCrypto
is not available for block sealing** and the AEAD must be pure Rust. This is
the single fact that settles the cipher choice below.

**Signing keys may be non-extractable.** On wasm, `KeyExport::NonExtractable`
wraps an opaque WebCrypto `CryptoKey`; the seed cannot be read, and WebCrypto
will not convert an Ed25519 key to X25519. `dialog-operator` already works
around this for operator-key derivation by signing a fixed domain-separated
message and running `blake3::derive_key` over the (deterministic) Ed25519
signature — see `dialog-operator/src/operator/builder.rs`. That idiom is
available to us, though the group layer below avoids needing it. Note in
passing that its `derive_key(ctx, seed || context)` shape puts variable-length
context into the *key material* rather than the context string, which is a mild
domain-separation smell worth tightening if we build on it.

**The dependency tree is delicately pinned.** Two `getrandom` majors coexist
deliberately (0.2 via the `js` cargo feature, 0.3 via a
`getrandom_backend="wasm_js"` rustflag), and `tempfile` is pinned below 3.22
specifically to stop a third from resolving. New crypto crates must land on the
0.10-generation RustCrypto stack — `chacha20poly1305 0.10` → `aead 0.5` →
`rand_core 0.6` → `getrandom 0.2`. The 0.11 release candidates pull `sha2 0.11`
and `getrandom 0.3` and would break that premise.

## Threat model

**In scope.** A storage provider that holds every byte we ever wrote and can
read, retain, and correlate all of it. A UCAN access service that sees every
authorization request. A network observer. A former collaborator who was
removed from the space. All of these are assumed to be honest-but-curious for
confidentiality and are already untrusted for integrity (block addresses and
signed heads cover that today).

**Out of scope for the first cut.** Traffic analysis; a provider that
selectively withholds blocks (availability, not confidentiality); compromise of
a current member's device; and metadata inherent to content addressing.

## Design

### 1. Ciphertext is the block

The single most consequential decision. There are two places encryption could
sit relative to the content address, and only one of them survives contact with
the existing code.

The current address of a tree node is
`blake3(rkyv_bytes)`, minted by `Buffer::blake3_hash()` inside
[`TransientNode::persist`](../rust/dialog-search-tree/src/node/transient.rs)
and threaded into the parent's `Link.node`. That address is then used verbatim
as the S3 object key (`{subject-did}/{catalog}/{base58(digest)}`,
`dialog-remote-s3/src/request/archive.rs`).

The tempting design — keep `blake3(plaintext)` as the logical address, encrypt
underneath it — does not work here, for two independent reasons:

1. `LocalIndex::set` (`dialog-repository/src/repository/archive/local.rs`)
   **discards the key it is given** and lets the provider re-derive the address
   from the bytes (`dialog-effects/src/archive.rs`, `digest_of`). An encryption
   decorator above that boundary silently stores blocks under
   `blake3(ciphertext)` while every `Link` points at `blake3(plaintext)`.
2. `archive::Put` embeds both the blake3 digest and a SHA-256 checksum of the
   block into the *signed* UCAN invocation, and S3 enforces the checksum via
   `x-amz-checksum-sha256`. Both describe whatever bytes are actually stored.
   Deriving them from plaintext would require the server to hold plaintext.

So: **the address is `blake3(sealed_block)`.** Links carry sealed addresses.
Everything below the tree — effects, UCAN scopes, S3 keys, checksums, the
`Put` deserializer's self-addressing check — keeps working unmodified.

The cost is that sealing must happen *inside* `persist`, before the parent
mints its link, rather than in a storage decorator. That is one seam, and it is
already the only encode site in the tree.

### 2. Sealing must be deterministic

Content addressing is load-bearing far beyond dedup. `TreeDifference::compute`
prunes whole subtrees when hashes match on both sides; the graft merge adopts
subtrees by hash without reading their interiors; fast-forward detection is
root-hash equality; caches, idempotent puts, and the spill cache all assume
"same content, same address, forever". Above all, two replicas that build the
same logical tree must produce byte-identical blocks or the prolly tree stops
converging.

Randomized nonces break every one of those. The sealing construction must be
deterministic: same plaintext plus same key implies same ciphertext.

We get that with a synthetic-IV construction — derive the nonce from the
plaintext under a secret key:

```
frame        = u32_le(plaintext_len) || brotli(plaintext) || zero_padding
nonce        = blake3_keyed(K_nonce, frame)[0..24]
header       = b"DLGE" || version:u8 || suite:u8 || generation:u32_le || nonce:[u8;24]
sealed_block = header || XChaCha20Poly1305(K_data, nonce, aad = header, frame)
address      = blake3(sealed_block)
```

XChaCha20-Poly1305's 192-bit nonce makes a derived nonce safe: the pair
`(K_data, nonce)` is only ever reused for the identical frame, so there is no
nonce-reuse exposure, and 192 bits leaves no meaningful collision risk across
any realistic number of distinct blocks. The construction leaks plaintext
equality — which is precisely what "dedup" means, and is a property we are
choosing to keep.

The cipher choice follows from the synchronous-seal constraint. WebCrypto is
promise-based and therefore unavailable inside `persist`, so the AEAD must be
pure Rust on both targets, which rules out the usual "native pure-Rust, web
SubtleCrypto" split this repo uses elsewhere. Given that, XChaCha20-Poly1305
beats AES-GCM on two counts: wasm has no AES-NI so software ChaCha is the
faster of the two software implementations, and AES-GCM's 96-bit nonce is
uncomfortably thin for a *derived* nonce (collisions at the `2^48` birthday
bound). The suite byte in the header exists so this can be revisited without a
format break.

Note the ordering: **compress, then pad, then encrypt.** Encrypting first makes
compression useless. `CompressedStorage` therefore moves inside the seal rather
than remaining a storage decorator — it is composed nowhere in production
today, so this costs nothing.

The header is cleartext because a reader must know the generation and nonce
before it can decrypt. It is authenticated as AEAD associated data, and the
whole sealed block is covered by the address, so it cannot be tampered with.

Sizes: 4 + 1 + 1 + 4 + 24 = 34 bytes of header, plus a 16-byte Poly1305 tag,
plus 4 bytes of length prefix — 54 bytes of overhead per block before padding.
Against a default `max_segment` in the low kilobytes this is a low-single-digit
percentage, and compression should more than pay for it.

### 3. Padding

Chunk boundaries are decided by hashing *plaintext* keys
(`Geometric::rank` → `weight_paced_cut` in `dialog-search-tree/src/distribution.rs`),
so block sizes are a direct function of the data. Left alone, the size
histogram of a space's blocks is a fingerprint of its contents.

Pad frames up to a small ladder of size buckets before sealing — powers of two,
or a fixed ladder tuned to the observed node-size distribution. Bucketing to
powers of two costs at most 2x storage in the worst case and far less in
practice, since node sizes already cluster around `max_segment`. Make the
ladder a space-level policy so it can be traded off explicitly, and record it
in the sealed frame so a reader can strip padding by the length prefix.

### 4. Key schedule

Two rotation rates, because they have wildly different costs.

**The seal key** encrypts data. Rotating it changes every block address in the
space, because every ciphertext changes and structural sharing cannot help. It
rotates rarely. Call its counter the **generation** `g`; it is carried in the
block header and announced in the head. Crucially, rotation does *not* imply an
immediate rewrite — see [lazy revocation](#rotation-is-lazy) below.

**The epoch key** comes from BeeKEM's group secret and rotates on every
membership change or key update — which is to say, constantly. It never
encrypts data. Its only job is to wrap the seal key.

```
S_e         = BeeKEM group secret for epoch e
K_epoch_e   = blake3::derive_key("dialog/e2ee/epoch/v1", S_e)
K_seal_g    = a random 32-byte key, minted when generation g opens
K_data_g    = blake3::derive_key("dialog/e2ee/data/v1",  K_seal_g)
K_nonce_g   = blake3::derive_key("dialog/e2ee/nonce/v1", K_seal_g)
K_head_g    = blake3::derive_key("dialog/e2ee/head/v1",  K_seal_g)
K_blob_g    = blake3::derive_key("dialog/e2ee/blob/v1",  K_seal_g)
```

Each epoch publishes a **key-wrap block**: `AEAD(K_epoch_e, K_seal_g)` for the
current generation only. Older generations are reachable through a chain of
*cryptographic links* — `K_seal_g → K_seal_{g-1}`, one 32-byte symmetric link
published when a generation opens — so a member who can derive `K_epoch_e` gets
the newest seal key and walks the chain back for history.

This is Cryptree's symmetric link, and the reason to use it rather than
enumerating every generation in the wrap block is the argument Cryptree makes
against key regression: a link permits an *arbitrary* new key, so re-keying
from an external source or out of order stays possible, and the wrap block
stays `O(1)` instead of `O(generations)`.

`blake3::derive_key` is used throughout rather than HKDF — blake3 is already a
dependency, its KDF mode is domain-separated by construction, and it keeps the
primitive count down.

#### Member identity keys

BeeKEM needs a non-interactive key exchange — X25519 in the paper's own
implementation. Members are already identified by an Ed25519 `did:key`, and the
obvious move is to convert that key to X25519. **Do not do this.** On the web
the Ed25519 key is an opaque non-extractable `CryptoKey` and WebCrypto offers
no such conversion, so the conversion would work natively and fail in the
browser — the worst possible shape for a platform difference.

Instead, mint a **separate long-lived X25519 identity keypair** per member and
bind it to the DID by signing it:

```
identity_announcement = { did, x25519_pk, valid_from }
                        signed by the member's Ed25519 DID key
```

This is exactly the PKI that BeeKEM treats as a black box, and it costs us
nothing to build because `dialog-credentials` already resolves DIDs to Ed25519
verifiers. The private half is persisted through the existing
`credential::Site` / `Secret` slot, which already stores opaque bytes (it holds
S3 auth material today).

All of BeeKEM's other keypairs — the `⌈log n⌉` fresh pairs minted per `Update`,
and every inner-node subgroup secret — are ephemeral protocol state, never
persisted as credentials, so `x25519-dalek` in pure Rust covers them on both
targets. Only the identity key needs storage, and only the identity key needs
to survive a restart.

Note the consequence for the credential store: it currently writes key material
to disk in the clear. Adding a second secret does not make that worse, but a
seal key ring sitting next to it does raise the stakes. See open question 2.

#### Inviting a member

`K_seal_0` is minted client-side from `getrandom` when the space is created and
never leaves the client unwrapped. Getting it to a second person means two
independent grants, and conflating them is the easiest way to get this wrong:

- The **UCAN delegation** gives Bob the right to *fetch bytes*. Already built.
- The **BeeKEM add** gives Bob the ability to *read* them. Not built.

Neither implies the other. Keeping them separate is useful — fetch rights can
be revoked without touching keys — but an invite is not complete until both
land, so it should be one API call rather than two things a caller must
remember to do.

The ordering is forced. Bob cannot read the BeeKEM operation graph until he can
fetch from the archive, so the delegation goes first, out of band. Then Alice
runs `Add(bob)` + `Update`, which bumps the epoch and puts Bob's X25519 public
key at a leaf; Bob walks up the BeeKEM tree from that leaf to derive the group
secret, hence `K_epoch`, hence the wrap block.

That requires Alice to already know Bob's X25519 identity key, which is not
always true. Two invite shapes cover the cases:

- **Directed invite.** Alice fetches Bob's signed identity announcement from
  his profile space and adds him by key. Requires Bob to exist and be
  reachable.
- **Claim invite.** Alice mints an *ephemeral* X25519 keypair, adds that as the
  member, and puts the private half in the invite link fragment — never sent to
  a server. Whoever claims it derives the group secret with the ephemeral key
  and immediately performs `Add(self)` + `Remove(ephemeral)` + `Update`. This
  is what "send someone a link" actually requires, since Alice does not know
  who will accept. It is a bearer token: single-use, short expiry, and the
  ephemeral member *must* be removed on claim or the link stays live forever.

The identity announcement itself — `{did, x25519_pk, valid_from}`, signed by
the DID key — should be **public and unencrypted**. It is public keys; there is
nothing to protect but correlation, and `Add` cannot work if it is not
discoverable.

#### Membership semantics that follow

- **Add.** BeeKEM `Add` + `Update` bumps the epoch. The new member receives the
  key-wrap block for the new epoch. Whether they can read history is decided by
  *which generations the wrap block contains* — include all of them for
  history-inclusive membership, include only `g_current` for join-forward. This
  is a per-space policy, not a protocol change.
- **Remove.** BeeKEM `Remove` + `Update` bumps the epoch; the removed member
  cannot derive `K_epoch_{e+1}` and so never sees another wrap block. **They
  retain the ability to decrypt every block written under generations they
  already held.** Removal is forward-protecting only.
- **Rotate.** To revoke read access to existing data, open generation `g+1`.
  See below — this is much cheaper than it sounds.

Being blunt about this in the API and the docs matters more than the mechanism.
"Removing a collaborator does not un-share the data they already had" is true
of every E2EE system and is routinely mis-sold.

#### Rotation is lazy

Opening a new generation does **not** rewrite the database. Following Cepheus
and Cryptree, rotation is *lazy*: generation `g` is marked dirty, `g+1` opens,
new writes seal under `g+1`, and blocks still at `g` are re-sealed only when
they are next written. Ordinary write traffic cleans the space incrementally.

The `g+1 → g` link keeps history readable for current members and does not help
the removed member, who already held `K_seal_g` and can never obtain
`K_seal_{g+1}` — it is only ever published wrapped under an epoch key they
cannot derive.

The accepted trade, exactly as in the Cryptree literature, is that a removed
member can still read whatever nobody has touched since. For spaces where that
is unacceptable, a **forced scrub** re-seals everything at `g` under `g+1` as a
resumable background job; it changes every address and therefore re-uploads the
whole space. That is the "we really mean it" button, not the default price of a
removal.

Rotation triggers differ sharply between the two keys, and it is worth stating
them separately:

| | Epoch (BeeKEM `Update`) | Generation (new seal key) |
| --- | --- | --- |
| Triggered by | every add, every remove, periodic PCS, lost device | only when a removal must deny access to *existing* data |
| Cost | `O(log n)` — `⌈log n⌉` X25519 keygens, one broadcast op | free at rotation time; amortized over subsequent writes |
| Rewrites blocks | never | lazily, or all at once under a forced scrub |
| Announced via | an operation block in the archive, found on normal sync | `generation` in the signed head and in every block header |

Because the generation is in each block header, a reader always knows which key
to use without a lookup. A writer that sees a head at a higher generation than
its own must fetch the new wrap block before writing, or it forks the
generation and the differential sees the whole tree as novel (§5).

A member offline across several epochs replays the operation graph on return
and lands on the current group secret, provided their leaf was not blanked. If
they were removed and re-added they get a fresh leaf. If they missed a fork
beyond `κ`, that is the republication case above.

#### Recovery from a lost epoch

If a member cannot derive some epoch's secret — the cross-fork case BeeKEM's
`κ` parameter governs — they have lost a key-wrap, not data. Any peer who
still holds `K_seal_g` can republish a wrap under the current epoch key. The
seal-key indirection converts BeeKEM's hardest failure mode from data loss into
a repairable gap, which is the main reason to keep the two layers separate.

### 5. Convergence and the generation

All replicas writing at generation `g` produce byte-identical blocks for
identical logical content, so canonicality holds exactly as it does today —
*per generation*. Two replicas at different generations writing the same
logical node produce different addresses.

That is not a correctness problem: the merge descends, finds identical entries,
and converges. It is a cost problem — a generation split makes the differential
see the whole tree as novel. Mitigation: carry the generation in the signed head
and require a writer to adopt the highest generation it has seen before writing.
Generation changes are rare by construction, so the window is small.

### 6. The head

The head is a signed `Revision` in a CAS memory cell at
`{subject-did}/branch/{name}/revision`. It currently sits in S3 as plaintext
DAG-CBOR containing the tree root, the branch entity, the edition, and the
per-origin watermark.

Seal the payload and keep an outer envelope in the clear so signature
verification and CAS still work without keys:

```rust
struct SealedRevision {
    issuer: Did,           // ephemeral session key; needed to verify without decrypting
    generation: u32,
    sealed: Vec<u8>,       // AEAD(K_head_g, aad = issuer||generation, Revision::payload())
    signature: Vec<u8>,    // ed25519 over the envelope, domain-tagged
}
```

The watermark is the interesting thing to hide: it is a version vector that
reveals per-origin write rates and collaborator count. The tree root has to be
*derivable* by anyone fetching blocks anyway, so hiding it in the head buys
little; hiding the watermark buys real metadata privacy.

Note the branch name still leaks — it is in the S3 object key path, not in the
head. If that matters, the cell path becomes
`blake3_keyed(K_name, branch_name)`, which is a small, self-contained change to
`dialog-repository/src/repository/branch/reference.rs`.

### 7. Blobs and spilled values

**Spilled values** (values over `inline_n`, stored as separate archive blocks
with the value's blake3 embedded in the tree key) seal like any other block.
The reference embedded in the key becomes the sealed address
`blake3(seal(value))` rather than `blake3(value)` — still a deterministic
function of the value, so identity and dedup semantics are unchanged. The
order-preserving `spill_prefix` bytes stay as they are; they live inside the
sealed node and are not exposed.

**Blobs** need chunked AEAD because `blob::Read` supports range requests and
`Import` streams. Seal in fixed 64 KiB chunks with
`nonce = blake3_keyed(K_bnonce_g, blob_id || u64_le(chunk_index))`, and make
the blob's declared digest the blake3/BAO root of the *ciphertext* stream — so
verified streaming keeps working against the ciphertext, unchanged, and range
reads translate to chunk-aligned ranges plus a trim. Blob length leaks up to
chunk granularity; pad the final chunk and optionally to a size ladder.

`notes/blob-replication.md` calls blob identity as BAO-over-plaintext a
load-bearing decision; this changes it to BAO-over-ciphertext. Worth confirming
nothing else depends on the plaintext root.

### 8. Access tiers, revisited

`privacy.md` proposes nesting three encryption layers inside each node so an L1
actor sees child links, an L2 actor additionally sees key ranges, and an L3
actor sees content. Recommendation: **do not build the onion.**

Two reasons. First, it is a deep node-format change — `PersistentIndex` would
have to split `hashes` from `prefix`/`suffixes`/`scales` from `novelty` into
three separately-sealed regions, with three AEAD tags per node, on the hottest
data structure in the system. Second, and decisively: **Dialog's sync needs
none of it.** Push computes the novel-node set entirely client-side via
`TreeDifference`; pull is lazy get-by-hash. The server never traverses
anything. L1 exists in the sketch to enable a bundling optimization for a
protocol that does not need it.

If bundling becomes worth having, get it without touching the node format: have
the pusher publish a per-revision **sync manifest** — a flat list of block
addresses reachable from the root, optionally with the edge list — as its own
block, sealed under a `K_sync` that can be delegated separately. An L1 service
holding `K_sync` can bundle and serve exactly as `privacy.md` describes, and a
space that does not delegate it is unaffected. This is strictly more flexible
than the onion and costs one optional block per commit.

L2 (key ranges) is the highest-leak, lowest-value tier — server-side tree
validation is not something we do today and the client already validates
structurally on read. Defer indefinitely.

Group-scoped L3 — different facts readable by different collaborators — is real
and worth having, but it is a *different layer* from everything above, and it
should not be built by putting keys in tree links. See
[Fact-level access control](#fact-level-access-control-a-second-layer).

## Fact-level access control: a second layer

Everything above encrypts **blocks**, with one key per space per generation.
That defends against the storage provider, and it is deliberately all-or-nothing
among collaborators: anyone who can open a block sees every fact in it.

Restricting *which facts* a collaborator can read is a second, independent
layer, and the temptation is to build it structurally — per-subtree keys
wrapped in tree links. That is wrong, for reasons worth writing down because
they are not obvious.

### Why not a Cryptree over the index tree

[Cryptree] lays cryptographic links over a *folder hierarchy*, so holding one
folder's key derives every descendant's key and grants a subtree in `O(1)`. The
prolly tree is a tree, so the analogy is tempting. It fails on three counts:

- **Shape is not user-meaningful.** Cryptree's unit of sharing is the folder
  because that is how people think. A prolly subtree is an arbitrary key range
  chosen by `blake3(key)` coin flips. "Grant Bob this subtree" means "grant Bob
  the entities whose keys happen to fall in this range" — not a sentence anyone
  wants to say.
- **Nodes are not stable.** Cryptree's downward inheritance works because the
  folder graph persists across edits. Our nodes split, merge, and shift
  boundaries on insert. "Bob holds the key for node X" stops meaning anything
  the moment X splits.
- **Three indexes, one grant.** The same facts are indexed EAV, AEV, and VAE.
  One semantic grant spans ranges in three trees that are not the same
  subtrees.

Write control is a fourth difference: Cryptree needed asymmetric links because
it had no capability layer and had to enforce writes cryptographically. We get
write control from UCAN, so we do not need them.

What *does* port from Cryptree is the primitive, not the topology:
cryptographic links (already used for the seal-key chain, §4) and lazy
revocation (§4).

### The EAV hierarchy is the hierarchy

[CrypTable] — an early draft from the authors of BeeKEM and WNFS — makes the
move that resolves this: put the cryptree on the **data model** rather than on
the index. A triple store already has a user-meaningful hierarchy:

```
Root → Store → Entity → Attribute → Value
```

Grant an entity key and every attribute and value under it derives. That is
exactly the granularity people ask for ("everything about this entity"), and
unlike a scope hierarchy we would have to invent and maintain, it is inherent
in the triple structure Dialog already has.

Two refinements from that draft are worth taking:

- **Derivation, not links, on the common path.** Classic Cryptree walks down by
  fetching and decrypting a link at each level. CrypTable derives —
  `K_attr = KDF(K_entity, attribute)` — so a reader can jump straight to any
  fact's key with no intermediate round trips. For a database with random
  access that is a large win over link-walking.
- **`causedBy` is not in the hierarchy.** Access to a fact must not imply
  access to its transitive causal history; the provenance relation and the
  access-control relation are simply different. This applies directly to
  Dialog's revision DAG and watermarks, and it is the kind of thing that is
  obvious once stated and easy to get wrong silently.

### The unresolved parts

Being clear about what this does *not* settle, because the draft does not
settle it either — sections 3.1, 3.2, 4 (Tag Derivation) and 5 (Key Rotation)
are one-line stubs, and the two most load-bearing for us are among them.

1. **Derivation fights revocation.** If `K_entity = KDF(K_store, entity)`, one
   entity's key cannot be rotated without rotating the store key. Pure
   derivation is key-regression-shaped, and Cryptree §7 argues against exactly
   that because it cannot accept an arbitrary new key. The likely answer is a
   hybrid: derive on the common path, and carry an explicit link as an override
   where a scope has been re-keyed. That needs designing.
2. **AEV and VAE become second-class.** An entity-rooted key hierarchy means an
   attribute-scoped grantee ("everyone's `name`") needs every entity key. The
   draft acknowledges this — it assumes grants are hierarchical even though
   access often is not. For us it is sharper, because AEV and VAE exist
   precisely to serve those patterns. Either access control is entity-rooted
   and those indexes stay readable only to full-space readers, or we need
   per-index hierarchies and 3x the key material.
3. **Encrypted values break value predicates.** Sealing values individually
   means range predicates over values stop working for a scoped reader, and the
   VAE index is useless to them. Real cost, needs to be stated in whatever API
   exposes this.
4. **Searchable tags need analysis.** The draft's tag construction —
   hash(scoped attribute nonce ‖ cleartext CID), stored as an associative map
   label — is cheap and avoids per-fact HMAC keys, but it leaks the entity and
   attribute of facts a holder *cannot* read. The draft calls this "a small
   amount of data"; in a store where facts reference each other densely, that
   deserves a bound before adoption.

The layering conclusion is the useful part: **block sealing and fact-level
encryption are complementary, not alternatives.** Block sealing hides
everything from the storage provider, including tree-shape metadata, and keeps
sync, dedup, and convergence intact. Fact-level EAV-derived keys restrict what
a collaborator who can already open blocks is allowed to read. Build the first;
the second is a separate design with its own open questions.

## What still leaks

Honest accounting, assuming everything above is built:

| Leaked | Why | Mitigable? |
| --- | --- | --- |
| DAG topology and per-commit change set | Content addressing; the server sees which addresses arrive together | No, not without abandoning content-addressed sync |
| Block count and total size | Inherent | No |
| Block size distribution | Chunk boundaries are plaintext-determined | Yes — size-bucket padding (§3) |
| Structural sharing across revisions | Unchanged subtrees keep their addresses | Only by rotating generations, which defeats sharing |
| Subject DID, catalog name | S3 key prefix | Partially — DID is needed for authorization |
| Branch name | S3 cell path | Yes — keyed hash of the name (§6) |
| Write timing and rate | Request timing | No |
| Blob sizes, to chunk granularity | Content-Length, `Import { size }` | Partially — padding |

The first row is the important one. A provider watching a space over time
learns the shape of the tree, how it changes per commit, and therefore
something about write locality. This is a property of content-addressed
synchronization, not of our encryption, and it is the price of the sync
efficiency the whole design is built on.

Also worth recording: `notes/version-control.md` already flags that hashes of
low-entropy values are brute-forceable, so strict content erasure needs
app-layer salting. Under this design, in-tree value hashes are sealed, so that
concern shrinks to spilled-value addresses — which are `blake3(seal(value))`
and therefore key-dependent, closing the oracle.

## Implementation plan

Sequenced so each phase is independently landable and testable, and so nothing
is encrypted until the plumbing is proven.

### Phase 1 — `dialog-crypto` and the block envelope

New crate `rust/dialog-crypto`: the sealing construction and key schedule, no
integration.

- `SealSuite` trait plus one implementation (XChaCha20-Poly1305 + blake3
  derive-key), so the suite byte in the header means something.
- `seal(frame, &SealKeys, generation) -> Vec<u8>` and `open(&[u8], &KeyRing)`.
- Padding ladder as a policy type.
- `BlockCodec` with two impls: `PlainCodec` (identity — byte-for-byte the
  current format, so encryption-off is a no-op and existing spaces keep
  working) and `SealedCodec`.
- Property tests: determinism, round-trip, wrong-key rejection, tamper
  rejection, padding-strip correctness.

Dependencies to add, all on the 0.10-generation RustCrypto stack so the
`getrandom 0.2` / `tempfile < 3.22` pin holds:

```toml
chacha20poly1305 = { version = "0.10", features = ["xchacha20poly1305"] }
zeroize          = { version = "1", features = ["zeroize_derive"] }
subtle           = "2"
x25519-dalek     = { version = "2", features = ["static_secrets"] }  # Phase 4
```

Add the same `[target.'cfg(all(target_arch = "wasm32", target_os = "unknown"))'.dependencies]`
`getrandom = { workspace = true, features = ["js"] }` stanza every other crate
in the workspace carries, and use `ConditionalSend`/`ConditionalSync` rather
than raw `Send`/`Sync` per the workspace rule. Check the wasm bundle-size delta
against `[profile.wasm-release]` — a pure-Rust AEAD is not free, and this is
the phase to find out how much it costs.

The sealing API stays **synchronous** on purpose (see the constraint above);
resist any temptation to make it async for symmetry with the signer traits.

### Phase 2 — thread the codec through the tree

Touch points are few and known:

- `TransientNode::persist` (`dialog-search-tree/src/node/transient.rs`) — seal
  after `body.as_bytes()`, before `delta.add`.
- `PersistentNode` (`node/persistent.rs`) — hold the plaintext buffer for
  in-place rkyv access but return the *sealed* address from `hash()`/`to_link`.
- `Accessor::get_node` (`accessor.rs`) and the direct `storage.retrieve` call
  in `differential.rs` — open on the way in.
- `dialog-search-tree/src/storage.rs` — the `identity.matches(&bytes)`
  assertions now compare against the sealed bytes, which is correct and needs
  no change beyond confirming ordering.

With `PlainCodec` wired in, the entire existing test suite must pass unchanged
and produce identical addresses. That is the gate for Phase 3.

### Phase 3 — sealed spaces with a static key

Introduce a space-level seal key supplied by configuration, no group
agreement yet. Generation pinned at 0. This makes single-user and
pre-shared-key spaces fully end-to-end encrypted and exercises everything
except membership.

- Seal spilled values (`dialog-artifacts/src/spill.rs`, `tree.rs`) with the
  reference becoming the sealed address.
- Seal the head (`dialog-artifacts/src/revision.rs`), keeping the signature
  over the envelope.
- Compression moves inside the seal; retire `CompressedStorage` as a decorator.
- End-to-end test against the S3 emulator asserting no plaintext key or value
  byte appears in any stored object.

### Phase 4 — BeeKEM group agreement

New crate `rust/dialog-group`.

- First: evaluate the Ink & Switch `beekem` crate for direct use. Reimplement
  only if licensing or dependency footprint rules it out.
- BeeKEM operations become blocks in the archive; the operation graph is a hash
  DAG synced by the existing push/pull. Members are `did:key` identities from
  `dialog-credentials`, so the PKI BeeKEM assumes is already there.
- Add the signed X25519 identity announcement and its `Secret`-slot
  persistence before anything else in this phase — every other piece depends on
  it, and it is where the wasm platform difference would otherwise bite.
- Key-wrap blocks per epoch; the seal-key ring is the interface Phase 3 already
  consumes.
- Choose `κ` deliberately and document it — argue for large `κ` per the
  reasoning above, and implement wrap republication so a lost epoch is
  repairable.
- Tests must cover partition-and-heal with concurrent updates on both sides,
  since that is the case BeeKEM exists to handle and the case a centralized
  scheme would get wrong.

### Phase 5 — rotation and revocation

- `rotate()`: open generation `g+1`, publish the wrap block and the
  `g+1 → g` link. Cheap — no data is touched.
- Lazy cleaning: writers seal under the current generation, so ordinary traffic
  converges the space without a dedicated job.
- `scrub()`: the optional forced re-seal, resumable, `O(database size)`.
- Wire removal to an epoch bump, with generation rotation as an explicit
  follow-on rather than an implicit consequence.
- Document the forward-only semantics of removal prominently.

### Phase 6 — optional, if warranted

- Sealed sync manifest for L1 delegation (§8).
- Keyed branch names.
- Fact-level access control over the EAV hierarchy — a design of its own, with
  the four unresolved problems listed above to settle first. Not a phase of
  this work so much as the next piece of work.

## Open questions

1. **Does `κ` need to be `∞`?** The wrap-republication argument says no, but it
   assumes at least one reachable peer retains `K_seal_g`. A space where every
   member is offline during a fork could still strand an epoch. Worth modelling
   before picking a default.
2. **Where does the seal key ring live at rest?** The credential store writes
   key material to disk in the clear today (`{space_root}/credential/key/...`
   is a raw 68-byte multicodec blob), and on the web the IndexedDB provider is
   not a secure store either. Signing keys at least have the WebCrypto
   non-extractable escape hatch; a symmetric seal key has no equivalent — it
   must be readable to be useful. Wrapping the ring under a
   passphrase-derived key is the conventional answer and would be the first
   `argon2`-shaped dependency in the tree.
3. **Padding ladder shape.** Powers of two is the safe default but may be
   wasteful given how tightly node sizes already cluster. Wants measurement
   against a real dataset before it is fixed.
4. **Blob BAO over ciphertext** — confirm nothing outside `dialog-remote-s3`
   depends on the plaintext blake3 root of a blob.
5. **Does the UCAN access service need to distinguish generations?** It
   authorizes over digests and checksums, which are generation-agnostic, so
   probably not — but revocation policy might want the distinction.

## References

- Yen, Fábrega, Da, Kleppmann, Mumm, Park, Zelenka. *BeeKEM: Decentralized,
  Secure and Efficient Group Key Agreement.* IACR ePrint 2026/1434.
- Weidner, Kleppmann, Hugenroth, Beresford. *Key Agreement for Decentralized
  Secure Group Messaging with Strong Security Guarantees.* (the `O(n)` DCGKA
  BeeKEM improves on)
- Alwen et al. on UPKE and forward secrecy in TreeKEM — relevant to the
  `BeeKEM_FS` variant we are declining.
- [`privacy.md`](./privacy.md) — the access-level model this note revises.
- [`version-control.md`](./version-control.md) — revision DAG, watermarks, and
  the graft merge whose by-hash subtree adoption constrains rotation.
- [`blob-replication.md`](./blob-replication.md) — blob identity and BAO
  streaming.
- Grolimund, Meisser, Schmid, Wattenhofer. *Cryptree: A Folder Tree Structure
  for Cryptographic File Systems.* SRDS 2006 — cryptographic links, lazy
  revocation, and the argument against key regression.
- Wilton, Zelenka. *CrypTable v0.1.0* (first draft),
  `github.com/RhizomeDB/cryptable` — a cryptree over the EAV hierarchy of a
  triple store. Incomplete, but the hierarchy-selection idea is the one we
  want.
- Fu, Kamara, Kohno. *Key Regression: Enabling Efficient Key Distribution for
  Secure Distributed Storage* — the alternative to cryptographic links.

[Cryptree]: https://raw.githubusercontent.com/ianopolous/peergos/master/papers/wuala-cryptree.pdf
[CrypTable]: https://github.com/RhizomeDB/cryptable/tree/first-draft
