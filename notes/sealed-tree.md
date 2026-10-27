# Sealed trees

How a Dialog line keeps its search tree encrypted at rest and in transit,
so that a replicator can copy every block, a validator can route keys, and
only members can read entries.

This note covers what is built on `claude/beekem-dialog-implementation-djpavv`
(PR #582): `dialog-keyring::layered` and the sealed lines in
`dialog-repository`. It starts by comparing that with the design proposal
["Dialog Sealed Trees"](https://claude.ai/artifact/Goq2m3XBa5ihqRhijAZ4FQ).
Benchmarks, test lists and the BeeKEM background are in `notes/beekem.md`.
The access levels come from `notes/privacy.md`.

## Is the proposal what we built?

Half of it. The node-level design is built, with two corrections. The
keyring is not built.

**Built as proposed:**

- Three nested access levels.
- One 32-byte structure key per link.
- `S → R → C` derived so that content access implies range access.
- Every block addressed by the BLAKE3 hash of its own bytes.
- Convergent sealing.
- Bottom-up persist that reuses unchanged children.
- A replicator walk that opens only structure regions.

The proposal's open question about spilled values is settled the way it
suggested: they are sealed on their own and recorded by ciphertext address
in the structure region.

**Corrected:**

- **The structure region does not use a fixed nonce.** `S` is derived from
  the node's own plaintext bytes, not from the regions' plaintexts. A node's
  bytes name its children by plaintext identity. Its structure region names
  them by envelope address, and that address depends on the generation the
  child was sealed under. So one `S` can seal two different structure
  regions. That is fatal for AES-GCM under one nonce. The structure region
  therefore takes a synthetic nonce, a keyed hash of what it seals, stored
  beside it. This costs 12 bytes and keeps sealing deterministic.
- **The tree is untouched.** The proposal changed `dialog-search-tree`'s
  node format: a public part, and a `Link` that gains a structure key.
  Instead, sealing is a projection at the storage boundary:
  - The tree keeps persisting the same nodes, linked by plaintext identity.
  - Each envelope wraps one node.
  - A party keeps a map from identity to `(envelope address, S)`, learned as
    each parent opens.

  Diffs, merges, the node cache and the live spine did not change.

**Not built: the keyring.** There is no public `0xFF` region, no public part
in any node, and no L1 secret wrapping the root's `S`. There are no grant,
seal, down or back-wrap rows, and no rotation. Instead:

- The signed revision head carries the root's envelope address and `S` in
  the clear, next to the plaintext root.
- Level secrets are values the caller hands in.

So the structure level is open to anyone who can read the head. That is
what a remote needs to copy the tree, but it is weaker than the proposal,
where structure access was a secret that could be rotated.

| | Proposal | Built |
| --- | --- | --- |
| Envelope header | version, flags, g2, g3, public payload | version, range gen, content gen, structure nonce |
| Public data in nodes | keyring rows under `0xFF`, public links down the right edge | none |
| Root's `S` | `FF 00` row in the root's public part, sealed under the L1 secret | the signed head, in the clear |
| L1 secret | yes, rotated alone when a replicator is removed | none: holding `S` is the structure level |
| `S` input | header ‖ ranges ‖ body plaintexts | the node's bytes |
| Structure nonce | fixed zero | synthetic, stored |
| Node format | native split format in `dialog-search-tree` | envelope around the unchanged node |
| Plaintext root | never published | in the head (`Revision::tree`) |
| Spilled values | open question | sealed under `V`, listed as attachments |
| Assets | not covered | sealed in 64 KiB pieces, opt-out per asset |
| Membership, rotation, merge of split heads | keyring rows | not built; secrets are supplied by the caller |
| Fixed bytes per node | 114 (estimate) | 133 |

## Who can do what

| Level | Holds | Can |
| --- | --- | --- |
| none | nothing | store blocks, see their sizes, check each against its address |
| structure (L1) | a root's `S` | walk the tree, check and copy every block, including sealed values |
| range (L2) | plus the range secret | route a key to the leaf that would hold it, read no entry |
| content (L3) | plus the content secret | read and write the tree, its values and its assets |

In code these are `Access::structure()`, `Access::ranges(level)` and
`Access::content(range, content)`. A `Writer` holds the generations new
nodes are sealed under. Each level secret is a `LevelSecret`, named by an
`EpochId`. A party holds every generation it was given, and each envelope
names the generations it needs.

## The envelope

![A layered envelope: a plaintext header, then structure, range and content regions opened by S, R and C](images/sealed-tree/envelope.svg)

Each node of the tree is sealed as one envelope (`layered/envelope.rs`):

```text
version(1) ‖ range generation(32) ‖ content generation(32)
  ‖ structure nonce(12) ‖ structure length(4) ‖ structure region
  ‖ range length(4)     ‖ range region
  ‖ content region
```

- **Structure region**, under `S`:
  - the child count;
  - each child's envelope address and `S`;
  - the attachment count;
  - each attachment's address, i.e. the sealed values the node spills.
- **Range region**, under `R`: each child's separator, length-prefixed.
- **Content region**, under `C`: the node's own bytes, exactly as the tree
  persists them without sealing.

Each region is AES-256-GCM. Its additional data is the header plus a region
label (1, 2, 3). That means:

- relabelling a node with another generation fails to open;
- presenting one region as another fails too.

The header is plaintext because a reader must know which generation to use
before it can open anything.

The envelope's address is `blake3` of every byte. So anyone at any level,
or with no key at all, can check a block against the address that named it.

## Keys

![Key derivation as built: S from the node plaintext and the content secret, R from S and the range secret, C from R and the content secret](images/sealed-tree/derivation.svg)

```text
S = keyed_hash(content_secret, STRUCTURE_DOMAIN ‖ node bytes)   carried by the parent, or the head
R = keyed_hash(range_secret,   RANGE_DOMAIN     ‖ S)
C = keyed_hash(content_secret, CONTENT_DOMAIN   ‖ R)
V = keyed_hash(content_secret, VALUE_DOMAIN     ‖ value hash)    a spilled value
A = keyed_hash(content_secret, ASSET_DOMAIN     ‖ salt)          an asset
```

Why it is shaped this way:

- **`S` comes from the plaintext.** A parent's structure region holds its
  children's `S`, so `S` is part of the parent's bytes and address. Two
  replicas that build the same node must derive the same `S`, or nothing
  above it would converge.
- **`S` is keyed with the content secret.** Otherwise a replicator, which
  holds every `S`, could confirm a guess at a node's contents by hashing it.
- **`C` comes from `R`.** Content access without range access cannot be
  expressed.
- **The range and content regions, and values, use a fixed zero nonce.**
  Each of those keys derives from the one message it encrypts. Two messages
  under one key would need a BLAKE3 collision.
- **The structure region uses a synthetic nonce.** The nonce is
  `keyed_hash(S, NONCE_DOMAIN ‖ header ‖ structure plaintext)[..12]`. The
  same node can link children at different addresses after a rotation, so
  its `S` can encrypt two different structure regions. The tests pin that
  the nonce never repeats across them.

For comparison, the proposal's derivation:

![The proposal's derivation, with L1/L2/L3 secrets](images/sealed-tree/proposal-derivation.svg)

The chain is the same. The proposal fed the regions' plaintexts into `S`.
That would also have made a fixed structure nonce safe, because the child
addresses would have been an input to `S`. We feed in the node's bytes
instead, because they are what the unchanged tree hands to persist.

## One node, two names

![The head carries both roots; the known map pairs each plaintext identity with an envelope address and structure key](images/sealed-tree/two-names.svg)

The search tree still names nodes by plaintext identity. That identity is
what `Revision::tree`, the node cache, diffs and merges use. The archive
holds envelopes under their own hashes. The bridge between the two is a
party's known map, `identity → (address, S)`, in `layered/party.rs`:

- **`admit(identity, root)`**: a revision's head says where its root lives.
- **`locate(identity)`**: where to fetch the envelope of a node the tree
  asks for.
- **`open(identity, bytes)`**: checks the bytes against the expected
  address, then opens the regions this party can open. It learns each
  child's `(address, S)` and each value's address.

So a party reaches exactly what descends from heads it was handed. There is
no other way to learn an address and its key.

What a party learns never displaces a location it already knows. The same
node can exist sealed under two generations, and a replica reads it where
it holds it. A party's own writes (`settle`) do replace what it knew.

The plaintext identity is never stored in the archive. It lives only inside
content regions and in the head.

## Writing: bottom-up, path only

![One edit reseals the leaf, its parent and the root; everything else keeps its envelope](images/sealed-tree/reseal.svg)

A parent records its children's envelope addresses, so children are sealed
first. `Space::seal(blocks, values, root)` walks what a persist staged, from
the root down to nodes it already knows:

- A known child contributes its recorded `(address, S)` and is not sealed
  again.
- The result is a `Sealing`: envelopes (children before parents), sealed
  values, and what will be learned.
- The caller stores every block, then calls `settle`.

Until it settles, the party has learned nothing, so a failed write leaves
it as it was. One insert into a 12-envelope tree reseals 2 envelopes and
shares 10. A root that is already known yields an empty `Sealing`, which
needs no writer. That is how a reader can adopt a head on pull.

## In the repository

![Write path: persist_line, seal, store values, import envelopes, settle and sign; read path: admit, locate, fetch or hydrate, open, learn](images/sealed-tree/repository.svg)

A branch opened with `.sealed(space)` is a sealed line. The space is a
`TreeSpace = Space<Key, State<Datum>, SpilledValues>`. It is built by
`writer_space(writer, access)` or `reader_space(access)`. Clones share what
has been learned.

**The head.** `Revision::sealed: Option<SealedTree { address, structure }>`
is covered by the head signature, so a relay cannot point a head at other
envelopes. The signed payload appends `0x02 ‖ address ‖ structure` when the
field is present. An unsealed head signs exactly the bytes it did before.
`Revision::tree` remains the plaintext root. That one choice kept the change
small: nothing above the archive learned a second kind of root.

**Admission.** Every place that turns a head into a tree first admits it:

- a branch's and a snapshot's head;
- the base, mine and theirs of a merge;
- an upstream head a pull has verified;
- the sync points in the tracking cell, which record their sealed root
  beside their tree, so the next sync can read its base after a restart.

**Writes** (commit, merge and the three pull persists) go through
`persist_line`:

1. Seal everything first. A refused seal writes nothing: `ReadOnly` for a
   reader that has something to seal, `UnknownNode` for a staged node that
   is missing.
2. Write the sealed values to the blob store.
3. Import the envelopes into the catalog. An envelope is an ordinary
   content-addressed block, so `Import` and `Get` work unchanged.

**Reads** go through `LocalIndex` / `NetworkedIndex` `.sealed(space)`:
locate, `Get` by address, open. A local miss hydrates the envelope from the
remote by its own address, so a remote serves sealed blocks exactly as it
serves plain ones.

**Push, export and download** move sealed bytes only:

- Push diffs through the sealed index and uploads each novel node's
  envelope.
- A spilled value crosses as its sealed copy.
- A forwarded subtree is probed and copied by envelope address.
- Export yields envelopes and sealed values.
- Download carries the space.

## Spilled values

A value too large for its key spills out of the leaf. On a sealed line it
is sealed on its own (`layered/value.rs`):

```text
version(1) ‖ content generation(32) ‖ AES-256-GCM(V, nonce 0, value, aad = header)
```

It is stored under the hash of those bytes. The repository's
`SpilledValues` names each value a node spills, and the leaf's envelope
lists their addresses as attachments after its children. So a replicator
copies values with the nodes, and a member finds each value from the leaf
that names it.

## Assets

![A sealed asset: header, then 64 KiB pieces; a range read fetches the header and only the pieces it touches](images/sealed-tree/asset.svg)

Assets added to a sealed line are sealed by default (`layered/asset.rs`):

```text
version(1) ‖ content generation(32) ‖ salt(32) ‖ piece₀ ‖ … ‖ pieceₙ
pieceᵢ = AES-256-GCM(A, nonceᵢ, plainᵢ, aad = header)
nonceᵢ = i (8, big-endian) ‖ last (1) ‖ 0 (3)
```

**Pieces.** Every piece holds 64 KiB of plaintext, except the last. The
nonce names each piece's position and whether it ends the asset, so pieces
cannot be reordered. Cutting a copy short at a piece boundary shows.

**Salt.** The salt is convergent when the content is in hand:
`keyed_hash(content_secret, ASSET_SALT_DOMAIN ‖ hash)`. Sealing the same
asset twice therefore mints nothing new. Streamed content gets a random
salt, because its hash is known only at the end.

**Recording.** The sealed copy is an ordinary blob under its own hash. The
fact `asset:<hash> dialog.asset/sealed` records where it is, with the value
`address ‖ size ‖ length` (48 bytes). It replaces `dialog.asset/size` and
sits inside the sealed tree. Only a party that can read the tree learns
where the copy is, and only one holding the content generation opens it.
The fact must stay inline: a spilled `dialog.asset/sealed` is refused.

**Reads.** A read opens the 65-byte header, then fetches only the sealed
span of the pieces the requested range touches. That span is computed from
the recorded size, without reading anything else. A whole read also checks
that the plaintext hashes to the asset's name (`DigestMismatch`). At commit,
a locally held copy is opened and hashed before it is recorded.

**Opting out.** `Asset::plaintext()`, `AssetStream::plaintext()` and
`Blob::import(..).plaintext()` keep an asset in the clear, for content meant
to be public. The rest of the line refuses mixing the two:

- A stored asset naming plaintext bytes without that mark is refused
  (`PlaintextAsset`).
- A plain line refuses a sealed asset (`SealedAssetOnPlainLine`).

## What each party learns

| Party | Learns | Does not learn |
| --- | --- | --- |
| Storage operator, no head | block sizes, which blocks arrive together | anything about shape, keys or values |
| Anyone who reads a head | the branch, issuer, edition and causal context; the root's `S`, hence the whole tree's shape (node count, links, envelope sizes) and which blocks each commit adds; whether an exact guess of the root node is right, via the plaintext root | separators, keys, values |
| Range holder | separators, i.e. key prefixes, which can include the first bytes of inline values | full keys, values |
| Content holder | everything under the generations it holds | nodes sealed under a generation it lacks |
| Any of the above, about assets | a sealed copy's length: the asset's length + 65 + 16 per piece | its content |

Assets marked `plaintext` and sync records (which stay on their replica)
are the only things kept in the clear by design.

## Costs

Measured with `cargo bench -p dialog-keyring --bench sealing`. The flat
column is whole-node sealing under a blinded address, the earlier
prototype.

| Workload | Plain | Flat | Layered | Layered vs flat |
| --- | ---: | ---: | ---: | ---: |
| Commit 1,000 entries | 870 µs | 920 µs | 956 µs | +3.9% |
| Commit 10,000 entries | 164 ms | 161 ms | 166 ms | +3.3% |
| Insert 100, flushing after each | 1.00 ms | 2.43 ms | 3.57 ms | +47% |
| Insert 1,000, flushing after each | 39.8 ms | 92.9 ms | 114 ms | +23% |
| 64 cold point reads over 10,000 entries | 58.5 µs | 1.19 ms | 1.40 ms | +18% |
| Cold full scan of 10,000 entries | 426 µs | 1.30 ms | 1.53 ms | +17% |
| Stored bytes per node | — | +61 B | +501 B | +1.8% |

- **Batched commits barely notice.** The tree's own work dominates.
- **Cold reads pay twice for links.** A member opens two regions per node.
  It then validates the node once to pair children with addresses, and the
  tree validates it again. Handing the tree the validated node would remove
  the duplicate.
- **Unbatched flushes pay most.** Each flush reprojects and reseals the
  path.
- **Bytes grow with fan-out.** An index node carries 64 bytes per child plus
  its separators, on top of 133 fixed bytes.

The deciding difference from flat sealing is not in the table. Envelopes are
addressed by their own hash, so the archive's content-addressed `Put` and
`Import` take them as they are. Every path that moves blocks moves them
unchanged and can check them with no key. Flat sealing's keyed addresses
could not go in without a keyed put.

## The keyring, as proposed and not yet built

The proposal's other half answers questions the built code leaves to its
caller:

- who holds which level;
- how secrets reach them;
- how secrets rotate.

Its figures are kept here as the direction, not as a description of the
code.

The keyring was to be ordinary tree entries under the largest tag, `0xFF`.
That keeps them at the right edge of the key space, readable by anyone who
can fetch blocks:

![The key space: sealed indexes 00–05, reserved 06–FE, keyring FF at the public right edge](images/sealed-tree/proposal-key-space.svg)

Each node would carry a plaintext public part for those rows, beside the
three sealed regions:

![The proposal's node: a public part, then header, ranges and body opened by S, R and C](images/sealed-tree/proposal-node.svg)

Only one node per level would straddle the boundary. Public links down the
right edge would let a keyless newcomer reach every row:

![A tree whose right edge is public: sealed nodes, one straddling node per level, public nodes](images/sealed-tree/proposal-right-edge.svg)

Level secrets would rotate as epochs:

- Down rows let a party in one group reach the levels below.
- Back wraps on L2 and L3 keep older nodes readable.
- L1 is deliberately not chained, so dropping old L1 secrets gives forward
  secrecy for superseded roots.

![Epochs over time: back wraps on L3 and L2, down rows between levels, L1 rotating alone when a replicator is removed](images/sealed-tree/proposal-chains.svg)

How it would fit what is built:

- **Mostly additive.** The envelope, the key schedule, the known map and
  the repository wiring do not change.
- **The root's `S` moves out of the head.** It would sit under an L1 secret,
  in a keyring row, which brings back a structure level that can be revoked.
- **Generations come from the keyring.** `Access` and `Writer` are already
  the seam where they plug in.
- **A public region needs a public part in the envelope.** It is the one
  format change: a fourth, unsealed region, and an `S` that still excludes
  it.
- **The plaintext root could leave the head.** It would need the node cache
  and readers to key by something other than plaintext identity, which is
  the cost this design avoided.

## Not done

- **Generations are handed out, not agreed.** Nothing mints, stores, delivers
  or rotates level secrets. That is the CGKA's job.
- **The known map lives in memory.** It is rebuilt from the heads a party
  reads in each session.
- **Sealing is per handle.** It is not recorded with the branch, and no
  migration seals an existing plain tree.
- **Sealed asset copies are not envelope attachments.** A replicator that
  only walks structure regions does not copy them; push does. Listing the
  copy's address as an attachment of the leaf that records the
  `dialog.asset/sealed` fact would close this.
- **Push and export read each envelope twice:** once to diff, once to ship.
- **Routing ignores buffered ops.** A range holder routes to the leaf a key
  belongs in without seeing ops buffered in index nodes.
