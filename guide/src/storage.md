# Blocks and Storage

A node, once written, never changes. That makes naming it easy: a node is named by the hash of its bytes.

<figure class="dg">
<svg class="dg" viewBox="0 0 650 116" width="650" height="116" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Node bytes go through BLAKE3 and the hash becomes the node&#x27;s name">
<defs><marker id="s-arrow" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto"><path d="M0,0 L8,4 L0,8 z"/></marker></defs>
<rect class="shade" x="10" y="30" width="170" height="44"/>
<text class="label" x="95" y="50" text-anchor="middle">node bytes</text>
<text class="small muted" x="95" y="66" text-anchor="middle">02 00 03 05 … body</text>
<line x1="180" y1="52" x2="226" y2="52" marker-end="url(#s-arrow)"/>
<rect class="hash solid" x="230" y="30" width="110" height="44"/>
<text class="title" x="285" y="57" text-anchor="middle" style="fill:var(--dg-paper)">BLAKE3</text>
<line x1="340" y1="52" x2="386" y2="52" marker-end="url(#s-arrow)"/>
<rect class="hash" x="390" y="30" width="250" height="44"/>
<text class="hash" x="515" y="50" text-anchor="middle">32-byte hash</text>
<text class="small muted" x="515" y="66" text-anchor="middle">written in base58: 8Ea3Qm…</text>
<text class="label small muted" x="325" y="104" text-anchor="middle">The hash is the node's name. Same bytes, same name, on every device.</text>
</svg>
</figure>

The hash is BLAKE3, 32 bytes, unkeyed, taken over the whole node, prelude included. Anything stored this way is called a block, and tree nodes are blocks.

Bytes that are not nodes are named the same way, by their BLAKE3 hash, but they are kept apart in a blob store, which can stream them rather than handling them as whole buffers. Two things live there. One is the spilled values from the [Keys](./keys.md) chapter. The other is assets: content such as photos that an app stores next to its facts.

Naming by hash has three useful consequences:

- **A name can be checked.** Whoever hands you a block, you can hash it and see whether it is the block you asked for. Dialog checks every tree node it reads and refuses bytes whose hash does not match. So a node can come from anywhere, including a server that is not trusted, without that server being able to change it.
- **Equal blocks are stored once.** Two trees that share a subtree share its blocks. Writing a block that already exists is harmless: it can only write the same bytes again.
- **A root names a whole tree.** A root holds its children's hashes, which hold theirs. Handing someone one 32-byte root hands them a name for every fact beneath it.

## Assets

An app stores a photo by asserting it in a transaction, like a fact. The commit writes the photo's bytes to the blob store and records one fact about them in the same revision: the `dialog.asset/size` of the entity `asset:{hash}` is the number of bytes. That entity is the photo's name, and the app's own facts can point at it, such as *the `demo.grocery/photo` of `item:2` is `asset:…`*.

The fact is what says a branch holds the asset. A push sends the bytes of every asset whose fact is new, a read fetches only bytes that such a fact vouches for, and retracting the fact drops the branch's hold on the bytes. Only a commit that stores the bytes can write the fact, since it is in a `dialog.` domain, so the size it states is measured, not claimed.

## Two kinds of storage

For its data, a repository keeps two kinds of things:

- **The archive** holds blocks and blobs. It is a map from hash to bytes. Entries are only ever added, and a name always means the same bytes.
- **Memory** holds cells. A cell is a small named value that can change, like the current head of a branch (see [Commits and History](./history.md)). A cell is updated with compare-and-swap: a writer says what it believes the cell holds now, and the update fails if someone else changed it first.

Almost everything is in the archive. A handful of cells point into it.

## Where blocks live

The same archive runs on several backends. Tree nodes go in a catalog named `index`, and a block's key is its hash written in base58:

| Backend | Where a tree node lives |
|---|---|
| Native file system | `{repository's directory}/archive/index/{base58 hash}` |
| Browser IndexedDB | object store `archive/index`, key `{base58 hash}` |
| Browser file system (OPFS, opt-in) | the same path layout as native |
| S3 or R2 | object `{repository DID}/index/{base58 hash}` |
| In memory | a map, for tests |

Blobs sit beside them: `{repository's directory}/blob/{base58 hash}` on a file system, and `{repository DID}/blob/{base58 hash}` on S3 or R2. In a browser, blobs always go to OPFS, even when blocks and cells are in IndexedDB.

The file system backend writes each block atomically, and skips the write when the file already exists, since a block with that name can only have those bytes.

## Writing: one import per commit

While a commit edits the tree, the new nodes it produces collect in memory, and so do the values it spills. When the commit is sealed, the spilled values are written to the blob store, each checked against its hash, and then every new node is written to the archive in one import. Only after that can the branch's head move to the new root. A head can therefore never point at a root whose blocks are not yet stored.

## Reading: fetch on demand

A replica does not need every block of a tree to use it. When a read reaches a node that is not in the local archive, and the branch has a remote peer, Dialog fetches that one block from the remote peer and keeps a copy:

<figure class="dg">
<svg class="dg" viewBox="0 0 660 234" width="660" height="234" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="A read misses locally, fetches from the remote peer, keeps a copy, and the reader checks the hash">
<defs><marker id="f-arrow" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto"><path d="M0,0 L8,4 L0,8 z"/></marker></defs>
<text class="title" x="80" y="18" text-anchor="middle">tree reader</text>
<line class="dashed" x1="80" y1="26" x2="80" y2="226"/>
<text class="title" x="320" y="18" text-anchor="middle">local archive</text>
<line class="dashed" x1="320" y1="26" x2="320" y2="226"/>
<text class="title" x="560" y="18" text-anchor="middle">remote peer</text>
<line class="dashed" x1="560" y1="26" x2="560" y2="226"/>
<line x1="80" y1="52" x2="316" y2="52" marker-end="url(#f-arrow)"/>
<text class="small" x="198.0" y="46" text-anchor="middle">get 8Ea3Qm…</text>
<text class="small muted" x="328" y="76">not here</text>
<line x1="320" y1="100" x2="556" y2="100" marker-end="url(#f-arrow)"/>
<text class="small" x="438.0" y="94" text-anchor="middle">get index/8Ea3Qm…</text>
<line x1="560" y1="136" x2="324" y2="136" marker-end="url(#f-arrow)"/>
<text class="small" x="442.0" y="130" text-anchor="middle">bytes</text>
<text class="small muted" x="328" y="158">keep a copy</text>
<line x1="320" y1="180" x2="84" y2="180" marker-end="url(#f-arrow)"/>
<text class="small" x="202.0" y="174" text-anchor="middle">bytes</text>
<text class="small hash" x="92" y="206">blake3(bytes) = 8Ea3Qm…?</text>
<text class="small muted" x="92" y="220">if not, refuse them</text>
</svg>
</figure>

So a phone can open a large shared repository by fetching its root, then mostly the nodes its queries walk through, plus some that a query fetches ahead of where it is reading. The hash check happens in the tree reader, which is why a remote peer can be any dumb store of bytes. Spilled values are fetched the same way, from the blob store, and checked the same way.

Fetches are scheduled, not fired one by one. Readers that want the same block share one fetch, and each remote peer serves a limited number of requests at a time, with a read that a query is waiting on going ahead of one made on speculation.

<div class="aside">

**Implementations.** Hash-checked reads are `LoadBlock` in [`dialog-search-tree/src/load.rs`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-search-tree/src/load.rs) for tree nodes, and `LoadBlob` in [`dialog-artifacts/src/archive.rs`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-artifacts/src/archive.rs) for spilled values. Assets are [`dialog-repository/src/repository/branch/asset.rs`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-repository/src/repository/branch/asset.rs). The local backends are under [`dialog-storage/src/storage/provider`](https://github.com/dialog-db/dialog-db/tree/main/rust/dialog-storage/src/storage/provider), and the S3 and R2 remote peer is [`dialog-remote-s3`](https://github.com/dialog-db/dialog-db/tree/main/rust/dialog-remote-s3). Fetching from a remote peer on a local miss is `NetworkedIndex` in [`dialog-repository/src/repository/archive/networked.rs`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-repository/src/repository/archive/networked.rs), and the scheduling is in [`dialog-peer/src/peer/hydrate.rs`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-peer/src/peer/hydrate.rs).

</div>
