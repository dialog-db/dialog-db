# One File per Space

[Blocks and Storage](./storage.md) described a space's archive as a map from hash to bytes, kept on a file system as one file per block under `archive/index/`. This chapter is about a second way to keep that same map: one file per space, called `archive.dialog`. It holds every block of the space, and, when the space also names it as its memory, every cell too, branch heads included.

It is opt-in. A `Space` gets it by naming `Chronicle` as its archive, and optionally as its memory. The tree, queries and sync read and write it as they would any archive, and every block reads back as exactly the bytes it was. It runs natively only, for now.

The chapter follows one fact into the file and back out, then walks through what happens when other processes write at the same time, when a peer asks for blocks, and when something goes wrong.

<div class="aside">

**The bytes in this chapter are real.** They come from one run of [`dialog-chronicle/examples/guide_bytes.rs`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-chronicle/examples/guide_bytes.rs), which builds Alice's grocery repository in a file and walks the file record by record after each commit, reading it from its bytes alone:

```sh
cargo run -p dialog-chronicle --example guide_bytes
```

Hashes, keys and the offsets that follow them differ from run to run, since each run's session has a key of its own. The layout does not.

</div>

## Why one file

A tree never changes a node. A commit writes a new version of each node it touches, as a whole block. A replica's history is therefore mostly old versions of the same few nodes, each one a separate file, and each version repeats almost everything the one before it held.

The example's repository is small: seven commits leave nine versions of its nodes. Here they are as nine files, and as what the file holds for them:

{{why}}

The file spends 1,434 bytes on the nine nodes and 5,042 bytes on their 49 tokens, against 68,536 bytes of blocks. Two things make the difference. Every distinct entity, attribute and value is written once, as a **token**, however many keys and versions mention it. And a new version of a node is written as a **frame**: its rows of token ids, mostly copied from the version before.

The gap grows with history. Over two measured histories:

| | a file per block | one file | |
|---|--:|--:|--:|
| Stack Exchange, 4096 commits | 1169 MiB | 13.4 MiB | 87× smaller |
| tonk-team's pushed history | 568.5 MiB | 25.4 MiB | 22× smaller |

<div class="aside">

**Implementations.** `Chronicle` is [`dialog-chronicle/src/archive.rs`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-chronicle/src/archive.rs). The measurements and the design behind them are in [`research/single-file-format`](https://github.com/dialog-db/dialog-db/tree/main/research/single-file-format).

</div>

## Following one fact into the file

Alice's grocery list already has two facts, from her first commit: the `demo.grocery/name` of `item:2` is "Rye bread", and the `demo.grocery/quantity` of `item:1` is "1". Now she commits a third:

*the <span class="chip attribute">demo.grocery/name</span> of <span class="chip entity">item:1</span> is <span class="chip value">"Oat milk"</span>*

This section follows that fact from the commit to the disk and back, one step at a time.

### Before the commit: what is in the file

When Alice's peer created the repository, its file looked like this:

{{opened}}

Every file starts with sixteen bytes of **magic**: `DIALOG`, the byte `b6`, a byte for the format's version, now 1, and eight zeros, reserved. The `b6` is a pilcrow in Latin-1 and is not valid UTF-8 on its own, so a tool that reads the file as text fails at once. A file that starts with anything else is not opened.

{{magic}}

Then come four **slots** of 64 KiB each: two **rolling** slots, which appends write in turn, and two copies of the **durable** slot, which only a sync point writes. A slot says what the file holds: where its records end, the root of the file's own index, which space is free. Most of what follows is about how slots are written and trusted.

After the slots, from byte 262,160, come **records**, appended one after another. A new file already has two. One is an **extent**, a megabyte a writer can use without waiting for anyone (it comes back in [When others are writing too](#when-others-are-writing-too)). It has never been written to, so the file system stores it as a hole: the file is 1.3 MB long but takes 28 KiB of disk. The other is a **cell** record, the repository's `dialog/version`.

### Step 1: the commit makes one new node

The commit edits the tree as [The Search Tree](./tree.md) describes. With buffered writes, a small commit does not reach the leaves: its operations wait in the root's buffer. So the commit produces exactly one new block, a new root:

{{commit_tree}}

The new root's buffer holds 16 operations, six more than before. Four are for Alice's fact: its EAV, AEV and VAE entries and its history entry (see [Keys](./keys.md) and [Commits and History](./history.md)). The other two record the new revision itself. As a block, the new root is 4,372 bytes. It is handed to the archive, and from here on the file decides how to keep it.

### Step 2: the keys become tokens

A tree node is made of keys, and a key is made of parts. The file splits each key into its parts and keeps each distinct part once, as a **token**. A row of the node then names its parts by token id.

Here is the fact's EAV key, split:

{{key_tokens}}

Two of the three parts are already in the file. Commit 1 wrote `item:1` for the quantity, and `demo.grocery/name` for item 2's name. The writer finds them by the hash of their bytes in the file's index, and only names them. Only `"Oat milk"` is new, so it is the only part of this fact that gets written.

A token's place in the file is its id:

{{token_bytes}}

Every token starts on a multiple of 16 bytes, with its length in the four bytes before it, and its id is its offset divided by 16. A reader that has an id has everything it needs: it reads four bytes back for the length, then the bytes. Nothing in memory has to map ids to places.

A value's token is the value's type byte and payload, here `03` for a string, so a value is one token in every ordering. The entity and attribute tokens include the zero byte that ends them in a key. Each of the fact's four keys names the same three tokens, and the history key adds one more for the version:

{{shared_tokens}}

The commit's five new tokens go into one **tokens record**: `"Oat milk"`, the version, the revision's entity and value, and the encoded state an entry maps to (one token per distinct `State<Datum>`, read in place as an archive, which is why tokens are aligned).

### Step 3: the node becomes a frame

The new root is written as a **frame**: its rows, naming tokens by id, written against the version of the root it replaces. The frame's data starts on a multiple of 16 too, so a frame also has an id, and it starts with a header:

{{frame_header}}

The hash comes first, so a parent read in place takes its child's name from where the child's frame starts. The kind says whether the node is a leaf (0), an index node (1), or a block kept whole (`ff`). The base names the frame this one is written against, and the depth says how many frames lie behind it.

An index node has three sections of rows: its links to children, its buffers, and the operations in them. Each section is written against the same section of the base. Here are the operations, with commit 1's root on the left:

{{ops_against}}

Rows are sorted by key in both versions, so the new frame is mostly copies: ten rows are copied, in six runs, and six are new. A copy is written as its length and where it starts, relative to where the last one ended, which is usually zero.

The six new rows are then written column by column:

{{columns}}

Neighboring rows mostly share their parts, so each column is written as the difference from the row above, and a run of equal values is written once. The whole section of 16 operations takes 76 bytes. The frame's data is 126 bytes, header included, for a block of 4,372.

A frame stands for its block exactly. The tree's own encoding is canonical, so the rows of a node dialog wrote rebuild its block byte for byte, and before writing a frame the archive rebuilds the block and compares. A block that parses as a node but is not canonical (padded, say, or in a layout its keys would not choose), a node in an older layout, and a block that is not a tree node are kept whole, as a frame of kind `ff` with the block's bytes after the header.

<div class="aside">

**Implementations.** Splitting a key is `key_tokens` in [`dialog-chronicle/src/node.rs`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-chronicle/src/node.rs). Tokens and their records are [`tokens.rs`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-chronicle/src/tokens.rs). Rows, columns and copies are [`rows.rs`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-chronicle/src/rows.rs), and the frame header is `FrameHeader` in [`archive/index.rs`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-chronicle/src/archive/index.rs).

</div>

### Step 4: the turn, the records and the slot

Splitting and encoding happen before the writer touches the file. Writing needs the writer's **turn**, so that two writers never write the same bytes. The writer takes it, lays the records out after the newest slot's end, writes them, and then writes a slot that names the new end.

Every record has the same nine-byte header:

{{record_header}}

| kind | record | its checksum covers |
|---|---|---|
| 4 | tokens: a count, then each token's pad byte, padding, length and bytes | the whole body |
| 5 | frame: a pad byte, padding, then the frame's data | the whole body |
| 7 | pages: pages of the file's index | until a page is reused; each page has its own |
| 8 | extent: space claimed by one writer | its header only; each frame inside has its own |
| 9 | cell: one value of a memory cell | the whole body |

The commit's append is the tokens record and the frame record. The new head follows as two cell records, `revision` and `induction` (see [step 5](#step-5-the-head-moves)), each its own append:

{{append_layout}}

A slot holds the file's whole state as of one append:

{{slot_layout}}

An append writes the older of the two rolling slots. That way one of them is always whole, whatever a crash does to the other:

{{rolling}}

The turn itself is not a lock on the file. It is a lock on one byte, far past the end of any data:

{{locks}}

Readers take no lock at all. The writer holding the turn waits on no other lock, so no cycle of waiting runs through it.

<div class="aside">

**Implementations.** Records are [`dialog-chronicle/src/file.rs`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-chronicle/src/file.rs), slots [`archive/slot.rs`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-chronicle/src/archive/slot.rs), and the lock bytes [`locks.rs`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-chronicle/src/locks.rs).

</div>

### Step 5: the head moves

The branch's head is a cell (see [Commits and History](./history.md)), and the file keeps cells the way SQLite keeps commits in its write-ahead log. An update appends a cell record holding the cell's name and its new value, and the cells' own index, rooted in the slot, files the name at its newest record. Nothing is written over.

{{cell_record}}

The name is the cell's memory space, `branch/main`, a zero byte, and the cell, `revision`. The value is the new revision, which names the new root by its hash: `45 a7 06 40 1b …` sits right in its bytes. The commit also moves a second cell, `induction`, the revision through which the branch's rules have run.

The order of the records is what keeps heads safe:

{{order}}

<div class="aside">

**Implementations.** Cells are [`dialog-chronicle/src/archive/cells.rs`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-chronicle/src/archive/cells.rs). A cell's edition is the BLAKE3 hash of its value, as on the file system store, so a compare-and-swap names the same version either way.

</div>

### Step 6: a sync point makes it durable

Until now, everything Alice wrote went to the operating system, not necessarily to the disk. A process that crashes loses nothing the operating system has. A power loss can lose any of it, in any order. A **sync point** draws the line:

{{sync_point}}

The **durable slot** is written only here. Nothing the durable slot reaches is ever written over, so after a power loss the file can always go back to it. In the example, the sync point after commit 3 made slot 12 durable.

A slot can span more than one sector, and a power loss during its write can keep some sectors and lose others. So the durable slot has two copies, and a sync point writes over the copy that does not hold the newest whole durable slot: a sync torn by a power loss leaves the other copy whole. Opening reads the newer copy that is whole. The last 8 bytes of each copy's 64 KiB hold the sequence of the copy last synced there, written after the fsync:

{{durable}}

### Step 7: reading it back

A query for item 1's name walks the tree as [The Search Tree](./tree.md) describes. Each node it needs, it asks the archive for by hash, and the file answers from its own index:

{{lookup}}

The file's index is itself a small tree, from 32-bit keys to places in the file, in three key spaces: blocks by hash, tokens by the hash of their bytes, and **lineages**, the latest frame of each node, which the next version is written against. Its root lives in the slot. In the example it is a single leaf of 25 entries after commit 2, so finding the frame reads nothing but the slot.

The node is then read **in place**: the archive decodes the frame's rows once, and the tree reads keys and values where their tokens sit, through a mapping of the file. A value is read as an archive where it sits, checked the first time a node that holds it is read. No block is built, hashed or decoded on the way.

<div class="aside">

**Implementations.** The file's index is [`dialog-chronicle/src/archive/tree.rs`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-chronicle/src/archive/tree.rs), and what it maps is [`archive/index.rs`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-chronicle/src/archive/index.rs). A node read in place is [`archive/served.rs`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-chronicle/src/archive/served.rs), through `NodeSource` and the node views in [`dialog-search-tree/src/node/view.rs`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-search-tree/src/node/view.rs).

</div>

## Changing the name

Alice buys the bigger carton and changes the name to `"Oat milk, 2 L"`. Under `Pick::Last`, the commit retracts the old claim and asserts the new one, so the root's buffer gains both:

{{edit_rows}}

The new root is written against commit 2's, which was written against commit 1's. Each version of a node is one more frame in a chain:

{{chain}}

<div class="aside">

**Implementations.** The chain's limit is `CHAIN` in [`dialog-chronicle/src/archive.rs`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-chronicle/src/archive.rs), and lineages are `lineage_key` in [`archive/index.rs`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-chronicle/src/archive/index.rs).

</div>

## When others are writing too

Any number of processes can have the file open: Alice's app, a sync daemon, a command-line tool. They share the file through the slots and the lock bytes, and nothing else.

### Bob's commit while Alice's process holds the turn

A writer holds the turn only while it lays out and writes its records. Even so, a writer that finds the turn taken does not wait. It writes its blocks whole into an **extent**, space it claimed earlier, in a turn of its own. In the example, a helper process plays Alice's and holds the turn, while the example's own peer plays Bob's and commits *the `demo.grocery/name` of `item:3` is "Eggs"*:

{{two_processes}}

An extent is a record whose body is written later, one frame at a time:

{{extent_bytes}}

The block is readable at once, by Bob's process and by any other: a reader that misses in the index reads the extents the slot lists. The head is another matter. It must come after what it names, so it waits for the turn. When it lands, its record names the extent, epoch and byte count it needs, with a CRC-32 of those bytes, and recovery keeps that head only if the extent holds whole frames that far and their bytes match the CRC. A power loss can keep an extent's new count and lose a frame written under it, and the CRC tells the frame this claim wrote from one an earlier claim of the extent left in the same place.

The next writer to hold the turn, usually the same one at its next commit, **folds** the extent in: its blocks become tokens and frames, and the extent is retired. Here the 7,996-byte block became a 122-byte frame. A retired extent is free to claim again after the next sync point. A process that writes keeps one extent ready for itself, and a process that opens the file while the turn is free leaves one that no process owns, for a process that has none. So even a process that has never held the turn can write without waiting.

### Two writers on one branch

Bob's commit and Alice's went to the same file but could have gone to different branches. When two writers move the same branch, the head's compare-and-swap decides:

{{cas}}

`Branch::transact` takes the code that builds the transaction, not the transaction, so a lost race runs that code again over the head that won, and what it read is read again. The losing try's root had already been written to the file, as frame 213,645. No head names it, and it stays there.

### A reader while a write is under way

{{reader}}

The file's index is the one thing written over: its pages are reused once no durable slot reaches them. A page records which append wrote it, and its parent names it by page and append. In the example, appending 2,100 small blocks at once filled the root's buffer, and the index grew its first pages:

{{page}}

<div class="aside">

**Implementations.** Extents are [`dialog-chronicle/src/archive/extent.rs`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-chronicle/src/archive/extent.rs). `Branch::transact` is [`dialog-repository/src/repository/branch/transaction/transact.rs`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-repository/src/repository/branch/transaction/transact.rs). The design, and the cases it is tested against with real processes, are in [`design/file-writers.md`](https://github.com/dialog-db/dialog-db/blob/main/design/file-writers.md) and [`archive/concurrency.rs`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-chronicle/src/archive/concurrency.rs).

</div>

## Serving blocks to a peer

Replicas exchange blocks, not frames (see [Sync](./sync.md)). When a peer asks for a block, the file builds it:

{{serve}}

A peer checks every block it receives against its hash, as [Blocks and Storage](./storage.md) describes, and it never knows the block came from a frame.

## When something goes wrong

### A process dies, or the power goes out

Opening a file is where a crash is noticed. A process holds a recovery byte while it opens, so no other process opens from a state being repaired, and decides which slot to trust:

{{recovery}}

Whether this works is tested, not argued. The tests record every write the archive makes, then build what a disk could hold had the power gone after any of them:

{{torn}}

### The last process closes

{{close}}

<div class="aside">

**Implementations.** Opening is `recover` in [`dialog-chronicle/src/archive.rs`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-chronicle/src/archive.rs). The power-loss tests are [`archive/power.rs`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-chronicle/src/archive/power.rs), over the recording disk in [`file/disk.rs`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-chronicle/src/file/disk.rs).

</div>

## What it does not do yet

<div class="aside caveat">

- **Native only.** The crate compiles to nothing on the web. Its tests run on Linux, macOS and Windows. Whether the same file can live in a browser's file system, served by one worker a tab hosts, is being measured in [`research/single-file-format/browser`](https://github.com/dialog-db/dialog-db/tree/main/research/single-file-format/browser), not built.
- **Opt-in.** Nothing switches a replica to it. A replica the file system store wrote can be moved in with the `chronicle` migration of the `dialog-migration` crate.
- **Nothing is collected.** A node no head reaches any more, such as a losing try's, stays in the file.
- **The format is still changing.** A file written now may not open in a later release.

</div>

## Reference

All integers are little-endian. *Varint* is LEB128, *zigzag* maps signed to unsigned before it, and an *id* is an offset in the file divided by 16.

| what | where | layout |
|---|---|---|
| magic | 0 | 16 bytes: `DIALOG`, the byte `b6`, the format version (1), then eight zeros, reserved |
| rolling slots | 16 and 65,552 | 64 KiB each: length u32, crc32 u32, body |
| durable slot | 131,088 and 196,624 | two copies, the same layout, written only at a sync point; the last 8 bytes of each 64 KiB hold the sequence of the copy last synced there |
| records | from 262,160 | kind u8, length u32, crc32 u32, body |
| lock bytes | from 2⁶²−3 | sync, recovery, present, turn, then an owner byte per extent |

| record | body |
|---|---|
| tokens (4) | count varint; per token: pad u8, padding, length u32, bytes (start on a multiple of 16) |
| frame (5) | pad u8, padding, then data on a multiple of 16: hash (32), kind u8, depth u8, catalog varint, base varint (frame id + 1, or 0), manifest varint (token id), then the rows; or, for kind `ff`, length varint and the block |
| pages (7) | padding length u16, padding, pages of 4,096 bytes on 4,096-byte boundaries |
| extent (8) | pad u8 to 8 bytes, count u64, its inverse u64, epoch u64, then frame records |
| cell (9) | pad u8, padding, then on a multiple of 16: name length u16, name, present u8, value length u32, value, needs count u16, per need record u64, epoch u64, bytes u64, crc32 of those bytes u32 |

| slot field | size |
|---|---|
| sequence, end, durable, boot | u64 each |
| counts: blocks, block bytes, frames, frames written against a base, frame bytes, tokens, token bytes | u64 each |
| catalogs | count varint; per catalog: length u8, name |
| the index's root | length u32, node |
| pages free, pages freed since the sync point | count varint, u32 each |
| extents held, free, freed since the sync point | count varint, u64 each |
| the cells' index root | length u32, node |

| rows | columns |
|---|---|
| a section | row count varint; against a base: step count varint, then per step either `n << 1` (n new rows) or `n << 1 \| 1` and the start as zigzag (copy n); then the new rows by column |
| entries and operations | form, then slots 0 to 3 as deltas (three slots for EAV, AEV and VAE, four for history and coverage, one for any other key), an asserted flag for operations, then the datum token as a delta |
| links | separator token as a delta, then scale << 1 \| no-frame, then the child frame or the child's hash token as deltas |
| buffers | child index and operation count, varints |
| a column | runs: a zigzag count, then one varint repeated, or that many varints |

| index | value |
|---|---|
| key | two bits of space (blocks 0, tokens 1, lineages 2), then thirty bits of a hash |
| value | the id of a frame or a token |
| page | append u32, crc32 u32, kind u8 (0 leaf, 1 internal), children u8, entries u16, 4 unused, then entries (key u32, value u32) or pivots, children (page u32, append u32) and buffered entries |
| limits | a leaf holds 510 entries, an internal node 8 children, the root's buffer 2,048 entries |

| setting | value |
|---|---|
| extent size | 1 MiB (`Options::extent`) |
| sync point | every 64 appends, 2 seconds, or 2,048 freed index pages; after 2 seconds a thread in each process makes one even if no append follows |
| blocks in one append | at most 32,768 (`COMMIT_BLOCKS`); a larger commit is appended in parts |
| longest chain of frames | 32 (`CHAIN`) |
