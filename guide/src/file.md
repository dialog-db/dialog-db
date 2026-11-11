# One File per Space

[Blocks and Storage](./storage.md) described a space's archive as a map from hash to bytes, and on a file system that map is a directory: one file per block under `archive/index/`. This chapter describes a second way to keep the same map, in one file per space. It is opt-in: a `Space` that names `FileArchive` as its archive keeps every block in a file called `archive.dialog` in the space's directory, and one that also names it as its memory keeps its cells there too, branch heads included. Nothing else changes. The tree, queries and sync read and write it as they do any archive, and every block reads back as exactly the bytes it was.

It runs natively only, for now: in a browser, blocks stay where [Blocks and Storage](./storage.md) puts them (see the end of the chapter).

## Why one file

A tree never changes a node: a commit writes new versions of the nodes it touches, each a whole block. So a replica's history is mostly old versions of the same nodes, and each one costs a file. Two measured histories:

| | a file per block | one file | |
|---|--:|--:|--:|
| Stack Exchange, 4096 commits | 1169 MiB | 13.4 MiB | 87× smaller |
| tonk-team's pushed history | 568.5 MiB | 25.4 MiB | 22× smaller |

The saving comes from two things a file per block cannot do. A value, an entity or an attribute that many nodes and many versions mention is written once, and a new version of a node is written as the difference from the version before it. The rest of this chapter is how the file does that while staying readable in place, by many processes at once, and safe through a power loss.

<div class="aside">

**Implementations.** `FileArchive` is [`dialog-archive-file/src/archive.rs`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-archive-file/src/archive.rs). The measurements are in [`research/single-file-format`](https://github.com/dialog-db/dialog-db/tree/main/research/single-file-format), with the design in its `design.md`.

</div>

## The file's layout

The file starts with eight bytes of magic, `DIALOGA` followed by the format's version byte. Then come three slots of 64 KiB each, then records, appended one after another to the end:

<figure class="dg">
<svg class="dg" viewBox="0 0 680 176" width="680" height="176" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="The file: magic, two rolling slots, a durable slot, then records; a record is a kind, a length, a checksum and a body">
<defs><marker id="fl-arrow" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto"><path d="M0,0 L8,4 L0,8 z"/></marker></defs>
<text class="label small" x="198" y="16" text-anchor="middle">the head of the file</text>
<path class="wire" d="M11,22 v-4 H385 v4"/>
<text class="label small" x="529" y="16" text-anchor="middle">records, appended</text>
<path class="wire" d="M389,22 v-4 H669 v4"/>
<rect class="hash" x="10" y="28" width="70" height="44"/>
<text class="small" x="45" y="48" text-anchor="middle">magic</text>
<text class="small muted" x="45" y="63" text-anchor="middle">8 bytes</text>
<rect class="shade" x="82" y="28" width="100" height="44"/>
<text class="small" x="132" y="48" text-anchor="middle">rolling slot</text>
<text class="small muted" x="132" y="63" text-anchor="middle">64 KiB</text>
<rect class="shade" x="184" y="28" width="100" height="44"/>
<text class="small" x="234" y="48" text-anchor="middle">rolling slot</text>
<text class="small muted" x="234" y="63" text-anchor="middle">64 KiB</text>
<rect class="shade" x="286" y="28" width="100" height="44"/>
<text class="small" x="336" y="48" text-anchor="middle">durable slot</text>
<text class="small muted" x="336" y="63" text-anchor="middle">64 KiB</text>
<rect x="390" y="28" width="52" height="44"/>
<text class="small" x="416" y="54" text-anchor="middle">tokens</text>
<rect x="444" y="28" width="48" height="44"/>
<text class="small" x="468" y="54" text-anchor="middle">frame</text>
<rect x="494" y="28" width="48" height="44"/>
<text class="small" x="518" y="54" text-anchor="middle">frame</text>
<rect x="544" y="28" width="48" height="44"/>
<text class="small" x="568" y="54" text-anchor="middle">pages</text>
<rect x="594" y="28" width="38" height="44"/>
<text class="small" x="613" y="54" text-anchor="middle">cell</text>
<rect class="ghost" x="634" y="28" width="36" height="44"/>
<text class="small muted" x="652" y="54" text-anchor="middle">…</text>
<line class="dashed" x1="444" y1="74" x2="150" y2="104"/>
<line class="dashed" x1="492" y1="74" x2="630" y2="104"/>
<rect class="shade" x="150" y="106" width="70" height="34"/>
<text class="small" x="185" y="127" text-anchor="middle">kind</text>
<rect class="shade" x="222" y="106" width="100" height="34"/>
<text class="small" x="272" y="127" text-anchor="middle">length</text>
<rect class="hash" x="324" y="106" width="100" height="34"/>
<text class="small" x="374" y="127" text-anchor="middle">crc32</text>
<rect x="426" y="106" width="204" height="34"/>
<text class="small" x="528" y="127" text-anchor="middle">body</text>
<text class="label small muted" x="185" y="158" text-anchor="middle">1 byte</text>
<text class="label small muted" x="272" y="158" text-anchor="middle">4 bytes</text>
<text class="label small muted" x="374" y="158" text-anchor="middle">4 bytes, of kind and body</text>
<text class="label small muted" x="528" y="158" text-anchor="middle">length bytes</text>
</svg>
</figure>

**Records** are the file's contents. Each is a kind, its body's length and a CRC-32 checksum, so a reader can walk the file record by record and tell a whole record from one a crash cut short. There are five kinds:

| kind | holds |
|---|---|
| tokens | the distinct pieces keys and values are made of |
| frame | a tree node as rows of token ids, or a block kept whole |
| pages | pages of the file's index |
| extent | space one writer claimed, to append to without waiting |
| cell | one update of a memory cell, such as a branch head |

Two kinds are written to after they are appended, pages and extents, so their own checksum holds only until then. What is inside each carries a checksum of its own.

**Slots** say what the file holds. A slot is the file's state as of one append: a sequence number, where the records end, the root of the file's index, the catalogs' names, which pages and extents are free, and the root of the cells' index. Every append writes its records past the end, and then writes the older of the two rolling slots. Opening reads both and takes the newer one that is whole. So one rolling slot is always whole, whatever an interrupted append left.

The third slot is the **durable** one. It is written only at a sync point, once everything before it is on the disk, and it is synced itself. Nothing the durable slot reaches is ever written over, so after a power loss the file can always go back to it.

<div class="aside">

**Implementations.** Records are [`dialog-archive-file/src/file.rs`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-archive-file/src/file.rs), and slots are [`archive/slot.rs`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-archive-file/src/archive/slot.rs).

</div>

## Tokens

The [Keys](./keys.md) chapter showed that a key is made of its parts: an EAV key is the entity, then the attribute, then the value. The same entity appears in every key about it, in all three orderings and in every version of every node that holds them. The file writes each distinct part once, as a **token**, and a node names its parts by token id:

<figure class="dg">
<svg class="dg" viewBox="0 0 680 236" width="680" height="236" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="An EAV key split into its parts; a row of token ids names them; each token is in the file once, at a multiple of 16">
<defs><marker id="tk-arrow" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto"><path d="M0,0 L8,4 L0,8 z"/></marker></defs>
<text class="label small" x="10" y="16">an EAV key</text>
<rect class="shade" x="10" y="24" width="34" height="34"/>
<text class="small" x="27" y="45" text-anchor="middle">00</text>
<rect class="entity" x="46" y="24" width="110" height="34"/>
<text class="entity" x="101" y="45" text-anchor="middle">item:1</text>
<rect class="attribute" x="158" y="24" width="200" height="34"/>
<text class="attribute" x="258" y="45" text-anchor="middle">demo.grocery/name</text>
<rect class="value" x="360" y="24" width="130" height="34"/>
<text class="value" x="425" y="45" text-anchor="middle">"Oat milk"</text>
<line x1="250" y1="62" x2="250" y2="92" marker-end="url(#tk-arrow)"/>
<text class="label small muted" x="260" y="82">split into parts</text>
<text class="label small" x="10" y="108">the entry's row: ids, 4 bytes each</text>
<rect class="shade" x="10" y="116" width="62" height="34"/>
<text class="small" x="41" y="137" text-anchor="middle">form 00</text>
<rect class="entity" x="74" y="116" width="62" height="34"/>
<text class="entity" x="105" y="137" text-anchor="middle">41</text>
<rect class="attribute" x="138" y="116" width="62" height="34"/>
<text class="attribute" x="169" y="137" text-anchor="middle">7</text>
<rect class="value" x="202" y="116" width="62" height="34"/>
<text class="value" x="233" y="137" text-anchor="middle">43</text>
<rect class="hash" x="266" y="116" width="62" height="34"/>
<text class="small" x="297" y="137" text-anchor="middle">88</text>
<text class="label small muted" x="297" y="166" text-anchor="middle">its state</text>
<text class="label small" x="380" y="108">tokens in the file</text>
<rect class="attribute" x="380" y="116" width="290" height="24"/>
<text class="small" x="390" y="132">at 112 = 7 × 16</text>
<text class="small attribute" x="660" y="132" text-anchor="end">demo.grocery/name</text>
<rect class="entity" x="380" y="142" width="290" height="24"/>
<text class="small" x="390" y="158">at 656 = 41 × 16</text>
<text class="small entity" x="660" y="158" text-anchor="end">item:1</text>
<rect class="value" x="380" y="168" width="290" height="24"/>
<text class="small" x="390" y="184">at 688 = 43 × 16</text>
<text class="small value" x="660" y="184" text-anchor="end">"Oat milk"</text>
<rect class="hash" x="380" y="194" width="290" height="24"/>
<text class="small" x="390" y="210">at 1408 = 88 × 16</text>
<text class="small" x="660" y="210" text-anchor="end">State&lt;Datum&gt;, archived</text>
<text class="label small muted" x="10" y="228">The ids are made up for the picture. A token's id is where it starts, divided by 16.</text>
</svg>
</figure>

Every token starts at a multiple of 16 bytes, with its length in the four bytes before it, so a token's id is simply where it is: its offset divided by 16. Nothing in memory has to say where tokens are. A reader maps the file and finds any token from its id alone.

A history or coverage key splits the same way, into its version and then the same entity, attribute and value, so a record of a change shares its fact's tokens, and every record of one revision shares one version. A value is one token, its type byte and payload together. An entry's state, the `State<Datum>` it maps to, is one token too: an archive that is read where it sits, which is why tokens are aligned. A key of any other shape is one opaque token.

<div class="aside">

**Implementations.** Tokens are [`dialog-archive-file/src/tokens.rs`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-archive-file/src/tokens.rs), and splitting a key is `key_tokens` in [`node.rs`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-archive-file/src/node.rs).

</div>

## Frames

A node of the tree is written as a **frame**: its entries as rows of token ids, the way the picture above showed one entry. Rows are written column by column, so all the entities of a leaf sit together, then all the attributes, and each column is written as small differences from the row before. Sorted keys share most of their parts with their neighbours, so those differences are mostly zero.

A frame is usually also written against the version of the node it replaces. A commit that changes a few entries of a leaf of a hundred makes a new version that is nearly the old one, so its frame copies runs of rows from the old version's and writes only the rows that are new:

<figure class="dg">
<svg class="dg" viewBox="0 0 680 202" width="680" height="202" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Versions of a node: the first written whole, each next one against the one before, and every 32nd whole again; a frame's header and rows">
<defs><marker id="fr-arrow" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto"><path d="M0,0 L8,4 L0,8 z"/></marker></defs>
<rect class="hash" x="10" y="20" width="96" height="44"/>
<text class="small" x="58" y="40" text-anchor="middle">version 1</text>
<text class="small muted" x="58" y="55" text-anchor="middle">whole</text>
<rect x="130" y="20" width="96" height="44"/>
<text class="small" x="178" y="40" text-anchor="middle">version 2</text>
<text class="small muted" x="178" y="55" text-anchor="middle">against 1</text>
<rect x="250" y="20" width="96" height="44"/>
<text class="small" x="298" y="40" text-anchor="middle">version 3</text>
<text class="small muted" x="298" y="55" text-anchor="middle">against 2</text>
<text class="small muted" x="380" y="46" text-anchor="middle">…</text>
<rect x="414" y="20" width="96" height="44"/>
<text class="small" x="462" y="40" text-anchor="middle">version 32</text>
<text class="small muted" x="462" y="55" text-anchor="middle">against 31</text>
<rect class="hash" x="574" y="20" width="96" height="44"/>
<text class="small" x="622" y="40" text-anchor="middle">version 33</text>
<text class="small muted" x="622" y="55" text-anchor="middle">whole again</text>
<line x1="130" y1="42" x2="110" y2="42" marker-end="url(#fr-arrow)"/>
<line x1="250" y1="42" x2="230" y2="42" marker-end="url(#fr-arrow)"/>
<line x1="364" y1="42" x2="350" y2="42" marker-end="url(#fr-arrow)"/>
<line x1="414" y1="42" x2="396" y2="42" marker-end="url(#fr-arrow)"/>
<text class="label small muted" x="340" y="86" text-anchor="middle">Reading version 3 decodes 1, then 2, then 3. No read decodes more than 32 frames.</text>
<text class="label small" x="10" y="118">a frame</text>
<rect class="hash" x="10" y="126" width="150" height="34"/>
<text class="small" x="85" y="147" text-anchor="middle">the node's hash</text>
<rect class="shade" x="162" y="126" width="50" height="34"/>
<text class="small" x="187" y="147" text-anchor="middle">kind</text>
<rect class="shade" x="214" y="126" width="54" height="34"/>
<text class="small" x="241" y="147" text-anchor="middle">depth</text>
<rect class="shade" x="270" y="126" width="68" height="34"/>
<text class="small" x="304" y="147" text-anchor="middle">catalog</text>
<rect class="shade" x="340" y="126" width="54" height="34"/>
<text class="small" x="367" y="147" text-anchor="middle">base</text>
<rect class="shade" x="396" y="126" width="80" height="34"/>
<text class="small" x="436" y="147" text-anchor="middle">manifest</text>
<rect x="478" y="126" width="192" height="34"/>
<text class="small" x="574" y="147" text-anchor="middle">rows, column by column</text>
<text class="label small muted" x="85" y="178" text-anchor="middle">32 bytes, at a multiple of 16</text>
<text class="label small muted" x="367" y="178" text-anchor="middle">the frame it is</text>
<text class="label small muted" x="367" y="194" text-anchor="middle">written against</text>
<text class="label small muted" x="436" y="178" text-anchor="middle">a token</text>
</svg>
</figure>

Every 32nd version of a node is written whole again, so a chain never grows past 32 frames. The file finds the version to write against through its index, which keeps, for every node's lineage (its catalog, its kind and its first key), where the latest frame of it is.

A frame stands for the exact block. The tree's encoding is canonical, so the rows of a node in the current layout rebuild its block byte for byte, and debug builds check that on every frame they write. A node in an older layout, or a block that is not a tree node, is kept whole, as a frame of the whole kind with its bytes after the header. Either way the block reads back byte for byte, and its name is still its hash.

### Reading a node in place

A frame is not turned back into a block to be read. When the tree asks for a node, the archive decodes its rows once, turns each id into where its token sits, and answers from the file's mapping:

- **keys** are compared part by part, where their tokens sit, so a binary search over a leaf copies nothing;
- **values** are read as archives where they sit, each checked the first time a node that holds it is read;
- **children** are named by the hash at the start of each child's frame.

A block is built only when one is asked for, as sync and push do, since what travels between replicas is still blocks.

<div class="aside">

**Implementations.** Rows are [`dialog-archive-file/src/rows.rs`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-archive-file/src/rows.rs). Serving a node in place is [`archive/served.rs`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-archive-file/src/archive/served.rs), through `NodeSource` and the node views in [`dialog-search-tree/src/node/view.rs`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-search-tree/src/node/view.rs).

</div>

## The file's index

To answer *where is block `8Ea3Qm…`?* the file keeps an index of its own, in the file, as another search tree. Its root is in the slot, and its other nodes are **pages** of 4096 bytes, written in pages records. It maps 32-bit keys, taken from hashes, to places in the file, in three spaces:

<figure class="dg">
<svg class="dg" viewBox="0 0 680 196" width="680" height="196" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="The file's index: a root in the slot, internal pages with buffers, leaf pages; it maps blocks, tokens and lineages to places in the file">
<defs><marker id="ix-arrow" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto"><path d="M0,0 L8,4 L0,8 z"/></marker></defs>
<rect class="shade" x="120" y="14" width="160" height="40"/>
<text class="small" x="200" y="32" text-anchor="middle">root, in the slot</text>
<text class="small muted" x="200" y="47" text-anchor="middle">children + buffer</text>
<line x1="170" y1="56" x2="90" y2="84" marker-end="url(#ix-arrow)"/>
<line x1="230" y1="56" x2="310" y2="84" marker-end="url(#ix-arrow)"/>
<rect x="20" y="86" width="140" height="40"/>
<text class="small" x="90" y="104" text-anchor="middle">page</text>
<text class="small muted" x="90" y="119" text-anchor="middle">children + buffer</text>
<rect x="240" y="86" width="140" height="40"/>
<text class="small" x="310" y="104" text-anchor="middle">page</text>
<text class="small muted" x="310" y="119" text-anchor="middle">children + buffer</text>
<line x1="60" y1="128" x2="40" y2="150" marker-end="url(#ix-arrow)"/>
<line x1="120" y1="128" x2="140" y2="150" marker-end="url(#ix-arrow)"/>
<line x1="280" y1="128" x2="260" y2="150" marker-end="url(#ix-arrow)"/>
<line x1="340" y1="128" x2="360" y2="150" marker-end="url(#ix-arrow)"/>
<rect x="10" y="152" width="70" height="30"/>
<text class="small" x="45" y="171" text-anchor="middle">leaf</text>
<rect x="110" y="152" width="70" height="30"/>
<text class="small" x="145" y="171" text-anchor="middle">leaf</text>
<rect x="230" y="152" width="70" height="30"/>
<text class="small" x="265" y="171" text-anchor="middle">leaf</text>
<rect x="330" y="152" width="70" height="30"/>
<text class="small" x="365" y="171" text-anchor="middle">leaf</text>
<text class="label small" x="420" y="30">it maps</text>
<rect class="hash" x="420" y="40" width="250" height="40"/>
<text class="small" x="428" y="57">a block: catalog + hash</text>
<text class="small muted" x="428" y="72">→ its frame</text>
<rect class="shade" x="420" y="84" width="250" height="40"/>
<text class="small" x="428" y="101">a token: hash of its bytes</text>
<text class="small muted" x="428" y="116">→ the token, so it is written once</text>
<rect class="shade" x="420" y="128" width="250" height="40"/>
<text class="small" x="428" y="145">a lineage: catalog, kind, first key</text>
<text class="small muted" x="428" y="160">→ the latest frame, to write against</text>
</svg>
</figure>

The index is a buffered tree, as the [Search Tree](./tree.md) chapter's buffered writes are: an insert lands in the root's buffer, and a full buffer sends the entries bound for its busiest child down into it. Most appends write nothing but the root, which is in the slot they write anyway.

A page is never changed where it is. A rewrite writes a new page and gives the old one back. A page given back is written over only after the next sync point, once no durable slot can still reach it, so the file does not grow with every rewrite. A reader holding an old root might find a page written over since: every page says which append wrote it and carries a checksum, so the reader sees that it is stale and looks again from a newer root.

When opening trusts the newest slot (see below), it reads the slots and nothing else. A reader holds a few tens of kilobytes, not an index that grows with the file.

<div class="aside">

**Implementations.** The index's pages are [`dialog-archive-file/src/archive/tree.rs`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-archive-file/src/archive/tree.rs), and what it maps is [`archive/index.rs`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-archive-file/src/archive/index.rs).

</div>

## Cells: heads in the log

A branch's head is a cell (see [Commits and History](./history.md)), and a cell changes. The file keeps cells the way SQLite keeps commits in its write-ahead log: every update appends a cell record holding the cell's name and its new value, and the cells' own index, rooted in the slot, files the name at its newest record. Nothing is written over.

<figure class="dg">
<svg class="dg" viewBox="0 0 680 168" width="680" height="168" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="The log: a commit's frames and pages, then its head as a cell record; the cells' index points at the newest record">
<defs><marker id="cl-arrow" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto"><path d="M0,0 L8,4 L0,8 z"/></marker></defs>
<text class="label small" x="10" y="16">the log, in the order it was written</text>
<rect x="10" y="24" width="64" height="40"/>
<text class="small" x="42" y="48" text-anchor="middle">frames</text>
<rect x="76" y="24" width="60" height="40"/>
<text class="small" x="106" y="48" text-anchor="middle">pages</text>
<rect class="hash" x="138" y="24" width="120" height="40"/>
<text class="small" x="198" y="41" text-anchor="middle">cell: main's head</text>
<text class="small muted" x="198" y="56" text-anchor="middle">root A</text>
<rect x="260" y="24" width="64" height="40"/>
<text class="small" x="292" y="48" text-anchor="middle">frames</text>
<rect x="326" y="24" width="60" height="40"/>
<text class="small" x="356" y="48" text-anchor="middle">pages</text>
<rect class="hash" x="388" y="24" width="120" height="40"/>
<text class="small" x="448" y="41" text-anchor="middle">cell: main's head</text>
<text class="small muted" x="448" y="56" text-anchor="middle">root B</text>
<rect class="ghost" x="510" y="24" width="160" height="40"/>
<text class="small muted" x="590" y="48" text-anchor="middle">the next append</text>
<rect class="shade" x="330" y="112" width="240" height="40"/>
<text class="small" x="450" y="130" text-anchor="middle">cells' index, rooted in the slot</text>
<text class="small muted" x="450" y="145" text-anchor="middle">main's head → the newest record</text>
<line x1="448" y1="110" x2="448" y2="68" marker-end="url(#cl-arrow)"/>
<text class="label small muted" x="10" y="96">Each head comes after the blocks it names.</text>
<text class="label small muted" x="10" y="112">An older record stays, and nothing names it.</text>
</svg>
</figure>

Because a cell's record comes after the blocks its value names, whatever a power loss keeps, recovery returns to a state in which every head names blocks the file holds. As with SQLite's `synchronous=NORMAL`, an update becomes durable at the next sync point rather than at one of its own, so a power loss can undo the last updates, but never leave a head naming what is gone.

There is one difference from SQLite. A writer's blocks need not be in the log yet when its head is written: they may still be in its extent (next section). The cell record names those instead, each extent that holds the writer's blocks with how far its blocks reach, and recovery takes the record as whole only if they are all there. A cell's value can be of any size, and a read of a cell takes no lock.

<div class="aside">

**Implementations.** Cells are [`dialog-archive-file/src/archive/cells.rs`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-archive-file/src/archive/cells.rs). A cell's edition is the BLAKE3 hash of its value, as on the file system store, so a compare-and-swap names the same version either way.

</div>

## Many writers, one file

Any number of processes can open the file. Readers take no lock: everything a slot names stays as it was, but for index pages, which a reader checks as it reads them. So a reader reads from the slot it took, and looks again only when the file grows. Writers take turns.

**The turn** is a lock on one byte of the file, far past its end where no data is. A writer holding it lays out its append against the newest slot, writes it and writes the slot. Splitting a node into rows and encoding them is done before taking the turn, so the turn is held only for laying out and writing.

**Extents** are how a writer avoids waiting. A writer that finds the turn taken does not wait for it. It writes its blocks whole into an extent of its own, space it claimed earlier during a turn, and they can be read at once, by its own process and by others. When the writer next holds the turn, usually at its next commit, it **folds** them in: it turns them into frames and tokens, and retires the extent, which becomes free again at the next sync point. An extent whose writer is gone is folded in by whichever writer holds the turn next. Each process that writes keeps an extent ready for itself, and a process that opens the file while the turn is free leaves one that no process owns, for a process that has none to take. So a process can write without waiting even if it has never held the turn.

<figure class="dg">
<svg class="dg" viewBox="0 0 680 300" width="680" height="300" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Two processes writing one file: B finds the turn taken and writes into its extent, then folds it in at its next turn; the sync point is made after the turn">
<defs><marker id="wr-arrow" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto"><path d="M0,0 L8,4 L0,8 z"/></marker></defs>
<text class="title" x="90" y="18" text-anchor="middle">process A</text>
<line class="dashed" x1="90" y1="26" x2="90" y2="292"/>
<text class="title" x="340" y="18" text-anchor="middle">the file</text>
<line class="dashed" x1="340" y1="26" x2="340" y2="292"/>
<text class="title" x="590" y="18" text-anchor="middle">process B</text>
<line class="dashed" x1="590" y1="26" x2="590" y2="292"/>
<line x1="90" y1="48" x2="336" y2="48" marker-end="url(#wr-arrow)"/>
<text class="small" x="213" y="42" text-anchor="middle">take the turn</text>
<line x1="590" y1="78" x2="344" y2="78" marker-end="url(#wr-arrow)"/>
<text class="small" x="467" y="72" text-anchor="middle">try the turn: taken</text>
<line x1="590" y1="110" x2="344" y2="110" marker-end="url(#wr-arrow)"/>
<text class="small" x="467" y="104" text-anchor="middle">write blocks whole into B's extent</text>
<text class="small muted" x="467" y="124" text-anchor="middle">no waiting; readable at once</text>
<line x1="90" y1="142" x2="336" y2="142" marker-end="url(#wr-arrow)"/>
<text class="small" x="213" y="136" text-anchor="middle">frames, pages, slot</text>
<line x1="90" y1="170" x2="336" y2="170" marker-end="url(#wr-arrow)"/>
<text class="small" x="213" y="164" text-anchor="middle">let go of the turn</text>
<line x1="590" y1="198" x2="344" y2="198" marker-end="url(#wr-arrow)"/>
<text class="small" x="467" y="192" text-anchor="middle">next commit: take the turn</text>
<line x1="590" y1="228" x2="344" y2="228" marker-end="url(#wr-arrow)"/>
<text class="small" x="467" y="222" text-anchor="middle">fold its extent in, then the head</text>
<line x1="590" y1="256" x2="344" y2="256" marker-end="url(#wr-arrow)"/>
<text class="small" x="467" y="250" text-anchor="middle">let go of the turn</text>
<line x1="590" y1="284" x2="344" y2="284" marker-end="url(#wr-arrow)"/>
<text class="small" x="467" y="278" text-anchor="middle">sync point, after the turn</text>
</svg>
</figure>

**Sync points** make what came before them durable: the file is synced, then the durable slot is written and synced. One follows every 64 appends, or every 2 seconds, or once 2048 index pages wait to be reused, and `FileArchive::sync` makes one at once. A sync point is made after the writer lets go of the turn, under a lock byte of its own, so writers waiting for the turn never wait for a disk sync.

A process holding the turn never waits for any other lock. A writer waits for the turn only when it cannot write into an extent: when it has none to take and must claim one, or when its blocks are of a catalog the file has not numbered yet.

<div class="aside">

**Implementations.** The lock bytes are [`dialog-archive-file/src/locks.rs`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-archive-file/src/locks.rs): POSIX record locks on Unix, `LockFileEx` on Windows. Extents are [`archive/extent.rs`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-archive-file/src/archive/extent.rs). The design and the cases it is tested against are in [`design/file-writers.md`](https://github.com/dialog-db/dialog-db/blob/main/design/file-writers.md).

</div>

## Opening, and after a power loss

A process opening the file holds a recovery byte while it does, so no other process opens from a state that is being repaired. Then it decides which slot to trust:

<figure class="dg">
<svg class="dg" viewBox="0 0 680 236" width="680" height="236" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Opening: trust the newest slot when another process holds the turn, or has the file open, or the slot was written in this boot; otherwise check everything since the durable slot">
<defs><marker id="op-arrow" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto"><path d="M0,0 L8,4 L0,8 z"/></marker></defs>
<rect class="shade" x="10" y="10" width="330" height="40"/>
<text class="small" x="175" y="27" text-anchor="middle">Does another process hold the turn?</text>
<text class="small muted" x="175" y="42" text-anchor="middle">then the file is in use, already opened</text>
<line x1="340" y1="30" x2="396" y2="30" marker-end="url(#op-arrow)"/>
<text class="small muted" x="368" y="24" text-anchor="middle">yes</text>
<rect x="400" y="10" width="270" height="40"/>
<text class="small" x="535" y="35" text-anchor="middle">take the newest slot as it is</text>
<line x1="175" y1="50" x2="175" y2="84" marker-end="url(#op-arrow)"/>
<text class="small muted" x="185" y="72">no</text>
<rect class="shade" x="10" y="86" width="330" height="40"/>
<text class="small" x="175" y="103" text-anchor="middle">Is another process present, or was the</text>
<text class="small" x="175" y="118" text-anchor="middle">newest slot written in this boot?</text>
<line x1="340" y1="106" x2="396" y2="106" marker-end="url(#op-arrow)"/>
<text class="small muted" x="368" y="100" text-anchor="middle">yes</text>
<rect x="400" y="86" width="270" height="40"/>
<text class="small" x="535" y="111" text-anchor="middle">take the newest slot</text>
<line x1="175" y1="126" x2="175" y2="160" marker-end="url(#op-arrow)"/>
<text class="small muted" x="185" y="148">no: a power loss may have cut it short</text>
<rect class="critical" x="10" y="162" width="660" height="56"/>
<text class="small" x="340" y="182" text-anchor="middle">Check everything written since the durable slot: every record whole, every cell's</text>
<text class="small" x="340" y="197" text-anchor="middle">extents holding what it names, every index page the slot reaches. Take the newest</text>
<text class="small" x="340" y="212" text-anchor="middle">rolling slot that passes, or else go back to the durable slot.</text>
</svg>
</figure>

A process that died without closing left what it wrote with the operating system, in order, so a slot written in this boot can be trusted as it is. A power loss is different: the disk may have kept some of what came after the last sync point and lost the rest, in any order. So a slot from another boot is checked, and if it fails, the file goes back to an older slot, at worst the durable one. That undoes the appends since the last sync point, and nothing before it.

These cases are tested by replaying the archive's writes onto an imaginary disk that loses power after any of them, keeping any subset of the unsynced writes, torn at any 512-byte sector, then opening each result as after a reboot.

**Closing.** The last handle of the file in a process folds what its extents hold into the file. The last process to close the file, the one that finds no other present, gives back the extents left for others, makes a sync point, and cuts the free extents that end the file off it, so a file nobody has open does not keep space it does not use.

<div class="aside">

**Implementations.** Opening is `recover` in [`dialog-archive-file/src/archive.rs`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-archive-file/src/archive.rs). The power-loss replay is [`archive/power.rs`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-archive-file/src/archive/power.rs) over the recording disk in [`file/disk.rs`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-archive-file/src/file/disk.rs), and the tests with real processes are [`archive/concurrency.rs`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-archive-file/src/archive/concurrency.rs).

</div>

## What it does not do yet

<div class="aside caveat">

- **Native only.** The crate compiles to nothing on the web. Its tests run on Linux, macOS and Windows. Whether the same file can live in the browser's file system, served by one worker that a tab hosts, is being measured in [`research/web-spike`](https://github.com/dialog-db/dialog-db/tree/main/research/web-spike), not built.
- **Opt-in.** Nothing switches a replica to it. A replica the file system store wrote can be moved into the file with the `archive-file` migration of the `dialog-migration` crate.
- **The format is still changing.** A file written now may not open in a later release.

</div>
