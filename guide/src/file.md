# One File per Space

[Blocks and Storage](./storage.md) described a space's archive as a map from hash to bytes, kept on a file system as one file per block under `archive/index/`. This chapter is about a second way to keep that same map: one file per space, called `archive.dialog`. It holds every block of the space, and, when the space also names it as its memory, every cell too, branch heads included.

It is opt-in. A `Space` gets it by naming `FileArchive` as its archive, and optionally as its memory. The tree, queries and sync read and write it as they would any archive, and every block reads back as exactly the bytes it was. It runs natively only, for now.

The chapter follows one fact into the file and back out, then walks through what happens when other processes write at the same time, when a peer asks for blocks, and when something goes wrong.

<div class="aside">

**The bytes in this chapter are real.** They come from one run of [`dialog-archive-file/examples/guide_bytes.rs`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-archive-file/examples/guide_bytes.rs), which builds Alice's grocery repository in a file and walks the file record by record after each commit, reading it from its bytes alone:

```sh
cargo run -p dialog-archive-file --example guide_bytes
```

Hashes, keys and the offsets that follow them differ from run to run, since each run's session has a key of its own. The layout does not.

</div>

## Why one file

A tree never changes a node. A commit writes a new version of each node it touches, as a whole block. A replica's history is therefore mostly old versions of the same few nodes, each one a separate file, and each version repeats almost everything the one before it held.

The example's repository is small: seven commits leave nine versions of its nodes. Here they are as nine files, and as what the file holds for them:

<figure class="dg">
<svg class="dg" viewBox="0 0 760 300" width="760" height="300" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Nine versions of the tree&#x27;s nodes as nine files, and as frames and tokens in one file">
<defs><marker id="why-arrow" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto"><path d="M0,0 L8,4 L0,8 z"/></marker></defs>
<text class="title" x="16" y="22">a file per block: archive/index/</text>
<rect class="hash" x="16" y="36" width="7.88782" height="20"/>
<text class="small" x="31.8878" y="50">b4ff23…  360 bytes</text>
<rect class="hash" x="16" y="61" width="55.8282" height="20"/>
<text class="small" x="79.8282" y="75">35a1e8…  2,548 bytes</text>
<rect class="hash" x="16" y="86" width="95.7932" height="20"/>
<text class="small" x="119.793" y="100">565412…  4,372 bytes</text>
<rect class="hash" x="16" y="111" width="133.479" height="20"/>
<text class="small" x="157.479" y="125">1a442d…  6,092 bytes</text>
<rect class="hash" x="16" y="136" width="175.197" height="20"/>
<text class="small" x="199.197" y="150">387ac8…  7,996 bytes</text>
<rect class="hash" x="16" y="161" width="216.74" height="20"/>
<text class="small" x="240.74" y="175">9afe62…  9,892 bytes</text>
<rect class="hash" x="16" y="186" width="258.107" height="20"/>
<text class="small" x="282.107" y="200">fc9fc5…  11,780 bytes</text>
<rect class="hash" x="16" y="211" width="258.633" height="20"/>
<text class="small" x="282.633" y="225">6c8933…  11,804 bytes</text>
<rect class="hash" x="16" y="236" width="300" height="20"/>
<text class="small" x="324" y="250">2c38c1…  13,692 bytes</text>
<text class="label small muted" x="16" y="275">nine blocks, 68,536 bytes</text>
<text class="title" x="470" y="22">one file: archive.dialog</text>
<rect class="value" x="470" y="36" width="31.3541" height="20"/>
<text class="small" x="509.354" y="50">9 frames, 1,431 bytes</text>
<rect class="attribute" x="470" y="61" width="110.495" height="20"/>
<text class="small" x="588.495" y="75">49 tokens, 5,043 bytes</text>
<text class="label small muted" x="470" y="104">6,474 bytes for the same nine</text>
<text class="label small muted" x="470" y="120">blocks, each rebuilt byte for byte</text>
<text class="label small muted" x="470" y="136">when asked for</text>
<text class="label small muted" x="380" y="290" text-anchor="middle">Drawn to one scale. The nodes come from the example&#x27;s seven commits.</text>
</svg>
</figure>

The file spends 1,431 bytes on the nine nodes and 5,043 bytes on their 49 tokens, against 68,536 bytes of blocks. Two things make the difference. Every distinct entity, attribute and value is written once, as a **token**, however many keys and versions mention it. And a new version of a node is written as a **frame**: its rows of token ids, mostly copied from the version before.

The gap grows with history. Over two measured histories:

| | a file per block | one file | |
|---|--:|--:|--:|
| Stack Exchange, 4096 commits | 1169 MiB | 13.4 MiB | 87× smaller |
| tonk-team's pushed history | 568.5 MiB | 25.4 MiB | 22× smaller |

<div class="aside">

**Implementations.** `FileArchive` is [`dialog-archive-file/src/archive.rs`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-archive-file/src/archive.rs). The measurements and the design behind them are in [`research/single-file-format`](https://github.com/dialog-db/dialog-db/tree/main/research/single-file-format).

</div>

## Following one fact into the file

Alice's grocery list already has two facts, from her first commit: the `demo.grocery/name` of `item:2` is "Rye bread", and the `demo.grocery/quantity` of `item:1` is "1". Now she commits a third:

*the <span class="chip attribute">demo.grocery/name</span> of <span class="chip entity">item:1</span> is <span class="chip value">"Oat milk"</span>*

This section follows that fact from the commit to the disk and back, one step at a time.

### Before the commit: what is in the file

When Alice's peer created the repository, its file looked like this:

<figure class="dg">
<svg class="dg" viewBox="0 0 760 190" width="760" height="190" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="The file as Alice&#x27;s peer created it: magic, three slots, an empty extent and one cell">
<defs><marker id="open-arrow" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto"><path d="M0,0 L8,4 L0,8 z"/></marker></defs>
<rect class="shade" x="16" y="40" width="8" height="44"/>
<line x1="16" y1="84" x2="16" y2="96"/>
<text class="small muted" x="16" y="110">0</text>
<rect x="28" y="40" width="100" height="44"/>
<text class="small" x="78" y="66" text-anchor="middle">rolling slot 0</text>
<line x1="28" y1="84" x2="28" y2="96"/>
<text class="small muted" x="28" y="110" text-anchor="middle">8</text>
<rect x="132" y="40" width="100" height="44"/>
<text class="small" x="182" y="66" text-anchor="middle">rolling slot 1</text>
<line x1="132" y1="84" x2="132" y2="96"/>
<text class="small muted" x="132" y="110" text-anchor="middle">65,544</text>
<rect x="236" y="40" width="100" height="44"/>
<text class="small" x="286" y="66" text-anchor="middle">durable slot</text>
<line x1="236" y1="84" x2="236" y2="96"/>
<text class="small muted" x="236" y="110" text-anchor="middle">131,080</text>
<rect class="dashed" x="340" y="40" width="270" height="44"/>
<text class="label small" x="475" y="66" text-anchor="middle">extent, empty: 1 MiB, never written</text>
<line x1="340" y1="84" x2="340" y2="96"/>
<text class="small muted" x="340" y="110" text-anchor="middle">196,616</text>
<rect class="hash" x="614" y="40" width="90" height="44"/>
<text class="small" x="659" y="66" text-anchor="middle">cell</text>
<line x1="614" y1="84" x2="614" y2="96"/>
<text class="small muted" x="614" y="110" text-anchor="middle">1,245,232</text>
<text class="small muted" x="704" y="124" text-anchor="end">1,245,272</text>
<text class="label small muted" x="16" y="28">archive.dialog, just created (offsets in bytes)</text>
<text class="small" x="54" y="66"></text>
<text class="label small muted" x="16" y="140">1,245,272 bytes long, 24,576 allocated: the extent is a hole the file system never stores.</text>
<text class="label small muted" x="16" y="158">The cell is the repository&#x27;s dialog/version; slots are 64 KiB each, so data starts at 196,616.</text>
</svg>
</figure>

Every file starts with eight bytes of **magic**: `DIALOGA` and a byte for the format's version. A file that starts with anything else is not opened.

<figure class="dg">
<svg class="dg" viewBox="0 0 520 92" width="520" height="92" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="The eight bytes of magic: DIALOGA and the format version 0e">
<defs><marker id="mag-arrow" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto"><path d="M0,0 L8,4 L0,8 z"/></marker></defs>
<rect class="shade" x="16" y="16" width="40" height="40"/>
<text x="36" y="34" text-anchor="middle">D</text>
<text class="small muted" x="36" y="50" text-anchor="middle">44</text>
<rect class="shade" x="56" y="16" width="40" height="40"/>
<text x="76" y="34" text-anchor="middle">I</text>
<text class="small muted" x="76" y="50" text-anchor="middle">49</text>
<rect class="shade" x="96" y="16" width="40" height="40"/>
<text x="116" y="34" text-anchor="middle">A</text>
<text class="small muted" x="116" y="50" text-anchor="middle">41</text>
<rect class="shade" x="136" y="16" width="40" height="40"/>
<text x="156" y="34" text-anchor="middle">L</text>
<text class="small muted" x="156" y="50" text-anchor="middle">4c</text>
<rect class="shade" x="176" y="16" width="40" height="40"/>
<text x="196" y="34" text-anchor="middle">O</text>
<text class="small muted" x="196" y="50" text-anchor="middle">4f</text>
<rect class="shade" x="216" y="16" width="40" height="40"/>
<text x="236" y="34" text-anchor="middle">G</text>
<text class="small muted" x="236" y="50" text-anchor="middle">47</text>
<rect class="shade" x="256" y="16" width="40" height="40"/>
<text x="276" y="34" text-anchor="middle">A</text>
<text class="small muted" x="276" y="50" text-anchor="middle">41</text>
<rect class="critical" x="296" y="16" width="40" height="40"/>
<text class="critical" x="316" y="34" text-anchor="middle">14</text>
<text class="small muted" x="316" y="50" text-anchor="middle">0e</text>
<text class="label small muted" x="356" y="34">bytes 0 to 7</text>
<text class="label small muted" x="356" y="50">the last is the format: 14</text>
<text class="label small muted" x="16" y="80">A file whose first bytes differ is not opened; a format change bumps the last byte.</text>
</svg>
</figure>

Then come three **slots** of 64 KiB each. A slot says what the file holds: where its records end, the root of the file's own index, which space is free. Most of what follows is about how slots are written and trusted.

After the slots, from byte 196,616, come **records**, appended one after another. A new file already has two. One is an **extent**, a megabyte a writer can use without waiting for anyone (it comes back in [When others are writing too](#when-others-are-writing-too)). It has never been written to, so the file system stores it as a hole: the file is 1.2 MB long but takes 24 KiB of disk. The other is a **cell** record, the repository's `dialog/version`.

### Step 1: the commit makes one new node

The commit edits the tree as [The Search Tree](./tree.md) describes. With buffered writes, a small commit does not reach the leaves: its operations wait in the root's buffer. So the commit produces exactly one new block, a new root:

<figure class="dg">
<svg class="dg" viewBox="0 0 760 250" width="760" height="250" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Commit 2 writes one new node: the root, its buffer grown from 10 to 16 operations; the leaf is reused">
<defs><marker id="ct-arrow" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto"><path d="M0,0 L8,4 L0,8 z"/></marker></defs>
<text class="title" x="16" y="20">after commit 1</text>
<text class="title" x="440" y="20">after commit 2</text>
<rect class="faded" x="16" y="34" width="300" height="74"/>
<text class="hash" x="26" y="54">root 35a1e8…</text>
<text class="small muted" x="26" y="70">2,548 bytes</text>
<rect class="shade" x="166" y="42" width="140" height="56"/>
<text class="small" x="236" y="66" text-anchor="middle">buffer</text>
<text class="small" x="236" y="82" text-anchor="middle">10 operations</text>
<rect x="440" y="34" width="300" height="74"/>
<text class="hash" x="450" y="54">root 565412…</text>
<text class="small muted" x="450" y="70">4,372 bytes</text>
<rect class="value" x="590" y="42" width="140" height="56"/>
<text class="small" x="660" y="66" text-anchor="middle">buffer</text>
<text class="small" x="660" y="82" text-anchor="middle">16 operations</text>
<line class="dashed" x1="120" y1="108" x2="318" y2="162" marker-end="url(#ct-arrow)"/>
<line x1="540" y1="108" x2="444" y2="162" marker-end="url(#ct-arrow)"/>
<rect x="270" y="164" width="220" height="46"/>
<text class="hash" x="380" y="184" text-anchor="middle">leaf b4ff23…</text>
<text class="small muted" x="380" y="200" text-anchor="middle">360 bytes, unchanged</text>
<text class="label small muted" x="380" y="238" text-anchor="middle">Buffered writes (see The Search Tree): new facts wait in the root&#x27;s buffer, so a small commit rewrites the root alone.</text>
</svg>
</figure>

The new root's buffer holds 16 operations, six more than before. Four are for Alice's fact: its EAV, AEV and VAE entries and its history entry (see [Keys](./keys.md) and [Commits and History](./history.md)). The other two record the new revision itself. As a block, the new root is 4,372 bytes. It is handed to the archive, and from here on the file decides how to keep it.

### Step 2: the keys become tokens

A tree node is made of keys, and a key is made of parts. The file splits each key into its parts and keeps each distinct part once, as a **token**. A row of the node then names its parts by token id.

Here is the fact's EAV key, split:

<figure class="dg">
<svg class="dg" viewBox="0 0 760 230" width="760" height="230" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="The EAV key of item:1&#x27;s name split into three tokens: two the file already holds, one new">
<defs><marker id="kt-arrow" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto"><path d="M0,0 L8,4 L0,8 z"/></marker></defs>
<text class="label small muted" x="16" y="20">the EAV key, as the Keys chapter draws it</text>
<rect class="shade" x="16" y="30" width="100" height="30"/>
<text x="66" y="50" text-anchor="middle">00</text>
<line class="dashed" x1="66" y1="60" x2="66" y2="96" marker-end="url(#kt-arrow)"/>
<rect class="shade" x="16" y="98" width="100" height="44"/>
<text x="66" y="116" text-anchor="middle">form 00</text>
<text class="small muted" x="66" y="132" text-anchor="middle">in the row</text>
<rect class="entity" x="120" y="30" width="160" height="30"/>
<text class="entity" x="200" y="50" text-anchor="middle">item:1 00</text>
<line class="dashed" x1="200" y1="60" x2="200" y2="96" marker-end="url(#kt-arrow)"/>
<rect class="entity" x="120" y="98" width="160" height="44"/>
<text class="entity" x="200" y="116" text-anchor="middle">token 77831</text>
<text class="small muted" x="200" y="132" text-anchor="middle">found, from commit 1</text>
<rect class="attribute" x="284" y="30" width="230" height="30"/>
<text class="attribute" x="399" y="50" text-anchor="middle">demo.grocery/name 00</text>
<line class="dashed" x1="399" y1="60" x2="399" y2="96" marker-end="url(#kt-arrow)"/>
<rect class="attribute" x="284" y="98" width="230" height="44"/>
<text class="attribute" x="399" y="116" text-anchor="middle">token 77873</text>
<text class="small muted" x="399" y="132" text-anchor="middle">found: item:2&#x27;s, commit 1</text>
<rect class="value" x="518" y="30" width="200" height="30"/>
<text class="value" x="618" y="50" text-anchor="middle">03 &quot;Oat milk&quot; 00</text>
<line x1="618" y1="60" x2="618" y2="96" marker-end="url(#kt-arrow)"/>
<rect class="value solid" x="518" y="98" width="200" height="44"/>
<text x="618" y="116" text-anchor="middle" style="fill:var(--dg-paper)">token 143526</text>
<text class="small" x="618" y="132" text-anchor="middle" style="fill:var(--dg-paper)">new: written now</text>
<text class="label small muted" x="16" y="176">The writer looks each part up by the hash of its bytes in the file&#x27;s index. Parts the file holds are only named:</text>
<text class="label small muted" x="16" y="194">of this fact, only &quot;Oat milk&quot; is written. The form byte is the key&#x27;s tag and stays in the row.</text>
</svg>
</figure>

Two of the three parts are already in the file. Commit 1 wrote `item:1` for the quantity, and `demo.grocery/name` for item 2's name. The writer finds them by the hash of their bytes in the file's index, and only names them. Only `"Oat milk"` is new, so it is the only part of this fact that gets written.

A token's place in the file is its id:

<figure class="dg">
<svg class="dg" viewBox="0 0 760 200" width="760" height="200" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="A token in the file: a pad byte, padding, a four-byte length, then the bytes, starting on a multiple of 16">
<defs><marker id="tb-arrow" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto"><path d="M0,0 L8,4 L0,8 z"/></marker></defs>
<text class="label small muted" x="16" y="20">inside commit 2&#x27;s tokens record, from offset 2,296,400</text>
<rect class="shade" x="16" y="32" width="21" height="24"/>
<text x="26.5" y="48.5" text-anchor="middle">0b</text>
<rect x="37" y="32" width="21" height="24"/>
<text class="muted" x="47.5" y="48.5" text-anchor="middle">00</text>
<rect x="58" y="32" width="21" height="24"/>
<text class="muted" x="68.5" y="48.5" text-anchor="middle">00</text>
<rect x="79" y="32" width="21" height="24"/>
<text class="muted" x="89.5" y="48.5" text-anchor="middle">00</text>
<rect x="100" y="32" width="21" height="24"/>
<text class="muted" x="110.5" y="48.5" text-anchor="middle">00</text>
<rect x="121" y="32" width="21" height="24"/>
<text class="muted" x="131.5" y="48.5" text-anchor="middle">00</text>
<rect x="142" y="32" width="21" height="24"/>
<text class="muted" x="152.5" y="48.5" text-anchor="middle">00</text>
<rect x="163" y="32" width="21" height="24"/>
<text class="muted" x="173.5" y="48.5" text-anchor="middle">00</text>
<rect x="184" y="32" width="21" height="24"/>
<text class="muted" x="194.5" y="48.5" text-anchor="middle">00</text>
<rect x="205" y="32" width="21" height="24"/>
<text class="muted" x="215.5" y="48.5" text-anchor="middle">00</text>
<rect x="226" y="32" width="21" height="24"/>
<text class="muted" x="236.5" y="48.5" text-anchor="middle">00</text>
<rect x="247" y="32" width="21" height="24"/>
<text class="muted" x="257.5" y="48.5" text-anchor="middle">00</text>
<rect class="hash" x="268" y="32" width="21" height="24"/>
<text class="hash" x="278.5" y="48.5" text-anchor="middle">0a</text>
<rect class="hash" x="289" y="32" width="21" height="24"/>
<text class="hash" x="299.5" y="48.5" text-anchor="middle">00</text>
<rect class="hash" x="310" y="32" width="21" height="24"/>
<text class="hash" x="320.5" y="48.5" text-anchor="middle">00</text>
<rect class="hash" x="331" y="32" width="21" height="24"/>
<text class="hash" x="341.5" y="48.5" text-anchor="middle">00</text>
<rect class="value" x="352" y="32" width="21" height="24"/>
<text class="value" x="362.5" y="48.5" text-anchor="middle">03</text>
<rect class="value" x="373" y="32" width="21" height="24"/>
<text class="value" x="383.5" y="48.5" text-anchor="middle">4f</text>
<rect class="value" x="394" y="32" width="21" height="24"/>
<text class="value" x="404.5" y="48.5" text-anchor="middle">61</text>
<rect class="value" x="415" y="32" width="21" height="24"/>
<text class="value" x="425.5" y="48.5" text-anchor="middle">74</text>
<rect class="value" x="436" y="32" width="21" height="24"/>
<text class="value" x="446.5" y="48.5" text-anchor="middle">20</text>
<rect class="value" x="457" y="32" width="21" height="24"/>
<text class="value" x="467.5" y="48.5" text-anchor="middle">6d</text>
<rect class="value" x="478" y="32" width="21" height="24"/>
<text class="value" x="488.5" y="48.5" text-anchor="middle">69</text>
<rect class="value" x="499" y="32" width="21" height="24"/>
<text class="value" x="509.5" y="48.5" text-anchor="middle">6c</text>
<rect class="value" x="520" y="32" width="21" height="24"/>
<text class="value" x="530.5" y="48.5" text-anchor="middle">6b</text>
<rect class="value" x="541" y="32" width="21" height="24"/>
<text class="value" x="551.5" y="48.5" text-anchor="middle">00</text>
<path class="wire" d="M16,58 L16,64 L37,64 L37,58"/>
<text class="small muted" x="16" y="78">pad: 11</text>
<path class="wire" d="M37,58 L37,64 L268,64 L268,58"/>
<text class="small muted" x="152.5" y="78" text-anchor="middle">11 bytes of padding</text>
<path class="wire" d="M268,58 L268,64 L352,64 L352,58"/>
<text class="small hash" x="310" y="78" text-anchor="middle">length 10</text>
<path class="wire" d="M352,58 L352,64 L562,64 L562,58"/>
<text class="small value" x="457" y="78" text-anchor="middle">03 &quot;Oat milk&quot; 00</text>
<line class="critical" x1="352" y1="96" x2="352" y2="112"/>
<text class="critical" x="352" y="126" text-anchor="middle">offset 2,296,416 = 16 × 143,526</text>
<text class="label small muted" x="16" y="156">The id is the offset over 16. A reader holding only the id reads the length just before it and the bytes after:</text>
<text class="label small muted" x="16" y="174">nothing in memory says where a token is, and a token is never moved.</text>
</svg>
</figure>

Every token starts on a multiple of 16 bytes, with its length in the four bytes before it, and its id is its offset divided by 16. A reader that has an id has everything it needs: it reads four bytes back for the length, then the bytes. Nothing in memory has to map ids to places.

A value's token is the value's type byte and payload, here `03` for a string, so a value is one token in every ordering. The entity and attribute tokens include the zero byte that ends them in a key. Each of the fact's four keys names the same three tokens, and the history key adds one more for the version:

<figure class="dg">
<svg class="dg" viewBox="0 0 760 210" width="760" height="210" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="The fact&#x27;s four entries name the same three tokens; the history entry adds a version token">
<defs><marker id="st-arrow" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto"><path d="M0,0 L8,4 L0,8 z"/></marker></defs>
<text class="label small muted" x="16" y="24">form</text>
<text class="label small muted" x="110" y="24">slot 0</text>
<text class="label small muted" x="250" y="24">slot 1</text>
<text class="label small muted" x="390" y="24">slot 2</text>
<text class="label small muted" x="530" y="24">slot 3</text>
<rect class="shade" x="16" y="34" width="86" height="28"/>
<text class="small" x="24" y="53">00 EAV</text>
<rect class="entity" x="110" y="34" width="130" height="28"/>
<text class="entity" x="175" y="53" text-anchor="middle">77831</text>
<rect class="attribute" x="250" y="34" width="130" height="28"/>
<text class="attribute" x="315" y="53" text-anchor="middle">77873</text>
<rect class="value" x="390" y="34" width="130" height="28"/>
<text class="value" x="455" y="53" text-anchor="middle">143526</text>
<rect class="ghost" x="530" y="34" width="130" height="28"/>
<rect class="shade" x="16" y="68" width="86" height="28"/>
<text class="small" x="24" y="87">01 AEV</text>
<rect class="entity" x="110" y="68" width="130" height="28"/>
<text class="entity" x="175" y="87" text-anchor="middle">77831</text>
<rect class="attribute" x="250" y="68" width="130" height="28"/>
<text class="attribute" x="315" y="87" text-anchor="middle">77873</text>
<rect class="value" x="390" y="68" width="130" height="28"/>
<text class="value" x="455" y="87" text-anchor="middle">143526</text>
<rect class="ghost" x="530" y="68" width="130" height="28"/>
<rect class="shade" x="16" y="102" width="86" height="28"/>
<text class="small" x="24" y="121">02 VAE</text>
<rect class="entity" x="110" y="102" width="130" height="28"/>
<text class="entity" x="175" y="121" text-anchor="middle">77831</text>
<rect class="attribute" x="250" y="102" width="130" height="28"/>
<text class="attribute" x="315" y="121" text-anchor="middle">77873</text>
<rect class="value" x="390" y="102" width="130" height="28"/>
<text class="value" x="455" y="121" text-anchor="middle">143526</text>
<rect class="ghost" x="530" y="102" width="130" height="28"/>
<rect class="shade" x="16" y="136" width="86" height="28"/>
<text class="small" x="24" y="155">03 history</text>
<rect class="hash" x="110" y="136" width="130" height="28"/>
<text class="hash" x="175" y="155" text-anchor="middle">143527</text>
<rect class="entity" x="250" y="136" width="130" height="28"/>
<text class="entity" x="315" y="155" text-anchor="middle">77831</text>
<rect class="attribute" x="390" y="136" width="130" height="28"/>
<text class="attribute" x="455" y="155" text-anchor="middle">77873</text>
<rect class="value" x="530" y="136" width="130" height="28"/>
<text class="value" x="595" y="155" text-anchor="middle">143526</text>
<text class="label small muted" x="16" y="192">A row stores the entity, attribute and value in the same slots whatever its ordering; the form says which ordering it is.</text>
</svg>
</figure>

The commit's five new tokens go into one **tokens record**: `"Oat milk"`, the version, the revision's entity and value, and the encoded state an entry maps to (one token per distinct `State<Datum>`, read in place as an archive, which is why tokens are aligned).

### Step 3: the node becomes a frame

The new root is written as a **frame**: its rows, naming tokens by id, written against the version of the root it replaces. The frame's data starts on a multiple of 16 too, so a frame also has an id, and it starts with a header:

<figure class="dg">
<svg class="dg" viewBox="0 0 760 220" width="760" height="220" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="A frame&#x27;s data starts on a multiple of 16 with the node&#x27;s hash, then kind, depth, catalog, base and manifest">
<defs><marker id="fh-arrow" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto"><path d="M0,0 L8,4 L0,8 z"/></marker></defs>
<text class="label small muted" x="16" y="20">commit 2&#x27;s frame: record at 2,296,472, data at 2,296,496 = 16 × 143,531</text>
<rect class="hash" x="16" y="32" width="17.6" height="24"/>
<text class="hash" x="24.8" y="48.5" text-anchor="middle">56</text>
<rect class="hash" x="33.6" y="32" width="17.6" height="24"/>
<text class="hash" x="42.4" y="48.5" text-anchor="middle">54</text>
<rect class="hash" x="51.2" y="32" width="17.6" height="24"/>
<text class="hash" x="60" y="48.5" text-anchor="middle">12</text>
<rect class="hash" x="68.8" y="32" width="17.6" height="24"/>
<text class="hash" x="77.6" y="48.5" text-anchor="middle">b9</text>
<rect class="hash" x="86.4" y="32" width="17.6" height="24"/>
<text class="hash" x="95.2" y="48.5" text-anchor="middle">09</text>
<rect class="hash" x="104" y="32" width="17.6" height="24"/>
<text class="hash" x="112.8" y="48.5" text-anchor="middle">7e</text>
<rect class="hash" x="121.6" y="32" width="17.6" height="24"/>
<text class="hash" x="130.4" y="48.5" text-anchor="middle">31</text>
<rect class="hash" x="139.2" y="32" width="17.6" height="24"/>
<text class="hash" x="148" y="48.5" text-anchor="middle">22</text>
<rect class="hash" x="156.8" y="32" width="17.6" height="24"/>
<text class="hash" x="165.6" y="48.5" text-anchor="middle">dd</text>
<rect class="hash" x="174.4" y="32" width="17.6" height="24"/>
<text class="hash" x="183.2" y="48.5" text-anchor="middle">82</text>
<rect class="hash" x="192" y="32" width="17.6" height="24"/>
<text class="hash" x="200.8" y="48.5" text-anchor="middle">98</text>
<rect class="hash" x="209.6" y="32" width="17.6" height="24"/>
<text class="hash" x="218.4" y="48.5" text-anchor="middle">73</text>
<rect class="hash" x="227.2" y="32" width="17.6" height="24"/>
<text class="hash" x="236" y="48.5" text-anchor="middle">1e</text>
<rect class="hash" x="244.8" y="32" width="17.6" height="24"/>
<text class="hash" x="253.6" y="48.5" text-anchor="middle">f7</text>
<rect class="hash" x="262.4" y="32" width="17.6" height="24"/>
<text class="hash" x="271.2" y="48.5" text-anchor="middle">d4</text>
<rect class="hash" x="280" y="32" width="17.6" height="24"/>
<text class="hash" x="288.8" y="48.5" text-anchor="middle">5e</text>
<rect class="hash" x="16" y="64" width="17.6" height="24"/>
<text class="hash" x="24.8" y="80.5" text-anchor="middle">49</text>
<rect class="hash" x="33.6" y="64" width="17.6" height="24"/>
<text class="hash" x="42.4" y="80.5" text-anchor="middle">5b</text>
<rect class="hash" x="51.2" y="64" width="17.6" height="24"/>
<text class="hash" x="60" y="80.5" text-anchor="middle">d6</text>
<rect class="hash" x="68.8" y="64" width="17.6" height="24"/>
<text class="hash" x="77.6" y="80.5" text-anchor="middle">24</text>
<rect class="hash" x="86.4" y="64" width="17.6" height="24"/>
<text class="hash" x="95.2" y="80.5" text-anchor="middle">60</text>
<rect class="hash" x="104" y="64" width="17.6" height="24"/>
<text class="hash" x="112.8" y="80.5" text-anchor="middle">c4</text>
<rect class="hash" x="121.6" y="64" width="17.6" height="24"/>
<text class="hash" x="130.4" y="80.5" text-anchor="middle">61</text>
<rect class="hash" x="139.2" y="64" width="17.6" height="24"/>
<text class="hash" x="148" y="80.5" text-anchor="middle">f6</text>
<rect class="hash" x="156.8" y="64" width="17.6" height="24"/>
<text class="hash" x="165.6" y="80.5" text-anchor="middle">6f</text>
<rect class="hash" x="174.4" y="64" width="17.6" height="24"/>
<text class="hash" x="183.2" y="80.5" text-anchor="middle">ae</text>
<rect class="hash" x="192" y="64" width="17.6" height="24"/>
<text class="hash" x="200.8" y="80.5" text-anchor="middle">37</text>
<rect class="hash" x="209.6" y="64" width="17.6" height="24"/>
<text class="hash" x="218.4" y="80.5" text-anchor="middle">2b</text>
<rect class="hash" x="227.2" y="64" width="17.6" height="24"/>
<text class="hash" x="236" y="80.5" text-anchor="middle">d2</text>
<rect class="hash" x="244.8" y="64" width="17.6" height="24"/>
<text class="hash" x="253.6" y="80.5" text-anchor="middle">cc</text>
<rect class="hash" x="262.4" y="64" width="17.6" height="24"/>
<text class="hash" x="271.2" y="80.5" text-anchor="middle">1a</text>
<rect class="hash" x="280" y="64" width="17.6" height="24"/>
<text class="hash" x="288.8" y="80.5" text-anchor="middle">54</text>
<path class="wire" d="M16,98 L16,104 L297.6,104 L297.6,98"/>
<text class="small hash" x="156.8" y="118" text-anchor="middle">the node&#x27;s BLAKE3 hash: 32 bytes</text>
<rect x="16" y="130" width="100" height="24"/>
<text x="66" y="146.5" text-anchor="middle">01</text>
<rect x="116" y="130" width="80" height="24"/>
<text x="156" y="146.5" text-anchor="middle">01</text>
<rect x="196" y="130" width="80" height="24"/>
<text x="236" y="146.5" text-anchor="middle">00</text>
<rect class="shade" x="276" y="130" width="56" height="24"/>
<text x="304" y="146.5" text-anchor="middle">ca</text>
<rect class="shade" x="332" y="130" width="56" height="24"/>
<text x="360" y="146.5" text-anchor="middle">e0</text>
<rect class="shade" x="388" y="130" width="56" height="24"/>
<text x="416" y="146.5" text-anchor="middle">04</text>
<rect class="attribute" x="444" y="130" width="56" height="24"/>
<text class="attribute" x="472" y="146.5" text-anchor="middle">94</text>
<rect class="attribute" x="500" y="130" width="56" height="24"/>
<text class="attribute" x="528" y="146.5" text-anchor="middle">e0</text>
<rect class="attribute" x="556" y="130" width="56" height="24"/>
<text class="attribute" x="584" y="146.5" text-anchor="middle">04</text>
<path class="wire" d="M16,158 L16,164 L116,164 L116,158"/>
<text class="small muted" x="66" y="178" text-anchor="middle">kind 1: index</text>
<path class="wire" d="M116,158 L116,164 L196,164 L196,158"/>
<text class="small muted" x="156" y="178" text-anchor="middle">depth 1</text>
<path class="wire" d="M196,158 L196,164 L276,164 L276,158"/>
<text class="small muted" x="236" y="178" text-anchor="middle">catalog 0</text>
<path class="wire" d="M276,158 L276,164 L444,164 L444,158"/>
<text class="small muted" x="360" y="178" text-anchor="middle">base: frame 77,897 (+1)</text>
<path class="wire" d="M444,158 L444,164 L612,164 L612,158"/>
<text class="small attribute" x="528" y="178" text-anchor="middle">manifest: token 77,844</text>
<text class="label small muted" x="620" y="147">then rows</text>
</svg>
</figure>

The hash comes first, so a parent read in place takes its child's name from where the child's frame starts. The kind says whether the node is a leaf (0), an index node (1), or a block kept whole (`ff`). The base names the frame this one is written against, and the depth says how many frames lie behind it.

An index node has three sections of rows: its links to children, its buffers, and the operations in them. Each section is written against the same section of the base. Here are the operations, with commit 1's root on the left:

<figure class="dg">
<svg class="dg" viewBox="0 0 760 480" width="760" height="480" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="The root&#x27;s 16 buffered operations, written as 10 rows copied from the version before and 6 new rows">
<defs><marker id="oa-arrow" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto"><path d="M0,0 L8,4 L0,8 z"/></marker></defs>
<text class="title" x="16" y="20">commit 1&#x27;s root: 10 operations</text>
<text class="title" x="420" y="20">commit 2&#x27;s root: 16 operations</text>
<rect class="shade" x="16" y="34" width="300" height="22"/>
<text class="small" x="24" y="50"> 0  00 rev(old) revision</text>
<rect class="shade" x="16" y="58" width="300" height="22"/>
<text class="small" x="24" y="74"> 1  00 item:2 name &quot;Rye bread&quot;</text>
<rect class="shade" x="16" y="82" width="300" height="22"/>
<text class="small" x="24" y="98"> 2  01 item:2 name &quot;Rye bread&quot;</text>
<rect class="shade" x="16" y="106" width="300" height="22"/>
<text class="small" x="24" y="122"> 3  01 item:1 quantity &quot;1&quot;</text>
<rect class="shade" x="16" y="130" width="300" height="22"/>
<text class="small" x="24" y="146"> 4  01 rev(old) revision</text>
<rect class="shade" x="16" y="154" width="300" height="22"/>
<text class="small" x="24" y="170"> 5  02 item:1 quantity &quot;1&quot;</text>
<rect class="shade" x="16" y="178" width="300" height="22"/>
<text class="small" x="24" y="194"> 6  02 item:2 name &quot;Rye bread&quot;</text>
<rect class="shade" x="16" y="202" width="300" height="22"/>
<text class="small" x="24" y="218"> 7  03 v1 item:1 quantity &quot;1&quot;</text>
<rect class="shade" x="16" y="226" width="300" height="22"/>
<text class="small" x="24" y="242"> 8  03 v1 item:2 name &quot;Rye bread&quot;</text>
<rect class="shade" x="16" y="250" width="300" height="22"/>
<text class="small" x="24" y="266"> 9  fe the revision&#x27;s own key</text>
<rect class="value" x="420" y="34" width="324" height="22"/>
<text class="small value" x="428" y="50"> 0  00 rev(new) revision</text>
<rect x="420" y="58" width="324" height="22"/>
<text class="small muted" x="428" y="74"> 1  00 rev(old) revision</text>
<line class="dashed" x1="316" y1="45" x2="418" y2="69" marker-end="url(#oa-arrow)"/>
<rect class="value" x="420" y="82" width="324" height="22"/>
<text class="small value" x="428" y="98"> 2  00 item:1 name &quot;Oat milk&quot;</text>
<rect x="420" y="106" width="324" height="22"/>
<text class="small muted" x="428" y="122"> 3  00 item:2 name &quot;Rye bread&quot;</text>
<line class="dashed" x1="316" y1="69" x2="418" y2="117" marker-end="url(#oa-arrow)"/>
<rect class="value" x="420" y="130" width="324" height="22"/>
<text class="small value" x="428" y="146"> 4  01 item:1 name &quot;Oat milk&quot;</text>
<rect x="420" y="154" width="324" height="22"/>
<text class="small muted" x="428" y="170"> 5  01 item:2 name &quot;Rye bread&quot;</text>
<line class="dashed" x1="316" y1="93" x2="418" y2="165" marker-end="url(#oa-arrow)"/>
<rect x="420" y="178" width="324" height="22"/>
<text class="small muted" x="428" y="194"> 6  01 item:1 quantity &quot;1&quot;</text>
<line class="dashed" x1="316" y1="117" x2="418" y2="189" marker-end="url(#oa-arrow)"/>
<rect class="value" x="420" y="202" width="324" height="22"/>
<text class="small value" x="428" y="218"> 7  01 rev(new) revision</text>
<rect x="420" y="226" width="324" height="22"/>
<text class="small muted" x="428" y="242"> 8  01 rev(old) revision</text>
<line class="dashed" x1="316" y1="141" x2="418" y2="237" marker-end="url(#oa-arrow)"/>
<rect x="420" y="250" width="324" height="22"/>
<text class="small muted" x="428" y="266"> 9  02 item:1 quantity &quot;1&quot;</text>
<line class="dashed" x1="316" y1="165" x2="418" y2="261" marker-end="url(#oa-arrow)"/>
<rect class="value" x="420" y="274" width="324" height="22"/>
<text class="small value" x="428" y="290">10  02 item:1 name &quot;Oat milk&quot;</text>
<rect x="420" y="298" width="324" height="22"/>
<text class="small muted" x="428" y="314">11  02 item:2 name &quot;Rye bread&quot;</text>
<line class="dashed" x1="316" y1="189" x2="418" y2="309" marker-end="url(#oa-arrow)"/>
<rect x="420" y="322" width="324" height="22"/>
<text class="small muted" x="428" y="338">12  03 v1 item:1 quantity &quot;1&quot;</text>
<line class="dashed" x1="316" y1="213" x2="418" y2="333" marker-end="url(#oa-arrow)"/>
<rect x="420" y="346" width="324" height="22"/>
<text class="small muted" x="428" y="362">13  03 v1 item:2 name &quot;Rye bread&quot;</text>
<line class="dashed" x1="316" y1="237" x2="418" y2="357" marker-end="url(#oa-arrow)"/>
<rect class="value" x="420" y="370" width="324" height="22"/>
<text class="small value" x="428" y="386">14  03 v2 item:1 name &quot;Oat milk&quot;</text>
<rect x="420" y="394" width="324" height="22"/>
<text class="small muted" x="428" y="410">15  fe the revision&#x27;s own key</text>
<line class="dashed" x1="316" y1="261" x2="418" y2="405" marker-end="url(#oa-arrow)"/>
<text class="small muted" x="16" y="432">steps: 1 new, copy 1 from row 0, 1 new, copy 1 from 1, 1 new, copy 2 from 2,</text>
<text class="small muted" x="16" y="448">       1 new, copy 2 from 4, 1 new, copy 3 from 6, 1 new, copy 1 from 9</text>
<text class="small" x="16" y="466">bytes: 10 0c 02 03 00 02 03 00 02 05 00 02 05 00 02 07 00 02 03 00</text>
</svg>
</figure>

Rows are sorted by key in both versions, so the new frame is mostly copies: ten rows are copied, in six runs, and six are new. A copy is written as its length and where it starts, relative to where the last one ended, which is usually zero.

The six new rows are then written column by column:

<figure class="dg">
<svg class="dg" viewBox="0 0 760 420" width="760" height="420" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="The six new rows column by column: ids, the deltas written, and their bytes">
<defs><marker id="col-arrow" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto"><path d="M0,0 L8,4 L0,8 z"/></marker></defs>
<text class="label small muted" x="140" y="20" text-anchor="middle">row 0</text>
<text class="label small muted" x="250" y="20" text-anchor="middle">row 2</text>
<text class="label small muted" x="360" y="20" text-anchor="middle">row 4</text>
<text class="label small muted" x="470" y="20" text-anchor="middle">row 7</text>
<text class="label small muted" x="580" y="20" text-anchor="middle">row 10</text>
<text class="label small muted" x="690" y="20" text-anchor="middle">row 14</text>
<text class="small" x="16" y="46">form</text>
<rect class="shade" x="90" y="30" width="100" height="24"/>
<text class="small" x="140" y="46" text-anchor="middle">00</text>
<rect class="shade" x="200" y="30" width="100" height="24"/>
<text class="small" x="250" y="46" text-anchor="middle">00</text>
<rect class="shade" x="310" y="30" width="100" height="24"/>
<text class="small" x="360" y="46" text-anchor="middle">01</text>
<rect class="shade" x="420" y="30" width="100" height="24"/>
<text class="small" x="470" y="46" text-anchor="middle">01</text>
<rect class="shade" x="530" y="30" width="100" height="24"/>
<text class="small" x="580" y="46" text-anchor="middle">02</text>
<rect class="shade" x="640" y="30" width="100" height="24"/>
<text class="small" x="690" y="46" text-anchor="middle">03</text>
<text class="small muted" x="90" y="70">runs: 2 × 00, 2 × 01, then 02 03</text>
<text class="small" x="740" y="70" text-anchor="end">04 00 04 01 03 02 03</text>
<text class="small" x="16" y="104">slot 0</text>
<rect class="entity" x="90" y="88" width="100" height="24"/>
<text class="small entity" x="140" y="104" text-anchor="middle">143487</text>
<rect class="entity" x="200" y="88" width="100" height="24"/>
<text class="small entity" x="250" y="104" text-anchor="middle">77831</text>
<rect class="entity" x="310" y="88" width="100" height="24"/>
<text class="small entity" x="360" y="104" text-anchor="middle">77831</text>
<rect class="entity" x="420" y="88" width="100" height="24"/>
<text class="small entity" x="470" y="104" text-anchor="middle">143487</text>
<rect class="entity" x="530" y="88" width="100" height="24"/>
<text class="small entity" x="580" y="104" text-anchor="middle">77831</text>
<rect class="entity" x="640" y="88" width="100" height="24"/>
<text class="small entity" x="690" y="104" text-anchor="middle">143527</text>
<text class="small muted" x="90" y="128">+143487 −65656 0 +65656 −65656 +65696</text>
<text class="small" x="740" y="128" text-anchor="end">0b fe c1 11 ef 81 08 00 f0 81 08 ef 81 08 c0 82 08</text>
<text class="small" x="16" y="162">slot 1</text>
<rect class="attribute" x="90" y="146" width="100" height="24"/>
<text class="small attribute" x="140" y="162" text-anchor="middle">77850</text>
<rect class="attribute" x="200" y="146" width="100" height="24"/>
<text class="small attribute" x="250" y="162" text-anchor="middle">77873</text>
<rect class="attribute" x="310" y="146" width="100" height="24"/>
<text class="small attribute" x="360" y="162" text-anchor="middle">77873</text>
<rect class="attribute" x="420" y="146" width="100" height="24"/>
<text class="small attribute" x="470" y="162" text-anchor="middle">77850</text>
<rect class="attribute" x="530" y="146" width="100" height="24"/>
<text class="small attribute" x="580" y="162" text-anchor="middle">77873</text>
<rect class="attribute" x="640" y="146" width="100" height="24"/>
<text class="small attribute" x="690" y="162" text-anchor="middle">77831</text>
<text class="small muted" x="90" y="186">+77850 +23 0 −23 +23 −42</text>
<text class="small" x="740" y="186" text-anchor="end">0b b4 c0 09 2e 00 2d 2e 53</text>
<text class="small" x="16" y="220">slot 2</text>
<rect class="value" x="90" y="204" width="100" height="24"/>
<text class="small value" x="140" y="220" text-anchor="middle">143491</text>
<rect class="value" x="200" y="204" width="100" height="24"/>
<text class="small value" x="250" y="220" text-anchor="middle">143526</text>
<rect class="value" x="310" y="204" width="100" height="24"/>
<text class="small value" x="360" y="220" text-anchor="middle">143526</text>
<rect class="value" x="420" y="204" width="100" height="24"/>
<text class="small value" x="470" y="220" text-anchor="middle">143491</text>
<rect class="value" x="530" y="204" width="100" height="24"/>
<text class="small value" x="580" y="220" text-anchor="middle">143526</text>
<rect class="value" x="640" y="204" width="100" height="24"/>
<text class="small value" x="690" y="220" text-anchor="middle">77873</text>
<text class="small muted" x="90" y="244">+143491 +35 0 −35 +35 −65653</text>
<text class="small" x="740" y="244" text-anchor="end">0b 86 c2 11 46 00 45 46 e9 81 08</text>
<text class="small" x="16" y="278">slot 3</text>
<rect class="ghost" x="90" y="262" width="100" height="24"/>
<rect class="ghost" x="200" y="262" width="100" height="24"/>
<rect class="ghost" x="310" y="262" width="100" height="24"/>
<rect class="ghost" x="420" y="262" width="100" height="24"/>
<rect class="ghost" x="530" y="262" width="100" height="24"/>
<rect class="value" x="640" y="262" width="100" height="24"/>
<text class="small value" x="690" y="278" text-anchor="middle">143526</text>
<text class="small muted" x="90" y="302">only the history row has a fourth part</text>
<text class="small" x="740" y="302" text-anchor="end">01 cc c2 11</text>
<text class="small" x="16" y="336">datum</text>
<rect class="hash" x="90" y="320" width="100" height="24"/>
<text class="small hash" x="140" y="336" text-anchor="middle">143517</text>
<rect class="hash" x="200" y="320" width="100" height="24"/>
<text class="small hash" x="250" y="336" text-anchor="middle">143517</text>
<rect class="hash" x="310" y="320" width="100" height="24"/>
<text class="small hash" x="360" y="336" text-anchor="middle">143517</text>
<rect class="hash" x="420" y="320" width="100" height="24"/>
<text class="small hash" x="470" y="336" text-anchor="middle">143517</text>
<rect class="hash" x="530" y="320" width="100" height="24"/>
<text class="small hash" x="580" y="336" text-anchor="middle">143517</text>
<rect class="hash" x="640" y="320" width="100" height="24"/>
<text class="small hash" x="690" y="336" text-anchor="middle">143517</text>
<text class="small muted" x="90" y="360">+143517, then a run of five zeros</text>
<text class="small" x="740" y="360" text-anchor="end">01 ba c2 11 0a 00</text>
<text class="label small muted" x="16" y="390">Each column is zigzag deltas from the row above, as runs: a signed count, then one value repeated or values one by one.</text>
<text class="label small muted" x="16" y="408">Slot 0 of rows 0 and 7 is the revision&#x27;s entity; a history row&#x27;s slot 0 is its version. Ids are token offsets over 16.</text>
</svg>
</figure>

Neighboring rows mostly share their parts, so each column is written as the difference from the row above, and a run of equal values is written once. The whole section of 16 operations takes 76 bytes. The frame's data is 126 bytes, header included, for a block of 4,372.

A frame stands for its block exactly. The tree's encoding is canonical, so the rows rebuild the block byte for byte. Every build checks that a frame decodes back to its rows before writing it, and a debug build, or one with `DIALOG_ARCHIVE_VERIFY` set, also rebuilds the block and compares. A node in an older layout, or a block that is not a tree node, is kept whole instead, as a frame of kind `ff` with the block's bytes after the header.

<div class="aside">

**Implementations.** Splitting a key is `key_tokens` in [`dialog-archive-file/src/node.rs`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-archive-file/src/node.rs). Tokens and their records are [`tokens.rs`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-archive-file/src/tokens.rs). Rows, columns and copies are [`rows.rs`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-archive-file/src/rows.rs), and the frame header is `FrameHeader` in [`archive/index.rs`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-archive-file/src/archive/index.rs).

</div>

### Step 4: the turn, the records and the slot

Splitting and encoding happen before the writer touches the file. Writing needs the writer's **turn**, so that two writers never write the same bytes. The writer takes it, lays the records out after the newest slot's end, writes them, and then writes a slot that names the new end.

Every record has the same nine-byte header:

<figure class="dg">
<svg class="dg" viewBox="0 0 760 170" width="760" height="170" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="A record&#x27;s header: kind, length and CRC-32, then the body">
<defs><marker id="rh-arrow" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto"><path d="M0,0 L8,4 L0,8 z"/></marker></defs>
<text class="label small muted" x="16" y="20">commit 2&#x27;s tokens record, at offset 2,295,767</text>
<rect class="critical" x="16" y="32" width="70" height="24"/>
<text class="critical" x="51" y="48.5" text-anchor="middle">04</text>
<rect class="shade" x="86" y="32" width="36" height="24"/>
<text x="104" y="48.5" text-anchor="middle">b8</text>
<rect class="shade" x="122" y="32" width="36" height="24"/>
<text x="140" y="48.5" text-anchor="middle">02</text>
<rect class="shade" x="158" y="32" width="36" height="24"/>
<text x="176" y="48.5" text-anchor="middle">00</text>
<rect class="shade" x="194" y="32" width="36" height="24"/>
<text x="212" y="48.5" text-anchor="middle">00</text>
<rect class="hash" x="230" y="32" width="36" height="24"/>
<text class="hash" x="248" y="48.5" text-anchor="middle">e8</text>
<rect class="hash" x="266" y="32" width="36" height="24"/>
<text class="hash" x="284" y="48.5" text-anchor="middle">52</text>
<rect class="hash" x="302" y="32" width="36" height="24"/>
<text class="hash" x="320" y="48.5" text-anchor="middle">3a</text>
<rect class="hash" x="338" y="32" width="36" height="24"/>
<text class="hash" x="356" y="48.5" text-anchor="middle">af</text>
<rect class="dashed" x="374" y="32" width="300" height="30"/>
<text class="small" x="524" y="52" text-anchor="middle">body: 696 bytes, five tokens</text>
<path class="wire" d="M16,66 L16,72 L86,72 L86,66"/>
<text class="small critical" x="51" y="86" text-anchor="middle">kind 4</text>
<path class="wire" d="M86,66 L86,72 L230,72 L230,66"/>
<text class="small muted" x="158" y="86" text-anchor="middle">length 696</text>
<path class="wire" d="M230,66 L230,72 L374,72 L374,66"/>
<text class="small hash" x="302" y="86" text-anchor="middle">crc32 af3a52e8</text>
<text class="label small muted" x="16" y="118">1 + 4 + 4 bytes, little-endian. The checksum covers the kind byte and the body, so a reader walking the file stops at</text>
<text class="label small muted" x="16" y="136">the first record that is cut short or does not match: everything before it is whole.</text>
</svg>
</figure>

| kind | record | its checksum covers |
|---|---|---|
| 4 | tokens: a count, then each token's pad byte, padding, length and bytes | the whole body |
| 5 | frame: a pad byte, padding, then the frame's data | the whole body |
| 7 | pages: pages of the file's index | until a page is reused; each page has its own |
| 8 | extent: space claimed by one writer | its header only; each frame inside has its own |
| 9 | cell: one value of a memory cell | the whole body |

The commit's append is the tokens record and the frame record. The new head follows as two cell records, `revision` and `induction` (see [step 5](#step-5-the-head-moves)), each its own append:

<figure class="dg">
<svg class="dg" viewBox="0 0 760 260" width="760" height="260" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Commit 2&#x27;s append in file order: tokens, the frame, then a slot; then two cell records, each followed by a slot">
<defs><marker id="al-arrow" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto"><path d="M0,0 L8,4 L0,8 z"/></marker></defs>
<rect class="attribute" x="16" y="40" width="324.3" height="40"/>
<text class="small" x="178.15" y="58" text-anchor="middle">tokens</text>
<text class="small muted" x="178.15" y="72" text-anchor="middle">705 B</text>
<line x1="16" y1="80" x2="16" y2="90"/>
<text class="small muted" x="16" y="100">2,295,767</text>
<rect class="value" x="340.3" y="40" width="69" height="40"/>
<text class="small" x="374.8" y="58" text-anchor="middle">frame</text>
<text class="small muted" x="374.8" y="72" text-anchor="middle">150 B</text>
<line x1="340.3" y1="80" x2="340.3" y2="104"/>
<text class="small muted" x="340.3" y="114">2,296,472</text>
<rect class="hash" x="409.3" y="40" width="159.62" height="40"/>
<text class="small" x="489.11" y="58" text-anchor="middle">cell: revision</text>
<text class="small muted" x="489.11" y="72" text-anchor="middle">347 B</text>
<line x1="409.3" y1="80" x2="409.3" y2="90"/>
<text class="small muted" x="409.3" y="100">2,296,622</text>
<rect class="hash" x="568.92" y="40" width="162.38" height="40"/>
<text class="small" x="650.11" y="58" text-anchor="middle">cell: induction</text>
<text class="small muted" x="650.11" y="72" text-anchor="middle">353 B</text>
<line x1="568.92" y1="80" x2="568.92" y2="104"/>
<text class="small muted" x="568.92" y="114">2,296,969</text>
<line x1="731.3" y1="80" x2="731.3" y2="90"/>
<text class="small muted" x="731.3" y="100" text-anchor="end">2,297,322</text>
<text class="label small muted" x="16" y="28">after commit 1, the file ended at 2,295,767</text>
<line class="critical" x1="409.3" y1="82" x2="409.3" y2="130" marker-end="url(#al-arrow)"/>
<rect class="critical" x="239.3" y="134" width="166" height="24"/>
<text class="small critical" x="322.3" y="150" text-anchor="middle">sequence 7 → rolling 1</text>
<line class="critical" x1="568.92" y1="82" x2="568.92" y2="162" marker-end="url(#al-arrow)"/>
<rect class="critical" x="398.92" y="166" width="166" height="24"/>
<text class="small critical" x="481.92" y="182" text-anchor="middle">sequence 8 → rolling 0</text>
<line class="critical" x1="731.3" y1="82" x2="731.3" y2="194" marker-end="url(#al-arrow)"/>
<rect class="critical" x="561.3" y="198" width="166" height="24"/>
<text class="small critical" x="644.3" y="214" text-anchor="middle">sequence 9 → rolling 1</text>
<text class="label small muted" x="16" y="250">Records go past the end first; the slot that names the new end is written last, over the older of the two rolling slots.</text>
</svg>
</figure>

A slot holds the file's whole state as of one append:

<figure class="dg">
<svg class="dg" viewBox="0 0 760 360" width="760" height="360" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="A slot&#x27;s fields with the values of slot 9">
<defs><marker id="sl-arrow" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto"><path d="M0,0 L8,4 L0,8 z"/></marker></defs>
<rect class="shade" x="16" y="16" width="410" height="24"/>
<text class="label small" x="24" y="32">length</text>
<text class="small muted" x="436" y="32">u32</text>
<text class="small" x="536" y="32">356</text>
<rect class="hash" x="16" y="43" width="410" height="24"/>
<text class="label small" x="24" y="59">crc32 of the body</text>
<text class="small muted" x="436" y="59">u32</text>
<text class="small" x="536" y="59">6624d565</text>
<rect x="16" y="70" width="410" height="24"/>
<text class="label small" x="24" y="86">sequence</text>
<text class="small muted" x="436" y="86">u64</text>
<text class="small" x="536" y="86">9</text>
<rect x="16" y="97" width="410" height="24"/>
<text class="label small" x="24" y="113">end</text>
<text class="small muted" x="436" y="113">u64</text>
<text class="small" x="536" y="113">2,297,322</text>
<rect x="16" y="124" width="410" height="24"/>
<text class="label small" x="24" y="140">durable: the last sync point&#x27;s sequence</text>
<text class="small muted" x="436" y="140">u64</text>
<text class="small" x="536" y="140">0</text>
<rect class="hash" x="16" y="151" width="410" height="24"/>
<text class="label small" x="24" y="167">boot it was written in</text>
<text class="small muted" x="436" y="167">u64</text>
<text class="small" x="536" y="167">1d7afa8f10579f35</text>
<rect class="shade" x="16" y="178" width="410" height="24"/>
<text class="label small" x="24" y="194">counts: blocks, bytes, frames, against, frame bytes, tokens, …</text>
<text class="small muted" x="436" y="194">7 × u64</text>
<text class="small" x="536" y="194">0, 0, 3, 1, 363, 20, 1,435</text>
<rect class="attribute" x="16" y="205" width="410" height="24"/>
<text class="label small" x="24" y="221">catalogs</text>
<text class="small muted" x="436" y="221">count, names</text>
<text class="small" x="536" y="221">index</text>
<rect class="value" x="16" y="232" width="410" height="24"/>
<text class="label small" x="24" y="248">the file&#x27;s index: its root</text>
<text class="small muted" x="436" y="248">u32, node</text>
<text class="small" x="536" y="248">a leaf of 25 entries</text>
<rect x="16" y="259" width="410" height="24"/>
<text class="label small" x="24" y="275">pages free; pages freed since the sync point</text>
<text class="small muted" x="436" y="275">lists of u32</text>
<text class="small" x="536" y="275">none; none</text>
<rect class="bob" x="16" y="286" width="410" height="24"/>
<text class="label small" x="24" y="302">extents held; free; freed since the sync point</text>
<text class="small muted" x="436" y="302">lists of u64</text>
<text class="small" x="536" y="302">196,616 and 1,246,469; –; –</text>
<rect class="hash" x="16" y="313" width="410" height="24"/>
<text class="label small" x="24" y="329">the cells&#x27; index: its root</text>
<text class="small muted" x="436" y="329">u32, node</text>
<text class="small" x="536" y="329">a leaf of 3 entries</text>
<text class="label small muted" x="16" y="352">Little-endian throughout. A slot has 64 KiB; this one uses 364 bytes. Its checksum is what makes it whole or not.</text>
</svg>
</figure>

There are two **rolling** slots, and an append writes the older of the two. That way one of them is always whole, whatever a crash does to the other:

<figure class="dg">
<svg class="dg" viewBox="0 0 760 230" width="760" height="230" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Two rolling slots take turns; a slot torn by a crash leaves the other, older one whole">
<defs><marker id="ro-arrow" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto"><path d="M0,0 L8,4 L0,8 z"/></marker></defs>
<text class="title" x="16" y="20">every append writes the older slot</text>
<text class="label small" x="16" y="46">append 7</text>
<rect class="shade" x="16" y="56" width="120" height="24"/>
<text class="small" x="76" y="72" text-anchor="middle">rolling 0: seq 6</text>
<rect class="critical" x="16" y="86" width="120" height="24"/>
<text class="small" x="76" y="102" text-anchor="middle">rolling 1: seq 7</text>
<text class="label small" x="166" y="46">append 8</text>
<rect class="critical" x="166" y="56" width="120" height="24"/>
<text class="small" x="226" y="72" text-anchor="middle">rolling 0: seq 8</text>
<rect class="shade" x="166" y="86" width="120" height="24"/>
<text class="small" x="226" y="102" text-anchor="middle">rolling 1: seq 7</text>
<text class="label small" x="316" y="46">append 9</text>
<rect class="shade" x="316" y="56" width="120" height="24"/>
<text class="small" x="376" y="72" text-anchor="middle">rolling 0: seq 8</text>
<rect class="critical" x="316" y="86" width="120" height="24"/>
<text class="small" x="376" y="102" text-anchor="middle">rolling 1: seq 9</text>
<text class="label small" x="500" y="46">append 10, cut short</text>
<rect class="ghost" x="500" y="56" width="120" height="24"/>
<text class="small critical" x="560" y="72" text-anchor="middle">rolling 0: torn</text>
<rect class="value" x="500" y="86" width="120" height="24"/>
<text class="small" x="560" y="102" text-anchor="middle">rolling 1: seq 9</text>
<line x1="624" y1="98" x2="670" y2="98" marker-end="url(#ro-arrow)"/>
<text class="label small" x="676" y="96">opening</text>
<text class="label small" x="676" y="110">takes this</text>
<text class="label small muted" x="16" y="160">Opening reads both rolling slots and takes the newer one whose checksum matches. The records an append wrote</text>
<text class="label small muted" x="16" y="178">before its slot was cut short are past that slot&#x27;s end: no slot names them, and the next writer writes over them.</text>
<text class="label small muted" x="16" y="206">The third slot, the durable one, is written only at a sync point (below).</text>
</svg>
</figure>

The turn itself is not a lock on the file. It is a lock on one byte, far past the end of any data:

<figure class="dg">
<svg class="dg" viewBox="0 0 760 190" width="760" height="190" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="The lock bytes sit at 2^62, far past any data, so locking them never blocks a read or a write">
<defs><marker id="lk-arrow" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto"><path d="M0,0 L8,4 L0,8 z"/></marker></defs>
<line class="dashed" x1="126" y1="60" x2="396" y2="60"/>
<rect class="value" x="16" y="48" width="110" height="24"/>
<text class="small" x="71" y="64" text-anchor="middle">the data, 3.5 MB</text>
<text class="small muted" x="261" y="50" text-anchor="middle">4.6 × 10¹⁸ empty bytes</text>
<rect class="shade" x="396" y="48" width="64" height="24"/>
<text class="small" x="428" y="64" text-anchor="middle">sync</text>
<text class="small muted" x="428" y="92" text-anchor="middle">2⁶²−3</text>
<rect class="shade" x="464" y="48" width="64" height="24"/>
<text class="small" x="496" y="64" text-anchor="middle">recovery</text>
<text class="small muted" x="496" y="92" text-anchor="middle">2⁶²−2</text>
<rect class="entity" x="532" y="48" width="64" height="24"/>
<text class="small" x="564" y="64" text-anchor="middle">present</text>
<text class="small muted" x="564" y="92" text-anchor="middle">2⁶²−1</text>
<rect class="critical" x="600" y="48" width="64" height="24"/>
<text class="small" x="632" y="64" text-anchor="middle">turn</text>
<text class="small muted" x="632" y="92" text-anchor="middle">2⁶²</text>
<rect class="bob" x="668" y="48" width="72" height="24"/>
<text class="small" x="704" y="64" text-anchor="middle">owner(x)</text>
<text class="small muted" x="704" y="92" text-anchor="middle">2⁶²+1+x</text>
<text class="label small muted" x="16" y="124">turn: held by the writer laying out and writing an append.   present: held shared by every process with the file open.</text>
<text class="label small muted" x="16" y="142">recovery: held while a process opens the file.   sync: held while a sync point is made.   owner(x): held by extent x&#x27;s writer.</text>
<text class="label small muted" x="16" y="166">POSIX record locks on Unix, LockFileEx on Windows: a process that dies lets go of every byte it held.</text>
</svg>
</figure>

Readers take no lock at all. The writer holding the turn waits on no other lock, so no cycle of waiting runs through it.

<div class="aside">

**Implementations.** Records are [`dialog-archive-file/src/file.rs`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-archive-file/src/file.rs), slots [`archive/slot.rs`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-archive-file/src/archive/slot.rs), and the lock bytes [`locks.rs`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-archive-file/src/locks.rs).

</div>

### Step 5: the head moves

The branch's head is a cell (see [Commits and History](./history.md)), and the file keeps cells the way SQLite keeps commits in its write-ahead log. An update appends a cell record holding the cell's name and its new value, and the cells' own index, rooted in the slot, files the name at its newest record. Nothing is written over.

<figure class="dg">
<svg class="dg" viewBox="0 0 760 344" width="760" height="344" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="The head&#x27;s cell record: its name, its value naming the new root, and the extents it needs, none here">
<defs><marker id="cr-arrow" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto"><path d="M0,0 L8,4 L0,8 z"/></marker></defs>
<text class="label small muted" x="16" y="20">commit 2&#x27;s head: record at 2,296,622, data at 2,296,640 = 16 × 143,540</text>
<rect class="shade" x="16" y="32" width="26" height="24"/>
<text x="29" y="48.5" text-anchor="middle">09</text>
<rect class="shade" x="42" y="32" width="26" height="24"/>
<text x="55" y="48.5" text-anchor="middle">52</text>
<rect class="shade" x="68" y="32" width="26" height="24"/>
<text x="81" y="48.5" text-anchor="middle">01</text>
<rect class="shade" x="94" y="32" width="26" height="24"/>
<text x="107" y="48.5" text-anchor="middle">00</text>
<rect class="shade" x="120" y="32" width="26" height="24"/>
<text x="133" y="48.5" text-anchor="middle">00</text>
<rect class="shade" x="146" y="32" width="26" height="24"/>
<text x="159" y="48.5" text-anchor="middle">11</text>
<rect class="shade" x="172" y="32" width="26" height="24"/>
<text x="185" y="48.5" text-anchor="middle">79</text>
<rect class="shade" x="198" y="32" width="26" height="24"/>
<text x="211" y="48.5" text-anchor="middle">cd</text>
<rect class="shade" x="224" y="32" width="26" height="24"/>
<text x="237" y="48.5" text-anchor="middle">1c</text>
<path class="wire" d="M16,60 L16,66 L250,66 L250,60"/>
<text class="small muted" x="133" y="80" text-anchor="middle">kind 9: cell, length 338, crc 1ccd7911</text>
<rect x="16" y="96" width="40" height="24"/>
<text x="36" y="112.5" text-anchor="middle">08</text>
<rect x="56" y="96" width="20" height="24"/>
<text class="muted" x="66" y="112.5" text-anchor="middle">00</text>
<rect x="76" y="96" width="20" height="24"/>
<text class="muted" x="86" y="112.5" text-anchor="middle">00</text>
<rect x="96" y="96" width="20" height="24"/>
<text class="muted" x="106" y="112.5" text-anchor="middle">00</text>
<rect x="116" y="96" width="20" height="24"/>
<text class="muted" x="126" y="112.5" text-anchor="middle">00</text>
<rect x="136" y="96" width="20" height="24"/>
<text class="muted" x="146" y="112.5" text-anchor="middle">00</text>
<rect x="156" y="96" width="20" height="24"/>
<text class="muted" x="166" y="112.5" text-anchor="middle">00</text>
<rect x="176" y="96" width="20" height="24"/>
<text class="muted" x="186" y="112.5" text-anchor="middle">00</text>
<rect x="196" y="96" width="20" height="24"/>
<text class="muted" x="206" y="112.5" text-anchor="middle">00</text>
<rect class="shade" x="216" y="96" width="40" height="24"/>
<text x="236" y="112.5" text-anchor="middle">14</text>
<rect class="shade" x="256" y="96" width="40" height="24"/>
<text x="276" y="112.5" text-anchor="middle">00</text>
<path class="wire" d="M16,124 L16,130 L56,130 L56,124"/>
<text class="small muted" x="36" y="144" text-anchor="middle">pad 8</text>
<path class="wire" d="M216,124 L216,130 L296,130 L296,124"/>
<text class="small muted" x="256" y="144" text-anchor="middle">name: 20 bytes</text>
<rect class="attribute" x="296" y="96" width="300" height="26"/>
<text class="attribute" x="446" y="114" text-anchor="middle">branch/main 00 revision</text>
<rect x="16" y="168" width="64" height="24"/>
<text x="48" y="184.5" text-anchor="middle">01</text>
<rect class="shade" x="80" y="168" width="30" height="24"/>
<text x="95" y="184.5" text-anchor="middle">2c</text>
<rect class="shade" x="110" y="168" width="30" height="24"/>
<text x="125" y="184.5" text-anchor="middle">01</text>
<rect class="shade" x="140" y="168" width="30" height="24"/>
<text x="155" y="184.5" text-anchor="middle">00</text>
<rect class="shade" x="170" y="168" width="30" height="24"/>
<text x="185" y="184.5" text-anchor="middle">00</text>
<rect class="hash" x="200" y="168" width="26" height="24"/>
<text class="hash" x="213" y="184.5" text-anchor="middle">a7</text>
<rect class="hash" x="226" y="168" width="26" height="24"/>
<text class="hash" x="239" y="184.5" text-anchor="middle">64</text>
<rect class="hash" x="252" y="168" width="26" height="24"/>
<text class="hash" x="265" y="184.5" text-anchor="middle">74</text>
<rect class="hash" x="278" y="168" width="26" height="24"/>
<text class="hash" x="291" y="184.5" text-anchor="middle">72</text>
<rect class="hash" x="304" y="168" width="26" height="24"/>
<text class="hash" x="317" y="184.5" text-anchor="middle">65</text>
<rect class="hash" x="330" y="168" width="26" height="24"/>
<text class="hash" x="343" y="184.5" text-anchor="middle">65</text>
<rect class="hash" x="356" y="168" width="26" height="24"/>
<text class="hash" x="369" y="184.5" text-anchor="middle">98</text>
<rect class="hash" x="382" y="168" width="26" height="24"/>
<text class="hash" x="395" y="184.5" text-anchor="middle">20</text>
<rect class="hash" x="408" y="168" width="26" height="24"/>
<text class="hash" x="421" y="184.5" text-anchor="middle">18</text>
<rect class="hash" x="434" y="168" width="26" height="24"/>
<text class="hash" x="447" y="184.5" text-anchor="middle">56</text>
<rect class="hash" x="460" y="168" width="26" height="24"/>
<text class="hash" x="473" y="184.5" text-anchor="middle">18</text>
<rect class="hash" x="486" y="168" width="26" height="24"/>
<text class="hash" x="499" y="184.5" text-anchor="middle">54</text>
<rect class="hash" x="512" y="168" width="26" height="24"/>
<text class="hash" x="525" y="184.5" text-anchor="middle">12</text>
<rect class="hash" x="538" y="168" width="26" height="24"/>
<text class="hash" x="551" y="184.5" text-anchor="middle">18</text>
<rect class="hash" x="564" y="168" width="26" height="24"/>
<text class="hash" x="577" y="184.5" text-anchor="middle">b9</text>
<rect class="hash" x="590" y="168" width="26" height="24"/>
<text class="hash" x="603" y="184.5" text-anchor="middle">09</text>
<text x="624" y="185">…</text>
<path class="wire" d="M16,196 L16,202 L80,202 L80,196"/>
<text class="small muted" x="48" y="216" text-anchor="middle">present</text>
<path class="wire" d="M80,196 L80,202 L200,202 L200,196"/>
<text class="small muted" x="140" y="216" text-anchor="middle">300 bytes</text>
<path class="wire" d="M408,196 L408,202 L616,202 L616,196"/>
<text class="small hash" x="512" y="216" text-anchor="middle">the root&#x27;s hash: 56 54 12 b9 09 …</text>
<text class="small muted" x="304" y="230" text-anchor="middle">CBOR: &quot;tree&quot; is the root&#x27;s 32 bytes</text>
<rect class="bob" x="16" y="262" width="56" height="24"/>
<text class="bob" x="44" y="278.5" text-anchor="middle">00</text>
<rect class="bob" x="72" y="262" width="56" height="24"/>
<text class="bob" x="100" y="278.5" text-anchor="middle">00</text>
<path class="wire" d="M16,290 L16,296 L128,296 L128,290"/>
<text class="small bob" x="72" y="310" text-anchor="middle">needs: 0 extents</text>
<text class="label small muted" x="140" y="278">each needed extent would follow: record u64, epoch u64, bytes of frames u64</text>
<text class="label small muted" x="16" y="336">The cells&#x27; index then files the name&#x27;s key, 1f2546b7, at 143,540. A cell read follows that and nothing else.</text>
</svg>
</figure>

The name is the cell's memory space, `branch/main`, a zero byte, and the cell, `revision`. The value is the new revision, which names the new root by its hash: `56 54 12 b9 09 …` sits right in its bytes. The commit also moves a second cell, `induction`, the revision through which the branch's rules have run.

The order of the records is what keeps heads safe:

<figure class="dg">
<svg class="dg" viewBox="0 0 760 250" width="760" height="250" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Why a head after its blocks survives any power loss: recovery keeps a prefix of the log, and a head is always behind what it names">
<defs><marker id="or-arrow" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto"><path d="M0,0 L8,4 L0,8 z"/></marker></defs>
<text class="title" x="16" y="20">the log, in the order it was written</text>
<rect class="attribute" x="16" y="34" width="170" height="34"/>
<text class="small" x="101" y="56" text-anchor="middle">tokens</text>
<rect class="value" x="190" y="34" width="170" height="34"/>
<text class="small" x="275" y="56" text-anchor="middle">frame 565412…</text>
<rect class="hash" x="364" y="34" width="170" height="34"/>
<text class="small" x="449" y="56" text-anchor="middle">head → 565412…</text>
<path class="wire hash" d="M 446 34 C 420 6, 300 6, 280 32" marker-end="url(#or-arrow)"/>
<line class="critical" x1="360" y1="30" x2="360" y2="76"/>
<text class="small critical" x="360" y="90" text-anchor="middle">1</text>
<text class="label small critical" x="16" y="114">1. power lost after the frame</text>
<text class="label small" x="250" y="114">the head is not there: the branch is at commit 1, whole</text>
<line class="critical" x1="538" y1="30" x2="538" y2="76"/>
<text class="small critical" x="538" y="90" text-anchor="middle">2</text>
<text class="label small critical" x="16" y="136">2. power lost after the head</text>
<text class="label small" x="250" y="136">everything is there: the branch is at commit 2, whole</text>
<text class="label small muted" x="16" y="168">The disk can reorder unsynced writes, so a record is kept only if everything before it in the slot is whole:</text>
<text class="label small muted" x="16" y="186">opening walks every record since the durable slot by its checksum. What a power loss can lose is a suffix.</text>
<text class="label small muted" x="16" y="212">Like SQLite&#x27;s synchronous=NORMAL: an update is durable at the next sync point, not at its own.</text>
</svg>
</figure>

<div class="aside">

**Implementations.** Cells are [`dialog-archive-file/src/archive/cells.rs`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-archive-file/src/archive/cells.rs). A cell's edition is the BLAKE3 hash of its value, as on the file system store, so a compare-and-swap names the same version either way.

</div>

### Step 6: a sync point makes it durable

Until now, everything Alice wrote went to the operating system, not necessarily to the disk. A process that crashes loses nothing the operating system has. A power loss can lose any of it, in any order. A **sync point** draws the line:

<figure class="dg">
<svg class="dg" viewBox="0 0 760 230" width="760" height="230" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="A sync point: the file synced, the durable slot written as slot 12 and synced, then rolling slot 13 says durable 12">
<defs><marker id="sp-arrow" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto"><path d="M0,0 L8,4 L0,8 z"/></marker></defs>
<circle class="shade" cx="32" cy="34" r="12"/>
<text x="32" y="39" text-anchor="middle">1</text>
<text class="label small" x="56" y="32">fsync the file</text>
<text class="small muted" x="56" y="48">everything up to slot 12&#x27;s end (2,299,109) is on the disk</text>
<circle class="critical" cx="32" cy="78" r="12"/>
<text x="32" y="83" text-anchor="middle">2</text>
<text class="label small" x="56" y="76">write the durable slot</text>
<text class="small muted" x="56" y="92">a copy of slot 12, freed pages and extents now free</text>
<circle class="shade" cx="32" cy="122" r="12"/>
<text x="32" y="127" text-anchor="middle">3</text>
<text class="label small" x="56" y="120">fsync again</text>
<text class="small muted" x="56" y="136">the durable slot is on the disk</text>
<circle class="value" cx="32" cy="166" r="12"/>
<text x="32" y="171" text-anchor="middle">4</text>
<text class="label small" x="56" y="164">write rolling slot 13</text>
<text class="small muted" x="56" y="180">durable = 12: pages and extents freed before 12 can be reused</text>
<text class="label small muted" x="16" y="210">One follows every 64 appends, 2 seconds, or 2,048 freed index pages, and FileArchive::sync. It is made after the turn is let go.</text>
</svg>
</figure>

The **durable slot**, the third one, is written only here. Nothing the durable slot reaches is ever written over, so after a power loss the file can always go back to it. In the example, the sync point after commit 3 made slot 12 durable.

### Step 7: reading it back

A query for item 1's name walks the tree as [The Search Tree](./tree.md) describes. Each node it needs, it asks the archive for by hash, and the file answers from its own index:

<figure class="dg">
<svg class="dg" viewBox="0 0 760 300" width="760" height="300" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Reading item:1&#x27;s name back: the block&#x27;s key in the slot&#x27;s index, the frame, then each token by its id">
<defs><marker id="lu-arrow" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto"><path d="M0,0 L8,4 L0,8 z"/></marker></defs>
<rect class="hash" x="16" y="30" width="200" height="30"/>
<text class="small hash" x="116" y="50" text-anchor="middle">hash 565412b9097e…</text>
<text class="small muted" x="16" y="76">a reader asks for the root</text>
<rect class="shade" x="16" y="100" width="200" height="30"/>
<text class="small " x="116" y="120" text-anchor="middle">key 39125456</text>
<text class="small muted" x="16" y="146">two bits of space (00: blocks), thirty of the hash</text>
<line x1="116" y1="60" x2="116" y2="98" marker-end="url(#lu-arrow)"/>
<rect class="value" x="16" y="170" width="200" height="30"/>
<text class="small value" x="116" y="190" text-anchor="middle">frame 143531</text>
<text class="small muted" x="16" y="216">the slot&#x27;s index leaf: 39125456 → 143531</text>
<line x1="116" y1="130" x2="116" y2="168" marker-end="url(#lu-arrow)"/>
<rect class="value" x="380" y="170" width="180" height="30"/>
<text class="small value" x="470" y="190" text-anchor="middle">offset 2,296,496</text>
<text class="small muted" x="380" y="216">16 × 143531: the frame&#x27;s data</text>
<line x1="216" y1="185" x2="378" y2="185" marker-end="url(#lu-arrow)"/>
<rect x="380" y="100" width="180" height="30"/>
<text class="small " x="470" y="120" text-anchor="middle">rows, decoded</text>
<text class="small muted" x="380" y="146">against frame 77,897, depth 1</text>
<line x1="470" y1="170" x2="470" y2="132" marker-end="url(#lu-arrow)"/>
<rect class="value" x="380" y="30" width="180" height="30"/>
<text class="small value" x="470" y="50" text-anchor="middle">token 143526</text>
<text class="small muted" x="380" y="76">row 2&#x27;s slot 2</text>
<line x1="470" y1="100" x2="470" y2="62" marker-end="url(#lu-arrow)"/>
<rect class="value" x="600" y="30" width="140" height="30"/>
<text class="small value" x="670" y="50" text-anchor="middle">03 &quot;Oat milk&quot; 00</text>
<text class="small muted" x="600" y="76">at 16 × 143526</text>
<line x1="560" y1="45" x2="598" y2="45" marker-end="url(#lu-arrow)"/>
<text class="label small muted" x="16" y="270">Inside a leaf, a search compares keys part by part where their tokens sit, through the mapping: no key is copied out.</text>
<text class="label small muted" x="16" y="288">When opening trusts the newest slot, it reads the slots and nothing else; everything after is mapped and read on demand.</text>
</svg>
</figure>

The file's index is itself a small tree, from 32-bit keys to places in the file, in three key spaces: blocks by hash, tokens by the hash of their bytes, and **lineages**, the latest frame of each node, which the next version is written against. Its root lives in the slot. In the example it is a single leaf of 25 entries after commit 2, so finding the frame reads nothing but the slot.

The node is then read **in place**: the archive decodes the frame's rows once, and the tree reads keys and values where their tokens sit, through a mapping of the file. A value is read as an archive where it sits, checked the first time a node that holds it is read. No block is built, hashed or decoded on the way.

<div class="aside">

**Implementations.** The file's index is [`dialog-archive-file/src/archive/tree.rs`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-archive-file/src/archive/tree.rs), and what it maps is [`archive/index.rs`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-archive-file/src/archive/index.rs). A node read in place is [`archive/served.rs`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-archive-file/src/archive/served.rs), through `NodeSource` and the node views in [`dialog-search-tree/src/node/view.rs`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-search-tree/src/node/view.rs).

</div>

## Changing the name

Alice buys the bigger carton and changes the name to `"Oat milk, 2 L"`. Under `Pick::Last`, the commit retracts the old claim and asserts the new one, so the root's buffer gains both:

<figure class="dg">
<svg class="dg" viewBox="0 0 760 200" width="760" height="200" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Changing the name: the old value&#x27;s entries are written again as retractions beside the new value&#x27;s">
<defs><marker id="er-arrow" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto"><path d="M0,0 L8,4 L0,8 z"/></marker></defs>
<rect class="critical" x="16" y="20" width="460" height="26"/>
<text class="small" x="24" y="37">00 item:1 name &quot;Oat milk&quot;</text>
<text class="small critical" x="484" y="37">retracted</text>
<rect class="value" x="16" y="50" width="460" height="26"/>
<text class="small" x="24" y="67">00 item:1 name &quot;Oat milk, 2 L&quot;</text>
<text class="small muted" x="484" y="67">datum 143614</text>
<rect class="critical" x="16" y="80" width="460" height="26"/>
<text class="small" x="24" y="97">01 item:1 name &quot;Oat milk&quot;</text>
<text class="small critical" x="484" y="97">retracted</text>
<rect class="value" x="16" y="110" width="460" height="26"/>
<text class="small" x="24" y="127">01 item:1 name &quot;Oat milk, 2 L&quot;</text>
<text class="small muted" x="484" y="127">datum 143614</text>
<rect class="shade" x="16" y="140" width="460" height="26"/>
<text class="small" x="24" y="157">02 …, 03 …, and 05: a coverage entry for what the change replaced</text>
<text class="small muted" x="484" y="157"></text>
<text class="label small muted" x="16" y="182">Commit 3&#x27;s root frame: 23 operations, 13 copied from commit 2&#x27;s and 10 new, 159 bytes for a 6,092-byte block.</text>
</svg>
</figure>

The new root is written against commit 2's, which was written against commit 1's. Each version of a node is one more frame in a chain:

<figure class="dg">
<svg class="dg" viewBox="0 0 760 220" width="760" height="220" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="The root&#x27;s frames, each written against the one before: reading the newest decodes the chain back to a whole frame">
<defs><marker id="ch-arrow" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto"><path d="M0,0 L8,4 L0,8 z"/></marker></defs>
<rect class="value solid" x="16" y="40" width="104" height="44"/>
<text class="small" x="68" y="58" text-anchor="middle" style="fill:var(--dg-paper)">frame 77897</text>
<text class="small" x="68" y="74" text-anchor="middle" style="fill:var(--dg-paper)">depth 0, 117 B</text>
<text class="small muted" x="68" y="100" text-anchor="middle">commit 1</text>
<rect class="value" x="138" y="40" width="104" height="44"/>
<text class="small" x="190" y="58" text-anchor="middle">frame 143531</text>
<text class="small" x="190" y="74" text-anchor="middle">depth 1, 126 B</text>
<text class="small muted" x="190" y="100" text-anchor="middle">commit 2</text>
<line x1="136" y1="62" x2="120" y2="62" marker-end="url(#ch-arrow)"/>
<rect class="value" x="260" y="40" width="104" height="44"/>
<text class="small" x="312" y="58" text-anchor="middle">frame 143642</text>
<text class="small" x="312" y="74" text-anchor="middle">depth 2, 159 B</text>
<text class="small muted" x="312" y="100" text-anchor="middle">commit 3</text>
<line x1="258" y1="62" x2="242" y2="62" marker-end="url(#ch-arrow)"/>
<rect class="value" x="382" y="40" width="104" height="44"/>
<text class="small" x="434" y="58" text-anchor="middle">frame 143847</text>
<text class="small" x="434" y="74" text-anchor="middle">depth 3, 128 B</text>
<text class="small muted" x="434" y="100" text-anchor="middle">commit 4, folded</text>
<line x1="380" y1="62" x2="364" y2="62" marker-end="url(#ch-arrow)"/>
<rect class="value" x="504" y="40" width="104" height="44"/>
<text class="small" x="556" y="58" text-anchor="middle">frame 209486</text>
<text class="small" x="556" y="74" text-anchor="middle">depth 4, 214 B</text>
<text class="small muted" x="556" y="100" text-anchor="middle">commit 6</text>
<line x1="502" y1="62" x2="486" y2="62" marker-end="url(#ch-arrow)"/>
<rect class="value" x="626" y="40" width="104" height="44"/>
<text class="small" x="678" y="58" text-anchor="middle">frame 209547</text>
<text class="small" x="678" y="74" text-anchor="middle">depth 5, 114 B</text>
<text class="small muted" x="678" y="100" text-anchor="middle">a lost try</text>
<line x1="624" y1="62" x2="608" y2="62" marker-end="url(#ch-arrow)"/>
<text class="label small muted" x="16" y="28">base ←</text>
<text class="label small muted" x="16" y="136">Reading frame 209,547 decodes six frames: the whole one, then each frame&#x27;s copies and new rows on top. A writer looks up the</text>
<text class="label small muted" x="16" y="154">node&#x27;s lineage (its catalog, its kind and its first key) to find the frame to write against.</text>
<text class="label small muted" x="16" y="180">At depth 32 (CHAIN) the next version is written whole again, so no read decodes more than 32 frames.</text>
<text class="label small muted" x="16" y="206">Commit 5&#x27;s root is frame 143,836, also written against 143,642: two versions can share a base.</text>
</svg>
</figure>

<div class="aside">

**Implementations.** The chain's limit is `CHAIN` in [`dialog-archive-file/src/archive.rs`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-archive-file/src/archive.rs), and lineages are `lineage_key` in [`archive/index.rs`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-archive-file/src/archive/index.rs).

</div>

## When others are writing too

Any number of processes can have the file open: Alice's app, a sync daemon, a command-line tool. They share the file through the slots and the lock bytes, and nothing else.

### Bob's commit while Alice's process holds the turn

A writer holds the turn only while it lays out and writes its records. Even so, a writer that finds the turn taken does not wait. It writes its blocks whole into an **extent**, space it claimed earlier, in a turn of its own. In the example, a helper process plays Alice's and holds the turn, while the example's own peer plays Bob's and commits *the `demo.grocery/name` of `item:3` is "Eggs"*:

<figure class="dg">
<svg class="dg" viewBox="0 0 760 430" width="760" height="430" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Bob&#x27;s process commits while Alice&#x27;s holds the turn: his block goes into his extent at once, and his head waits for the turn">
<defs><marker id="tp-arrow" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto"><path d="M0,0 L8,4 L0,8 z"/></marker></defs>
<text class="alice" x="130" y="20" text-anchor="middle">Alice&#x27;s process</text>
<text class="title" x="380" y="20" text-anchor="middle">the file</text>
<text class="bob" x="630" y="20" text-anchor="middle">Bob&#x27;s process</text>
<line class="dashed" x1="130" y1="28" x2="130" y2="410"/>
<line class="dashed" x1="380" y1="28" x2="380" y2="410"/>
<line class="dashed" x1="630" y1="28" x2="630" y2="410"/>
<line class="alice" x1="130" y1="52" x2="376" y2="52" marker-end="url(#tp-arrow)"/>
<text class="small alice" x="250" y="46" text-anchor="middle">takes the turn</text>
<rect class="alice" x="118" y="58" width="24" height="248"/>
<line class="bob" x1="630" y1="86" x2="384" y2="86" marker-end="url(#tp-arrow)"/>
<text class="small bob" x="506" y="80" text-anchor="middle">tries the turn: taken</text>
<line class="bob" x1="630" y1="134" x2="384" y2="134" marker-end="url(#tp-arrow)"/>
<text class="small bob" x="506" y="110" text-anchor="middle">writes the block whole into</text>
<text class="small bob" x="506" y="126" text-anchor="middle">its extent at 1,246,469</text>
<line class="bob" x1="630" y1="168" x2="384" y2="168" marker-end="url(#tp-arrow)"/>
<text class="small bob" x="506" y="162" text-anchor="middle">count: 8,059 bytes</text>
<text class="small muted" x="392" y="192">readers in any process</text>
<text class="small muted" x="392" y="206">find the block now</text>
<rect class="bob" x="618" y="226" width="24" height="80"/>
<text class="small bob" x="650" y="256">the head</text>
<text class="small bob" x="650" y="270">waits for</text>
<text class="small bob" x="650" y="284">the turn</text>
<line class="alice" x1="130" y1="306" x2="376" y2="306" marker-end="url(#tp-arrow)"/>
<text class="small alice" x="250" y="300" text-anchor="middle">lets go</text>
<line class="bob" x1="630" y1="350" x2="384" y2="350" marker-end="url(#tp-arrow)"/>
<text class="small bob" x="506" y="326" text-anchor="middle">takes the turn, appends the head:</text>
<text class="small bob" x="506" y="342" text-anchor="middle">needs extent 1,246,469, epoch 4</text>
<line class="bob" x1="630" y1="396" x2="384" y2="396" marker-end="url(#tp-arrow)"/>
<text class="small bob" x="506" y="372" text-anchor="middle">its next commit, in the turn,</text>
<text class="small bob" x="506" y="388" text-anchor="middle">folds the block in as frame 143,847</text>
<text class="label small muted" x="16" y="424">In the example, a helper process holds the turn for 300 ms while the example&#x27;s peer commits item:3.</text>
</svg>
</figure>

An extent is a record whose body is written later, one frame at a time:

<figure class="dg">
<svg class="dg" viewBox="0 0 760 260" width="760" height="260" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="An extent&#x27;s header: count, its inverse and the epoch, then whole frames">
<defs><marker id="eb-arrow" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto"><path d="M0,0 L8,4 L0,8 z"/></marker></defs>
<text class="label small muted" x="16" y="20">the extent at 1,246,469 while it holds Bob&#x27;s block</text>
<rect class="shade" x="16" y="32" width="26" height="24"/>
<text x="29" y="48.5" text-anchor="middle">08</text>
<rect class="shade" x="42" y="32" width="26" height="24"/>
<text x="55" y="48.5" text-anchor="middle">1a</text>
<rect class="shade" x="68" y="32" width="26" height="24"/>
<text x="81" y="48.5" text-anchor="middle">00</text>
<rect class="shade" x="94" y="32" width="26" height="24"/>
<text x="107" y="48.5" text-anchor="middle">10</text>
<rect class="shade" x="120" y="32" width="26" height="24"/>
<text x="133" y="48.5" text-anchor="middle">00</text>
<rect class="shade" x="146" y="32" width="26" height="24"/>
<text x="159" y="48.5" text-anchor="middle">6e</text>
<rect class="shade" x="172" y="32" width="26" height="24"/>
<text x="185" y="48.5" text-anchor="middle">a1</text>
<rect class="shade" x="198" y="32" width="26" height="24"/>
<text x="211" y="48.5" text-anchor="middle">c1</text>
<rect class="shade" x="224" y="32" width="26" height="24"/>
<text x="237" y="48.5" text-anchor="middle">2c</text>
<path class="wire" d="M16,60 L16,66 L250,66 L250,60"/>
<text class="small muted" x="133" y="80" text-anchor="middle">kind 8, length 1,048,602</text>
<rect x="258" y="32" width="26" height="24"/>
<text class="muted" x="271" y="48.5" text-anchor="middle">01</text>
<rect x="284" y="32" width="26" height="24"/>
<text class="muted" x="297" y="48.5" text-anchor="middle">00</text>
<path class="wire" d="M258,60 L258,66 L310,66 L310,60"/>
<text class="small muted" x="284" y="80" text-anchor="middle">pad to 8</text>
<rect class="bob" x="16" y="104" width="28" height="24"/>
<text class="bob" x="30" y="120.5" text-anchor="middle">7b</text>
<rect class="bob" x="44" y="104" width="28" height="24"/>
<text class="bob" x="58" y="120.5" text-anchor="middle">1f</text>
<rect class="bob" x="72" y="104" width="28" height="24"/>
<text class="bob" x="86" y="120.5" text-anchor="middle">00</text>
<rect class="bob" x="100" y="104" width="28" height="24"/>
<text class="bob" x="114" y="120.5" text-anchor="middle">00</text>
<rect class="bob" x="128" y="104" width="28" height="24"/>
<text class="bob" x="142" y="120.5" text-anchor="middle">00</text>
<rect class="bob" x="156" y="104" width="28" height="24"/>
<text class="bob" x="170" y="120.5" text-anchor="middle">00</text>
<rect class="bob" x="184" y="104" width="28" height="24"/>
<text class="bob" x="198" y="120.5" text-anchor="middle">00</text>
<rect class="bob" x="212" y="104" width="28" height="24"/>
<text class="bob" x="226" y="120.5" text-anchor="middle">00</text>
<rect class="bob" x="248" y="104" width="28" height="24"/>
<text class="muted" x="262" y="120.5" text-anchor="middle">84</text>
<rect class="bob" x="276" y="104" width="28" height="24"/>
<text class="muted" x="290" y="120.5" text-anchor="middle">e0</text>
<rect class="bob" x="304" y="104" width="28" height="24"/>
<text class="muted" x="318" y="120.5" text-anchor="middle">ff</text>
<rect class="bob" x="332" y="104" width="28" height="24"/>
<text class="muted" x="346" y="120.5" text-anchor="middle">ff</text>
<rect class="bob" x="360" y="104" width="28" height="24"/>
<text class="muted" x="374" y="120.5" text-anchor="middle">ff</text>
<rect class="bob" x="388" y="104" width="28" height="24"/>
<text class="muted" x="402" y="120.5" text-anchor="middle">ff</text>
<rect class="bob" x="416" y="104" width="28" height="24"/>
<text class="muted" x="430" y="120.5" text-anchor="middle">ff</text>
<rect class="bob" x="444" y="104" width="28" height="24"/>
<text class="muted" x="458" y="120.5" text-anchor="middle">ff</text>
<rect class="shade" x="480" y="104" width="28" height="24"/>
<text x="494" y="120.5" text-anchor="middle">04</text>
<rect class="shade" x="508" y="104" width="28" height="24"/>
<text x="522" y="120.5" text-anchor="middle">00</text>
<rect class="shade" x="536" y="104" width="28" height="24"/>
<text x="550" y="120.5" text-anchor="middle">00</text>
<rect class="shade" x="564" y="104" width="28" height="24"/>
<text x="578" y="120.5" text-anchor="middle">00</text>
<rect class="shade" x="592" y="104" width="28" height="24"/>
<text x="606" y="120.5" text-anchor="middle">00</text>
<rect class="shade" x="620" y="104" width="28" height="24"/>
<text x="634" y="120.5" text-anchor="middle">00</text>
<rect class="shade" x="648" y="104" width="28" height="24"/>
<text x="662" y="120.5" text-anchor="middle">00</text>
<rect class="shade" x="676" y="104" width="28" height="24"/>
<text x="690" y="120.5" text-anchor="middle">00</text>
<path class="wire" d="M16,132 L16,138 L240,138 L240,132"/>
<text class="small bob" x="128" y="152" text-anchor="middle">count: 8,059 bytes of frames</text>
<path class="wire" d="M248,132 L248,138 L472,138 L472,132"/>
<text class="small muted" x="360" y="152" text-anchor="middle">its inverse: a torn count is none</text>
<path class="wire" d="M480,132 L480,138 L704,138 L704,132"/>
<text class="small muted" x="592" y="152" text-anchor="middle">epoch 4: claimed by append 4</text>
<rect class="value" x="16" y="180" width="620" height="28"/>
<text class="small" x="24" y="198">frame record, 8,050 bytes: hash 387ac86c…, kind ff (whole), then the 7,996-byte block</text>
<text class="label small muted" x="16" y="238">Only the count is written over, after the frames it counts. The epoch tells a new claim of the same space from an old one.</text>
</svg>
</figure>

The block is readable at once, by Bob's process and by any other: a reader that misses in the index reads the extents the slot lists. The head is another matter. It must come after what it names, so it waits for the turn. When it lands, its record names the extent, epoch and byte count it needs, and recovery keeps that head only if the extent holds whole frames that far.

The next writer to hold the turn, usually the same one at its next commit, **folds** the extent in: its blocks become tokens and frames, and the extent is retired. Here the 7,996-byte block became a 128-byte frame. A retired extent is free to claim again after the next sync point. A process that writes keeps one extent ready for itself, and a process that opens the file while the turn is free leaves one that no process owns, for a process that has none. So even a process that has never held the turn can write without waiting.

### Two writers on one branch

Bob's commit and Alice's went to the same file but could have gone to different branches. When two writers move the same branch, the head's compare-and-swap decides:

<figure class="dg">
<svg class="dg" viewBox="0 0 760 300" width="760" height="300" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Two handles commit to main: the second&#x27;s compare-and-swap fails, and its transaction runs again over the winner">
<defs><marker id="cas-arrow" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto"><path d="M0,0 L8,4 L0,8 z"/></marker></defs>
<text class="alice" x="120" y="20" text-anchor="middle">handle 1</text>
<text class="title" x="380" y="20" text-anchor="middle">main&#x27;s head</text>
<text class="bob" x="640" y="20" text-anchor="middle">handle 2</text>
<line class="dashed" x1="120" y1="28" x2="120" y2="280"/>
<line class="dashed" x1="380" y1="28" x2="380" y2="280"/>
<line class="dashed" x1="640" y1="28" x2="640" y2="280"/>
<line class="bob" x1="640" y1="50" x2="384" y2="50" marker-end="url(#cas-arrow)"/>
<text class="small bob" x="510" y="44" text-anchor="middle">reads head h1</text>
<line class="alice" x1="120" y1="80" x2="376" y2="80" marker-end="url(#cas-arrow)"/>
<text class="small alice" x="250" y="74" text-anchor="middle">publish h2, expecting h1: ok</text>
<line class="bob" x1="640" y1="120" x2="384" y2="120" marker-end="url(#cas-arrow)"/>
<text class="small bob" x="510" y="114" text-anchor="middle">publish h3, expecting h1</text>
<line class="critical" x1="380" y1="140" x2="636" y2="140" marker-end="url(#cas-arrow)"/>
<text class="small critical" x="510" y="156" text-anchor="middle">version mismatch: the head is h2</text>
<rect class="bob" x="628" y="170" width="24" height="40"/>
<text class="small bob" x="600" y="194" text-anchor="end">the closure runs again over h2</text>
<line class="bob" x1="640" y1="230" x2="384" y2="230" marker-end="url(#cas-arrow)"/>
<text class="small bob" x="510" y="224" text-anchor="middle">publish h3&#x27;, expecting h2: ok</text>
<text class="label small muted" x="16" y="290">Measured with the example: the second handle&#x27;s Branch::transact closure ran 2 times.</text>
</svg>
</figure>

`Branch::transact` takes the code that builds the transaction, not the transaction, so a lost race runs that code again over the head that won, and what it read is read again. The losing try's root had already been written to the file, as frame 209,547. No head names it, and it stays there.

### A reader while a write is under way

<figure class="dg">
<svg class="dg" viewBox="0 0 760 250" width="760" height="250" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="A reader holds a snapshot: the slot it took and a mapping up to that slot&#x27;s end; a writer appends past it">
<defs><marker id="rd-arrow" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto"><path d="M0,0 L8,4 L0,8 z"/></marker></defs>
<rect class="value" x="16" y="50" width="420" height="36"/>
<text class="small" x="226" y="72" text-anchor="middle">up to slot 9&#x27;s end (2,297,322): never written again</text>
<rect class="attribute" x="436" y="50" width="160" height="36"/>
<text class="small" x="516" y="72" text-anchor="middle">commit 3&#x27;s records</text>
<line class="critical" x1="436" y1="40" x2="436" y2="100"/>
<text class="small critical" x="436" y="116" text-anchor="middle">the reader&#x27;s snapshot ends here</text>
<text class="label small muted" x="16" y="36">the file, while commit 3 is written</text>
<text class="label small muted" x="16" y="152">Everything a slot names stays as it was, except index pages, which are reused. So a reader reads its snapshot without</text>
<text class="label small muted" x="16" y="170">a lock, and the writer never waits for it. A lookup that misses looks for a newer slot, once the file has grown.</text>
<text class="label small muted" x="16" y="196">An index page reused since is caught: each page records the append that wrote it and a checksum, and its parent names</text>
<text class="label small muted" x="16" y="214">both (page 860, append 29). A mismatch is stale: the reader takes the newest slot and looks again.</text>
</svg>
</figure>

The file's index is the one thing written over: its pages are reused once no durable slot reaches them. A page records which append wrote it, and its parent names it by page and append. In the example, appending 2,100 small blocks at once filled the root's buffer, and the index grew its first pages:

<figure class="dg">
<svg class="dg" viewBox="0 0 760 220" width="760" height="220" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="An index page: the append that wrote it, a checksum, a header, then entries; the slot&#x27;s root names pages by page and append">
<defs><marker id="pg-arrow" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto"><path d="M0,0 L8,4 L0,8 z"/></marker></defs>
<text class="label small muted" x="16" y="20">page 860 (offset 3,522,560), written by append 29 when 2,100 blocks were appended at once</text>
<rect class="critical" x="16" y="32" width="30" height="24"/>
<text class="critical" x="31" y="48.5" text-anchor="middle">1d</text>
<rect class="critical" x="46" y="32" width="30" height="24"/>
<text class="critical" x="61" y="48.5" text-anchor="middle">00</text>
<rect class="critical" x="76" y="32" width="30" height="24"/>
<text class="critical" x="91" y="48.5" text-anchor="middle">00</text>
<rect class="critical" x="106" y="32" width="30" height="24"/>
<text class="critical" x="121" y="48.5" text-anchor="middle">00</text>
<rect class="hash" x="136" y="32" width="30" height="24"/>
<text class="hash" x="151" y="48.5" text-anchor="middle">2c</text>
<rect class="hash" x="166" y="32" width="30" height="24"/>
<text class="hash" x="181" y="48.5" text-anchor="middle">06</text>
<rect class="hash" x="196" y="32" width="30" height="24"/>
<text class="hash" x="211" y="48.5" text-anchor="middle">0e</text>
<rect class="hash" x="226" y="32" width="30" height="24"/>
<text class="hash" x="241" y="48.5" text-anchor="middle">cd</text>
<rect class="shade" x="256" y="32" width="30" height="24"/>
<text x="271" y="48.5" text-anchor="middle">00</text>
<rect class="shade" x="286" y="32" width="30" height="24"/>
<text x="301" y="48.5" text-anchor="middle">00</text>
<rect class="shade" x="316" y="32" width="30" height="24"/>
<text x="331" y="48.5" text-anchor="middle">68</text>
<rect class="shade" x="346" y="32" width="30" height="24"/>
<text x="361" y="48.5" text-anchor="middle">01</text>
<rect x="376" y="32" width="30" height="24"/>
<text class="muted" x="391" y="48.5" text-anchor="middle">00</text>
<rect x="406" y="32" width="30" height="24"/>
<text class="muted" x="421" y="48.5" text-anchor="middle">00</text>
<rect x="436" y="32" width="30" height="24"/>
<text class="muted" x="451" y="48.5" text-anchor="middle">00</text>
<rect x="466" y="32" width="30" height="24"/>
<text class="muted" x="481" y="48.5" text-anchor="middle">00</text>
<rect class="value" x="496" y="32" width="220" height="30"/>
<text class="small" x="606" y="52" text-anchor="middle">360 entries: key, value</text>
<path class="wire" d="M16,66 L16,72 L136,72 L136,66"/>
<text class="small critical" x="76" y="86" text-anchor="middle">append 29</text>
<path class="wire" d="M136,66 L136,72 L256,72 L256,66"/>
<text class="small hash" x="196" y="86" text-anchor="middle">crc32</text>
<path class="wire" d="M256,66 L256,72 L376,72 L376,66"/>
<text class="small muted" x="316" y="86" text-anchor="middle">leaf, 360 entries</text>
<path class="wire" d="M376,66 L376,72 L496,72 L496,66"/>
<text class="small muted" x="436" y="86" text-anchor="middle">unused</text>
<text class="small muted" x="16" y="118">The slot&#x27;s root became internal: children (860, 29) … (865, 29),</text>
<text class="small muted" x="16" y="134">pivots 0ac5a1f9, 15a8af75, 21212262, 2c05ce66, 36b4584b.</text>
<text class="label small muted" x="16" y="166">4,096 bytes a page, on a 4,096-byte boundary of the file. A leaf holds up to 510 entries; an internal node 8 children and a buffer.</text>
<text class="label small muted" x="16" y="184">Most appends write only the root, whose buffer holds up to 2,048 entries.</text>
</svg>
</figure>

<div class="aside">

**Implementations.** Extents are [`dialog-archive-file/src/archive/extent.rs`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-archive-file/src/archive/extent.rs). `Branch::transact` is [`dialog-repository/src/repository/branch/transaction/transact.rs`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-repository/src/repository/branch/transaction/transact.rs). The design, and the cases it is tested against with real processes, are in [`design/file-writers.md`](https://github.com/dialog-db/dialog-db/blob/main/design/file-writers.md) and [`archive/concurrency.rs`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-archive-file/src/archive/concurrency.rs).

</div>

## Serving blocks to a peer

Replicas exchange blocks, not frames (see [Sync](./sync.md)). When a peer asks for a block, the file builds it:

<figure class="dg">
<svg class="dg" viewBox="0 0 760 270" width="760" height="270" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Serving a block to a peer: the frame chain rebuilds the exact bytes, and their BLAKE3 hash is the name asked for">
<defs><marker id="sv-arrow" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto"><path d="M0,0 L8,4 L0,8 z"/></marker></defs>
<rect class="hash" x="16" y="20" width="380" height="28"/>
<text class="small" x="206" y="38" text-anchor="middle">peer asks for 565412…</text>
<rect class="value" x="16" y="58" width="380" height="28"/>
<text class="small" x="206" y="76" text-anchor="middle">frame 143531, depth 1</text>
<line x1="206" y1="48" x2="206" y2="56" marker-end="url(#sv-arrow)"/>
<rect class="value" x="16" y="96" width="380" height="28"/>
<text class="small" x="206" y="114" text-anchor="middle">rows of frame 77,897, then copies and new rows</text>
<line x1="206" y1="86" x2="206" y2="94" marker-end="url(#sv-arrow)"/>
<rect class="attribute" x="16" y="134" width="380" height="28"/>
<text class="small" x="206" y="152" text-anchor="middle">each token&#x27;s bytes, by id</text>
<line x1="206" y1="124" x2="206" y2="132" marker-end="url(#sv-arrow)"/>
<rect class="shade" x="16" y="172" width="380" height="28"/>
<text class="small" x="206" y="190" text-anchor="middle">the block: 4,372 bytes, encoded</text>
<line x1="206" y1="162" x2="206" y2="170" marker-end="url(#sv-arrow)"/>
<rect class="hash" x="16" y="210" width="380" height="28"/>
<text class="small" x="206" y="228" text-anchor="middle">BLAKE3 = 565412…  ✓</text>
<line x1="206" y1="200" x2="206" y2="208" marker-end="url(#sv-arrow)"/>
<text class="label small" x="420" y="40">The example rebuilt all nine nodes this way:</text>
<text class="label small" x="420" y="58">every hash matched.</text>
<text class="label small muted" x="420" y="92">A tree query never asks for bytes: it reads</text>
<text class="label small muted" x="420" y="110">the rows in place. Only sync and push need</text>
<text class="label small muted" x="420" y="128">blocks, since replicas exchange blocks.</text>
<text class="label small muted" x="420" y="162">Blocks that arrive are appended like a</text>
<text class="label small muted" x="420" y="180">commit&#x27;s: tree nodes become frames, other</text>
<text class="label small muted" x="420" y="198">blocks are kept whole.</text>
</svg>
</figure>

A peer checks every block it receives against its hash, as [Blocks and Storage](./storage.md) describes, and it never knows the block came from a frame.

## When something goes wrong

### A process dies, or the power goes out

Opening a file is where a crash is noticed. A process holds a recovery byte while it opens, so no other process opens from a state being repaired, and decides which slot to trust:

<figure class="dg">
<svg class="dg" viewBox="0 0 760 330" width="760" height="330" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="What opening trusts: the newest slot when the file is in use or the boot is the same, otherwise only what checks out since the durable slot">
<defs><marker id="rc-arrow" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto"><path d="M0,0 L8,4 L0,8 z"/></marker></defs>
<rect class="shade" x="260" y="16" width="240" height="30"/>
<text class="small" x="380" y="36" text-anchor="middle">take the recovery byte</text>
<line x1="380" y1="46" x2="380" y2="64" marker-end="url(#rc-arrow)"/>
<rect x="240" y="66" width="280" height="30"/>
<text class="small" x="380" y="86" text-anchor="middle">another process holds the turn?</text>
<line x1="520" y1="81" x2="600" y2="81" marker-end="url(#rc-arrow)"/>
<text class="small muted" x="560" y="76" text-anchor="middle">yes</text>
<rect class="value" x="602" y="66" width="142" height="30"/>
<text class="small" x="673" y="86" text-anchor="middle">newest slot, as is</text>
<line x1="380" y1="96" x2="380" y2="114" marker-end="url(#rc-arrow)"/>
<text class="small muted" x="388" y="110">no</text>
<rect x="200" y="116" width="360" height="30"/>
<text class="small" x="380" y="136" text-anchor="middle">another process present, or same boot?</text>
<line x1="560" y1="131" x2="600" y2="131" marker-end="url(#rc-arrow)"/>
<text class="small muted" x="580" y="126" text-anchor="middle">yes</text>
<rect class="value" x="602" y="116" width="142" height="30"/>
<text class="small" x="673" y="136" text-anchor="middle">newest slot</text>
<line x1="380" y1="146" x2="380" y2="164" marker-end="url(#rc-arrow)"/>
<text class="small muted" x="388" y="160">no: maybe a power loss</text>
<rect class="critical" x="160" y="166" width="440" height="44"/>
<text class="small" x="380" y="184" text-anchor="middle">check every record since the durable slot by its checksum,</text>
<text class="small" x="380" y="200" text-anchor="middle">the index pages it reaches, and each head&#x27;s needed extents</text>
<line x1="320" y1="210" x2="260" y2="228" marker-end="url(#rc-arrow)"/>
<line x1="440" y1="210" x2="500" y2="228" marker-end="url(#rc-arrow)"/>
<rect class="value" x="130" y="230" width="240" height="30"/>
<text class="small" x="250" y="250" text-anchor="middle">newest rolling slot that passes</text>
<rect class="shade" x="390" y="230" width="240" height="30"/>
<text class="small" x="510" y="250" text-anchor="middle">else the durable slot</text>
<text class="label small muted" x="16" y="290">A process that died holding the turn left its writes with the operating system, in order: a slot of this boot is trusted.</text>
<text class="label small muted" x="16" y="308">Its records past the slot&#x27;s end are dropped by the next writer. A power loss can reorder writes, so only then is the file checked.</text>
</svg>
</figure>

Whether this works is tested, not argued. The tests record every write the archive makes, then build what a disk could hold had the power gone after any of them:

<figure class="dg">
<svg class="dg" viewBox="0 0 760 170" width="760" height="170" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="A power-loss image: everything up to the last sync, then any subset of later writes, each torn at 512-byte sectors">
<defs><marker id="tn-arrow" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto"><path d="M0,0 L8,4 L0,8 z"/></marker></defs>
<text class="title" x="16" y="20">what the test&#x27;s disk keeps after a power loss</text>
<rect class="value" x="16" y="34" width="300" height="30"/>
<text class="small" x="166" y="54" text-anchor="middle">up to the last fsync: all kept</text>
<rect class="value" x="320" y="34" width="60" height="30"/>
<text class="small muted" x="350" y="82" text-anchor="middle">kept</text>
<rect class="ghost" x="384" y="34" width="50" height="30"/>
<text class="small muted" x="409" y="82" text-anchor="middle">lost</text>
<rect class="value" x="438" y="34" width="20" height="30"/>
<rect class="ghost" x="458" y="34" width="20" height="30"/>
<rect class="value" x="478" y="34" width="20" height="30"/>
<rect class="ghost" x="498" y="34" width="20" height="30"/>
<text class="small muted" x="478" y="82" text-anchor="middle">torn</text>
<rect class="value" x="522" y="34" width="40" height="30"/>
<text class="small muted" x="542" y="82" text-anchor="middle">kept</text>
<rect class="ghost" x="566" y="34" width="70" height="30"/>
<text class="small muted" x="601" y="82" text-anchor="middle">lost</text>
<text class="label small muted" x="16" y="116">The archive&#x27;s tests replay their own writes onto such images, at every point and in 30 ways at each,</text>
<text class="label small muted" x="16" y="134">and open each as after a reboot. Each must open, keep every block synced before the loss, read no block as</text>
<text class="label small muted" x="16" y="152">other bytes, and hold heads whose every block is there.</text>
</svg>
</figure>

### The last process closes

<figure class="dg">
<svg class="dg" viewBox="0 0 760 176" width="760" height="176" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="The last close: extents are freed and a sync point made; free extents that end the file are cut off">
<defs><marker id="cl-arrow" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto"><path d="M0,0 L8,4 L0,8 z"/></marker></defs>
<text class="label small muted" x="16" y="20">the example&#x27;s file after its last close: 3,547,136 bytes long, 241,664 allocated</text>
<rect class="shade" x="16" y="34" width="20" height="30"/>
<rect class="ghost" x="36" y="34" width="40" height="30"/>
<text class="small" x="56" y="54" text-anchor="middle">free</text>
<rect class="value" x="76" y="34" width="170" height="30"/>
<rect class="ghost" x="246" y="34" width="40" height="30"/>
<text class="small" x="266" y="54" text-anchor="middle">free</text>
<rect class="value" x="286" y="34" width="120" height="30"/>
<rect class="ghost" x="406" y="34" width="40" height="30"/>
<text class="small" x="426" y="54" text-anchor="middle">free</text>
<rect class="value" x="446" y="34" width="190" height="30"/>
<text class="small" x="541" y="54" text-anchor="middle">index pages last</text>
<text class="label small muted" x="16" y="92">All three extents are free, but the file ends in records, so nothing is cut here. A file whose last</text>
<text class="label small muted" x="16" y="110">records are free extents is cut to their start. Until then, an extent that was never written is a hole the</text>
<text class="label small muted" x="16" y="128">file system does not store: about 3 MB of the length above.</text>
<text class="label small muted" x="16" y="162">The last close is the process that takes the present byte exclusive: no other has the file open.</text>
</svg>
</figure>

<div class="aside">

**Implementations.** Opening is `recover` in [`dialog-archive-file/src/archive.rs`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-archive-file/src/archive.rs). The power-loss tests are [`archive/power.rs`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-archive-file/src/archive/power.rs), over the recording disk in [`file/disk.rs`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-archive-file/src/file/disk.rs).

</div>

## What it does not do yet

<div class="aside caveat">

- **Native only.** The crate compiles to nothing on the web. Its tests run on Linux, macOS and Windows. Whether the same file can live in a browser's file system, served by one worker a tab hosts, is being measured in [`research/web-spike`](https://github.com/dialog-db/dialog-db/tree/main/research/web-spike), not built.
- **Opt-in.** Nothing switches a replica to it. A replica the file system store wrote can be moved in with the `archive-file` migration of the `dialog-migration` crate.
- **Nothing is collected.** A node no head reaches any more, such as a losing try's, stays in the file.
- **The format is still changing.** A file written now may not open in a later release.

</div>

## Reference

All integers are little-endian. *Varint* is LEB128, *zigzag* maps signed to unsigned before it, and an *id* is an offset in the file divided by 16.

| what | where | layout |
|---|---|---|
| magic | 0 | `DIALOGA`, then the format version (14) |
| rolling slots | 8 and 65,544 | 64 KiB each: length u32, crc32 u32, body |
| durable slot | 131,080 | the same, written only at a sync point |
| records | from 196,616 | kind u8, length u32, crc32 u32, body |
| lock bytes | from 2⁶²−3 | sync, recovery, present, turn, then an owner byte per extent |

| record | body |
|---|---|
| tokens (4) | count varint; per token: pad u8, padding, length u32, bytes (start on a multiple of 16) |
| frame (5) | pad u8, padding, then data on a multiple of 16: hash (32), kind u8, depth u8, catalog varint, base varint (frame id + 1, or 0), manifest varint (token id), then the rows; or, for kind `ff`, length varint and the block |
| pages (7) | padding length u16, padding, pages of 4,096 bytes on 4,096-byte boundaries |
| extent (8) | pad u8 to 8 bytes, count u64, its inverse u64, epoch u64, then frame records |
| cell (9) | pad u8, padding, then on a multiple of 16: name length u16, name, present u8, value length u32, value, needs count u16, per need record u64, epoch u64, bytes u64 |

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
| sync point | every 64 appends, 2 seconds, or 2,048 freed index pages |
| longest chain of frames | 32 (`CHAIN`) |
