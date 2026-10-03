# The Search Tree

Every key from the last chapter goes into one sorted sequence. Alice's grocery list is small, but a real repository holds millions of keys. Dialog stores that sequence as a tree of nodes, so that finding one key reads a few nodes rather than all of them.

The tree has one property that everything else in Dialog leans on: **the same entries always make the same tree**. An entry is a key and the small payload stored with it. It does not matter in what order the entries were added, or who added them. Two replicas holding the same entries in canonical form hold byte-for-byte identical trees, with identical hashes at the top. The rest of this chapter explains how the tree gets that property, what it buys, and when Dialog chooses to give it up for a while.

## Cutting the sequence into leaves

To keep the pictures small, pretend the keys are just grocery names. In a real tree each of these is a full EAV, AEV or VAE key.

The sorted sequence is cut into runs, and each run becomes a leaf node. The question is where to cut. A B-tree cuts when a node gets full, which depends on the order things were inserted. Dialog instead lets each key decide for itself, with a coin flip that always lands the same way for the same key:

<figure class="dg">
<svg class="dg" viewBox="0 0 736 170" width="736" height="170" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Twelve keys, coins fire after butter, jam and pears, giving four leaves">
<text class="label small muted" x="10" y="16">keys in sorted order, each with a coin drawn from its own hash</text>
<rect class="attribute" x="10" y="30" width="56" height="26"/>
<text class="attribute" x="38.0" y="47" text-anchor="middle">apple</text>
<circle class="hash" cx="38.0" cy="74" r="7"/>
<rect class="attribute" x="70" y="30" width="56" height="26"/>
<text class="attribute" x="98.0" y="47" text-anchor="middle">bread</text>
<circle class="hash" cx="98.0" cy="74" r="7"/>
<rect class="attribute" x="130" y="30" width="56" height="26"/>
<text class="attribute" x="158.0" y="47" text-anchor="middle">butter</text>
<circle class="hash solid" cx="158.0" cy="74" r="7"/>
<line class="critical dashed" x1="188.0" y1="24" x2="188.0" y2="90"/>
<rect class="attribute" x="190" y="30" width="56" height="26"/>
<text class="attribute" x="218.0" y="47" text-anchor="middle">cheese</text>
<circle class="hash" cx="218.0" cy="74" r="7"/>
<rect class="attribute" x="250" y="30" width="56" height="26"/>
<text class="attribute" x="278.0" y="47" text-anchor="middle">eggs</text>
<circle class="hash" cx="278.0" cy="74" r="7"/>
<rect class="attribute" x="310" y="30" width="56" height="26"/>
<text class="attribute" x="338.0" y="47" text-anchor="middle">flour</text>
<circle class="hash" cx="338.0" cy="74" r="7"/>
<rect class="attribute" x="370" y="30" width="56" height="26"/>
<text class="attribute" x="398.0" y="47" text-anchor="middle">jam</text>
<circle class="hash solid" cx="398.0" cy="74" r="7"/>
<line class="critical dashed" x1="428.0" y1="24" x2="428.0" y2="90"/>
<rect class="attribute" x="430" y="30" width="56" height="26"/>
<text class="attribute" x="458.0" y="47" text-anchor="middle">milk</text>
<circle class="hash" cx="458.0" cy="74" r="7"/>
<rect class="attribute" x="490" y="30" width="56" height="26"/>
<text class="attribute" x="518.0" y="47" text-anchor="middle">oats</text>
<circle class="hash" cx="518.0" cy="74" r="7"/>
<rect class="attribute" x="550" y="30" width="56" height="26"/>
<text class="attribute" x="578.0" y="47" text-anchor="middle">pears</text>
<circle class="hash solid" cx="578.0" cy="74" r="7"/>
<line class="critical dashed" x1="608.0" y1="24" x2="608.0" y2="90"/>
<rect class="attribute" x="610" y="30" width="56" height="26"/>
<text class="attribute" x="638.0" y="47" text-anchor="middle">rice</text>
<circle class="hash" cx="638.0" cy="74" r="7"/>
<rect class="attribute" x="670" y="30" width="56" height="26"/>
<text class="attribute" x="698.0" y="47" text-anchor="middle">tea</text>
<circle class="hash" cx="698.0" cy="74" r="7"/>
<text class="label small muted" x="10" y="108">● the coin fired: cut after this key      ○ it did not</text>
<rect class="shade" x="10" y="130" width="176" height="30" rx="3"/>
<text class="label" x="98.0" y="149" text-anchor="middle">leaf 1</text>
<rect class="shade" x="190" y="130" width="236" height="30" rx="3"/>
<text class="label" x="308.0" y="149" text-anchor="middle">leaf 2</text>
<rect class="shade" x="430" y="130" width="176" height="30" rx="3"/>
<text class="label" x="518.0" y="149" text-anchor="middle">leaf 3</text>
<rect class="shade" x="610" y="130" width="116" height="30" rx="3"/>
<text class="label" x="668.0" y="149" text-anchor="middle">leaf 4</text>
</svg>
</figure>

The coin is the key's BLAKE3 hash. Dialog reads the first 8 bytes of the hash as a number between 0 and 2<sup>64</sup>, and cuts after the key when that number falls below a threshold. Since the hash of a key never changes, neither does its draw. Given the same entries around it, `butter` cuts after itself on every replica.

The threshold depends on how heavy the entry is. An entry weighs its key's length in bytes, plus the size of its payload, plus a fixed overhead of 64. The chance of a cut after an entry is its weight divided by 65,536. A typical fact entry weighs a few hundred, so the coin fires after roughly one entry in a few hundred. Heavier entries are more likely to end a leaf, so leaves average about 64 KiB of weight no matter how big their entries are.

<div class="aside">

**The exact rule.** Take `blake3(key)`, read bytes 0 to 7 as a little-endian unsigned 64-bit number `draw`, and cut after the entry when `draw × 65536 < weight × 2`<sup>`64`</sup>. The arithmetic is done in 128 bits, so it never overflows. Here `weight` also includes any weight carried over from a run of vetoed spots right before it (see the safety valves below), and a vetoed spot never takes the coin at all. An entry that weighs 65,536 or more always cuts, unless its spot is vetoed.

</div>

## Separators and index nodes

Leaves need a parent that says which leaf holds which keys. For every cut, Dialog writes a **separator**: the shortest prefix of the first key after the cut that still sorts above the last key before it.

<figure class="dg">
<svg class="dg" viewBox="0 0 640 110" width="640" height="110" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="The separator between butter and cheese is c">
<text class="label small muted" x="10" y="27">last key of leaf 1</text>
<rect class="shade" x="150" y="10" width="26" height="26"/>
<text x="163" y="27" text-anchor="middle">b</text>
<rect class="shade" x="178" y="10" width="26" height="26"/>
<text x="191" y="27" text-anchor="middle">u</text>
<rect class="shade" x="206" y="10" width="26" height="26"/>
<text x="219" y="27" text-anchor="middle">t</text>
<rect class="shade" x="234" y="10" width="26" height="26"/>
<text x="247" y="27" text-anchor="middle">t</text>
<rect class="shade" x="262" y="10" width="26" height="26"/>
<text x="275" y="27" text-anchor="middle">e</text>
<rect class="shade" x="290" y="10" width="26" height="26"/>
<text x="303" y="27" text-anchor="middle">r</text>
<text class="label small muted" x="10" y="63">first key of leaf 2</text>
<rect class="attribute" x="150" y="46" width="26" height="26"/>
<text x="163" y="63" text-anchor="middle">c</text>
<rect class="shade" x="178" y="46" width="26" height="26"/>
<text x="191" y="63" text-anchor="middle">h</text>
<rect class="shade" x="206" y="46" width="26" height="26"/>
<text x="219" y="63" text-anchor="middle">e</text>
<rect class="shade" x="234" y="46" width="26" height="26"/>
<text x="247" y="63" text-anchor="middle">e</text>
<rect class="shade" x="262" y="46" width="26" height="26"/>
<text x="275" y="63" text-anchor="middle">s</text>
<rect class="shade" x="290" y="46" width="26" height="26"/>
<text x="303" y="63" text-anchor="middle">e</text>
<path class="wire" d="M151,78 v4 H175 v-4"/>
<text class="label small" x="163" y="98" text-anchor="middle">separator "c"</text>
<text class="label small muted" x="330" y="30">differs at the first byte, so one byte</text>
<text class="label small muted" x="330" y="46">of "cheese" is enough to tell the leaves apart</text>
</svg>
</figure>

Real keys share long prefixes. The last key of one leaf might be the EAV key for `item:1` and the first key of the next the one for `item:2`. Those differ only at the seventh byte, so their separator is the seven bytes `00 69 74 65 6d 3a 32`, which reads as a zero byte followed by `item:2`. A separator is almost always much shorter than the key it stands for, which keeps parent nodes small.

An index node holds a list of links, one per child. Each link has the child's separator, the child's hash, and a one-byte estimate of how big the child's subtree is, which the query planner uses. Here is Alice's tree, with a root index node over four leaves:

<figure class="dg">
<svg class="dg" viewBox="0 0 660 334" width="660" height="334" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="A root index node with separators and child hashes, pointing at four leaves">
<defs><marker id="t-arrow" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto"><path d="M0,0 L8,4 L0,8 z"/></marker></defs>
<text class="label small muted" x="200" y="20">index node (the root)</text>
<rect x="200" y="30" width="260" height="106"/>
<rect class="hash" x="390" y="8" width="70" height="18"/>
<text class="small hash" x="425" y="21" text-anchor="middle">9c1e…</text>
<text class="attribute" x="214" y="51">·</text>
<rect class="hash" x="260" y="38" width="70" height="18"/>
<text class="small hash" x="295" y="51" text-anchor="middle">3f9a…</text>
<g>
<line x1="230.0" y1="136" x2="90.0" y2="192" marker-end="url(#t-arrow)"/>
<rect class="shade" x="15.0" y="196" width="150" height="100" rx="3"/>
<rect class="hash" x="95.0" y="202" width="64" height="18"/>
<text class="small hash" x="127.0" y="215" text-anchor="middle">3f9a…</text>
<text class="attribute" x="27.0" y="240">apple</text>
<text class="attribute" x="27.0" y="262">bread</text>
<text class="attribute" x="27.0" y="284">butter</text>
</g>
<text class="attribute" x="214" y="75">c</text>
<rect class="hash" x="260" y="62" width="70" height="18"/>
<text class="small hash" x="295" y="75" text-anchor="middle">b21c…</text>
<g>
<line x1="296.6666666666667" y1="136" x2="250.0" y2="192" marker-end="url(#t-arrow)"/>
<rect class="shade" x="175.0" y="196" width="150" height="122" rx="3"/>
<rect class="hash" x="255.0" y="202" width="64" height="18"/>
<text class="small hash" x="287.0" y="215" text-anchor="middle">b21c…</text>
<text class="attribute" x="187.0" y="240">cheese</text>
<text class="attribute" x="187.0" y="262">eggs</text>
<text class="attribute" x="187.0" y="284">flour</text>
<text class="attribute" x="187.0" y="306">jam</text>
</g>
<text class="attribute" x="214" y="99">m</text>
<rect class="hash" x="260" y="86" width="70" height="18"/>
<text class="small hash" x="295" y="99" text-anchor="middle">07de…</text>
<g>
<line x1="363.33333333333337" y1="136" x2="410.0" y2="192" marker-end="url(#t-arrow)"/>
<rect class="shade" x="335.0" y="196" width="150" height="100" rx="3"/>
<rect class="hash" x="415.0" y="202" width="64" height="18"/>
<text class="small hash" x="447.0" y="215" text-anchor="middle">07de…</text>
<text class="attribute" x="347.0" y="240">milk</text>
<text class="attribute" x="347.0" y="262">oats</text>
<text class="attribute" x="347.0" y="284">pears</text>
</g>
<text class="attribute" x="214" y="123">r</text>
<rect class="hash" x="260" y="110" width="70" height="18"/>
<text class="small hash" x="295" y="123" text-anchor="middle">e45b…</text>
<g>
<line x1="430.0" y1="136" x2="570.0" y2="192" marker-end="url(#t-arrow)"/>
<rect class="shade" x="495.0" y="196" width="150" height="78" rx="3"/>
<rect class="hash" x="575.0" y="202" width="64" height="18"/>
<text class="small hash" x="607.0" y="215" text-anchor="middle">e45b…</text>
<text class="attribute" x="507.0" y="240">rice</text>
<text class="attribute" x="507.0" y="262">tea</text>
</g>
</svg>
</figure>

To find `oats`, a reader starts at the root. `oats` sorts after `m` and before `r`, so it follows the third link, fetches the node with hash `07de…`, and finds `oats` inside it.

Index nodes are cut into groups the same way leaves are. Each separator flips its own coin, from its own hash. A link weighs its separator's length, plus the 32-byte child hash, plus 16 bytes of overhead, and its coin fires with probability weight / 65,536. A link whose coin fires starts a new index node, and its separator becomes that node's separator one level up. The first four index levels each draw from a different 8 bytes of the separator's hash, and a separator cuts a level only if it also fired at every level below, so one lucky separator does not cut every level at once. Short separators mean light links, so a single index node holds hundreds of them, and a tree three levels deep already covers a very large repository.

## Why the order does not matter

Every cut depends only on the entries at that point: their hashes and their weights. Nothing depends on what was inserted first. So Alice and Bob can build the list in completely different orders and still end up in the same place:

<figure class="dg">
<svg class="dg" viewBox="0 0 660 170" width="660" height="170" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Alice and Bob insert the same keys in different orders and get the same root">
<defs><marker id="o-arrow" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto"><path d="M0,0 L8,4 L0,8 z"/></marker></defs>
<text class="title alice" x="10" y="37">▲ Alice</text>
<rect class="attribute" x="100" y="20" width="58" height="26"/>
<text class="attribute" x="129" y="37" text-anchor="middle">milk</text>
<rect class="attribute" x="162" y="20" width="58" height="26"/>
<text class="attribute" x="191" y="37" text-anchor="middle">apple</text>
<rect class="attribute" x="224" y="20" width="58" height="26"/>
<text class="attribute" x="253" y="37" text-anchor="middle">tea</text>
<rect class="attribute" x="286" y="20" width="58" height="26"/>
<text class="attribute" x="315" y="37" text-anchor="middle">jam</text>
<rect class="attribute" x="348" y="20" width="58" height="26"/>
<text class="attribute" x="377" y="37" text-anchor="middle">bread</text>
<rect class="attribute" x="410" y="20" width="58" height="26"/>
<text class="attribute" x="439" y="37" text-anchor="middle">…</text>
<text class="title bob" x="10" y="137">● Bob</text>
<rect class="attribute" x="100" y="120" width="58" height="26"/>
<text class="attribute" x="129" y="137" text-anchor="middle">tea</text>
<rect class="attribute" x="162" y="120" width="58" height="26"/>
<text class="attribute" x="191" y="137" text-anchor="middle">eggs</text>
<rect class="attribute" x="224" y="120" width="58" height="26"/>
<text class="attribute" x="253" y="137" text-anchor="middle">rice</text>
<rect class="attribute" x="286" y="120" width="58" height="26"/>
<text class="attribute" x="315" y="137" text-anchor="middle">apple</text>
<rect class="attribute" x="348" y="120" width="58" height="26"/>
<text class="attribute" x="377" y="137" text-anchor="middle">oats</text>
<rect class="attribute" x="410" y="120" width="58" height="26"/>
<text class="attribute" x="439" y="137" text-anchor="middle">…</text>
<line x1="480" y1="33" x2="540" y2="80" marker-end="url(#o-arrow)"/>
<line x1="480" y1="133" x2="540" y2="92" marker-end="url(#o-arrow)"/>
<rect class="hash" x="560" y="60" width="80" height="20"/>
<text class="small hash" x="600" y="74" text-anchor="middle">9c1e…</text>
<line x1="600" y1="80" x2="558" y2="104"/>
<rect class="shade" x="548" y="104" width="20" height="16"/>
<line x1="600" y1="80" x2="584" y2="104"/>
<rect class="shade" x="574" y="104" width="20" height="16"/>
<line x1="600" y1="80" x2="610" y2="104"/>
<rect class="shade" x="600" y="104" width="20" height="16"/>
<line x1="600" y1="80" x2="636" y2="104"/>
<rect class="shade" x="626" y="104" width="20" height="16"/>
<text class="label small muted" x="600" y="140" text-anchor="middle">same entries,</text>
<text class="label small muted" x="600" y="154" text-anchor="middle">same tree, same root</text>
</svg>
</figure>

A node's hash covers its whole content, including the hashes of its children. So the root hash covers everything. If two roots are equal, the two trees hold exactly the same keys. If two roots differ, a reader can compare them link by link, skip every child whose hash matches, and descend only where they differ. That is how two replicas find what they disagree about without reading everything they agree on. [Sync](./sync.md) is built on this.

## Changing the tree

A node is never changed in place. To add `pasta`, Dialog reads the path from the root down to the leaf where `pasta` belongs, writes a new copy of that leaf with `pasta` in it, and then a new copy of every node above it, since each parent must point at its child's new hash:

<figure class="dg">
<svg class="dg" viewBox="0 0 660 350" width="660" height="350" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="A root index node with separators and child hashes, pointing at four leaves">
<defs><marker id="t-arrow" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto"><path d="M0,0 L8,4 L0,8 z"/></marker></defs>
<text class="label small muted" x="200" y="20">index node (the root)</text>
<rect x="200" y="30" width="260" height="106"/>
<rect class="critical" x="390" y="8" width="70" height="18"/>
<text class="small critical" x="425" y="21" text-anchor="middle">d80b…</text>
<text class="attribute" x="214" y="51">·</text>
<rect class="hash" x="260" y="38" width="70" height="18"/>
<text class="small hash" x="295" y="51" text-anchor="middle">3f9a…</text>
<g class="faded">
<line x1="230.0" y1="136" x2="90.0" y2="192" marker-end="url(#t-arrow)"/>
<rect class="shade" x="15.0" y="196" width="150" height="100" rx="3"/>
<rect class="hash" x="95.0" y="202" width="64" height="18"/>
<text class="small hash" x="127.0" y="215" text-anchor="middle">3f9a…</text>
<text class="attribute" x="27.0" y="240">apple</text>
<text class="attribute" x="27.0" y="262">bread</text>
<text class="attribute" x="27.0" y="284">butter</text>
</g>
<text class="attribute" x="214" y="75">c</text>
<rect class="hash" x="260" y="62" width="70" height="18"/>
<text class="small hash" x="295" y="75" text-anchor="middle">b21c…</text>
<g class="faded">
<line x1="296.6666666666667" y1="136" x2="250.0" y2="192" marker-end="url(#t-arrow)"/>
<rect class="shade" x="175.0" y="196" width="150" height="122" rx="3"/>
<rect class="hash" x="255.0" y="202" width="64" height="18"/>
<text class="small hash" x="287.0" y="215" text-anchor="middle">b21c…</text>
<text class="attribute" x="187.0" y="240">cheese</text>
<text class="attribute" x="187.0" y="262">eggs</text>
<text class="attribute" x="187.0" y="284">flour</text>
<text class="attribute" x="187.0" y="306">jam</text>
</g>
<text class="attribute" x="214" y="99">m</text>
<rect class="critical" x="260" y="86" width="70" height="18"/>
<text class="small critical" x="295" y="99" text-anchor="middle">5a7f…</text>
<g>
<line x1="363.33333333333337" y1="136" x2="410.0" y2="192" marker-end="url(#t-arrow)"/>
<rect class="shade" x="335.0" y="196" width="150" height="122" rx="3"/>
<rect class="critical" x="415.0" y="202" width="64" height="18"/>
<text class="small critical" x="447.0" y="215" text-anchor="middle">5a7f…</text>
<text class="attribute" x="347.0" y="240">milk</text>
<text class="attribute" x="347.0" y="262">oats</text>
<text class="attribute" x="347.0" y="284">pasta</text>
<text class="attribute" x="347.0" y="306">pears</text>
</g>
<text class="attribute" x="214" y="123">r</text>
<rect class="hash" x="260" y="110" width="70" height="18"/>
<text class="small hash" x="295" y="123" text-anchor="middle">e45b…</text>
<g class="faded">
<line x1="430.0" y1="136" x2="570.0" y2="192" marker-end="url(#t-arrow)"/>
<rect class="shade" x="495.0" y="196" width="150" height="78" rx="3"/>
<rect class="hash" x="575.0" y="202" width="64" height="18"/>
<text class="small hash" x="607.0" y="215" text-anchor="middle">e45b…</text>
<text class="attribute" x="507.0" y="240">rice</text>
<text class="attribute" x="507.0" y="262">tea</text>
</g>
<text class="label small muted" x="330.0" y="338" text-anchor="middle">Only leaf 3 and the root are new blocks. Leaves 1, 2 and 4 are reused as they are.</text>
</svg>
</figure>

Everything off the path is untouched and reused. An edit to a tree with millions of keys writes a handful of new nodes. The old root still names the old tree, whole and readable, which is what makes history cheap (see [Commits and History](./history.md)).

After the edit, Dialog re-checks the coins along the new path. If `pasta`'s own coin had fired, leaf 3 would have split in two. The tree after an edit is exactly the tree a fresh build from the same keys would give.

## A few safety valves

Coin flips are fair on average, but some key sets are not. Three rules keep the tree well shaped anyway:

- **No huge separators.** If two neighboring keys share their first 512 bytes, a separator between them would be over 512 bytes long. Such a spot is vetoed: its coin never cuts, at any level. Only the next rule may split there, at the leaf level, with a deliberately long separator that never cuts an index level.
- **No endless leaves.** A run where cuts were forbidden and that grows heavier than 65,536 is split anyway, at a spot chosen deterministically from the keys, so every replica picks the same spot.
- **A hard ceiling.** Any run between coin cuts that is heavier than three times 65,536 is split at allowed spots, where there are any, even if no coin fired.

The numbers used above, 65,536 and 512 and 3, the entry overhead 64 and the link overhead 16, and the fanout setting 8 behind the 256 below, are the tree's format. They are the defaults, and every node records which values its tree uses. The [Nodes](./nodes.md) chapter shows where.

## Buffered writes

As [Changing the tree](#changing-the-tree) showed, changing one fact rewrites every node on the path from its leaf up to the root: each node on that spine gets a new hash, so each is written again. When facts arrive a few per commit, every commit pays for a whole spine, and a commit that touches facts in several places pays for several. So by default, a commit does not push its changes all the way down. It parks them in a buffer in the root, on the link that leads toward where they belong:

<figure class="dg">
<svg class="dg" viewBox="0 0 660 200" width="660" height="200" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="A buffered write sits on the root&#x27;s link to leaf 1 until the buffer is flushed">
<defs><marker id="b-arrow" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto"><path d="M0,0 L8,4 L0,8 z"/></marker></defs>
<text class="label small muted" x="150" y="20">root index node, with a buffer riding on one link</text>
<rect x="150" y="30" width="360" height="106"/>
<text class="attribute" x="164" y="51">·</text>
<rect class="hash" x="210" y="38" width="70" height="18"/>
<text class="small hash" x="245" y="51" text-anchor="middle">3f9a…</text>
<text class="attribute" x="164" y="75">c</text>
<rect class="hash" x="210" y="62" width="70" height="18"/>
<text class="small hash" x="245" y="75" text-anchor="middle">b21c…</text>
<text class="attribute" x="164" y="99">m</text>
<rect class="hash" x="210" y="86" width="70" height="18"/>
<text class="small hash" x="245" y="99" text-anchor="middle">07de…</text>
<text class="attribute" x="164" y="123">r</text>
<rect class="hash" x="210" y="110" width="70" height="18"/>
<text class="small hash" x="245" y="123" text-anchor="middle">e45b…</text>
<rect class="critical" x="300" y="38" width="196" height="18"/>
<text class="small critical" x="398" y="51" text-anchor="middle">pending: + bagels, − bread</text>
<line class="dashed" x1="398" y1="56" x2="398" y2="180"/>
<text class="label small muted" x="406" y="176">flushed into leaf 1 later,</text>
<text class="label small muted" x="406" y="190">when the buffer fills up</text>
</svg>
</figure>

Reads look at the buffers on the way down and merge what they find with what the leaves hold, so a buffered write is visible immediately. When the buffers on one node hold more than 256 operations, or more than 64 KiB of weight, the heaviest of them are pushed one level down until the node is back under half that limit. Over time, every operation reaches the leaves. Both limits come from the tree's format: 256 is 2<sup>8</sup>, the format's fanout setting, and 64 KiB is its node size target. A commit then rewrites the root, plus whatever nodes a flush reaches, instead of a whole spine for every change.

<div class="aside caveat">

**A buffered tree is not canonical.** Two replicas with the same facts can hold them in different buffers and so have different roots. Nothing breaks when that happens. A node's hash still covers its buffers, so a root still names its content exactly, and the comparison described above still works. It just cannot stop early at the root, and does a little work to find that nothing differs.

When the canonical form matters, a commit can ask for it, and Dialog pushes every buffer down before sealing the tree. Bulk imports do this once at the end. Ordinary commits and sync do not.

</div>

## Why buffer by default

Giving up the canonical form sounds like a steep price, since it is what lets two replicas holding the same facts recognize each other at the root. But that is rarely how replicas meet. It is unlikely that two peers start from nothing, insert the same facts in different orders, and then sync. The usual case is the grocery list: Alice and Bob start from the same tree, each adds a few facts of their own, and then they reconcile.

In that case what matters is how many blocks differ between their trees, because every differing block has to be exchanged. With canonical edits, each new fact rewrites the spine from its leaf to the root, and facts that land in different parts of the tree rewrite different spines. With buffering, the new facts sit in the root, and everything below it is still the base both replicas started from. Here Bob adds two facts that belong in different leaves:

<figure class="dg">
<svg class="dg" viewBox="0 0 670 220" width="670" height="220" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Two new facts in different leaves: canonical edits make five new blocks, buffered edits make one">
<text class="title" x="160" y="18" text-anchor="middle">canonical</text>
<line x1="160.0" y1="60" x2="80.0" y2="96"/>
<line x1="80.0" y1="120" x2="47.0" y2="156"/>
<line x1="80.0" y1="120" x2="122.0" y2="156"/>
<line x1="160.0" y1="60" x2="240.0" y2="96"/>
<line x1="240.0" y1="120" x2="197.0" y2="156"/>
<line x1="240.0" y1="120" x2="272.0" y2="156"/>
<rect class="critical" x="130" y="34" width="60" height="26"/>
<rect class="critical" x="50" y="96" width="60" height="24"/>
<rect class="critical" x="210" y="96" width="60" height="24"/>
<rect class="shade" x="15" y="156" width="64" height="22"/>
<rect class="critical" x="90" y="156" width="64" height="22"/>
<rect class="critical" x="165" y="156" width="64" height="22"/>
<rect class="shade" x="240" y="156" width="64" height="22"/>
<text class="label small muted" x="160" y="206" text-anchor="middle">5 new blocks: two spines</text>
<text class="title" x="500" y="18" text-anchor="middle">buffered</text>
<line x1="500.0" y1="60" x2="420.0" y2="96"/>
<line x1="420.0" y1="120" x2="387.0" y2="156"/>
<line x1="420.0" y1="120" x2="462.0" y2="156"/>
<line x1="500.0" y1="60" x2="580.0" y2="96"/>
<line x1="580.0" y1="120" x2="537.0" y2="156"/>
<line x1="580.0" y1="120" x2="612.0" y2="156"/>
<rect class="critical" x="470" y="34" width="60" height="26"/>
<rect class="shade" x="390" y="96" width="60" height="24"/>
<rect class="shade" x="550" y="96" width="60" height="24"/>
<rect class="shade" x="355" y="156" width="64" height="22"/>
<rect class="shade" x="430" y="156" width="64" height="22"/>
<rect class="shade" x="505" y="156" width="64" height="22"/>
<rect class="shade" x="580" y="156" width="64" height="22"/>
<text class="small critical" x="500.0" y="51" text-anchor="middle">+2</text>
<text class="label small muted" x="500" y="206" text-anchor="middle">1 new block: the root</text>
</svg>
</figure>

So replicas that have diverged a little sync by exchanging a root or two, rather than a path of nodes for every change. The rare case where replicas diverge a lot, such as importing a large dataset, is where the canonical form earns its keep: a bulk import pushes every buffer down once when it finishes, and an app can ask for the canonical form on any commit that needs it.

<div class="aside">

**Implementations.** The tree is [`dialog-search-tree`](https://github.com/dialog-db/dialog-db/tree/main/rust/dialog-search-tree). The coins and separators are in [`src/distribution.rs`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-search-tree/src/distribution.rs) (`weight_paced_cut`, `weight_paced_seam_rank`, `shortest_separator`). Canonical edits are `TransientTree` in [`src/tree/transient.rs`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-search-tree/src/tree/transient.rs). Buffered writes are `HitchhikerTree` in [`src/hitchhiker.rs`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-search-tree/src/hitchhiker.rs). Comparing two trees is `TreeDifference` in [`src/differential.rs`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-search-tree/src/differential.rs).

</div>
