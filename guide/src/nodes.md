# Nodes

The last chapter drew nodes as boxes. This one opens a box and looks at its bytes.

A node has to answer two questions before anyone can use it. The first is *how do I read you?* A reader must know the node's layout before it can make sense of anything inside it. The second is *what tree are you part of?* The tree's format, its node size target and the other numbers from the last two chapters, decides where keys go. A reader that guessed those numbers wrong would look for keys in the wrong places.

Dialog answers both questions at the front of every node. A node starts with a short prelude, then its body:

<figure class="dg">
<svg class="dg" viewBox="0 0 692 126" width="692" height="126" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="A node: version, kind, header length, manifest fields, zero padding, then the body">
<text class="label small" x="45.0" y="18" text-anchor="middle">version</text>
<rect class="shade" x="10" y="26" width="70" height="30"/>
<path class="wire" d="M11,62 v4 H79 v-4"/>
<text class="label small muted" x="45.0" y="82" text-anchor="middle">1 byte</text>
<text class="label small" x="117.0" y="18" text-anchor="middle">kind</text>
<rect class="shade" x="82" y="26" width="70" height="30"/>
<path class="wire" d="M83,62 v4 H151 v-4"/>
<text class="label small muted" x="117.0" y="82" text-anchor="middle">1 byte</text>
<text class="label small" x="209.0" y="18" text-anchor="middle">header length</text>
<rect class="shade" x="154" y="26" width="110" height="30"/>
<path class="wire" d="M155,62 v4 H263 v-4"/>
<text class="label small muted" x="209.0" y="82" text-anchor="middle">1+ bytes</text>
<text class="label small" x="351.0" y="18" text-anchor="middle">manifest fields</text>
<rect class="hash" x="266" y="26" width="170" height="30"/>
<path class="wire" d="M267,62 v4 H435 v-4"/>
<text class="label small muted" x="351.0" y="82" text-anchor="middle">header length bytes</text>
<text class="label small" x="498.0" y="18" text-anchor="middle">padding</text>
<rect class="ghost" x="438" y="26" width="120" height="30"/>
<path class="wire" d="M439,62 v4 H557 v-4"/>
<text class="label small muted" x="498.0" y="82" text-anchor="middle">to a multiple of 16</text>
<text class="label small" x="620.0" y="18" text-anchor="middle">body</text>
<rect class="value" x="560" y="26" width="120" height="30"/>
<path class="wire" d="M561,62 v4 H679 v-4"/>
<text class="label small muted" x="620.0" y="82" text-anchor="middle">the rest</text>
<path class="wire" d="M11,96 v4 H558 v-4"/>
<text class="label small" x="279.0" y="116" text-anchor="middle">the prelude: 16 bytes for a default tree</text>
<text class="label small" x="622" y="116" text-anchor="middle">rkyv, read in place</text>
</svg>
</figure>

It works like an HTTP message: a fixed opening line, a few headers, then a body of plain bytes.

## The prelude

The prelude has four parts:

- **version** says how to read everything after it. The layout described here is version `02`.
- **kind** says what the body is: `00` for a leaf, `01` for an index node.
- **header length** is the number of bytes of manifest fields that follow, so a reader can skip them without understanding them.
- **manifest fields** hold the tree's format, as a list of settings.

After the fields come zero bytes, up to the next multiple of 16. The body starts right after, on a 16-byte boundary, because the body is read in place and needs that alignment.

Here is the whole prelude of a leaf in a tree that uses the default format:

<figure class="dg">
<svg class="dg" viewBox="0 0 462 107" width="462" height="107" xmlns="http://www.w3.org/2000/svg" role="img">
<rect class="shade" x="8" y="6" width="26" height="26"/>
<text x="21.0" y="24.0" text-anchor="middle">02</text>
<rect class="shade" x="36" y="6" width="26" height="26"/>
<text x="49.0" y="24.0" text-anchor="middle">00</text>
<rect class="shade" x="64" y="6" width="26" height="26"/>
<text x="77.0" y="24.0" text-anchor="middle">00</text>
<rect class="ghost" x="92" y="6" width="26" height="26"/>
<text x="105.0" y="24.0" text-anchor="middle">00</text>
<rect class="ghost" x="120" y="6" width="26" height="26"/>
<text x="133.0" y="24.0" text-anchor="middle">00</text>
<rect class="ghost" x="148" y="6" width="26" height="26"/>
<text x="161.0" y="24.0" text-anchor="middle">00</text>
<rect class="ghost" x="176" y="6" width="26" height="26"/>
<text x="189.0" y="24.0" text-anchor="middle">00</text>
<rect class="ghost" x="204" y="6" width="26" height="26"/>
<text x="217.0" y="24.0" text-anchor="middle">00</text>
<rect class="ghost" x="232" y="6" width="26" height="26"/>
<text x="245.0" y="24.0" text-anchor="middle">00</text>
<rect class="ghost" x="260" y="6" width="26" height="26"/>
<text x="273.0" y="24.0" text-anchor="middle">00</text>
<rect class="ghost" x="288" y="6" width="26" height="26"/>
<text x="301.0" y="24.0" text-anchor="middle">00</text>
<rect class="ghost" x="316" y="6" width="26" height="26"/>
<text x="329.0" y="24.0" text-anchor="middle">00</text>
<rect class="ghost" x="344" y="6" width="26" height="26"/>
<text x="357.0" y="24.0" text-anchor="middle">00</text>
<rect class="ghost" x="372" y="6" width="26" height="26"/>
<text x="385.0" y="24.0" text-anchor="middle">00</text>
<rect class="ghost" x="400" y="6" width="26" height="26"/>
<text x="413.0" y="24.0" text-anchor="middle">00</text>
<rect class="ghost" x="428" y="6" width="26" height="26"/>
<text x="441.0" y="24.0" text-anchor="middle">00</text>
<path class="wire" d="M9,37 v4 H33 v-4"/>
<text class="label small" x="21.0" y="55" text-anchor="middle">version</text>
<path class="wire" d="M37,37 v4 H61 v-4"/>
<line x1="49.0" y1="41" x2="49.0" y2="59" class="dashed"/>
<text class="label small" x="49.0" y="70" text-anchor="middle">kind</text>
<path class="wire" d="M65,37 v4 H89 v-4"/>
<line x1="77.0" y1="41" x2="77.0" y2="74" class="dashed"/>
<text class="label small" x="77.0" y="85" text-anchor="middle">header length</text>
<path class="wire" d="M93,37 v4 H453 v-4"/>
<text class="label small" x="273.0" y="55" text-anchor="middle">padding</text>
</svg>
</figure>

There are no fields at all, because a setting equal to its default is never written. So every node of every default tree starts with the same 16 bytes, give or take the kind byte. A reader uses the kind byte to validate the body as a leaf or an index, then checks the 16 bytes with a single comparison.

## Numbers in the prelude

Every number in the prelude is written as a [bijou64](https://www.inkandswitch.com/tangents/bijou64/) varint. Small numbers take one byte. Bigger numbers take a marker byte that says how many bytes follow:

<figure class="dg">
<svg class="dg" viewBox="0 0 520 118" width="520" height="118" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="bijou64 examples: 42, 300 and 16384">
<text class="label" x="70" y="28" text-anchor="end">42</text>
<text class="label muted" x="84" y="28">→</text>
<rect class="hash" x="104" y="10" width="26" height="26"/>
<text x="117" y="28" text-anchor="middle">2a</text>
<text class="label small muted" x="200" y="28">one byte: 0 to 247</text>
<text class="label" x="70" y="64" text-anchor="end">300</text>
<text class="label muted" x="84" y="64">→</text>
<rect class="shade" x="104" y="46" width="26" height="26"/>
<text x="117" y="64" text-anchor="middle">f8</text>
<rect class="hash" x="132" y="46" width="26" height="26"/>
<text x="145" y="64" text-anchor="middle">34</text>
<text class="label small muted" x="200" y="64">f8 + (300 − 248)</text>
<text class="label" x="70" y="100" text-anchor="end">16,384</text>
<text class="label muted" x="84" y="100">→</text>
<rect class="shade" x="104" y="82" width="26" height="26"/>
<text x="117" y="100" text-anchor="middle">f9</text>
<rect class="hash" x="132" y="82" width="26" height="26"/>
<text x="145" y="100" text-anchor="middle">3e</text>
<rect class="hash" x="160" y="82" width="26" height="26"/>
<text x="173" y="100" text-anchor="middle">08</text>
<text class="label small muted" x="200" y="100">f9 + (16,384 − 504), 2 bytes big-endian</text>
</svg>
</figure>

| First byte | Total bytes | Range |
|---|---|---|
| `00` to `f7` | 1 | 0 to 247 |
| `f8` | 2 | 248 to 503 |
| `f9` | 3 | 504 to 66,039 |
| `fa` | 4 | 66,040 to 16,843,255 |
| `fb` to `ff` | 5 to 9 | larger, up to 2<sup>64</sup> − 1 |

The ranges are offset so they never overlap: `f8 00` means 248, not 0. So every number has exactly one encoding. That matters more than it looks. A node is named by the hash of its bytes (see [Blocks and Storage](./storage.md)), so if the same number could be written two ways, the same node could have two names. LEB128, the more common varint, allows padded encodings unless every reader checks for them. (Inside the body, the key columns do use LEB128 lengths.)

## The manifest fields

Each field is three things in a row: a code saying which setting it is, the length of its value in bytes, and the value. Here is a leaf from a tree whose only difference from the defaults is a fanout setting of 4:

<figure class="dg">
<svg class="dg" viewBox="0 0 462 107" width="462" height="107" xmlns="http://www.w3.org/2000/svg" role="img">
<rect class="shade" x="8" y="6" width="26" height="26"/>
<text x="21.0" y="24.0" text-anchor="middle">02</text>
<rect class="shade" x="36" y="6" width="26" height="26"/>
<text x="49.0" y="24.0" text-anchor="middle">00</text>
<rect class="shade" x="64" y="6" width="26" height="26"/>
<text x="77.0" y="24.0" text-anchor="middle">03</text>
<rect class="hash" x="92" y="6" width="26" height="26"/>
<text x="105.0" y="24.0" text-anchor="middle">02</text>
<rect class="hash" x="120" y="6" width="26" height="26"/>
<text x="133.0" y="24.0" text-anchor="middle">01</text>
<rect class="hash" x="148" y="6" width="26" height="26"/>
<text x="161.0" y="24.0" text-anchor="middle">04</text>
<rect class="ghost" x="176" y="6" width="26" height="26"/>
<text x="189.0" y="24.0" text-anchor="middle">00</text>
<rect class="ghost" x="204" y="6" width="26" height="26"/>
<text x="217.0" y="24.0" text-anchor="middle">00</text>
<rect class="ghost" x="232" y="6" width="26" height="26"/>
<text x="245.0" y="24.0" text-anchor="middle">00</text>
<rect class="ghost" x="260" y="6" width="26" height="26"/>
<text x="273.0" y="24.0" text-anchor="middle">00</text>
<rect class="ghost" x="288" y="6" width="26" height="26"/>
<text x="301.0" y="24.0" text-anchor="middle">00</text>
<rect class="ghost" x="316" y="6" width="26" height="26"/>
<text x="329.0" y="24.0" text-anchor="middle">00</text>
<rect class="ghost" x="344" y="6" width="26" height="26"/>
<text x="357.0" y="24.0" text-anchor="middle">00</text>
<rect class="ghost" x="372" y="6" width="26" height="26"/>
<text x="385.0" y="24.0" text-anchor="middle">00</text>
<rect class="ghost" x="400" y="6" width="26" height="26"/>
<text x="413.0" y="24.0" text-anchor="middle">00</text>
<rect class="ghost" x="428" y="6" width="26" height="26"/>
<text x="441.0" y="24.0" text-anchor="middle">00</text>
<path class="wire" d="M9,37 v4 H33 v-4"/>
<text class="label small" x="21.0" y="55" text-anchor="middle">version</text>
<path class="wire" d="M37,37 v4 H61 v-4"/>
<line x1="49.0" y1="41" x2="49.0" y2="59" class="dashed"/>
<text class="label small" x="49.0" y="70" text-anchor="middle">kind</text>
<path class="wire" d="M65,37 v4 H89 v-4"/>
<line x1="77.0" y1="41" x2="77.0" y2="74" class="dashed"/>
<text class="label small" x="77.0" y="85" text-anchor="middle">header length</text>
<path class="wire" d="M93,37 v4 H173 v-4"/>
<text class="label small" x="133.0" y="55" text-anchor="middle">fanout_n = 4</text>
<path class="wire" d="M177,37 v4 H453 v-4"/>
<text class="label small" x="315.0" y="55" text-anchor="middle">padding</text>
</svg>
</figure>

The field is `02 01 04`: code 2 (`fanout_n`), one byte long, value 4. The header length says 3, and ten zero bytes pad the prelude out to 16.

Codes come from a public table. These are the ones defined today:

| Code | Name | Default | What it sets |
|---|---|---|---|
| `01` | `inline_n` | 4096 | values longer than this spill out of the key |
| `02` | `fanout_n` | 8 | buffers flush at 2<sup>n</sup> ops; with `max_segment` 0, the expected fanout |
| `03` | `spill_prefix` | 64 | how many bytes of a spilled value its key keeps |
| `04` | `max_separator` | 512 | separators longer than this never cut |
| `06` | `max_segment` | 65536 | the node size target the coins aim for |
| `08` | `frame_ceiling_factor` | 3 | nodes over this many targets are force-split |
| `0a` | `anchor_selector` | 1 | how a forced split picks its spot |
| `0c` | `entry_overhead` | 64 | weight added per leaf entry |
| `0e` | `key_overhead` | 32 | weight added per key where the value is unknown |
| `10` | `link_overhead` | 16 | weight added per index link |

A few rules keep the fields canonical, so the same format always gives the same bytes:

- Codes appear in ascending order, each at most once.
- A field equal to its default is left out.
- Every length is exact, and every number uses its one bijou64 encoding.

A reader refuses a node that breaks any of these rules. A node that spells out a default, for example, is not a valid node, even though a lenient reader could make sense of it.

The table only ever grows. A code is never reused and a default never changes. Changing a default would silently change the meaning of every node that left that field out. A new default is a new field with a new code.

## Fields a reader does not know

Suppose Bob's phone runs a newer build of Dialog than Alice's laptop. Bob's build knows a setting Alice's build has never heard of, and Bob creates a tree that uses it. What should Alice's build do with that tree?

It depends on what the setting does. Some settings only shape the tree: they move where the cuts fall. A reader that ignores one builds a tree that is shaped a little differently from the canonical one. That costs some extra work when replicas compare trees, but it never loses a fact. Other settings change how bytes are read. `inline_n` and `spill_prefix` decide how a value is laid out inside its key. A reader that ignored them would build different keys and miss facts.

A reader that has never seen a code cannot look it up, so the code itself carries the answer:

- An **even** code says *"I only shape the tree. If you don't know me, carry on."* Alice's build keeps the field anyway, and writes it back into every node it edits, so it does not strip Bob's setting from his tree.
- An **odd** code says *"You must understand me to read this tree."* A build that does not know the code refuses the tree.

The two settings that change how keys are read, `inline_n` (`01`) and `spill_prefix` (`03`), are odd. Here is a leaf that changes one of each:

<figure class="dg">
<svg class="dg" viewBox="0 0 462 107" width="462" height="107" xmlns="http://www.w3.org/2000/svg" role="img">
<rect class="shade" x="8" y="6" width="26" height="26"/>
<text x="21.0" y="24.0" text-anchor="middle">02</text>
<rect class="shade" x="36" y="6" width="26" height="26"/>
<text x="49.0" y="24.0" text-anchor="middle">00</text>
<rect class="shade" x="64" y="6" width="26" height="26"/>
<text x="77.0" y="24.0" text-anchor="middle">0a</text>
<rect class="critical" x="92" y="6" width="26" height="26"/>
<text x="105.0" y="24.0" text-anchor="middle">01</text>
<rect class="critical" x="120" y="6" width="26" height="26"/>
<text x="133.0" y="24.0" text-anchor="middle">03</text>
<rect class="critical" x="148" y="6" width="26" height="26"/>
<text x="161.0" y="24.0" text-anchor="middle">f9</text>
<rect class="critical" x="176" y="6" width="26" height="26"/>
<text x="189.0" y="24.0" text-anchor="middle">00</text>
<rect class="critical" x="204" y="6" width="26" height="26"/>
<text x="217.0" y="24.0" text-anchor="middle">08</text>
<rect class="hash" x="232" y="6" width="26" height="26"/>
<text x="245.0" y="24.0" text-anchor="middle">06</text>
<rect class="hash" x="260" y="6" width="26" height="26"/>
<text x="273.0" y="24.0" text-anchor="middle">03</text>
<rect class="hash" x="288" y="6" width="26" height="26"/>
<text x="301.0" y="24.0" text-anchor="middle">f9</text>
<rect class="hash" x="316" y="6" width="26" height="26"/>
<text x="329.0" y="24.0" text-anchor="middle">3e</text>
<rect class="hash" x="344" y="6" width="26" height="26"/>
<text x="357.0" y="24.0" text-anchor="middle">08</text>
<rect class="ghost" x="372" y="6" width="26" height="26"/>
<text x="385.0" y="24.0" text-anchor="middle">00</text>
<rect class="ghost" x="400" y="6" width="26" height="26"/>
<text x="413.0" y="24.0" text-anchor="middle">00</text>
<rect class="ghost" x="428" y="6" width="26" height="26"/>
<text x="441.0" y="24.0" text-anchor="middle">00</text>
<path class="wire" d="M9,37 v4 H33 v-4"/>
<text class="label small" x="21.0" y="55" text-anchor="middle">version</text>
<path class="wire" d="M37,37 v4 H61 v-4"/>
<line x1="49.0" y1="41" x2="49.0" y2="59" class="dashed"/>
<text class="label small" x="49.0" y="70" text-anchor="middle">kind</text>
<path class="wire" d="M65,37 v4 H89 v-4"/>
<line x1="77.0" y1="41" x2="77.0" y2="74" class="dashed"/>
<text class="label small" x="77.0" y="85" text-anchor="middle">header length</text>
<path class="wire" d="M93,37 v4 H229 v-4"/>
<text class="label small" x="161.0" y="55" text-anchor="middle">inline_n = 512</text>
<path class="wire" d="M233,37 v4 H369 v-4"/>
<text class="label small" x="301.0" y="55" text-anchor="middle">max_segment = 16384</text>
<path class="wire" d="M373,37 v4 H453 v-4"/>
<text class="label small" x="413.0" y="55" text-anchor="middle">padding</text>
</svg>
</figure>

Within this layout, the only field that can lock out an older build is an odd one the tree actually uses. Trees that stick to shape settings stay readable by every build that reads this layout. A new layout version or node kind would lock older builds out too, which is why those are reserved for changes that need it.

## Reading a node

There is no magic number that marks the tagged layout. Nodes written before it, in the older untagged layout, can start with any byte at all, including `02`. So a reader works through the checks in order:

<figure class="dg">
<svg class="dg" viewBox="0 0 620 274" width="620" height="274" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Reading a node: try the tagged layout, then the legacy one, else refuse">
<defs><marker id="r-arrow" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto"><path d="M0,0 L8,4 L0,8 z"/></marker></defs>
<rect class="hash" x="10" y="20" width="120" height="44" rx="4"/>
<text class="label" x="70.0" y="42.0" text-anchor="middle">node bytes</text>
<text class="label small muted" x="70.0" y="56.0" text-anchor="middle">from the store</text>
<rect x="180" y="10" width="220" height="64" rx="4"/>
<text class="label" x="290.0" y="42.0" text-anchor="middle">read as tagged?</text>
<text class="label small muted" x="290.0" y="56.0" text-anchor="middle">version, kind, lengths, sorted</text>
<text class="label small muted" x="290" y="66" text-anchor="middle">fields, zero padding, body</text>
<rect class="value" x="460" y="20" width="150" height="44" rx="4"/>
<text class="label" x="535.0" y="46.0" text-anchor="middle">tagged node</text>
<rect x="180" y="120" width="220" height="54" rx="4"/>
<text class="label" x="290.0" y="147.0" text-anchor="middle">read as legacy?</text>
<text class="label small muted" x="290.0" y="161.0" text-anchor="middle">the untagged layout</text>
<rect class="value" x="460" y="125" width="150" height="44" rx="4"/>
<text class="label" x="535.0" y="147.0" text-anchor="middle">legacy node</text>
<text class="label small muted" x="535.0" y="161.0" text-anchor="middle">same Manifest in memory</text>
<rect class="critical" x="180" y="220" width="220" height="44" rx="4"/>
<text class="label" x="290.0" y="246.0" text-anchor="middle">unreadable</text>
<line x1="130" y1="42" x2="176" y2="42" marker-end="url(#r-arrow)"/>
<line x1="400" y1="42" x2="456" y2="42" marker-end="url(#r-arrow)"/>
<text class="label small muted" x="428" y="36" text-anchor="middle">yes</text>
<line x1="290" y1="74" x2="290" y2="116" marker-end="url(#r-arrow)"/>
<text class="label small muted" x="298" y="100">no</text>
<line x1="400" y1="147" x2="456" y2="147" marker-end="url(#r-arrow)"/>
<text class="label small muted" x="428" y="141" text-anchor="middle">yes</text>
<line x1="290" y1="174" x2="290" y2="216" marker-end="url(#r-arrow)"/>
<text class="label small muted" x="298" y="200">no</text>
</svg>
</figure>

Tagged reading is strict: the version must be known, every length must fit, the fields must be sorted and canonical, the padding must be exactly zero, and the node must validate as an archive of the kind the prelude names, with the body never at offset 0. Old node bytes whose second byte is neither `00` nor `01` are turned away at once. Others fail the body or prelude checks and fall through to the legacy reader. Both readers produce the same in-memory format, so the rest of Dialog never has to know which layout a node used.

A tree's nodes all carry the same field bytes, so a reader decodes them once and remembers the result by those bytes. After the first node of a tree, reading its format is usually one lookup.

## The body

The body is the node itself, stored with [rkyv](https://rkyv.org/) so that its structure can be read in place without copying. A leaf's compressed keys are decoded as they are read. The body's shape depends on the kind:

- **A leaf** stores its keys column by column. In an EAV, AEV or VAE leaf, each key is split into its parts, tag, entity, attribute, value type and value, in its index's order, and each part gets its own column. A history leaf keeps whole keys in a single column. A column where the same few values repeat, like the attribute column, becomes a small dictionary plus an index per entry. A column of mostly distinct values, like entities, is front-coded: each entry stores only how much it shares with the previous one and the bytes that differ. The payloads come after the keys, one per entry.
- **An index node** stores the prefix its separators share once, then the rest of each separator and where each one ends, then the child hashes, then the one-byte size estimates, then any buffered writes, grouped by the link they ride on.

## The empty tree

An empty tree still has a format, so it is still a node: a leaf with no entries, carrying the manifest like any other. With the default format that is the 16-byte prelude and an empty leaf body. It is only ever a root. The first insert replaces it.

## Mixing layouts

Dialog reads both layouts and writes only the tagged one. Editing a tree written in the old layout writes tagged nodes along the edited path, and keeps linking to untouched old subtrees, which keep their hashes. Each node describes itself, so a tree can mix both layouts safely.

Two consequences are worth knowing. A build from before this layout cannot read a tagged node, and the first commit by a newer build rewrites the root, so the whole branch becomes unreadable to older builds at that moment. And a tree that mixes layouts is not content-only: it can hold the same entries as a fully tagged tree under a different root, until the old paths are rewritten.

<div class="aside">

**Implementations.** The prelude and both readers are in [`dialog-search-tree/src/node/persistent.rs`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-search-tree/src/node/persistent.rs). The manifest and its field codec are in [`src/manifest.rs`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-search-tree/src/manifest.rs), checked against the code table [`format/table.csv`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-search-tree/format/table.csv). The full design, with its cost measurements, is [`notes/tree-node-format.md`](https://github.com/dialog-db/dialog-db/blob/main/notes/tree-node-format.md).

</div>

<div class="aside">

**Status.** This layout is a draft. The code table marks every field `draft`, and the layout may change before a stable release.

</div>
