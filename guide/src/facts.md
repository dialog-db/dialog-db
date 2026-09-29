# Facts

Everything Dialog stores is a fact. A fact is one small statement about one thing, and it always has the same three parts:

<figure class="dg">
<svg class="dg" viewBox="0 0 640 120" width="640" height="120" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="A fact: the grocery/name of item:1 is Oat milk">
<text class="label muted" x="20" y="52">the</text>
<rect class="attribute" x="50" y="30" width="170" height="34"/>
<text class="attribute" x="135" y="52" text-anchor="middle">grocery/name</text>
<text class="label muted" x="236" y="52">of</text>
<rect class="entity" x="262" y="30" width="110" height="34"/>
<text class="entity" x="317" y="52" text-anchor="middle">item:1</text>
<text class="label muted" x="388" y="52">is</text>
<rect class="value" x="410" y="30" width="150" height="34"/>
<text class="value" x="485" y="52" text-anchor="middle">"Oat milk"</text>
<path class="wire" d="M50,74 v4 H220 v-4"/>
<text class="label small" x="135" y="96" text-anchor="middle">attribute</text>
<path class="wire" d="M262,74 v4 H372 v-4"/>
<text class="label small" x="317" y="96" text-anchor="middle">entity</text>
<path class="wire" d="M410,74 v4 H560 v-4"/>
<text class="label small" x="485" y="96" text-anchor="middle">value</text>
</svg>
</figure>

Read aloud, it is a sentence: *the grocery name of item 1 is "Oat milk"*. Dialog's own vocabulary follows that sentence. The attribute is called `the`, the entity `of`, and the value `is`.

A grocery list is a pile of such sentences. Alice's list, after she adds two items, looks like this:

| the | of | is |
|---|---|---|
| `grocery/name` | `item:1` | `"Oat milk"` |
| `grocery/done` | `item:1` | `false` |
| `grocery/name` | `item:2` | `"Eggs"` |
| `grocery/done` | `item:2` | `false` |
| `grocery/tag` | `item:2` | `"breakfast"` |

There are no tables and no rows here. An item is whatever facts share its entity. Adding a new kind of information, a quantity say, means writing facts with a new attribute. Nothing else has to change first.

## Entities

An entity is the thing a fact is about. It is named by a URI. Apps usually mint a fresh entity for each new thing with a random `did:key:` URI, like `did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK`. Those are long, so this guide writes short made-up URIs such as `item:1` instead. Any URI the standard URL parser accepts works as an entity, and Dialog stores it in its normalized form.

An entity carries no data of its own. It exists only as long as some fact mentions it.

## Attributes

An attribute says what a fact is about the entity. It is a `namespace/name` pair of at most 64 bytes, like `grocery/name` or `person/email`. The namespace keeps two apps that both say `name` from colliding.

Attributes in the `dialog.` namespace belong to Dialog itself, which stores some of its own bookkeeping as facts. Apps cannot write there, except for rules and concepts (see [Queries](./queries.md)), which live under `dialog.rule/` and `dialog.concept/`.

## Values

A value is one of nine types. Each type has a number, and that number is written into every key that holds the value (see [Keys](./keys.md)):

| Type byte | Type | Example |
|---|---|---|
| `00` | Bytes | raw binary data |
| `01` | Entity | `item:2`, to point at another thing |
| `02` | Boolean | `false` |
| `03` | String | `"Oat milk"` |
| `04` | Unsigned integer | 128-bit, `2` |
| `05` | Signed integer | 128-bit, `-3` |
| `06` | Float | 64-bit, `1.5` |
| `07` | Record | opaque structured bytes |
| `08` | Symbol | an attribute name used as a value |

An Entity value is how facts link things together. If Bob adds `the grocery/store of item:2 is store:corner-shop`, the item now points at a store, and the store can have facts of its own.

## Saying things: assert, replace, retract

An app never edits a fact in place. Facts cannot change. Instead, an app sends instructions, and each instruction adds or removes whole facts. There are three:

- **Assert** says *"this is also true."* It adds the fact and leaves every other fact alone.
- **Replace** says *"this is now the only true value."* It removes every other value for the same entity and attribute, then adds the fact.
- **Retract** says *"this is no longer true."* It removes the fact.

Here is item 2 changing under a few instructions. Each panel is the item's facts after the instruction above it:

<figure class="dg">
<svg class="dg" viewBox="0 0 720 232" width="720" height="232" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Item 2 under assert, replace and retract">
<text class="title" x="10" y="18">Assert tag "dairy-free"</text>
<rect class="shade" x="10" y="30" width="220" height="150" rx="4"/>
<text x="20" y="56">name  "Eggs"</text>
<text x="20" y="80">done  false</text>
<text x="20" y="104">tag   "breakfast"</text>
<rect class="value" x="16" y="113" width="208" height="24"/>
<text x="20" y="130">tag   "dairy-free"</text>
<text class="small muted" x="20" y="200">Tags can have many values:</text>
<text class="small muted" x="20" y="214">both stay.</text>
<text class="title" x="250" y="18">Replace done true</text>
<rect class="shade" x="250" y="30" width="220" height="150" rx="4"/>
<text x="260" y="56">name  "Eggs"</text>
<rect class="value" x="256" y="65" width="208" height="24"/>
<text x="260" y="82">done  true</text>
<text x="260" y="104">tag   "breakfast"</text>
<text x="260" y="128">tag   "dairy-free"</text>
<text class="small muted" x="260" y="200">The old value false is</text>
<text class="small muted" x="260" y="214">removed first.</text>
<text class="title" x="490" y="18">Retract tag "breakfast"</text>
<rect class="shade" x="490" y="30" width="220" height="150" rx="4"/>
<text x="500" y="56">name  "Eggs"</text>
<text x="500" y="80">done  true</text>
<text class="muted" x="500" y="104" text-decoration="line-through">tag   "breakfast"</text>
<text x="500" y="128">tag   "dairy-free"</text>
<text class="small muted" x="500" y="200">Only the named fact goes.</text>
</svg>
</figure>

Notice what is missing: there is no schema flag that says "an item has one `done`". Whether an attribute holds one value or many is decided by the instruction the app uses. A checkbox is written with Replace. A set of tags is written with Assert and Retract. The query layer lets an app declare this once, so it does not have to remember each time (see [Queries](./queries.md)).

Retracting a fact that is not there does nothing. Replacing a fact with the value it already has does nothing either. Asserting a fact that is already there does not make a second copy: two writers asserting the same entity, attribute and value share one stored fact.

## Nothing is ever null

Storage holds only facts that exist. If Bob has not given the eggs a quantity, there is no `grocery/quantity` fact for `item:2`. There is no null, and nothing is written to say a value is missing. Absence is something a query can ask about, not something storage records.

## What a fact carries along

The three parts above are the whole meaning of a fact. When a fact is stored, it also carries some bookkeeping: which version of the data wrote it. That is what lets two replicas tell whose change came first and which facts a retraction was meant to remove. It is covered in [Commits and History](./history.md).

<div class="aside">

**Implementations.** A fact is `Artifact` in [`dialog-artifacts/src/artifacts/artifact.rs`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-artifacts/src/artifacts/artifact.rs). The value types are in [`artifacts/value.rs`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-artifacts/src/artifacts/value.rs). The three instructions are `Instruction` in [`artifacts/instruction.rs`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-artifacts/src/artifacts/instruction.rs), and their meaning is implemented once, in `write_instructions` in [`dialog-artifacts/src/tree.rs`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-artifacts/src/tree.rs).

</div>

<div class="aside">

**An older field.** `Artifact` still has an optional `cause`: the hash of a fact this one was meant to replace. Older design notes lean on it heavily. Today Dialog uses it only to break ties that versions cannot order, and the real record of what replaced what lives in the history described in [Commits and History](./history.md).

</div>
