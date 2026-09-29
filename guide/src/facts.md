# Facts

Everything Dialog stores is a fact. A fact is one small statement about one thing, and it always has the same three parts:

<figure class="dg">
<svg class="dg" viewBox="0 0 640 120" width="640" height="120" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="A fact: the demo.grocery/name of item:1 is Oat milk">
<text class="label muted" x="20" y="52">the</text>
<rect class="attribute" x="50" y="30" width="200" height="34"/>
<text class="attribute" x="150" y="52" text-anchor="middle">demo.grocery/name</text>
<text class="label muted" x="266" y="52">of</text>
<rect class="entity" x="292" y="30" width="100" height="34"/>
<text class="entity" x="342" y="52" text-anchor="middle">item:1</text>
<text class="label muted" x="408" y="52">is</text>
<rect class="value" x="430" y="30" width="150" height="34"/>
<text class="value" x="505" y="52" text-anchor="middle">"Oat milk"</text>
<path class="wire" d="M50,74 v4 H250 v-4"/>
<text class="label small" x="150" y="96" text-anchor="middle">attribute</text>
<path class="wire" d="M292,74 v4 H392 v-4"/>
<text class="label small" x="342" y="96" text-anchor="middle">entity</text>
<path class="wire" d="M430,74 v4 H580 v-4"/>
<text class="label small" x="505" y="96" text-anchor="middle">value</text>
</svg>
</figure>

Read aloud, it is a sentence: *the grocery name of item 1 is "Oat milk"*. Dialog's own vocabulary follows that sentence. The attribute is called `the`, the entity `of`, and the value `is`.

A grocery list is a pile of such sentences. Alice's list, after she adds two items, looks like this:

| the | of | is |
|---|---|---|
| `demo.grocery/name` | `item:1` | `"Oat milk"` |
| `demo.grocery/done` | `item:1` | `false` |
| `demo.grocery/name` | `item:2` | `"Eggs"` |
| `demo.grocery/done` | `item:2` | `false` |
| `demo.grocery/tag` | `item:2` | `"breakfast"` |

There are no tables and no rows here. An item is whatever facts share its entity. Adding a new kind of information, a quantity say, means writing facts with a new attribute. Nothing else has to change first.

## Entities

An entity is the thing a fact is about. It is named by a URI. Dialog usually derives an entity from the data a thing starts out with: it hashes that data with BLAKE3 and writes the hash as a `did:key:z6Mk…` URI. It has the shape of a public key, but it is only a hash, and no one holds a private key for it. The same starting data always gives the same entity.

Those URIs are long, so this guide writes short ones such as `item:1` instead. Any URI the standard URL parser accepts works as an entity, and Dialog stores it in its normalized form.

An entity carries no data of its own. It exists only as long as some fact mentions it.

## Attributes

An attribute says what a fact is about the entity. It is written as a domain and a name, separated by a slash, like `demo.grocery/name`:

- **The domain** says whose vocabulary the attribute belongs to. It is written in reverse domain notation, the way Java names its packages: `demo.grocery` stands for `grocery.example`. Two apps that both say `name` do not collide, because their domains differ. A domain is lowercase letters, digits, hyphens and dots.
- **The name** is lowercase kebab-case: letters, digits and hyphens, starting with a letter, like `name` or `picked-up-at`.

The whole attribute is at most 64 bytes. This guide uses the domain `demo.grocery`.

An app declares every attribute it uses, and the declaration says more than the name:

- a **description**, for people reading the data;
- the **value type**, such as String or Boolean;
- a **cardinality**: whether an entity has one value for the attribute, or many.

A grocery item has one name and one done flag, and any number of tags.

Attributes in the `dialog.` domains belong to Dialog itself, which stores some of its own bookkeeping as facts. Apps cannot write there, except for rules and concepts (see [Queries](./queries.md)), which live under `dialog.rule/` and `dialog.concept/`.

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

An Entity value is how facts link things together. If Bob adds `the demo.grocery/store of item:2 is store:corner-shop`, the item now points at a store, and the store can have facts of its own.

## Saying things: assert and retract

An app never edits a fact in place. Facts cannot change. Instead, an app asserts and retracts whole facts:

- **Assert** says *"this is true."* It adds the fact.
- **Retract** says *"this is no longer true."* It removes the fact.

What an assertion does to the facts already there depends on the attribute's cardinality. If the attribute has many values, the new fact joins the others. If it has one value, the new fact supersedes the old one: asserting that the eggs are done removes the fact that they were not.

Here is item 2 changing under two assertions and a retraction. Each panel shows the item's facts after the change above it:

<figure class="dg">
<svg class="dg" viewBox="0 0 720 232" width="720" height="232" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Item 2 under two assertions and a retraction">
<text class="title" x="10" y="18">Assert tag "dairy-free"</text>
<rect class="shade" x="10" y="30" width="220" height="150" rx="4"/>
<text x="20" y="56">name  "Eggs"</text>
<text x="20" y="80">done  false</text>
<text x="20" y="104">tag   "breakfast"</text>
<rect class="value" x="16" y="113" width="208" height="24"/>
<text x="20" y="130">tag   "dairy-free"</text>
<text class="small muted" x="20" y="200">Tags can have many values:</text>
<text class="small muted" x="20" y="214">both stay.</text>
<text class="title" x="250" y="18">Assert done true</text>
<rect class="shade" x="250" y="30" width="220" height="150" rx="4"/>
<text x="260" y="56">name  "Eggs"</text>
<rect class="value" x="256" y="65" width="208" height="24"/>
<text x="260" y="82">done  true</text>
<text x="260" y="104">tag   "breakfast"</text>
<text x="260" y="128">tag   "dairy-free"</text>
<text class="small muted" x="260" y="200">Done has one value:</text>
<text class="small muted" x="260" y="214">false is superseded.</text>
<text class="title" x="490" y="18">Retract tag "breakfast"</text>
<rect class="shade" x="490" y="30" width="220" height="150" rx="4"/>
<text x="500" y="56">name  "Eggs"</text>
<text x="500" y="80">done  true</text>
<text class="muted" x="500" y="104" text-decoration="line-through">tag   "breakfast"</text>
<text x="500" y="128">tag   "dairy-free"</text>
<text class="small muted" x="500" y="200">Only the named fact goes.</text>
</svg>
</figure>

The stored facts carry no cardinality of their own. It comes from the attribute's declaration, and Dialog applies it twice: when an assertion is written, and again when a query reads the attribute (see [Queries](./queries.md)).

Retracting a fact that is not there does nothing. Asserting a fact that is already there does not make a second copy: two writers asserting the same entity, attribute and value share one stored fact.

## Nothing is ever null

Storage holds only facts that exist. If Bob has not given the eggs a quantity, there is no `demo.grocery/quantity` fact for `item:2`. There is no null, and nothing is written to say a value is missing. Absence is something a query can ask about, not something storage records.

## When a fact became true

The three parts above say what a fact means. A stored fact also carries temporal information: the version of the data that asserted it. That is what lets two replicas tell which change came first, and which facts a retraction or a superseding assertion was meant to remove. It is covered in [Commits and History](./history.md).

<div class="aside">

**An older field.** A stored fact can also have an optional `cause`: the hash of a fact this one was meant to supersede. Older design notes lean on it heavily. Today Dialog uses it only to break ties that versions cannot order, and the real record of what superseded what lives in the history described in [Commits and History](./history.md).

</div>
