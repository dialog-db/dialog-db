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

## Relations and attributes

The middle part of a fact says what the fact is about the entity. What a fact stores there is a name, like `demo.grocery/name`, and that name is called a **relation**: the set of all the facts that share it. It is written as a domain and a name, separated by a slash:

- **The domain** says whose vocabulary the relation belongs to. It is written in reverse domain notation, like the domain authority of an [AT Protocol NSID](https://atproto.com/specs/nsid): `demo.grocery` stands for `grocery.demo`. Two apps that both say `name` do not collide, because their domains differ. A domain is lowercase letters, digits, hyphens and dots.
- **The name** is lowercase kebab-case: letters, digits and hyphens, starting with a letter, like `name` or `picked-up-at`.

The whole relation is at most 64 bytes. This guide uses the domain `demo.grocery`.

A relation is a name and nothing more. It does not say what type its values have, or how many of them an entity has. An app says that when it declares an **attribute**, which is a relation together with how to read it:

- a **description**, for people reading the data;
- the **value type**, such as `text:` or `boolean:`;
- a **pick**: which of an entity's facts in the relation a read returns.

The pick an attribute gets unless it says otherwise is `last`, the newest fact, so the attribute reads one value. The pick `all` reads every fact, so the attribute reads many. A grocery item has one name and one done flag, and any number of tags: the name and the done flag are read under `last`, the tags under `all`. The other picks, `max`, `min` and `top`, also read one value: the greatest, the least, or the best ranked of a list.

Two apps can declare different attributes over the same relation, one reading a quantity as a whole number and another as it is. The facts do not change. Most of this guide can say attribute for both, since the grocery list reads each relation one way. The indexes in the next chapter do too: the relation's place in a key is called the attribute, which is where EAV, AEV and VAE get their A.

Domains that start with `dialog.` are reserved. An app must not assert or retract facts in them, and Dialog rejects a transaction that tries. They are reserved so that Dialog can keep its own things in the same tree as an app's data, as facts, with no risk that the two collide: rules and concepts, the record of each commit, stored assets, and other bookkeeping the later chapters describe.

> ℹ️ If you are building something that Dialog itself might adopt one day, do not put it in a `dialog.` domain. Put your own name in front, as in `myorg.dialog.`, so the relation reads `myorg.dialog.presence/cursor`. The facts are yours and cannot collide with anything Dialog adds later, and the name still shows where the idea is meant to end up.

## Values

A value is one of seven types. Each type has a number, and that number is written into every key that holds the value (see [Keys](./keys.md)):

| Type byte | Type | Example |
|---|---|---|
| `00` | `bytes:` | raw binary data |
| `01` | `entity:` | `item:2`, to point at another thing |
| `02` | `boolean:` | `false` |
| `03` | `text:` | `"Oat milk"` |
| `07` | `record:` | opaque structured bytes |
| `08` | `symbol:` | a relation name used as a value |
| `09` | `number:` | `2`, `-3`, `1.5` |

The types are themselves identified by entities. `text:` is a URI like `item:1`, one that happens to have nothing after its scheme, and it names the type everywhere a type is written down: in an attribute's declaration, in a rule, and in the hash that gives an attribute its identity. So a type is something facts can be about, like anything else.

An entity value is how facts link things together. If Bob adds `the demo.grocery/store of item:2 is store:corner-shop`, the item now points at a store, and the store can have facts of its own.

### Numbers

Numbers in Dialog have one universal representation, and it can hold a number of any precision. There is no stored integer `2` next to a stored float `2.0`. There is the number two, and `2`, `2.0` and `+2` are three ways of writing it. NaN is not a number, and Dialog refuses it wherever a value comes in.

Types like `integer:` and `float:` are not kinds of stored number. They are how an attribute chooses to interpret the numbers in its relation:

| Type | How it interprets a number |
|---|---|
| `number:` | as it is |
| `integer:` | as a whole number |
| `natural:` | as a whole number that is not negative |
| `float:` | as the nearest 64-bit float |
| `natural:8` to `natural:128`, `integer:8` to `integer:128`, `float:32`, `float:64` | the same, fitted to a width |

A type has a range, and a number outside it does not fit the attribute's definition. For that attribute the number does not exist, even though it exists in the relation and another attribute over the same relation may read it. Through `natural:8`, a stored `300` or `-1` is simply not there.

A number inside the range is viewed at the precision of the type. A relation holding `4.3` is observed as `4` by an attribute that reads it as `integer:`, and as `4.3` by one that reads it as `number:`. A fraction is always rounded down, so `-4.3` is observed as `-5`.

> ℹ️ Writes go through the same lens. Under the pick `last`, asserting `4` through an `integer:` attribute does nothing when the value observed through that attribute is already `4`, so a stored `4.3` stays `4.3`. Asserting `5` replaces the `4.3` with `5`.

Dialog works this way so that apps using different lenses on the same relation can share it. An app with lower precision does not destroy detail that an app with higher precision cares about, because writing back what it read changes nothing. And the app with lower precision still observes the data at the level of precision it is able to handle, instead of failing on a number it did not expect.

## Saying things: assert and retract

An app never edits a fact in place. Facts cannot change. Instead, an app asserts and retracts whole facts:

- **Assert** says *"this is true."* It adds the fact.
- **Retract** says *"this is no longer true."* It removes the fact.

What an assertion does to the facts already there depends on the attribute's pick. Under `all`, the new fact joins the others. Under any other pick, the new fact supersedes one fact: the one a read under that pick would have returned. Asserting that the eggs are done removes the fact that they were not.

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

The stored facts carry no pick of their own. It comes from the attribute's declaration, and Dialog applies it twice: when an assertion is written, and again when a query reads the attribute (see [Queries](./queries.md)). A write follows the read. Whatever a reader of the attribute would have seen is what the assertion supersedes, and nothing else. If two devices had each given the eggs a done flag without seeing the other's, a read elects one of the two, and the next assertion supersedes that one. The other stays stored. A read under `last` never notices it, and a read under `all` can still find it.

Retracting a fact that is not there does nothing. Asserting a value that a read already returns writes nothing at all. Asserting a fact that is already there does not make a second copy: two writers asserting the same entity, attribute and value share one stored fact.

## Nothing is ever null

Storage holds only facts that exist. If Bob has not given the eggs a quantity, there is no `demo.grocery/quantity` fact for `item:2`. There is no null, and nothing is written to say a value is missing. Absence is something a query can ask about, not something storage records.

## When a fact became true

The three parts above say what a fact means. A stored fact also carries temporal information: the version of the data that asserted it. That is what lets two replicas tell which change came first, and which facts a retraction or a superseding assertion was meant to remove. It is covered in [Commits and History](./history.md).

<div class="aside">

**An older field.** A stored fact can also have an optional `cause`: the hash of a fact this one was meant to supersede. Older design notes lean on it heavily. Today Dialog uses it only to break ties that versions cannot order, and the real record of what superseded what lives in the history described in [Commits and History](./history.md).

</div>
