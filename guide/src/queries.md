# Queries

Everything so far has been about getting facts stored and shared. This chapter is about getting them back out. Bob's phone wants to show the grocery list: every item, its name, and whether it is done.

Dialog's query language is in the family of [Datalog](https://en.wikipedia.org/wiki/Datalog). A query is a set of patterns with blanks in them, and the answer is every way of filling the blanks from the facts.

## Patterns over facts

The smallest pattern is a single fact with blanks. *The grocery name of `?item` is `?name`* matches every name fact, and fills `?item` and `?name` from each one. Add a second pattern that shares a blank, *the grocery done of `?item` is `?done`*, and the two must agree on `?item`. The answer is one row per item, with its name and its done flag.

An app rarely writes patterns one at a time. It declares a **concept**: a named shape made of attributes. In Rust, attributes and concepts are types:

```rust
mod grocery {
    /// The name of a grocery item.
    #[derive(dialog_query::Attribute, Clone, PartialEq, Eq, PartialOrd, Ord)]
    #[domain("demo.grocery")]
    pub struct Name(pub String);

    /// Whether the item has been picked up.
    #[derive(dialog_query::Attribute, Clone, PartialEq, Eq, PartialOrd, Ord)]
    #[domain("demo.grocery")]
    pub struct Done(pub bool);
}

#[derive(Concept, Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Item {
    pub this: Entity,
    pub name: grocery::Name,
    pub done: grocery::Done,
}
```

The domain comes from `#[domain]` and the name from the type, kebab-cased, so `grocery::Name` is an attribute over the relation `demo.grocery/name`. The doc comment becomes its description, the wrapped type its value type, and it reads one value, the newest, unless it says another pick such as `#[pick(all)]`. A wrapped `String` declares `text:` and a `bool` declares `boolean:`. A Rust number declares the number type of its own width, so a `u8` declares `natural:8`. Each field of the concept is one pattern, and `this` is the entity they share. Writing an `Item` asserts its facts. Querying for `Item` with blanks in every field asks for every item:

```rust
use dialog_query::query::Output;

let items: Vec<Item> = branch
    .query()
    .select(Query::<Item> {
        this: Term::var("this"),
        name: Term::var("name"),
        done: Term::var("done"),
    })
    .perform(&operator)
    .try_vec()
    .await?;
```

A concept is a way of reading facts, not a table. The facts do not know which concepts they belong to. Any entity with a `demo.grocery/name` and a `demo.grocery/done` is an `Item`, and a second app can declare its own concept over the same relations without asking the first.

Attributes and concepts have identities of their own. An attribute is named by a hash of what it means: the relation it reads, its type and its pick. A concept is named by a hash of the attributes its fields hold, and whether each is optional. Descriptions and field names are left out, so rewording a description or renaming a field changes no name. The names are entities, written `the:…` and `concept:…`, so two apps that declare the same attribute have declared the same thing.

## Planning

Before running a query, Dialog decides how to match its patterns. Each pattern has a cost, and the cost depends on which of its parts are already known. A pattern with only its attribute known has to scan a whole run of AEV. A pattern whose entity is known is a short lookup in EAV. At each step, the planner picks the cheapest pattern it can run given the blanks filled so far.

Which index a pattern reads follows directly from the [Keys](./keys.md) chapter: if the entity is known, EAV; if not, but the value is, VAE; otherwise AEV. For the grocery list, both patterns know only their attribute, so both are AEV scans. AEV sorts each attribute's run by entity, so Dialog walks the two runs side by side and joins them on the entity, like a zipper:

<figure class="dg">
<svg class="dg" viewBox="0 0 640 244" width="640" height="244" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Two AEV scans sorted by entity, walked in step and joined on the entity">
<defs><marker id="p-arrow" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto"><path d="M0,0 L8,4 L0,8 z"/></marker></defs>
<text class="title" x="10" y="20">1. two scans of AEV, both sorted by entity</text>
<text class="title" x="400" y="20"></text>
<text class="small muted" x="10" y="40">01 demo.grocery/name …</text>
<text class="small muted" x="420" y="40">01 demo.grocery/done …</text>
<rect class="entity" x="10" y="50" width="70" height="22"/>
<text class="entity" x="45.0" y="65" text-anchor="middle">item:1</text>
<rect class="value" x="84" y="50" width="130" height="22"/>
<text class="value" x="149.0" y="65" text-anchor="middle">&quot;Oat milk, 1 L&quot;</text>
<rect class="entity" x="10" y="76" width="70" height="22"/>
<text class="entity" x="45.0" y="91" text-anchor="middle">item:1</text>
<rect class="value" x="84" y="76" width="130" height="22"/>
<text class="value" x="149.0" y="91" text-anchor="middle">&quot;Soy milk&quot;</text>
<rect class="entity" x="10" y="102" width="70" height="22"/>
<text class="entity" x="45.0" y="117" text-anchor="middle">item:2</text>
<rect class="value" x="84" y="102" width="130" height="22"/>
<text class="value" x="149.0" y="117" text-anchor="middle">&quot;Eggs&quot;</text>
<rect class="entity" x="420" y="50" width="70" height="22"/>
<text class="entity" x="455.0" y="65" text-anchor="middle">item:1</text>
<rect class="value" x="494" y="50" width="60" height="22"/>
<text class="value" x="524.0" y="65" text-anchor="middle">false</text>
<rect class="entity" x="420" y="76" width="70" height="22"/>
<text class="entity" x="455.0" y="91" text-anchor="middle">item:2</text>
<rect class="value" x="494" y="76" width="60" height="22"/>
<text class="value" x="524.0" y="91" text-anchor="middle">true</text>
<line class="dashed" x1="218" y1="61" x2="416" y2="61"/>
<line class="dashed" x1="218" y1="113" x2="416" y2="87"/>
<text class="small muted" x="318" y="140" text-anchor="middle">walk both runs in step,</text>
<text class="small muted" x="318" y="154" text-anchor="middle">matching on the entity</text>
<text class="title" x="10" y="158">2. rows out</text>
<text class="small muted" x="10" y="176">item:1 has two names; a name has one value, so the election picks one</text>
<rect class="entity" x="10" y="186" width="70" height="22"/>
<text class="entity" x="45.0" y="201" text-anchor="middle">item:1</text>
<rect class="value" x="84" y="186" width="130" height="22"/>
<text class="value" x="149.0" y="201" text-anchor="middle">the elected name</text>
<rect class="value" x="218" y="186" width="60" height="22"/>
<text class="value" x="248.0" y="201" text-anchor="middle">false</text>
<rect class="entity" x="10" y="212" width="70" height="22"/>
<text class="entity" x="45.0" y="227" text-anchor="middle">item:2</text>
<rect class="value" x="84" y="212" width="130" height="22"/>
<text class="value" x="149.0" y="227" text-anchor="middle">&quot;Eggs&quot;</text>
<rect class="value" x="218" y="212" width="60" height="22"/>
<text class="value" x="248.0" y="227" text-anchor="middle">true</text>
</svg>
</figure>

When one pattern is much narrower than the other, say `done` pinned to `false`, the planner drives from that pattern instead and looks up each row's other fields in EAV. Either way, every step is a range scan over sorted keys, streamed. A query never loads a whole index. On a phone that has fetched only part of the tree, a query fetches only the nodes along the ranges it reads: its patterns' ranges, plus the range where rules are stored.

## Picks

An attribute is declared with a pick, which says which of an entity's facts a read returns (see [Facts](./facts.md)). `demo.grocery/name` reads under `last`, which is the default, and `demo.grocery/tag` could be declared with `#[pick(all)]`. Under `last`, a query that finds two values runs the election from the [Sync](./sync.md) chapter and returns one. Every replica elects the same value. Under `all`, a query returns every value.

The other picks also return one value. `max` and `min` return the greatest and the least. `top` takes a list, of values or of relations, and returns the first on the list that the entity has: a status that is `"urgent"` if any fact says so, or a display name that falls back to a username.

A number is compared the way its attribute's type reads it (see [Facts](./facts.md#numbers)). A query for a quantity of `2` through an attribute declared `natural:` also finds a stored `2.5`, because that is what a reader of the attribute sees there.

## Optional fields

A concept field can be optional: `quantity: Option<grocery::Quantity>`. An item without a quantity still matches, and the field reports that the value is absent. Storage never records that an attribute has no value (see [Facts](./facts.md)). The query infers it from the fact not being there.

## Rules

A rule derives facts from other facts. An app could say *an item is urgent if it is tagged `"breakfast"` and not done*, and then query for urgent items as if `urgent` were stored.

What a rule derives are facts in relations, like the ones an app asserts. A rule that concludes a concept derives one fact for each of the concept's fields. So any concept that reads one of those relations sees the derived values next to the stored ones, not only the concept the rule was written for.

Rules are installed by asserting them, and they are stored as facts in the tree, under `dialog.rule/`. So a rule syncs with the data like everything else. When Bob's phone pulls Alice's rule, his queries start using it.

A rule is named by a hash of what it means, not of the bytes its author wrote. Before hashing, its variables are renamed by where they appear and its patterns are put in order, and the concepts it uses enter by their own names. Two people who write the same rule with different variable names install the same entity. That canonical form is also what gets stored, so the names an author chose are not kept.

Most rules run at query time. A rule can also run when a commit is made, and write its conclusions into the tree, which suits derived data that is read far more often than it changes.

### Rules that say "not"

The urgent rule asks for something to be absent: the item is *not* done. A rule can say that with `unless`, and an optional field or a pick like `top` asks the same kind of question, since "the best one" means "this one, and nothing better".

Absence is only well defined once everything that could be there has been derived. Usually that is easy: Dialog derives what a rule tests in full before running the rule. It stops being possible when rules form a loop through such a test, where a rule tests for the absence of something that depends on the rule's own conclusion. No single author has to write that loop. Alice and Bob can each install rules that are fine alone and close a loop when their replicas merge.

Dialog does not refuse such a set of rules, because rules arrive by sync and something has to be done with whatever arrives. It sets one rule of the loop aside instead, and evaluates the rest. It picks the rule installed last, ordered by the commits that installed them, which every replica sees the same way. So replicas with the same history set aside the same rule and agree on every answer. A rule that is set aside comes back by itself once the loop is broken, and a query can ask which rules are set aside by reading `dialog.rule/quarantined`.

<div class="aside">

**Rules from an older release.** A rule installed by a release before version 0.3.0 is stored under a name computed the older way, and no read looks for it there, so it does nothing. `Branch::migrate` installs every such rule again under its current name. It is the same call that rewrites a tree's number keys (see [Keys](./keys.md#older-trees-key-numbers-by-type)), so an app that opens an older branch runs it once.

</div>

## Subscriptions

A list on screen should update when the data does. A subscription keeps a query live. It remembers which key ranges the last evaluation read. Each time the app polls it after the branch's head moves, it compares the old and new trees, again skipping every subtree whose hash matches, and looks at whether any change falls inside those ranges. If none does, the list stays as it is and nothing is re-run. If some do, it re-derives only the entities that changed when the query allows it, and re-runs the whole query when a rule changed. The app receives what was added and what was removed.

So when Alice's merge arrives on Bob's phone, the diff between the two roots is small, the list's ranges catch the renamed milk, and one row updates.

<div class="aside">

**Implementations.** The query engine is [`dialog-query`](https://github.com/dialog-db/dialog-db/tree/main/rust/dialog-query): concepts in `src/concept`, the planner in `src/planner.rs`, rules in `src/rule.rs`, and the identities of attributes, concepts and rules in `src/identity.rs` and `src/rule/canonical.rs`, built on [`merkle-reference`](https://github.com/dialog-db/dialog-db/tree/main/rust/merkle-reference). The attribute and concept derives are in [`dialog-macros`](https://github.com/dialog-db/dialog-db/tree/main/rust/dialog-macros). Choosing an index for a pattern is `selector_range` in [`dialog-artifacts/src/tree.rs`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-artifacts/src/tree.rs). Subscriptions are [`branch/subscription.rs`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-repository/src/repository/branch/subscription.rs) in `dialog-repository`.

</div>
