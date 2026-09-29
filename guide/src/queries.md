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
    pub struct Name(pub String);

    /// Whether the item has been picked up.
    #[derive(dialog_query::Attribute, Clone, PartialEq, Eq, PartialOrd, Ord)]
    pub struct Done(pub bool);
}

#[derive(Concept, Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Item {
    pub this: Entity,
    pub name: grocery::Name,
    pub done: grocery::Done,
}
```

The attribute's name comes from the module and the type: `grocery::Name` is the attribute `grocery/name`. Each field of the concept is one pattern, and `this` is the entity they share. Writing an `Item` asserts its facts. Querying for `Item` with blanks in every field asks for every item:

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

A concept is a way of reading facts, not a table. The facts do not know which concepts they belong to. Any entity with a `grocery/name` and a `grocery/done` is an `Item`, and a second app can declare its own concept over the same attributes without asking the first.

## Planning

Before running a query, Dialog decides how to match its patterns. Each pattern has a cost, and the cost depends on which of its parts are already known. A pattern with only its attribute known has to scan a whole run of AEV. A pattern whose entity is known is a short lookup in EAV. At each step, the planner picks the cheapest pattern it can run given the blanks filled so far.

Which index a pattern reads follows directly from the [Keys](./keys.md) chapter: if the entity is known, EAV; if not, but the value is, VAE; otherwise AEV. For the grocery list, both patterns know only their attribute, so both are AEV scans. AEV sorts each attribute's run by entity, so Dialog walks the two runs side by side and joins them on the entity, like a zipper:

<figure class="dg">
<svg class="dg" viewBox="0 0 640 244" width="640" height="244" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Two AEV scans sorted by entity, walked in step and joined on the entity">
<defs><marker id="p-arrow" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto"><path d="M0,0 L8,4 L0,8 z"/></marker></defs>
<text class="title" x="10" y="20">1. two scans of AEV, both sorted by entity</text>
<text class="title" x="400" y="20"></text>
<text class="small muted" x="10" y="40">01 grocery/name …</text>
<text class="small muted" x="420" y="40">01 grocery/done …</text>
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

## Cardinality

An attribute is declared as having one value or many. `grocery/name` has one, which is the default, and `grocery/tag` could be declared with `#[cardinality(many)]`. For a one-valued attribute, a query that finds two values runs the election from the [Sync](./sync.md) chapter and returns one. Every replica elects the same value. For a many-valued attribute, a query returns every value.

## Optional fields

A concept field can be optional: `quantity: Option<grocery::Quantity>`. An item without a quantity still matches, and the field reports that the value is absent. Storage never records that an attribute has no value (see [Facts](./facts.md)). The query infers it from the fact not being there.

## Rules

A rule derives facts from other facts. An app could say *an item is urgent if it is tagged `"breakfast"` and not done*, and then query for urgent items as if `urgent` were stored.

Rules are installed by asserting them, and they are stored as facts in the tree, under `dialog.rule/`. So a rule syncs with the data like everything else. When Bob's phone pulls Alice's rule, his queries start using it. A rule is named by the hash of its body, so two replicas that install the same rule install the same entity.

Most rules run at query time. A rule can also run when a commit is made, and write its conclusions into the tree, which suits derived data that is read far more often than it changes.

## Subscriptions

A list on screen should update when the data does. A subscription keeps a query live. It remembers which key ranges the last evaluation read. Each time the app polls it after the branch's head moves, it compares the old and new trees, again skipping every subtree whose hash matches, and looks at whether any change falls inside those ranges. If none does, the list stays as it is and nothing is re-run. If some do, it re-derives only the entities that changed when the query allows it, and re-runs the whole query when a rule changed. The app receives what was added and what was removed.

So when Alice's merge arrives on Bob's phone, the diff between the two roots is small, the list's ranges catch the renamed milk, and one row updates.

<div class="aside">

**Implementations.** The query engine is [`dialog-query`](https://github.com/dialog-db/dialog-db/tree/main/rust/dialog-query): concepts in `src/concept`, the planner in `src/planner.rs`, rules in `src/rule.rs`. The attribute and concept derives are in [`dialog-macros`](https://github.com/dialog-db/dialog-db/tree/main/rust/dialog-macros). Choosing an index for a pattern is `selector_range` in [`dialog-artifacts/src/tree.rs`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-artifacts/src/tree.rs). Subscriptions are [`branch/subscription.rs`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-repository/src/repository/branch/subscription.rs) in `dialog-repository`.

</div>
