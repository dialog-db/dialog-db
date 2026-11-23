# Composite table order: keys first

**Status: Exploring.** Asked by the maintainer: "Measure keys-first ordering
before the layout is fixed." Measured on the branch
`research/composite-table-order`, from the head of `feat/composite-value`
([#843](https://github.com/dialog-db/dialog-db/pull/843)), `aa7c98c`.

## The question

A composite's canonical bytes are `table · body`, and the table holds every
scalar atom the composite uses (map keys and scalar values alike) once,
sorted by its canonical bytes, the type byte first
(`rust/dialog-corpus/src/model/composite.rs`, `composite/encode.rs`). A
composite of up to 4,096 bytes sits inline in the value payload of its key
in the entity (EAV), attribute (AEV) and value (VAE) orderings, and that
payload is an arena column of a leaf: front-coded against the previous
entry, so a value shares exactly its leading bytes with its neighbour.

In the attribute ordering, neighbours are values of one attribute on
successive entities, so composites of one shape sit side by side. Because a
record's keys are symbols (type byte `8`) and its entity, boolean and text
values sort below them (`1`, `2`, `3`), today a record's table begins with
its values, and two records with the same fields and different values part
almost at once.

Does putting the keys first make same-shape neighbours share their key run,
so the tree's leaves get smaller?

## The two layouts

- **A (today).** The table is every distinct atom, sorted by canonical slot
  bytes (bytewise, the type byte first).
- **B (keys first).** The table is two runs, `K · V`:
  - `K` is every distinct atom that is the key of an entry of any map
    anywhere in the composite (the top level or nested), sorted by its
    canonical slot bytes, bytewise, as A sorts.
  - `V` is every other distinct atom (an atom used only as a value or a list
    item), sorted the same way.
  - An atom used both as a key and as a value is in `K` only.
  - The body is written exactly as A writes it, with each slot's index the
    atom's place in `K · V`. Map entries are sorted by key index and then by
    value: since `K` keeps the keys in the same relative order A does, the
    entries come out in the same order under both layouts, and only the
    index numbers differ.
  - For a record every key is an attribute, a symbol whose slot is `08`
    followed by its `domain/name` text, `00`-escaped and `00`-terminated.
    "Sorted" for a record's key run is therefore the bytewise order of
    `domain/name`, a name that is a prefix of another first.
  - A list of scalars has no keys: `K` is empty and B is A.

B is as canonical as A: `K` and `V` are fixed by the content (which atoms
are keys), each is sorted, and the body is fixed by the table, so a value
has one byte string. A reader that looks up a key in a map needs to search
only `K`; where the table has offsets, it binary-searches indices
`0..=k`, `k` the key index of the map's last entry (every key the map holds
sorts at or below it, and they are all in `K`). The merkle reference is
computed from the entries, not the layout, so it does not change.

## Hypothesis

**H1.** Under B, same-shape records in attribute-ordered leaves share their
key run through front coding, so the tree's leaf bytes fall.

**H0.** The difference is within noise, or it goes the other way.

## Bars (fixed before any run)

- **Adopt B** if, on the same-shape record corpus, the total bytes of the
  tree's leaf nodes fall by at least 10 percent, **and** on every other
  corpus no total rises by more than 1 percent, **and** encoding and
  field-read time are within 5 percent of A, or better.
- **Reject B** otherwise. A result between the bars is reported as "not
  worth a layout change".

## Method

One example, `dialog-baseline`'s `composite_table_order`, with B behind a
research-only feature of `dialog-corpus` (`research-table-order`, off by
default) and a runtime switch, so A and B are one build. For each corpus
and layout it commits every fact to a branch on the volatile provider in
one transaction, walks every node of the tree, and reports leaf and total
node bytes, split by the key tag of each leaf (EAV, AEV, VAE, history), the
bytes of the composites themselves, the prefix each composite payload
shares with its neighbour in the attribute ordering, and the size of the
same blocks appended to one Chronicle file. Timing (release build): encode
per composite, `get` of one field, and an item of a list, on 2, 5 and 9
field maps, A and B interleaved.

Corpora:

1. Same-shape records: one attribute, a record of 5 fields (two short
   texts, a number, a boolean, an entity reference) on 4,000 entities;
   and a variant where 2 of the 5 values are the same on most entities.
2. The notation's syntax trees, one fact per form (see the journal for what
   was available).
3. Mixed: three record shapes interleaved under one attribute, and lists of
   scalars.
4. Control: the same entities with only scalar facts, no composites.

The journal is in [composite-table-order/journal.md](composite-table-order/journal.md).
