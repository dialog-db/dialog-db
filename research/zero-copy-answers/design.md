# Zero-copy answers: what the prototype does

Two changes, each a setting or a separate path so every variant runs from
one build. Neither is decided; this says what was built to measure.

## Late materialization in the engine

### Before

A scan yields `FactView`s that borrow the leaf's decoded-keys arena (a
`KeyHandle` into `DecodedKeys`). That is where borrowing stops: the
attribute scan (`attribute/query/all.rs`, `attribute/query/only.rs`)
calls `FactView::to_owned` on every admitted row, then `merge` copies
the fact into a `Claim`, the `Claim` into an `Arc`, and its value and
entity into the row's bindings. A concept's rule body binds parameter
names, and `merge_parameters` copies every value again into the caller's
names. `ConceptConclusion` then materializes `this`, and tonk's
`project` clones every value into `Ipld`.

### After

- `dialog-corpus` gains a borrowed value, `ValueView<'a>` (text, bytes,
  entities and symbols borrow; numbers are read out), and
  `FactView::checked`, which validates a row once (everything `to_owned`
  would refuse) into a `CheckedRow`: the row plus where its entity,
  attribute and text value sit in the key. Reads of a `CheckedRow` slice
  the key without walking or validating it again.
- `Match` stores a `Slot` per binding instead of a `Binding`:
  `Slot::Bound(Binding)` as before, or `Slot::Scanned { row, part }`,
  a share of the scanned row (`Arc<Scanned>`) and which component it
  binds (`the`, `of`, `is`). The row is shared by every slot it binds
  and by the claim it cites (`Cited::Scanned`), and holds one
  `OnceLock` per component, so a value is materialized at most once per
  scanned row, the first time something reads it as a `Binding`,
  however many rows the slot is copied into.
- The public reads (`Match::get`, `lookup`, `prove`) are unchanged:
  they materialize. New reads hand values on without materializing:
  `Match::view(name)` and `ConceptConclusion::view(field)` return a
  `ValueView`.
- The plumbing moves slots instead of values: `merge_parameters` moves
  a scanned slot over (`Match::bind_slot_of`), `combine` and a rebinding
  check compare two scanned slots by their bytes (`Slot::agrees`), and
  `environment` reads presence without materializing.
- A scan binds a row in place only when its kinds admit the row as
  stored (no kind, or a primitive kind containing the stored type) and
  no constant value pins it; otherwise it takes main's eager path, so
  typed reads through another type's view and refinements are unchanged.
- `ConceptConclusion::this` stays eager. A lazy one (tried) helped only a
  caller that never reads the entity, and moved the work into
  subscription maintenance, which reads every row's; the answer path
  reads `this` in place with `view` instead.
- A scan decides its binding policy once (`Placement`): building a
  term's kind per row cloned a range's boxed interval for every row.
- `DIALOG_LAZY_BINDINGS=0` turns it all off: every scan takes main's
  eager path, and the engine behaves as main does. Every
  `dialog-query` test passes in both modes.

### What it does not change

The scan, the planner and the join are main's. A probe of one entity's
field still builds its index keys, a row still clones its `Match` into
every extension, and the entity a nested probe pins is still
materialized (once per row now, not once per probe).

## The answer frame (`rust/dialog-answer`)

A crate with no dependencies, so a page or a guest can take the reader
alone.

```text
frame    := "DAF1" kind:u8 base:u32 tokens columns section+
tokens   := count:u32 (tag:u8 length:u32 bytes)*      ids base, base+1, ...
columns  := count:u16 name:u32*                        names are tokens
section  := rows:u32 lane*                             one lane per column
lane     := id:u32 * rows                              ABSENT, UNBOUND or a token id
```

- A **token** is a distinct value: an entity's URI, a text, a number,
  a column's name. A frame writes the tokens that are new to the
  session, and its lanes refer to every token by id.
- The **session** (`Session` writing, `Dictionary` reading) holds every
  token sent so far, as a chronicle holds every token of its file. A
  subscription's snapshot and all its deltas are one session, so a
  delta carries only values the receiver has never seen; a status
  change carries the issue's id and the status's id, not the issue.
- The **reader** keeps each accepted frame's bytes and a table of where
  each token sits in them. A cell is a lane read (4 bytes) and a slice
  of the frame; text is decoded (in a page, by `TextDecoder`) only when
  read, and at most once per token.
- Frames are sequenced: a frame whose first token id is not the
  dictionary's next one is refused (`Error::Sequence`), so a lost frame
  is detected, not misread. A reconnect starts a new session with a
  snapshot.
- `Absent` and unbound are lane ids of their own, so the frame keeps
  the engine's three states (unbound, absent, present) where tonk's
  JSON drops the first two.

The JS reader (`bench/frame.js`) is about 100 lines and is what the
browser measurement runs.

## Open choices the measurements would settle

- Whether `Value` itself should become the borrowed form (a value
  backed by a shared slab, cloned by refcount) instead of a `Slot`
  beside it: one representation instead of two, at the cost of every
  `Value` match in the code base.
- Whether the engine should hand the encoder rows (`Match` streams)
  without `ConceptConclusion` at all.
- Where the encoder lives: next to the engine (`dialog-query` behind a
  feature) or in `dialog-answer` behind a `dialog-corpus` feature.
- Whether lanes should be front-coded or varint-coded: a lane of u32s
  is 4 bytes a cell, and a snapshot's lanes are most of its bytes.
