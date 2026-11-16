# Zero-copy answers

Status: Exploring. Its working material is in
[`zero-copy-answers/`](zero-copy-answers/): the
[journal](zero-copy-answers/journal.md) says where the research stands,
the [evaluation](zero-copy-answers/evaluation.md) what decides it, and
the [design](zero-copy-answers/design.md) what the prototype does. See the
[research index](README.md) for what the statuses mean.

## Idea

A leaf stores facts columnar: a node's entities, attributes and values
are front-coded or dictionary-coded once per node. The chronicle goes
further: every token is stored once in the file and a node is a frame
of offsets. The scan keeps part of that. A warm leaf's keys are one
decoded arena (`DecodedKeys`), and a scanned row (`FactView`) is a
handle into it plus the offsets of its components.

The query engine throws that away at the first premise. Every admitted
row is materialized (`FactView::to_owned`): a URI string and an `Arc`
for the entity, a Blake3 hash of the URI to rebuild the entity's key
bytes, a string and an `Arc` for the attribute, and a string for a text
value. Then it is copied again into the claim the row cites, into the
row's bindings, into the conclusion, into tonk's projection
(`Conclusion { this: String, fields: BTreeMap<String, Ipld> }`), and into
JSON. The JSON travels over SSE from the service worker to the page,
where `JSON.parse` builds a JS object graph and `serde_wasm_bindgen`
turns it back into the same Rust structs. Then a template reads a few
fields of each row.

So a value is copied five or six times between the leaf that holds it
once and the DOM that shows it, and most rows are never read by a
premise at all: they are read only so they can be handed on.

Two changes, which compound:

1. **Late materialization in the engine.** A binding the scan makes
   holds the scanned row and which component it is, not an owned value.
   It is materialized only when something asks for the value (a formula,
   a comparison between kinds, a caller reading a `Value`), and the
   answer's encoder reads its bytes straight from the arena.
2. **A binary answer frame, in place of JSON.** Columnar like a leaf:
   per column a lane of token ids, and the distinct tokens once. The
   dictionary belongs to the session, as the chronicle's token log
   belongs to the file: a token sent once (an entity, a status, a
   person's name) is referenced by id in every later frame and delta of
   that subscription. The receiver reads a field in place from the frame
   bytes, so it decodes only what the template reads.

## Question

How much of the cost of answering a query, from the scan to the bytes
the page holds, is materialization and copying, and how much of it do
late materialization and a binary frame take away?

## Hypothesis

Most of it. Per row today: about 16 allocations and 3 KB allocated on
`read-scan`, with about a third of the instructions in the allocator.
The tonk projection and JSON add a comparable cost on top. Late
materialization should remove most of the engine's per-row allocations,
and the frame should cost a fraction of JSON to write and almost nothing
to read.

## Measured against

`main` as it is (`04965aa`), and the branch with each change apart:

- **main:** engine → `Vec<ConceptConclusion>` → tonk's `project` →
  `serde_json` → receiver `serde_json::from_slice` into `Vec<Conclusion>`
  (the path tonk runs, with the browser's `JSON.parse` +
  `serde_wasm_bindgen` stood in for natively);
- **branch, eager bindings, frame:** the engine as main, the frame in
  place of `project` + JSON;
- **branch, lazy bindings, JSON:** late materialization, tonk's path
  after it;
- **branch, lazy bindings, frame:** both.

Lazy bindings are a setting of one build (`DIALOG_LAZY_BINDINGS`), so
every variant runs from the same binary.

## Success

- Engine (query to conclusions), per row: allocations down by half or
  more and instructions down by 25% or more on the scan and the
  eight-field concept, with every existing `dialog-query` test passing.
- The whole path (query to the receiver holding an answer it can read):
  2× or better in instructions and allocations for the eight-field
  concept at 3,000 rows.
- Bytes on the wire: the frame no larger than the JSON; a delta that
  repeats tokens of an earlier frame of the session clearly smaller.

## Failure

- Engine allocations down by less than 25%, or instructions by less than
  10%: late materialization is not worth widening `Match` for.
- The whole path under 1.5×: the copies are not where the cost is, and
  the place to look is the scan or the join.

## The middle

Between those bars, the maintainer weighs the gain against the API it
asks for: a borrowed value type the engine exposes, and a wire format
tonk and its guests must speak.
