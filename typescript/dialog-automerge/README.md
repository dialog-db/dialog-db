# @dialog-db/automerge

Automerge CRDT documents as Dialog **record values**, for JavaScript — the
browser/worker mirror of the Rust [`dialog-automerge`](../../rust/dialog-automerge)
edge crate.

An automerge document lives as a single atomic value in one `{the, of, is}`
claim: written, read, replicated, and content-addressed like any other value,
opaque to Dialog's query layer. Concurrent edits on different replicas
**converge via CRDT merge** rather than surfacing as divergent siblings.

This is an *edge* package. Only applications that hold automerge-typed record
attributes import it; the Dialog core wasm blob takes no automerge dependency,
and stored record bytes are naked automerge `save()` output.

## The read-fold-edit-write discipline

A `Cardinality::One` record attribute that has diverged holds **one stored
sibling per concurrent write**. A *typed* native reader already collapses them
into one merged value before the row leaves the query engine. The untyped
JavaScript `select` surface does **not** — it hands back every sibling. So an
app that reads one sibling and blindly writes it back issues a data-losing
`Replace`: the exact failure CRDTs exist to prevent.

The rule, in four steps:

1. **Read** all siblings for the `(the, of)` group (an ordinary untyped query).
2. **Fold** them into one value with `foldRecords` (or `foldRecordGroups` over a
   whole result), and merge in any sibling that arrives mid-session with
   `RecordSession.absorb`.
3. **Edit** the folded value.
4. **Write** it back with an ordinary typed commit. The write is a `Replace`
   that supersedes every stored sibling with the merged document, physically
   converging the store.

Never write a record you did not first fold from every sibling.

```ts
import {
  foldRecords,
  RecordSession,
  TextDocument,
  spliceText,
  getText,
} from '@dialog-db/automerge'

// 1. read: gather the sibling byte payloads for one (the, of) group.
const siblings = rows.map((row) => row.is.value)

// 2. fold: one merged value, inclusive of every sibling.
const opened = RecordSession.open(TextDocument, siblings)
if (opened?.session) {
  const session = opened.session
  console.log(getText(session.value)) // the merged document

  // 3. edit locally; absorb anything that arrives from sync mid-session so the
  //    next commit stays inclusive.
  session.edit((doc) => spliceText(doc, 0, 0, 'PS: '))
  session.absorb(incomingSiblingBytes) // when sync delivers one

  // 4. write: commit session.encode() as a typed Replace.
  await commitRecord(the, of, session.encode())
}
```

For a one-shot read with no live editing, `foldRecords(siblings, TextDocument)`
returns the merged value and its canonical `bytes` directly.

## Canonical bytes are identity

A record is stored, keyed, and compared by its bytes, so every encode of the
same document state must produce the same bytes — a document written by a
browser replica and one written by a native replica for the same edits must
mint the identical value. `canonicalBytes` (used by every format's `encode`)
reorders changes into a deterministic topological order and applies **no
compression**, matching the Rust crate's `canonical_bytes` / `canonical_options`
convention. Canonical output is stable per automerge **major** version — every
participant that writes record bytes must pin the same one.

## Threading

All fold/merge/decode work is synchronous CPU and is meant to run **off the UI
thread**, beside the transactor (in the browser, in the worker). The main thread
is handed one merged value, never a set of unfolded siblings.

## API

- `foldRecords(rows, format)` / `foldRecordGroups(rows, format)` — the read-side
  fold; drops undecodable siblings and never fails the whole read.
- `RecordSession` — a live editing session: `open` (fold-on-open), `edit`,
  `absorb` (merge a mid-session arrival), `encode`.
- `TextDocument` — the automerge collaborative-text `RecordFormat`, plus
  `createTextDocument` / `spliceText` / `getText` / `forkTextDocument`.
- `RecordFormat<T>` — implement `decode` / `encode` / `merge` for other formats.
- `canonicalBytes` — the canonical automerge encoder.
- `Automerge` — the pinned `@automerge/automerge`, re-exported so callers share
  one version.

## Tests

```
npm install
npm test
```
