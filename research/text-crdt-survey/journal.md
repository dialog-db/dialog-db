# Journal: text CRDT survey

## Where things stand

Exploring. First pass done from docs and papers (survey.md). Open: Diamond
Types' sync and format (its BINARY.md), json-joy and DeltaDB rows; whether Automerge changes can be served
since heads by a store that does not run Automerge.

## Datasets

None; this topic reads, it does not measure. The cost topic will use the
editing traces: two concurrent traces of about 21 KB each, two authors,
one keystroke per transaction, with parents and the expected final text
for a convergence check [12 in survey.md].

## 2026-11-06 — first pass over the list

**Question.** On the matrix's rows, what does each entry do?

**Method.** Primary docs and papers, one per entry; nothing run.

**Results.** survey.md. Two shapes of "changes since a version":

- **Version vector** (Yjs, Yrs, Loro): the asker sends per-peer clocks
  and the holder computes the difference. Computing it means parsing the
  updates, so the holder runs the library.
- **Change graph** (Automerge changes with dependencies, Eg-walker
  events with parents): each change is immutable and names its parents;
  merging is a union. Serving "changes since these heads" needs only the
  ids and parent links, not the operations.

**Decision.** The hypothesis holds for the change-graph shape and not
for the version-vector shape, provided parent links are readable without
the library. Not yet checked that Automerge exposes them that way.

Commits: see branch history.

## 2026-11-06 — Diamond Types row

**Question.** Fill the row the first pass could not reach.

**Method.** The repository README.

**Results.** Rust, Wasm through npm packages, P2P without a server, plain
text only, work in progress with an out-of-date crates.io release. How
peers exchange changes and the file format are not in the README.

**Decision.** Row filled except sync and format; BINARY.md is next for
those, since it decides whether Diamond Types is a change graph a store
could serve without running it.

## 2026-11-07 — can a content-blind store serve Automerge changes?

**Question.** Are a change's hash and parents readable without running
Automerge?

**Method.** The binary format spec; automerge-repo's storage docs; the
Sedimentree design; the Loro Protocol post.

**Results.** Yes for Automerge: dependencies are the first field of a
change chunk, and the hash is SHA-256 over the chunk bytes. automerge-repo
already stores one chunk per change keyed by hash and compacts into
snapshots keyed by heads. Sedimentree syncs the graph through a server
that cannot read content, grouping changes by hash boundaries so the
summary stays small. Loro's protocol relays encrypted records without
decrypting them, experimentally; otherwise its server hooks hold the
document.

**Decision.** Hypothesis confirmed for Automerge, not for version-vector
libraries. The per-change block count is the cost to watch; compaction
or Sedimentree-style chunking is the known answer.
