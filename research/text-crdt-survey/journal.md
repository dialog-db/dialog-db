# Journal: text CRDT survey

## Where things stand

Exploring. First pass done from docs and papers (survey.md). Open: Diamond
Types, json-joy and DeltaDB rows; whether Automerge changes can be served
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
