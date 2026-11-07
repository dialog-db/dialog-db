# Text CRDT survey

Status: **Exploring**. Answers questions from the design topic
[collaborative text editing](../design/collaborative-text.md).

## Idea

**Question.** What do existing text CRDT libraries and papers do on the
axes the collaborative text matrix turns on, which could Dialog integrate,
and what should it learn from the rest?

**Hypothesis.** At least one library exposes its changes as opaque bytes
it can compute "since a version" and apply on any peer, so a remote that
does not understand the CRDT can relay them. That would make "resource
synced by Dialog" workable without a CRDT-aware remote.

**Covers.** Automerge, Yjs and Yrs, Loro, Diamond Types, Eg-walker,
Fugue, Peritext, json-joy, Zed's DeltaDB.

**Success.** Every row below filled for every entry from a primary source
(docs, paper, code), or marked unknown with what would settle it.

**Failure.** None of them separates "changes since a version" from a
CRDT-aware peer. Then "resource synced by Dialog" needs a remote that
runs the library.

## Rows

- how it syncs, especially "changes since a version" and what the other
  side must understand;
- storage and sync cost on published editing traces;
- peer to peer without a server;
- interleaving of concurrent runs;
- embeds in Rust and WebAssembly.

The survey table is in [`text-crdt-survey/survey.md`](text-crdt-survey/survey.md).
