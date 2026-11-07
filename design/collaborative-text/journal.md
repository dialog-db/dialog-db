# Journal: collaborative text editing

## Reflective inquiry

|  | Understanding | Activity |
|---|---|---|
| **Status** | Problem stated: no-server, offline, converge without lost edits; last writer wins per field loses one side. Two earlier spikes known, one more said to exist in unmerged PRs. Write amplification is the cost that sank the first spike, and it applies beyond it. | Directing. |
| **Agenda** | The bar for acceptable write amplification (was being answered: "a rough measure should be if..."). A second, different use case. The spikes in notes and unmerged PRs, and the tradeoff that sank each. Whether evaluators named text as the dealbreaker. | Set the bar, then decide whether amplification is a research topic. Fill the matrix from the spikes. |

## 2026-11-06, Describing: opening the topic

**Asked.** What is the situation? The maintainer: many users ask for
collaborative text editing and Dialog has no good solution for it at the
moment.

**Learned.** The request arrives as a feature. The ordered relations note
already rules collaborative text out of its own scope.

**Decided.** Not decided. Next: concrete requests.

## 2026-11-06, Describing: three concrete cases

**Asked.** One concrete request: who, building what, doing what instead?
Answer: three cases. A Tonk document editor where racing collaborators
drop changes; builder and maintainer accepted single editor. An
Obsidian-style notebook whose builder does not know how to allow editing
across replicas and is considering a text CRDT library. The maintainer's
Observable-style playground, where concurrent editors override each other.

**Learned.** The symptoms are worded three ways: dropped, overridden, and
"don't know how". The playground edits cells, not one long text.

**Decided.** Not decided. No real names in this topic.

## 2026-11-06, Describing to Directing: by voice

**Asked.** Did others leave, or run a CRDT library alongside? Most
evaluated Dialog and passed, picking a text CRDT library. Was text the
core of what they built? Not known. Does "support" mean built in? No:
composing with a library, using one from within Dialog, or built in are
all on the table; I had assumed built in, and that was mine, not the
maintainer's. What do you want a developer to be able to do? Two people
edit the same document and it converges without losing either's edits.
Offline? Yes, and there is no server: often peer to peer. Partial
replicas, queryable text, rich text, long documents: nice to have, rows
in the matrix, not requirements. Why are edits lost? Last writer wins
per text field; per-paragraph fields limit loss to one paragraph.

**Learned.** Two spikes. CRDT document as value: dropped because each
edit copies the document; napkin math, unmeasured. Key plus side lane:
not built, the engineering is large and mostly outside Dialog's domain;
the maintainer thinks it is still worth testing. Pushed on the first: at
paragraph size the copy is small, but the real cost is the write itself.
Every commit rewrites a spine of tree nodes (a timer reached gigabytes in
a day or two), so a built-in CRDT committing per keystroke pays it too.
The maintainer agreed it is worth measuring, split into spine cost and
payload copy. Batching edits was raised and not ruled out: "we don't
have it" says unbuilt, not unworkable.

**Decided.** Problem statement (top story). Do not name specific CRDT
libraries. Not decided: the bar for acceptable growth, which a
measurement needs before it starts.
