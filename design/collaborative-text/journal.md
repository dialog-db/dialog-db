# Journal: collaborative text editing

## Reflective inquiry

|  | Understanding | Activity |
|---|---|---|
| **Status** | Changes as facts is out (56× raw before commit cost). A text region stores at 1.8× raw but pays ~10 KB per flush. Libraries' snapshots are 5 to 7× smaller than raw. H2 holds; Yrs fits best on numbers. H3 refuted, H4 confirmed. | Directing; first measurements in. |
| **Agenda** | Which reading of the 2× bar: against library snapshots (disk) or per-change streams (sync)? Can the per-flush floor drop (H0 for the region, H5)? | Settle the bar; then measure region sync between replicas and H5 outside a commit. |

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

## 2026-11-06, Directing: bar, columns, challenges (by voice)

**Asked.** What bar for growth? Within 2× of a text CRDT library on the
same edit history, on disk and on sync bytes. Which history? Published
editing traces, including concurrent ones, so merge correctness is checked
too. Interleaving: nice to have. A third spike? No. Is a template with the
library's own sync legitimate? Yes, though reusing Dialog's sync is
preferable to two engines. Is the key plus side lane one idea? No: it
split into where changes travel (Dialog facts, Dialog's sync outside the
tree, another engine) and whether Dialog can read the text. Reading the
current text through a query: nice to have. Library agnostic: nice to
have; one library done well is enough. Built in: a hunch that Dialog
already does much of what a CRDT does.

**Learned.** My claim that each commit rewrites a spine was wrong: with
the hitchhiker buffer a small commit writes the root and its buffer
(`notes/novelty-buffer.md`); the single-file note's wording predates it.
Doc as value was shelved for the core changes it needed as much as for
cost, and shelved, not refuted. Partial replication holds per database,
not per document: a peer may fetch a whole document first, which removes
the reason ordered relations accepted interleaving. Evaluators were
building collaborative editors, so text was the dealbreaker; that claim
became a fact.

**Decided.** Six columns (direction.md). Two research topics: a survey of
text CRDT libraries and papers on the matrix's rows (the list is in
research/text-crdt-survey.md); and
the write-cost measurement against the 2× bar. Not decided: the
direction; it waits on both.

## 2026-11-06, Directing: what a built-in would reuse (by voice)

**Asked.** Which pieces of Dialog would a native text CRDT reuse? Not
known; finding out is part of the work. Dialog stores changes, reconciles
them, has compact encodings and efficient lookups, so pieces are likely
reusable. Precedent: for encryption, a smaller primitive built on beat
integrating a whole external system.

**Learned.** The built-in column rests on a hypothesis and an analogy.
The candidate pieces are named at the level of capabilities, not code.

**Decided.** Not decided. The hypothesis stays open until each candidate
is tied to the Dialog code a text CRDT would call. No product names for
the precedent.

**Asked (later).** Why did the smaller primitive win for encryption?
Fewer moving parts: keys need to sit next to the data and sync
efficiently, and putting them in the tree that is already replicated
makes that simpler.

**Learned.** That reason is a criterion, not just a preference for
building: count the moving parts each column adds. The "sync engines"
row became "moving parts". It favours the columns that keep text in the
replicated tree, which are the ones whose write cost is unmeasured. Keys
change rarely; text changes per keystroke, so the precedent transfers
only if that cost is within the bar.

**Asked (later).** If text in the tree comes back over 2×, switch or
engineer it down? Neither matters: in or outside the tree is not a
preference. The goal is a solution that is easy to maintain and
integrates well; blobs outside the tree are a better solution for
binary data, and the same may be true for text.

**Learned.** The encryption precedent was not "in the tree", it was
"fewer moving parts"; blobs show one sync mechanism can carry data
outside the tree. The row is now "sync mechanisms the developer deals
with", which no longer favours in-tree columns. "Easy to maintain" and
"integrates well" are judgements, not criteria yet.

**Decided.** Location of text follows the cost measurement: over 2× in
the tree is a reason to go outside it, not to engineer the tree.

**Asked (later).** What does "integrates well" mean? First concrete
piece: as with blobs, sync must know which documents need syncing.

**Learned.** The blob design does this with an index inside the tree
while the bytes stay outside. Copying it for text keeps text outside the
tree but puts a per-document version inside it, so some write per change
returns, small, and its frequency is a design choice.

**Corrected (later).** Blobs and the blob index are deprecated; assets
replaced them. An asset is recognized by an ordinary fact
(`dialog.asset/size`) in the revision that adds it, and push ships its
bytes with that revision. The analogy for text is a version fact per
document, not an index. The difference stands: an asset's fact is
written once, a document's version fact is rewritten as it changes.

## 2026-11-07, Directing: following the leads

**Asked.** The maintainer asked for the leads to be followed and the
tradeoffs evaluated.

**Learned.**

- The doc-as-value spike is closed PR #388 (closed 2026-08-10, "we have
  reconsidered approach"). Its spec stores the document as one record
  value, merges siblings at read time inside the existing winner
  selection, and lets the next edit's replace converge storage. It
  claims no storage, sync or on-disk changes; it commits per edit
  session, and says plainly it does not compete with a library's delta
  sync for live editing. The PR touched 88 files.
- Commit cost is documented: six keys and a signed record per one-fact
  commit, ~120 to 160 KB written to disk per small commit. On the
  concurrent traces that puts per-keystroke commits in gigabytes. This
  is a napkin hypothesis, but the gap is large enough that the cost row
  is decided by commit frequency and the storage work under way, not by
  which column.
- "Last writer wins loses an edit" is imprecise: both values survive
  sync in the log; a read shows one; the loss happens when a later write
  that saw both supersedes them. The spike used exactly that window.
- Automerge's change chunk puts dependency hashes before the operations
  and its hash is computed from the bytes, so a store that does not run
  Automerge can serve "changes since these heads". Sedimentree is prior
  art for syncing such a graph through a content-blind server, chunked
  so per-change blocks do not pile up. automerge-repo's own storage is
  incremental chunks keyed by change hash, compacted into snapshots keyed
  by heads.
- Version-vector libraries need the library wherever "changes since" is
  computed, so "resource synced by Dialog" is not library agnostic.

**Contradiction raised, not resolved.** The maintainer shelved the spike
for "the amount of changes required to the core". Its spec says storage,
sync and format were untouched. Both can be true only if "core" means
the value type and query engine. Ask.

**Decided.** Not decided. The matrix now says the cost row is about
commit frequency, which answers the earlier question (version fact per
edit or per sync point) in favour of sync points, pending the
measurement.

## 2026-11-09, Directing: does the search tree give us the primitives?

**Asked.** The maintainer read the two DeltaDB posts and thinks the
dialog search tree may provide primitives collaborative text needs.

**Learned.** Split DeltaDB into what it replicates and what it keeps local:

```
replicated                         local, per replica
┌──────────────────────────┐       ┌─────────────────────────────┐
│ delta graph              │       │ fragment index              │
│ delta = parent, offset,  │──────▶│ positional B-tree: position │
│         text             │ replay│ at a version → fragment     │
│ ancestry apart from text │       │ copy-on-write, shared nodes │
└──────────────────────────┘       └─────────────────────────────┘
```

Primitive by primitive:

- **Structural sharing between versions**: Dialog has it.
- **Ancestry walks, logarithmic search for a common ancestor**: Dialog has
  it (skip links, tree differential).
- **Storing a delta graph keyed by writer and sequence**: the history
  region's shape; a fit, untested.
- **A positional index**: Dialog does not have it. The search tree is
  ordered by sorted key; text order is not a key. DeltaDB never replicates
  its index, so it does not need one that is content addressed.
- **Per-keystroke deltas**: DeltaDB's are not commits. Dialog's commit
  cost is per commit (six keys, a signed record), so the fit depends on
  deltas entering the tree without a revision each.
- **Frontier over version vector**: DeltaDB rejected the growing map that
  Dialog's watermark is. A tension to note, not to fix here.

**Decided.** Not decided. The hypothesis splits: the tree fits the
replicated half; the positional half is new work, local, and need not be
in the tree.

## 2026-11-09, Directing: why the doc-as-value spike was reconsidered

**Asked.** The maintainer gave the reason not captured before: merging
two edits already keeps every change from both, so storing pre-merged
copies is waste; a document resource with a pointer into it avoids it;
like assets, but mutable, though only accreting.

**Learned.** This is the "resource synced by Dialog" column, arrived at
from the other side, and it matches the change-graph finding from the
survey. Pushback: the copies are a cache, not only waste; one value is
readable alone, while a pointer needs the whole change graph (or a
snapshot) on the reading peer. Accreting also means deleted text is kept
forever.

**Decided.** The column is now "accreting resource plus heads pointer".
Next: numbers (text-write-cost), starting with this column against doc
as value.

## 2026-11-09, Directing: from questions to hypotheses

**Asked.** The maintainer wants a library that fits what Dialog offers,
ideas transplanted where they help, synergy with DeltaDB. Two new ideas:
a tree region replicated whole while the rest is partial, or text
changes in the history region. And: the agent should bring hypotheses to
test, not ask for each idea to be justified.

**Learned.** Fair. The method asks the agent to add columns once the
maintainer has given his; I kept asking instead. Writing the hypotheses
out corrected one of my own claims: version-vector libraries fit if
Dialog's differential does the sync (H2).

**Decided.** Hypotheses H0 to H6 and experiments E1 to E4. E1 first,
with no library.

## 2026-11-09, Directing: first numbers

**Asked.** Run the experiments, in parallel.

**Learned.** Three runs (results under research/text-write-cost/results).
Changes as facts costs 56× the raw trace before any per-commit cost and
is out. A text region is cheap per entry (1.8× raw) but pays ~10 KB per
flush, so only pause-length flushes come near. Libraries store a snapshot
5 to 7× smaller than the raw trace, which makes "within 2× of a library"
ambiguous: against snapshots nothing in Dialog passes; against the
per-change stream Dialog would sync, a region at a pause interval does.
H2 confirmed for three libraries; Yrs is cheapest per change and the
only one that rebuilds from changes under 100 ms. H3 refuted on key
order; H4 confirmed.

**Decided.** Not decided. The bar's reading decides more than any
remaining run.

## 2026-11-09, Directing: the agent's read

**Asked.** What is the read after the research, and the recommendation?

**Learned.** The agent's recommendation, an opinion until H0 for the
region, H7 and H8 are measured:

```
keystroke ──▶ library (merge engine) ──▶ ephemeral channel ──▶ peers (live)
                     │
                     └─ at pauses ──▶ text region  tag ‖ doc ‖ origin ‖ seq
                                         │  tree differential = sync
                                         └─ compaction into chunks (H7)
open ──▶ Download::within(doc range) ──▶ rebuild positional index locally
```

Out: changes as facts, doc as value, history region, a template with two
syncs. Undecided: region versus accreting resource (H5 unmeasured; the
region wins on moving parts unless H5 is much cheaper); Yrs versus
Diamond Types at main (pending).

**Decided.** Not decided; the maintainer decides.

## 2026-11-09, Directing: layers, rich text, automerge-repo

**Asked.** Build a prototype next. The realtime channel is the right
framing, and the layer model's broadcast layer points the same way:
reconcile the two. Rich text will be needed eventually; could that tilt
Yrs versus Diamond Types? Transplanting ideas pays only if we gain
something, not obvious today. Learn from automerge-repo.

**Learned.** layers.md. The layer model turns the text region into a
question: a batch could be a fact (H9), which removes the format gate
and merge changes, at the replicated commit floor. Each keystroke as a
memory-layer commit (H10) is the other cost to check. Rich text tilts to
Yrs. automerge-repo's lessons are the document handle, a per-document
share policy, per-document broadcast and per-connection sync signals.

**Decided.** Yrs is the default engine. H9 measured before the prototype
picks region or fact.
