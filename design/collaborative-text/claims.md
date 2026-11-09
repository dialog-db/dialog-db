# Claims: collaborative text editing

| claim | kind | backing | status |
|---|---|---|---|
| builders ask for collaborative text editing | fact | three apps: a Tonk document editor, an Obsidian-style notebook, an Observable-style playground (journal, 2026-11-06) | holds |
| more developers evaluated Dialog and passed for lack of a text answer, picking a text CRDT library | fact | maintainer, 2026-11-06: they were building collaborative editors, so text was the whole product | holds |
| some developers would use a text CRDT library together with Dialog if we said how | opinion | maintainer, 2026-11-06; no specific request cited yet | - |
| concurrent writes to one text field resolve by last writer wins, losing one edit | fact | maintainer, 2026-11-06 | refined below: nothing is discarded at sync; the loss happens at the next write |
| concurrent values for one entity and attribute both survive in the log; reads show a deterministic winner; a later write that has seen both supersedes both | fact | `notes/version-control.md`, pulling from several remotes and conflict detection | holds |
| the conflict unit is one field; splitting text into per-paragraph fields limits the loss to one paragraph | fact | maintainer's playground design, 2026-11-06 | holds |
| "dropped" (editor) and "overridden" (playground) are the same mechanism | hypothesis | confirmed if the editor also stores text in fields under last writer wins | open |
| a small commit writes a new root with its buffer, not a whole path; deeper nodes are rewritten only when a buffer overflows | fact | `notes/novelty-buffer.md`, maintainer 2026-11-06 | holds |
| every commit supersedes a spine of nodes | fact | `research/single-file-format.md` | refuted as worded: corrected by the hitchhiker buffer above; superseded nodes are still never reclaimed |
| superseded nodes are never reclaimed, so a replica grows with commits | fact | `research/single-file-format.md`: a Tonk replica ~2 GB after ~1000 commits | holds |
| a timer updating every tick grew a replica to gigabytes in a day or two | fact | maintainer, 2026-11-06, observed; ? link the case | holds |
| a CRDT document stored as a value is copied whole on every change | fact | by construction | holds |
| doc as value was shelved for the changes it needed in the core and its cost, to explore alternatives; not rejected | fact | maintainer, 2026-11-06 | holds |
| doc as value is too costly to be viable | opinion | maintainer, 2026-11-06, napkin math; to be measured with the others | - |
| a per-edit commit, through any column that routes changes through facts, lands within 2× of a text CRDT library | hypothesis | research/text-write-cost.md; refuted beyond 2× on disk or sync bytes | open |
| batching edits (per pause, per second) cuts the cost enough to matter | hypothesis | same measurement, with and without batching | open |
| "changes since a version" can be served by a remote that holds an opaque append-only log and does not understand the CRDT | hypothesis | research/text-crdt-survey.md: how each library computes and applies changes since a version | open |
| Dialog already does enough of what a text CRDT does that a native one could reuse it | hypothesis | maintainer, 2026-11-06, speculation by his own account; candidates: storing changes, reconciling changes, compact encodings, lookups; confirmed per piece by naming the Dialog code a text CRDT would call | open |
| a precedent: for encryption, building on a smaller existing primitive beat bringing a whole external system into Dialog, because it had fewer moving parts: keys sit next to the data in the tree that is already replicated | opinion | maintainer, 2026-11-06; an analogy, not evidence about text | - |
| the precedent transfers to text only if text changes in the tree stay cheap: keys change rarely, text changes per keystroke | hypothesis | Claude, 2026-11-06; settled by research/text-write-cost.md | open |
| a database is always partial, but a peer may hold a whole document before editing it | fact | maintainer, 2026-11-06 | holds |
| with a whole document per peer, a built-in can order text without fractional indexing's interleaving | hypothesis | follows from the line above and `notes/ordered-relations.md`; checked when the built-in column gets a design | open |
| fractional-index ordered relations interleave concurrent runs; acceptable for lists, not text | fact | `notes/ordered-relations.md`, maintainer 2026-11-06 | holds |
| supporting one library well is enough; library agnostic is nice to have | opinion | maintainer, 2026-11-06 | - |
| a template with a separate sync engine is a legitimate option | opinion | maintainer, 2026-11-06; preferable to avoid two sync engines | - |
| for change-graph libraries (immutable changes naming their parents), a store that does not run the library can serve "changes since these heads" from ids and parent links alone | hypothesis | research/text-crdt-survey/journal.md | confirmed for Automerge by its format spec; Diamond Types unchecked |
| version-vector libraries need the library on whoever computes "changes since" | fact | Yjs diff and merge are Yjs functions (survey.md [1]) | holds |
| a resource synced by Dialog could be a graph of immutable change blocks with a head pointer, the way a memory cell points at a branch revision, so it stays content addressed | hypothesis | Claude, 2026-11-06, from the line above; a column variant to examine, not a recommendation | open |
| whether text lives in the tree or outside it does not matter; what matters is easy to maintain and integrates well | opinion | maintainer, 2026-11-06; assets, bytes kept outside the tree but shipped by the same push, are the precedent | - |
| blobs and their blob index are deprecated; assets replaced them | fact | maintainer, 2026-11-06; `notes/blob-replication.md` describes the older design | holds |
| an asset is an entity `asset:<hash>`; asserting it writes the bytes to the blob store and records `asset:<hash> dialog.asset/size <size>` in the same revision; push ships the bytes of new assets with the revision that records them | fact | `rust/dialog-repository/src/repository/branch/asset.rs` module docs; `guide/src/sync.md`, push step 3 | holds |
| a document could be recognized the same way: a fact recording its current version, so the revision that records a new version tells push which document changes to ship | hypothesis | Claude, 2026-11-06, by analogy; differs because an asset never changes (one fact for its life) and a document does (the version fact is rewritten on change) | open |
| a commit of a couple of facts writes ~120 to 160 KB to disk today | fact | `research/single-file-format.md` ("root-frame re-encode") | holds |
| a one-fact commit touches six keys (three data orderings, a claim record, the revision record's two orderings) plus a signed record; costs are per commit, so batching amortizes them | fact | `notes/version-control.md`, write amplification and commit cost | holds |
| claim records keep value bytes in the log indefinitely | fact | `notes/version-control.md`, content erasure | holds |
| values above the inline threshold spill to content-addressed blocks outside tree nodes, which push ships beside the novel nodes | fact | `rust/dialog-artifacts/src/spill.rs` | holds |
| per-keystroke commits on the concurrent editing traces (~26k edits, ~21 KB final text) would write gigabytes; per-second batching still hundreds of megabytes | hypothesis | napkin: edits × ~140 KB; settled by research/text-write-cost.md | open |
| the doc-as-value spike committed snapshots per edit session, not per keystroke, and said it does not compete with a library's delta sync for live editing | fact | closed PR #388, `notes/automerge-integration-rationale.md` step 15 on that branch | holds |
| the doc-as-value spike changed no storage, sync or on-disk format, only the value type, winner selection and an edge crate | fact | closed PR #388 spec, "the shape of the result"; the PR itself touched 88 files, much of it query engine work | holds |
| Automerge change chunks store dependency hashes before the operations, and a change's hash is computed from its bytes, so both are readable without decoding operations | fact | Automerge binary format spec, change chunk and change hash (survey.md) | holds |
| a content-blind store can sync a change graph using only hashes and parent links, chunked by hash boundaries | fact | Sedimentree design (survey.md) | holds |
| Dialog's search tree provides primitives a text CRDT needs | hypothesis | maintainer, 2026-11-09, after the DeltaDB posts; examined per primitive in the 2026-11-09 journal entry | open |
| DeltaDB replicates deltas, not its tree: each replica keeps its own positional fragment index | fact | delta.dev posts (survey.md [18]) | holds |
| a text position index needs order by document position (a positional, summed tree); the dialog search tree orders by sorted key and keeps no positional summaries | fact | `rust/dialog-search-tree` (`PersistentTree<Key, Value>`), no count or rank summaries | holds |
| document order can become a sort key only through fractional positions (interleaving) or CRDT tree paths (keys that grow with nesting) | hypothesis | `notes/ordered-relations.md`; Fugue paper; confirmed by finding a third encoding or ruling one out | open |
| the delta graph itself fits the search tree: deltas keyed by (origin, sequence), parents beside them, as the history region keys revisions | hypothesis | `notes/version-control.md`, one tree for data and history; costed by text-write-cost | open |
| what Dialog already has that DeltaDB built: persistent structurally shared trees, ancestry walks with logarithmic skip links, diff-based sync of only what is missing | fact | `notes/version-control.md` (skip links, tree differential); delta.dev posts | holds |
| a delta does not have to be a commit; DeltaDB persists per keystroke without a signed revision per delta | fact | delta.dev posts | holds |
| Dialog's watermark is a per-origin map that never shrinks, the cost DeltaDB rejected version vectors for | fact | `notes/version-control.md`, "Where the watermark lives" | holds |
