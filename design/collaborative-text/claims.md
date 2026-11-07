# Claims: collaborative text editing

| claim | kind | backing | status |
|---|---|---|---|
| builders ask for collaborative text editing | fact | three apps: a Tonk document editor, an Obsidian-style notebook, an Observable-style playground (journal, 2026-11-06) | holds |
| more developers evaluated Dialog and passed for lack of a text answer, picking a text CRDT library | fact | maintainer, 2026-11-06: they were building collaborative editors, so text was the whole product | holds |
| some developers would use a text CRDT library together with Dialog if we said how | opinion | maintainer, 2026-11-06; no specific request cited yet | - |
| concurrent writes to one text field resolve by last writer wins, losing one edit | fact | maintainer, 2026-11-06; ? link the code or a test | holds |
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
| a precedent: for encryption, building on a smaller existing primitive beat bringing a whole external system into Dialog | opinion | maintainer, 2026-11-06; an analogy, not evidence about text | - |
| a database is always partial, but a peer may hold a whole document before editing it | fact | maintainer, 2026-11-06 | holds |
| with a whole document per peer, a built-in can order text without fractional indexing's interleaving | hypothesis | follows from the line above and `notes/ordered-relations.md`; checked when the built-in column gets a design | open |
| fractional-index ordered relations interleave concurrent runs; acceptable for lists, not text | fact | `notes/ordered-relations.md`, maintainer 2026-11-06 | holds |
| supporting one library well is enough; library agnostic is nice to have | opinion | maintainer, 2026-11-06 | - |
| a template with a separate sync engine is a legitimate option | opinion | maintainer, 2026-11-06; preferable to avoid two sync engines | - |
| for change-graph libraries (immutable changes naming their parents), a store that does not run the library can serve "changes since these heads" from ids and parent links alone | hypothesis | research/text-crdt-survey/journal.md; refuted if parent links are not readable without the library | open |
| version-vector libraries need the library on whoever computes "changes since" | fact | Yjs diff and merge are Yjs functions (survey.md [1]) | holds |
| a resource synced by Dialog could be a graph of immutable change blocks with a head pointer, the way a memory cell points at a branch revision, so it stays content addressed | hypothesis | Claude, 2026-11-06, from the line above; a column variant to examine, not a recommendation | open |
