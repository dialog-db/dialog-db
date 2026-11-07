# Claims: collaborative text editing

| claim | kind | backing | status |
|---|---|---|---|
| builders ask for collaborative text editing | fact | three apps: a Tonk document editor, an Obsidian-style notebook, an Observable-style playground (journal, 2026-11-06) | holds |
| more developers evaluated Dialog and passed on it for lack of a text answer, picking a text CRDT library | opinion | maintainer, 2026-11-06; whether text was their stated dealbreaker, and whether text was core or one feature, is not known | - |
| some developers would use a text CRDT library together with Dialog if we said how | opinion | maintainer, 2026-11-06; no specific request cited yet | - |
| concurrent writes to one text field resolve by last writer wins, losing one edit | fact | maintainer, 2026-11-06; ? link the code or a test | holds |
| the conflict unit is one field; splitting text into per-paragraph fields limits the loss to one paragraph | fact | maintainer's playground design, 2026-11-06 | holds |
| "dropped" (editor) and "overridden" (playground) are the same mechanism | hypothesis | confirmed if the editor also stores text in fields under last writer wins | open |
| every commit supersedes a spine of search tree nodes and nothing reclaims the old ones | fact | `research/single-file-format.md`: replay ~9× live size; a Tonk replica ~2 GB after ~1000 commits | holds |
| a timer updating every tick grew a replica to gigabytes in a day or two | fact | maintainer, 2026-11-06, observed; ? link the case | holds |
| a CRDT document stored as a value is copied whole on every edit | fact | by construction | holds |
| storing a CRDT document as a value is too costly to be viable | opinion | maintainer, 2026-11-06, napkin math; worth measuring (maintainer) | - |
| a built-in text CRDT writing each keystroke as facts pays the spine cost per keystroke too | hypothesis | follows if each edit is a commit; refuted if edits batch into fewer commits | open |
| batching edits (per pause, per second) cuts write amplification enough to matter | hypothesis | measure growth per hour of typing with and without batching; bar not set | open |
| key plus side lane is worth testing | opinion | maintainer, 2026-11-06; untested because the engineering is large and mostly outside Dialog's domain | - |
| fractional-index ordered relations interleave concurrent runs, so are wrong for text | fact | `notes/ordered-relations.md`, "Out of scope / accepted tradeoffs" | holds |
