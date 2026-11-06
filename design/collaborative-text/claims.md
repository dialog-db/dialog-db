# Claims: collaborative text editing

| claim | kind | backing | status |
|---|---|---|---|
| builders ask for collaborative text editing | fact | three apps: a Tonk document editor, an Obsidian-style notebook, an Observable-style playground (journal, 2026-11-06) | holds |
| Dialog has no good solution for collaborative text today | opinion | maintainer, 2026-11-06; compared to what is not stated | - |
| concurrent edits to the same text lose one side | hypothesis | reported by two apps; confirmed by a test where two replicas edit one text and sync, refuted if both edits survive | open |
| "dropped" (editor) and "overridden" (playground) are the same mechanism | hypothesis | confirmed if both store text the same way and lose to the same rule | open |
| the notebook builder sees Automerge as a way out | opinion | notebook builder, via maintainer, 2026-11-06 | - |
| fractional-index ordered relations interleave concurrent runs, so are wrong for text | fact | `notes/ordered-relations.md`, "Out of scope / accepted tradeoffs" | holds |
