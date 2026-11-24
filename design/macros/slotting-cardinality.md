# Where the cardinality work goes, so tonk's rules are moved once

Asked by the maintainer on 2026-11-20. Read the same day by a thread of its own from #827 (`claude/pick-strategy-rules-crdt-tvdpm7`), tonk's draft tonk-labs/tonk#1329 and the two cardinality sessions; nothing was run.

## What #827 is

| | before | after |
|---|---|---|
| an attribute | a relation with a pick (`last`, `all`, a ranking) | one relation, a type and a `cardinality`, `one` by default or `many`. A ranking pick, `"pick":"x:"` or a list of relations under `the` is refused by name |
| a ranked read | a pick on the attribute | a concept field's `pick`: `last`, `all`, `max`, `min`, `top`. The field's cardinality follows from it |
| rules | `top` an aggregator, `among` | both gone; an authored `reduce` is still refused |
| stored | | the tree unchanged; every attribute, concept and rule identity moves; rule bodies are format 4. Old rules are inert, listed under `dialog.rule/invalid`, until `Branch::upgrade_rules` |
| Rust | `Pick` | `Cardinality`; `#[cardinality(..)]`, `#[dialog(pick = ..)]` |

It is ready, has main merged in up to #838, and merges into main with no conflict.

## What it asks of each thing

| | what moving past #827 takes |
|---|---|
| tonk on YAML, as tonk#1329 does it (draft, 242 files, its pin stale) | 147 YAML files, about 290 lines of `pick: :all` to `cardinality: :many`; three ranked fields rewritten to `pick: :top` with their rules concluding into new concepts; tonk-notation and tonk-analyzer; tonk-schema with an `upgrade.rs`; the CLI, the worker; stored spaces through `upgrade_rules` |
| the notation's design (#728) | nothing: it already says `cardinality:` on a line and electors only in a concept's head |
| the notation's code | 🟡 it is on the old model. `expand.rs` 254-264 turns `cardinality:` back into picks, `kernel.rs` 22 has no `top`, `compile.rs` 854-855 writes `"pick": "<pick>:"`, which #827 refuses, and nothing lowers an elector in a head |
| the patterns (#778) | five conflicts, one of meaning, and seven uses of `Pick` that merge and do not compile |
| the composite (`feat/composite-value`) | the changelog, and one `Pick::All` |

🔴 Two things are called rule format 4: #827's bodies, and #799's, a stored rule that holds a pattern. One of them has to give.

## The orders

| order | tonk's rules are moved | what is ported onto what | what it risks |
|---|---|---|---|
| a. #827 now; tonk's main keeps its pin until the notation | once, into the notation | the composite, the patterns and the notation onto #827 | tonk stays on rc.7 unless a pre-release is cut on main before #827 |
| b. #827 now; tonk moves its YAML at once | twice | as a | the library's work done and thrown away |
| c. #827 waits for the notation | once | #827 merged with main again and again, seven times so far | the notation has to be ported onto it whenever it lands |
| d. #827 now with old rules still read | | | there is no such path |

Tonk's main pins `v0.5.0-rc.7`. This week's cost work (#825, #838 to #841) is on main after it and before #827. A pre-release cut on main at `433f106`, before #827 is merged, gives tonk all of it with no rule touched; after #827 is merged every revision of main holds it.

## Recommended, the agent's

```
 1  cut a pre-release on main at 433f106       tonk's main may take it: the cost work, no rules moved
 2  merge #827
 3  port onto it, in this order:  the composite (one line)  ->  the patterns  ->  the notation
 4  tonk moves once: its YAML into the notation, on #827's model, with tonk#1329's Rust and its upgrade of stored spaces
```

Of tonk#1329, the Rust is kept and the 147 YAML files are not: they are written again in the notation at step 4.

For the maintainer: which of the two is format 4; and, left open in the cardinality thread, whether a listed `as` with no pick keeps meaning `top`, as on 0.4.

## 2026-11-20, decided by the maintainer

- **Formats.** #827's rule bodies are format 4. A stored rule that holds a pattern, #799's, is format 5.
- **A listed `as` implies nothing.** `as: [:todo :done]` with no pick does not mean `top`. A ranked read is written, `(top ?state)` in the concept's head or perhaps `is: (top ?state)`, and it no longer means that a write takes the other winner away. With `cardinality: :one` a write does retract the last and assert the new, and the last is the last written, not the top.
- **Not decided:** whether `cardinality: :one` and `top` may stand together at all. #827 as it is refuses `cardinality: one` beside a ranking pick.

So #827 reads such a field as he means it, the newest claim. Its upgrade rewriting a 0.4 field of this kind to `top` keeps what old data meant and is not a rule for what is written now.

Also from him, same day: #844 need not be in 0.5; and he is landing #827 next.
