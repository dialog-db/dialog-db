# Notation: evaluation

How a spelling is measured. A reader is the least capable model we can get, given a sketch and a task, with nothing else to go on. What it writes and predicts is scored against what the notation means. Results go in the [journal](journal.md) as rounds come in; each round's prompts, tally and answers are in [`results/`](results).

## What is compared

Every round compares variants of one form against each other, with everything else in the prompt the same. Where a round has a baseline, it is the spec as the PR first had it: R1's `default` line, R4's `(assert head …)`, R7's plain head, R15's prompt with no rule.

## Readers

- **Model:** `haiku`, one subagent per reader, all of a round's readers started at once.
- **Agent:** a general agent up to R13; from R14 a read-only agent (Read, Glob, Grep, web), after readers in R2 to R13 called tools in the session and on the user's linked computer (see [hygiene](#hygiene)).
- **Runs:** three readers per variant at least, four or five where the round turned on one variant. A difference of one reader in three is noise.
- **Blind:** the first answer is scored. In R1 readers were told the intent afterwards and asked what spelling would have shown it; those answers are kept apart and not scored.

## Axes

| axis | workload | reports | status |
|---|---|---|---|
| **Construction** | save rows that have no entity (R7, R8, R10 to R15) | readers who leave the entity out, rather than invent `bug:01` | run |
| **Derivation** | say which entity the first write lands on and why (R1, R5, R6, R9, R12 to R15) | readers who say it is derived, and from which fields | run |
| **Convergence** | the same row arrives from a device that has not synced (R6 to R8, R10, R14, R15) | readers who predict one entity | run |
| **Update and query** | change a bug by its entity; find one by its fields (R12, R14, R15) | right call, right query | run: R12 12/12, R14 8/8 (two wrote no `!`), R15 19/19 |
| **Lifetime** | what is left of a command's facts a tick later (R3, R6) | readers who say they are gone | run, 0/9 and 2/3; the `layer: :now` design is not yet run |
| **Timing** | which rules are computed, stored, and when they fire (R2, R4, R10) | readers who can say | run; the revised `rule` / `when` is not yet run |
| **Sending** | send a command to a peer; what the peer sees | | not run |
| **Documentation** | the same tasks after four ways of writing the spec (R16) | points of 15 per reader, per item per variant | planned: [r16-plan](results/r16-plan.md) |
| **Clarity** | 1 to 5, the most confusing thing, and a spelling that would fix it | the rating, and what readers propose | run every round |

Behavior and understanding are scored apart from R12 on: a reader who leaves the entity out because none was given has not read a derivation.

## Scoring

Each results file states the criterion its tally uses. A hedged answer ("probably one, if …") counts only when the round says so, and the tally reports hedges beside the count.

## Hygiene

A reader is only as blind as its context. Readers through R13 ran as general agents, which carry the session's tool instructions and its tools. Ten of 249 called a tool they were told not to:

| round | reader | tool |
|---|---|---|
| R2 | shorthand 1, 3 | `current_time` |
| R8 | U-eq 3 | `current_time` |
| R8 | M-inf 3 | a hand-back through the user's linked computer |
| R9 | map 2, hash 1 | `send_later` (no message was given) |
| R10 | D-kset 2 | `end_conversation` |
| R11 | vec2 2 | `send_later` |
| R13 | listcolon 2 | `get_device_info` on the user's linked computer |
| R13 | mapglue 3 | `send_later` |

Two R9 readers wrote in the style of a Tonk guide that was in the session's tool instructions, not in their prompt. One R15 reader found unrelated instructions appended to its file read and ignored them. Their answers are kept and scored like the others; none of them changes a tally's decision.

From R14 on, readers run as a read-only agent and are told the task is reading comprehension. No reader since has called anything but Read.
