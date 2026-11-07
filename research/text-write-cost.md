# Text write cost

Status: **Proposed**. Answers a question from the design topic
[collaborative text editing](../design/collaborative-text.md).

## Idea

**Question.** What does committing text edits to Dialog cost on disk and
in sync bytes, compared with a text CRDT library replaying the same edit
history?

**Hypothesis.** Unknown in direction. Each commit writes at least a root
node with its buffer (`notes/novelty-buffer.md`) and nothing is
reclaimed, so per-keystroke commits may exceed the bar; batching may
bring them under it.

**Measured against.** A text CRDT library replaying the same trace, its
document saved on disk and its updates as sent between peers.

**Dataset.** Published editing traces, single author and concurrent
(the traces text CRDT libraries benchmark against). Named in every
result by file hash; not committed.

**Variants.** Doc as value; changes as facts, one commit per edit;
changes as facts, batched (per pause, per second).

**Success.** Within 2× of the library on both disk and sync bytes, and
every replica converges to the trace's final text.

**Failure.** Beyond 2× on either, or any replica diverges.

**The middle.** Within 2× on one and not the other: the maintainer judges.

## Prior, before measuring

Napkin, to be confirmed or refuted by the run: a commit of a couple of
facts writes ~120 to 160 KB to disk today (`research/single-file-format.md`).
The concurrent traces have ~26k edits for ~21 KB of final text, so one
commit per edit is in gigabytes, and one per second still in hundreds of
megabytes. If that holds, the question this topic answers shifts from
"which column" to "how often can text commit", and the single-file
format and a durable novelty buffer become dependencies of any in-tree
column.
