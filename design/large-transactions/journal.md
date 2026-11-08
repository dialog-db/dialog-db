# Journal: large transactions

## Reflective inquiry

|  | Understanding | Activity |
|---|---|---|
| **Status** | What do we know? A replay of large commits writes about 1,440 node versions a commit, 54 times the tree it leaves; buffers flush past 256 ops or 64 KiB; within a commit a node is lifted and encoded once; a commit persists nothing until its stream ends. One commit of all 4.47 M facts did not fit in 15 GiB. Sealing replays leaf-bound ops one canonical edit each, except into an empty tree, which is planted bottom-up; a second commit of 447,000 facts ran over two hours in those edits. | Where are we at? **Describing**, with evidence for diagnosis gathering. |
| **Agenda** | What do we need to know? Who writes large transactions and what they are trying to accomplish; how a commit's cost grows with its size against the buffers; what one large commit holds in memory. | Where are we going? **Diagnosing**: the open hypotheses in the ledger, measured on the MusicBrainz facts (1, 10, 100 and 2,000 commits; a one-commit import's memory). |

## 2026-11-08, Describing: the replay that wrote 54 times its tree

**Asked.** Why does ingesting MusicBrainz take over an hour? Irakli: is that
the history emulation; what would one transaction cost; a single transaction
should not hold everything in memory. Then: the tree should not reach leaves
on every commit, because of the novelty buffers; there is an importer; when
a flush reaches a node, is it transient or persisted?

**Learned.** The buffers do absorb small commits; a MusicBrainz commit is
many times their capacity, so its flushes cascade to the leaves. Within a
commit the flushed-into child is transient and edited in place, encoded
once at persist: no intermediate node is realized. The waste is across
commits, each persisting every node it touched. The agent's first account
(every touched leaf rewrites its path) ignored the buffers, and its claim
that a bulk-built tree would hash differently from an incremental one is
wrong for a canonicalized commit. Both corrected in the ledger.

**Decided.** Not decided. Three remedies were raised and are recorded as
opinions, not approaches: a side file for a transaction's nodes, copied over
at commit (Irakli); a bottom-up build from sorted runs (the agent); buffers
that scale with the fan-out below them (the agent). They are features; the
problem they answer is not stated yet.

**Open question for the next sitting.** Who writes a transaction this
large, and what are they trying to accomplish: importing a dataset once,
syncing a peer's history, a migration rewriting a tree, an application's
own batch? The answer decides whether the problem is imports or large
commits in general.

## 2026-11-08, Describing: what large commits cost, measured

**Asked.** What would the MusicBrainz facts cost as one commit, and as 10,
100 and 2,000? (Irakli: go ahead.)

**Learned.** One commit of all 4.47 M facts was killed by the kernel at 13.6
GiB, the commit holding over 8 GiB. As 10 commits, the first (into an empty
tree) finished, through the bottom-up `plant`; the second ran for over two
hours, every stack sample in the per-op canonical edit that sealing uses to
apply the ops the buffers sent toward the leaves. So there are at least two
costs, not one: the memory a commit holds until it seals, and an apply that
looks quadratic in how many ops a commit lands per leaf. The agent's
proposed bottom-up build exists already, for an empty tree only.

**Decided.** Not decided. The 100- and 2,000-commit runs continue; the
quadratic cost is a hypothesis in the ledger with its test.

**Open question for the next sitting.** Still the one before: who writes
transactions this large, and to accomplish what.
