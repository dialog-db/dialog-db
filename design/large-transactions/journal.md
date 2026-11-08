# Journal: bringing a large dataset into a replica

## Reflective inquiry

|  | Understanding | Activity |
|---|---|---|
| **Status** | What do we know? Large commits fail two ways: one commit holds every new node until it seals (over 8 GiB for 4.47 M facts), and sealing into a non-empty tree applies its leaf-bound ops one canonical edit each (over two hours for 447,000 facts); small commits rewrite the same nodes again and again (37 versions a node over 2,000 commits). Commits of about 45,000 facts were fastest. The tree is 77,808 nodes in three levels. | Where are we at? **Delimiting**: a problem statement is proposed, not yet confirmed. |
| **Agenda** | What do we need to know? Whether the objective in the proposed statement is Irakli's (bringing a dataset in: an import, or a replica's current state into a new file), and whether there is a second one; whether the per-op seal is quadratic (ledger). | Where are we going? Confirm or rewrite the problem statement, then **Directing**: use cases, then the strategy matrix with the side file, the bottom-up build and the batched seal as columns. |

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

## 2026-11-08, Dropped: no one writes them

**Asked.** Who writes transactions this large, and to accomplish what?
Irakli: no one, as far as we know; the question was how large the database
would be without much history, and large commits came up on the way.

**Learned.** The 100-commit and 2,000-commit replays' timers put the tree's
seal at about a third of a 2,000-commit replay and the archive at the rest;
the per-op seal is what makes very large commits fail.

**Decided.** Dropped: there is no unmet objective to state a problem for.
The findings stay in the ledger; the topic reopens when an importer, a
migration or a sync path writes commits of tens of thousands of facts.

## 2026-11-08, Delimiting: reopened, diagnosed, a problem proposed

**Asked.** Irakli: large transactions are a topic of their own; reopen it
on its own branch and fill in the problem statement. Earlier the same day:
no one is known to write transactions this large, and a tool offering just
a replica's state would be useful.

**Backtracked.** From Dropped to Describing: the topic was dropped for
having no one with the objective, and Irakli reopened it. The two
statements, "no one writes them" and "worth a topic of its own", are
reconciled by an objective that is ahead of us rather than behind: bringing
a dataset that already exists into a replica. Irakli's state-snapshot tool
is one such load if it writes the whole current tree as a commit (a
hypothesis in the ledger: it need not, if it copies nodes).

**Learned (Diagnosing).** Three causes, each with its evidence in the
ledger:

1. A commit holds every new node until it seals: the code says so, and one
   commit of all 4.47 M facts was killed at 13.6 GiB.
2. Sealing applies the ops its buffers sent toward the leaves one canonical
   edit at a time, except into an empty tree: the code says so, and a second
   commit of 447,000 facts spent over two hours there. That it is quadratic
   in the ops a leaf receives is a hypothesis, with its test.
3. Each commit persists every node its ops reached, and commits much larger
   than a buffer reach most of the leaves: 2,000 commits wrote 37 versions
   for every node of the tree they left.

Ruled out: that the time is the archive's alone. Over a 2,000-commit
replay the archive took about 63% of commit time and the tree the rest; at
447,000 facts a commit the seal alone ran for hours. Not ruled out: the
buffers' size against the fan-out below them (a hypothesis).

**Decided.** A problem statement is proposed in the top story, marked as
proposed: unmet objective, bringing a large dataset into a replica, and
the three causes. Not decided: whether that objective is Irakli's, which
the next sitting asks first.

**Open question for the next sitting.** Is bringing a dataset that already
exists into a replica the objective, in both forms (records from another
system, a replica's current state into a new file)? And think of a second
one: is there anyone else who would write a commit this large?
