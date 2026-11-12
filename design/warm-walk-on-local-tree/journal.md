# A replica that holds every block still pays to warm it: journal

## Reflective inquiry

|  | Understanding | Activity |
|---|---|---|
| **Status** | The warm walk asks only whether the repository can fetch, never whether this tree has anything to fetch, and nothing records the second. A record of wholly local tree hashes, written by commits on a recorded or empty tree and by full downloads and read only by the walk, removes `query-member`'s walks and cannot change a result. It does nothing for a head adopted by reference, and, kept in memory, nothing after a restart. | Designing. Column D and persistence step P1 are decided by the agent, without the maintainer; nothing is built. |
| **Agenda** | Whether tonk's walks are on wholly local trees (H4): it decides whether this topic serves tonk at all, and whether column E or step P2 comes next. Whether H1 holds for every path that applies instructions to a tree. Whether a scoped download should record (tree, scope). | The maintainer reads the decision cold and confirms or overturns it. Then the Design phase's open decisions: where a commit learns both its base tree and that its nodes persisted; the bound on the set; how a test observes a skip without a new perf counter. Then H4 in tonk. |

## 2026-11-12, Describing to Designing: what a walk would need to know

This sitting ran without the maintainer. The agent took the brief as the
maintainer's answers to the Describing and Diagnosing questions, read the
code to check each, and answered the rest itself. Every answer that is
the maintainer's to give is in the claims ledger as the agent's opinion,
for the maintainer to keep or overturn.

**Asked.**

- Is "skip a warm walk whose range is known local" the problem? No, it is
  a remedy. Underneath it: a query on a replica that holds its whole tree
  pays for speculation that cannot return anything, and no guard can
  stop that without knowing which trees are complete.
- What does the walk know before it starts? Only `routes.fetches`, which
  is about the repository's remotes, not about the tree. That is the
  cause, and it is a fact (`fetch.rs`, `routes.rs`).
- Who can know that a tree is wholly local, and when? Whoever made every
  block of it local, at the moment it finished: a full download that
  drained, and a commit that applied instructions to a tree already known
  local (H1). Not a pull that fast-forwards or grafts, since it takes the
  upstream's subtrees by hash. A replay arm that rewrites the local tree
  might qualify; left out until H1 is checked for it.
- Per head, per branch, or per tree hash? Per tree hash. `query-member`'s
  member never commits: it adopts the main branch's tree by a local pull.
  A per-branch or per-head flag would miss it, or need copying in the
  pull. A per-hash record transfers by itself, and a head adopted by
  reference cannot inherit it, because its tree hash is one nobody
  recorded.
- Can a wrong record lose correctness? Only if something other than the
  walk reads it. The walk is advisory: with speculation switched off,
  queries return the same rows. So the decision includes an invariant:
  no download, pull or read consults the record.
- Think of a second approach: six columns (status quo, the two rejected
  earlier, a root record, a subtree record, fewer hints).

**Learned.**

- The perf gate allows no growth in any `span.*` counter. A new counter
  for skipped walks would fail `query-member` until its baseline is
  re-recorded, which is the maintainer's decision alone. So the first
  step adds no counter: the existing `warm_walk` and `warm_node` going to
  0 is the measurement, and a unit test observes skips.
- `forget` in the perf environment releases caches but not routes or
  writers. A record held beside the routes survives it; a record held
  with the caches would not, and the scenario would show nothing.
- Under an in-memory record, `query-member` measures a long-lived
  process. A restarted member would still walk, and no scenario shows
  that. Tonk's browser build restarts on every page load.
- The brief's tonk motivation may not be served. Per the held-across-pull
  topic, tonk pulls its spaces with `QueriedRanges`, which adopts or
  grafts by hash, so its trees would never be recorded. If tonk's 18.5K
  walks are on such trees, the subtree record (E) is what would help, not
  D. This is H4, open, and the most important thing to find out next.
- The subtree record contains the root record as a special case, so
  building D first does not close E off.

**Decided.** Column D of the direction, kept in memory beside the routes
(P1 of the persistence matrix), by the agent, pending the maintainer.
It rests on H1 (commits on a complete tree make a complete tree), H2 (the
scenario's walks go to 0) and H3 (the replication gate does not move),
all open, and on three opinions of the agent's: the record is not a
cache, persistence waits for H4, and a pull's replay arm is not a writer
yet. Not taken on: restarts, scoped downloads, the replay arm, the
subtree record, fewer hints. Not built in this session.
