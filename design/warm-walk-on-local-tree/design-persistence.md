# A replica that holds every block still pays to warm it: how the record survives

Decision: where the record of wholly local trees (column D of the
[direction](direction.md)) is kept, and so whether a restarted replica
still has it. For: a walk over a tree whose every block is local can
fetch nothing and is run anyway, because nothing records that a tree is
wholly local.

Decided 2026-11-12: P1, by the agent, as the first step. P2 is the next
step if H4 in the [claims ledger](claims.md) finds wholly local members in
tonk that restart. P3 is not taken (see below).

| criterion \ approach | P1. In memory, held beside the routes | P2. A replica-local cell per repository holding the few most recent recorded trees | P3. In memory, and a proving walk: the first hint for an unrecorded root walks the whole tree once, reading only the local archive, and records it if nothing is absent |
|---|---|---|---|
| a member after a restart | 🔴 walks every hint until a full download records its tree; its commits cannot, since their base is not recorded | 🟢 no walk: the cell names its head's tree | 🟡 one traversal of the whole tree, from the local archive, per process, then no walk |
| a partly local or adopted replica | no cost: nothing is recorded for it | no cost: nothing is written for it | 🔴 a proving walk for each new unrecorded root, and every pull adopts one; it stops at the first absent block, which may come after most of the local part; ? how much it reads per pull |
| the cost of a commit | an insert into a small set | 🟡 one more local cell write per commit on a recorded tree; ? what the `commit-*` perf scenarios show | an insert into a small set |
| `query-member` as measured | 🟢 no walk: setup and phase share a process, and `forget` releases caches, not the record | 🟢 no walk | 🟢 no walk: setup's commits record the tree |
| what is stored | nothing | 🟡 a new replica-local cell: an older release ignores it and nothing is rewritten, which `CHANGELOG.md`'s compatibility section must still say | nothing |
| a lost or stale entry | lost at restart: costs walks | a lost write (a crash, two branches racing on the cell) costs walks; a cell that outlives the archive's blocks skips walks the demand reads then pay for; ? whether any storage loses blocks and keeps cells | lost at restart: costs one proving walk |
| the replication gate | unchanged | unchanged: no soak client records a tree | 🔴 soak clients adopt by reference and would run proving walks; ? whether the soak counts their local reads |

Why P1 first:

- It is the part every other column needs: the set, the two writers, the
  one reader. P2 adds a write of the set to a cell and a read of it at
  open, and changes nothing else.
- Whether P2's cost (a cell write per commit, a new stored cell) buys
  anything depends on a fact nobody has: whether tonk has members that
  hold every block across restarts (H4). Tonk reloads a page, which is a
  restart, so if H4 holds, P1 alone barely helps tonk.
- P3 adds a cost to every pull of a partial replica to save walks on a
  complete one: the trade the topic is meant not to make.

What P1 does not show: `query-member` keeps its process between setup and
phase, so the gain it measures is a long-lived process's. A restarted
member would still walk, and no scenario today can show that, since
nothing short of a new environment drops the record.
