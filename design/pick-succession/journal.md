# Journal

## Reflective inquiry

|  | Understanding | Activity |
|---|---|---|
| **Status** | What do we know? A write retires only the elected claim, and a read does not check causes, both confirmed in the source. The maintainer's direction is column D. `history::causality` reads "seen" from claim lineage, which would retire other origins' `all` values. | Where are we at? Directing: six approaches in a matrix, D stated, not decided. |
| **Agenda** | What do we need to know? Whether revision causality tells "before" from "concurrent" where lineage cannot, and whether a partial replica holds what it needs (tests). What a write does when causality cannot be decided locally: [design-undecided](design-undecided.md), E stated. Whether fetching revision records on a write is cheap. | Where are we going? Settle the open hypotheses, decide the direction and the undecided case, then the first implementation decision: claim lineage or revision DAG for "concurrent". |

## 2026-11-08, Describing to Directing: from the max bug to a direction

**Asked.**
- How do picks run today? The maintainer remembered a second select after the scan; the source confirms it, and shows the write keeps the operational index at the claim a read returns.
- What happens to concurrent claims? A write retires only the elected one (`tree.rs`, `instruction.rs`), and a test pins it.
- Should a write retire every live claim? No: the pick is the reader's refinement, and a `max` write must not erase an `all` writer's values (Irakli).
- Then which? Only the claims concurrent with the elected one (Irakli).
- Can merge retire? No: replication is partial (Irakli). Retiring happens on writes, and a write also retires claims that came back.
- Does the read already skip a claim another present claim's cause names? The maintainer thought so. The source shows it does not.

**Learned.**
- The first diagnosis, that a write should succeed every live claim it holds, was wrong. It breaks use case 3.
- "Concurrent" is not one thing. `history::causality` judges it by claim lineage, by which an `all` value succeeds nothing and so counts as concurrent with anything from another origin. The rule needs revision causality. That is a hypothesis in the claims, not a fact.
- A claim that comes back has to be both skipped on read and retired on the next write. Retiring only the elected claim would let it resurface once its successor is gone.

**Decided.** Not decided. Column D is the maintainer's stated direction and rests on three opinions and two open hypotheses in the claims: that partial sync can deliver a claim after its successor, and that revision causality is available and right at write. E and F are out of scope, on the maintainer's opinions.

## 2026-11-08, Directing: can a claim arrive after its successor?

**Asked.** Can partial sync deliver a claim after the claim that succeeded it?

**Learned.** Not with two replicas, but with three it can (Irakli). A writes, B writes having seen it, A writes again succeeding B's write. C syncs with A, which no longer holds B's write live, then with B, and receives B's write after A's. On C it must read as succeeded, not concurrent. A's cause names B's write directly, so claim lineage finds it at the first tier: this case does not need revision causality. The description's earlier two-replica example of it was impossible and is replaced.

**Decided.** The hypothesis is confirmed by the scenario; the test is still to write. The read filter and the retiring of claims that come back stay in column D. The remaining gate is what a write does when it cannot decide concurrency locally.

## 2026-11-08, Designing: a write that cannot decide

**Asked.** What does a write do when it cannot decide locally whether a live claim is concurrent with the elected one?

**Learned.**
- Whatever it does must converge (Irakli), and it should be able to decide offline (Irakli).
- A write can make a decision durable only through the cause it records, so retiring an undecided claim without naming it would differ between replicas, and naming it would make a guess permanent.
- Filling the gap by fetching revision records lets the write decide. When it cannot fetch, the honest result is to write the value as concurrent with the claim and let a later write settle it (Irakli).

**Decided.** Not decided. E is stated: fetch when the records can be reached, otherwise stand concurrent. Its matrix is [design-undecided](design-undecided.md). It rests on two open hypotheses: that standing concurrent converges, and that fetching revision records costs little.

## 2026-11-08, Diagnosing and back to Directing: the second case does not arise

**Asked.** Does the three-replica example hold, with A writing having seen B's claim?

**Learned.**
- It does not. If A had seen B's claim, C would have learned of it from A (Irakli). With A not having seen it, B's claim and A's are concurrent, which is correct and should stay. Irakli's sequence: 7 ← 5 from A, C writes 2 over 5, then 7 ← 3 from B; 3 and 2 are concurrent.
- The merge already screens claims that come back (`merge.rs`): R1 never re-applies a live claim whose revision the receiver observed, and R3 removes a claim an incoming history record covers.
- That contradicts the claim that a merge cannot retire: R3 does, from the records it receives. Read as "a merge applies coverage it is given but decides nothing", the two fit; the claims ledger asks which.
- The goal is CRDT behavior from the operational index alone, without keeping every value and recomputing on read (Irakli). The problem statement now says so.

**Decided.** The read filter and the retiring of what the elected claim succeeded leave column D. D is now: a write retires the elected claim and those concurrent with it, by revision.

**Backtracked.** From Directing to Diagnosing for the second case, then back, once the merge screens explained it away.
