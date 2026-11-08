# Journal

## Reflective inquiry

|  | Understanding | Activity |
|---|---|---|
| **Status** | What do we know? A write retires only the elected claim, and a read does not check causes, both confirmed in the source. The maintainer's direction is column D. `history::causality` reads "seen" from claim lineage, which would retire other origins' `all` values. | Where are we at? Directing: six approaches in a matrix, D stated, not decided. |
| **Agenda** | What do we need to know? Whether partial sync can deliver a claim after its successor (a test). Whether revision causality tells "before" from "concurrent" where lineage cannot, and whether a partial replica holds what it needs (tests). What a write does when causality cannot be decided locally. | Where are we going? Settle the open hypotheses, decide the direction, then the first implementation decision: claim lineage or revision DAG for "concurrent". |

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
