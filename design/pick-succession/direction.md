# Direction

Decision: what a write under an electing pick retires, and what a read skips. For the problem in the [top story](../pick-succession.md#problem).

| criterion \ approach | A. Status quo: retire the elected claim | B. Retire every live claim the writer holds | C. Retire the elected claim and those concurrent with it, by claim lineage | D. Retire the elected claim and those concurrent with it by revision | E. Retire at merge, as claims arrive | F. Key the cell by pick |
|---|---|---|---|---|---|---|
| use case 1, a write reads back | 🔴 a concurrent claim can win the next read | 🟢 only the written claim is live | 🟢 the concurrent claims leave with the elected one | 🟢 as C | 🟡 as A at write; merge alone does not cover a write after a merge | 🟡 as A within a pick |
| use case 2, a later write settles a conflict | 🔴 the losing claim stays live, under `last` as well | 🟢 the cell returns to one claim | 🟢 the cell returns to one claim | 🟢 the cell returns to one claim | 🟡 settles what merges see, not what a write saw | 🟡 as A |
| use case 3, `all` values stay | 🟢 only the elected claim leaves | 🔴 a `max` write erases every `all` value | 🔴 an `all` value from another origin counts as concurrent and leaves | 🟢 values written before the elected claim and never succeeded stay; one larger than the new value then wins a `max` read | 🟡 depends on what merge retires | 🟢 the cells never meet |
| use case 4, a partial replica reads right | 🔴 a claim that comes back can win | 🔴 as A between writes | 🔴 as A between writes | 🟡 claims that come back are screened at merge (R1, R3); the write needs causality a partial replica may not hold, see [design-undecided](design-undecided.md) | 🟡 already done where a record covers a claim (R3); it cannot decide concurrency it has not seen | 🔴 as A |
| use case 5, same claims, same read | 🟢 elections are deterministic | 🟢 | 🟢 | 🟢 | 🟡 depends on arrival order unless merge is order-free | 🟢 |
| the pick stays the reader's refinement | 🟢 | 🟢 | 🟢 | 🟢 | 🟢 | 🔴 the pick becomes part of the cell; a `max` reader no longer sees `all` writes |
| work at write | one election | one election, then retire all | one election and a lineage walk per live claim | one election, a revision-DAG check per live claim ? | none beyond today | none beyond today |
| work at read | one election | one election | one election | one election | one election | one election |
| stored data | nothing changes | nothing changes | nothing changes | nothing changes; existing cells settle on their next write | nothing changes | 🔴 every cell's key changes |
| tests to change | none | `it_succeeds_the_claim_the_succession_elects` | the same | the same, and the new cases in #759 | sync tests | most of the artifact tests |

Scope: not taking on deciding at merge (E) beyond the coverage it already applies, on the maintainer's opinion that replication stays partial, nor keying cells by pick (F), on the opinion that the pick is the reader's refinement. Both are in the [claims](claims.md).
