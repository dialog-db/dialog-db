# Design: a write that cannot decide whether a claim is concurrent

Decision: what a write does with a live claim when it cannot tell, from what the replica holds, whether that claim was written before the elected claim or concurrently with it. For the problem in the [top story](../pick-succession.md#problem).

This only arises for the revision-DAG check. A claim that the elected claim succeeded is named in its cause and is found without reading any history (see the journal). So the undecided case is a live claim, not named by the elected claim's lineage, whose revision records the replica does not hold (`causality` returns `IncompleteHistory`).

The outcome of any column has to converge: replicas that end up holding the same claims read the same value, whatever each one knew when it wrote. A write can only make a decision durable through what it records, the new claim's cause. Removing a claim from its own operational index changes nothing anywhere else.

| criterion \ approach | A. Fail the write | B. Retire it, as if concurrent | C. Keep it and do not succeed it: the write stands concurrent with it | D. Fetch the missing revision records, then decide | E. D when the records can be reached, C when they cannot |
|---|---|---|---|---|---|
| converges | 🟢 nothing is written | the cause names the claim, so every replica's read skips it | the cause does not name the claim, so every replica elects between the two by the pick's order | as with full history | each write converges as D or C does; the same write can record different causes online and offline |
| use case 1, a write reads back | 🔴 no write | 🟢 | 🟡 the kept claim can win the next read, until a later write that can decide settles it | 🟢 | 🟢 online; 🟡 offline, as C |
| use case 3, `all` values stay | 🟢 | 🔴 an `all` value written before the elected claim is succeeded, and every replica's read skips it from then on | 🟢 | 🟢 | 🟢 |
| works offline | 🔴 | 🟢 | 🟢 | 🔴 | 🟢 |
| partial replication stays partial | 🟢 | 🟢 | 🟢 | 🟡 fetches revision records, not claims ? | 🟡 as D, only when it can |
| what the write records | nothing | a decision it could not check | only what it could check | what it checked | only what it checked |
| work at write | none | none | none | a fetch on the write path | a fetch, when reachable |

The direction stated so far is E (Irakli, 2026-11-08): fill the gaps when the records can be reached, so the write decides; when they cannot, write the value as concurrent with the claim, and let a later write settle it. Not decided.

Questions:
- Is the cost of fetching revision records on the write path small enough to do by default? ? Revision records are small, but this is a measurement.
- Should a read that finds a write standing concurrent with an undecided claim fetch and settle it, or only report the conflict? ?
