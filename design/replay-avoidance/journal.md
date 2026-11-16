# Journal

## Reflective inquiry

|  | Understanding | Activity |
|---|---|---|
| **Status** | Split out of file-writers: a commit that lost its branch's race always replays there. Three read sets considered: key ranges, matched triples, ranges as a filter with patterns as the test. Matched triples miss phantoms. | Describing. |
| **Agenda** | How often a replay happens, and what one costs, in the workloads that matter. Whether a replay's cost is worth a read set at all. | Measure replays before designing a read set. |

## 2026-11-10, Describing: split out of file-writers

**Asked.** Irakli: handle this separately; here just rerun, and separately
develop a system that captures what was considered, so a rerun is avoided
when there is no overlap.

**Learned.** The options and the phantom analysis carried over from
file-writers are in the top story.

**Decided.** A topic of its own, Describing. Not on the path of
file-writers.
