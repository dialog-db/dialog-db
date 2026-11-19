# Literature: maintaining a fold when facts are retracted

Status: a research thread's report, 2026-11-19, from web search and fetch only. Nothing here was measured on dialog. Section D lists what was not read or not verified; a claim taken from here into the ledger keeps that caveat.

All of this comes from web search and fetch. I wrote and changed no files. Quotes are verbatim from pages I fetched today. A fetch tool summarized the long PDFs for me, so a few fine details depend on its reading (see D).

---

## A. Techniques

### 1. Counting and DRed (Gupta, Mumick, Subrahmanian, SIGMOD 1993, pp. 157-166, DOI 10.1145/170036.170066), and the Gupta and Mumick survey

**Sources**
- The chapter text I read is the reprint in the MIT Press *Materialized Views* reader: https://pages.iai.uni-bonn.de/manthey_rainer/Seminars2018/matView.pdf (around characters 400k to 500k).
- Survey: Gupta and Mumick, "Maintenance of Materialized Views: Problems, Techniques, and Applications", IEEE Data Engineering Bulletin, June 1995: https://dcmpx.remotevs.com/org/computer/sites/PL/debull/95JUN-CD.pdf

**Counting (non-recursive views)**
- Each view tuple stores the number of its derivations, "a count of the number of derivations for each view tuple" (survey).
- A deletion decrements the count. The tuple disappears when the count reaches 0.
- The counts cost "little or no cost above the cost of deriving the tuple."
- For a GROUPBY subgoal it works per group: it computes the new group tuple from the old one and the change ΔU, then swaps the old tuple out and the new one in. SUM adds the changed attribute.
- If the function is not incrementally computable, the group value "has to be computed using the tuples of relation U". So the base relation must stay available.
- On MIN and MAX: "incremental computation is not possible with respect to deletions". Insertions are fine.

**DRed (recursive views, stratified negation and aggregation, set semantics)**
- Three steps: "Delete a superset of the derived tuples that need to be deleted", then "Put back those deleted tuples that have alternative derivations", then insert the new tuples.
- The chapter claims it is "the first algorithm to handle aggregation in recursive views."
- The survey says DRed does not apply to SQL views with duplicate semantics.

**Self-maintainability (survey)**
- A view is self-maintainable if it can be maintained "using only the materialized view and key constraints".
- The survey's results cover SPJ and outer-join views. It does not give a per-aggregate table.
- The per-aggregate self-maintainability results are in Mumick, Quass and Mumick, SIGMOD 1997, "Maintenance of Data Cubes and Summary Tables in a Warehouse" (DOI 10.1145/253260.253277). I could not read that paper (see D).

**Substrate it needs:** signed changes and per-tuple counts. Per-group recomputation needs the base data, indexed by group.

### 2. Data Cube (Gray, Chaudhuri, Bosworth, Layman, Reichart, Venkatrao, Pellow, Pirahesh; Data Mining and Knowledge Discovery 1(1):29-53, 1997; MSR-TR-97-32)

Source: https://arxiv.org/pdf/cs/0701155v1

**The three classes**
- Distributive: F over the whole set equals G over the per-part results of F. "COUNT(), MIN(), MAX(), SUM() are all distributive."
- Algebraic: a fixed-size M-tuple of partial results plus a final function H. Average keeps (sum, count). Standard deviation, MaxN and MinN are also algebraic.
- Holistic: "no constant bound on the size of the storage needed to describe a sub-aggregate". Examples are median, mode and rank.
- Interface: Init / Iter / Final, plus Iter_super for merging two partial results.

**The point that matters here: the class depends on the operation**
- MAX is distributive for SELECT and INSERT but holistic for DELETE. When the maximum is deleted, "2N elements of the cube must be recomputed".
- The paper describes "orthogonal hierarchies for SELECT, INSERT, and DELETE functions."
- COUNT and SUM are algebraic under all three operations, so they are cheap to maintain.

### 3. DBToaster and the ring of databases

**Sources**
- Koch, "Incremental Query Evaluation in a Ring of Databases", PODS 2010: https://infoscience.epfl.ch/record/166019/
- Ahmad, Kennedy, Koch, Nikolic, "DBToaster: Higher-order Delta Processing for Dynamic, Frequently Fresh Views", PVLDB 5(10), 2012: https://arxiv.org/pdf/1207.0137

**Mechanism**
- Relations have rational or integer multiplicities, so "a delete is a relation with negative multiplicities". A delete trigger is the insert trigger with += replaced by -=.
- The view is maintained by its delta query. The delta query is itself materialized and maintained by its own delta, and so on. This recursion ends because "the n-th order delta is guaranteed to be constant."
- For non-nested queries, each aggregate is maintained with constant work per update (Koch).

**State and cost**
- State is a set of auxiliary maps keyed by group or join variables.
- Equi-joins disappear from the update path: "joins are only needed in the presence of inequality joins and nested aggregates".

**Aggregates**
- Sum is the only native aggregate. Count is Sum of 1. There is "no explicit syntax for ... aggregates other than Sum".
- Nested aggregates can make the naive transform loop ("will not terminate"). DBToaster then decorrelates, or picks re-evaluation for those parts.
- The paper does not mention MIN or MAX.

**Recursion:** not covered.

### 4. Differential dataflow (McSherry, Murray, Isaacs, Isard, CIDR 2013) and arrangements

**Sources**
- CIDR paper: https://www.cidrdb.org/cidr2013/Papers/CIDR13_Paper111.pdf
- McSherry, Lattuada, Schwarzkopf, Roscoe, "Shared Arrangements", PVLDB 13 (2020): https://arxiv.org/pdf/1812.02639

**Mechanism**
- A collection is a set of signed differences indexed by versions drawn from a partial order. "δAt(r) may be negative, corresponding to the removal of a record."
- A generic operator rebuilds its input at a version from the stored differences and applies the user function. Work is limited to keys that appear in incoming differences.

**Per-operator state (CIDR)**
- Count retains only "the difference trace of the number of records for each key."
- Min and Max "must keep their full input difference traces". They can often skip the rebuild by comparing the update with the previous output.
- Join keeps both input traces.

**Reduce over arrangements (Shared Arrangements, where the operator is called "group")**
- At each time the output might change, it re-applies the reduction function to that key's whole input. It then diffs the result against the key's previous output, which it keeps as an output arrangement.
- So the cost of one change is proportional to the size of the touched group.
- An arrangement is "a list of immutable, indexed batches of updates", sorted by data then time, and merged LSM-style so there are about log many batches.

**Compaction:** once readers advance their frontiers, updates at times that can no longer be told apart are consolidated (the CIDR paper's term is "checkpoint").

**Recursion:** loop coordinates are added to the timestamp. Convergence is reached when no differences remain outstanding. Deletions inside recursion then reuse the per-iteration differences.

### 5. DBSP (Budiu, Chajed, McSherry, Ryzhyk, Tannen; PVLDB 16(7):1601-1614, 2023) and Feldera

**Sources**
- VLDB paper: https://www.vldb.org/pvldb/vol16/p1601-budiu.pdf
- arXiv version: https://arxiv.org/pdf/2203.16684
- Feldera aggregate docs: https://docs.feldera.com/sql/aggregates/

**Z-sets and operator classes**
- A Z-set maps elements to integer weights, which can be negative.
- Linear operators (filter, map, projection, grouping) are their own incremental version.
- Bilinear operators (join): Δ(a⋈b) = Δa⋈Δb + a⋈Δb + Δa⋈b. This needs the integrated state of both inputs, O(|R|) space.

**DISTINCT**
- Incremental DISTINCT keeps the integrated input, O(|DB|) space.
- A helper function H emits ±1 only when an element's weight crosses zero. It touches "only multiplicities of the elements that appear in the change".

**GROUP BY aggregates**
- Grouping is linear. COUNT and SUM are linear. AVG is SUM and COUNT combined.
- MIN is "not incremental in general". The generic form re-integrates the input.
- Nested aggregates "only needs to be re-evaluated for groupings that have changed".
- The VLDB paper points to custom operators such as Reactive Aggregator that bring the amortized cost to O(log |DB|).

**Recursion**
- Fixed-point loops yield semi-naive evaluation.
- Incremental recursion uses nested streams. Space grows with the number of iterations.
- Correctness is proved for stratified programs. A non-monotonic "while" loop is "not guaranteed to terminate".

**Feldera costs (docs).** N is collection size, D change size, G number of groups, M total size of the touched groups.

| Aggregate | Space | Work per change |
|---|---|---|
| Linear (COUNT, SUM/AVG over integer or decimal) | O(G) | O(D) |
| MIN, MAX, ARG_MIN, ARG_MAX | O(N) | O(D log M) |
| DISTINCT | O(N) | O(D log M) |
| ARRAY_AGG | O(N) | O(M) |
| MIN/MAX on append-only input | O(G) | O(D) |

- SUM over FLOAT becomes non-linear.
- FILTER usually makes an aggregate non-linear.
- Feldera's blog says the old value has to be retracted when a group changes: "the previous value has to be removed from the view." https://www.feldera.com/blog/why-incremental-aggregates-are-difficult-part-1

**dbsp crate:** the linear aggregate path still "creates two integrals: for input and output streams". https://docs.rs/crate/dbsp/latest/source/src/operator/aggregate.rs

### 6. Noria (Gjengset et al., OSDI 2018)

**Sources**
- Paper (NSF copy): https://par.nsf.gov//servlets/purl/10087018
- Thesis: https://pdos.csail.mit.edu/papers/jfrg:thesis.pdf

**Partial state**
- Operators keep "the subset of records that the application has queried".
- When state is missing, an operator sends an upquery. Upqueries can cascade "up to base tables".
- Eviction: updates that hit evicted entries are dropped. Everything downstream must evict too, so it "never see[s] updates for that entry".
- A join upqueries its other input for every update.

**Aggregates**
- Count and sum read their own output state, then emit -old and +new.
- Min and max are not supported, because they "may need to scan the state of their ancestor if the current extremum is removed".
- Top-k "will not eagerly fetch its replacement".
- Partial state is turned off for operators that depend on all of their upstream (top-k).

**Recursion:** none.

### 7. F-IVM (Nikolic, Olteanu, SIGMOD 2018) and follow-ups

**Sources**
- SIGMOD paper: https://arxiv.org/pdf/1703.07484
- Journal-length version (Kara, Nikolic, Olteanu, Zhang): https://arxiv.org/pdf/2303.08583
- Olteanu, "Recent Increments in IVM", Gems of PODS 2024: https://arxiv.org/pdf/2404.17679v1

**Mechanism**
- A view tree follows a variable order. Each view maps keys to ring payloads. Marginalization is pushed below joins.
- Only the views that a sibling update needs are materialized.
- Deletions are negative payloads, so the payload must be a ring, with additive inverses.

**Cost per single-tuple update**
- O(1) for q-hierarchical queries.
- O(N) for free-connex acyclic queries.
- Under the OuMv conjecture, non-q-hierarchical queries cannot get sub-√N update time together with low delay. With deletes, constant update time fails beyond the q-hierarchical class.
- Insert-only workloads are asymptotically cheaper.
- Inserts and deletes commute, so a batch can be applied in any order (Olteanu).

**Payloads**
- Rings exist for count/sum (ℤ), the covariance matrix, and the relational data ring (relations, with union as + and join as ×). The last is what lets a payload carry a listing or a factorized set of values.
- Min and max form only a semiring. The deletion framework needs inverses, so it does not cover them.

### 8. Incremental Datalog with aggregates and negation

**DDlog** (Ryzhyk and Budiu, "Differential Datalog"): https://mihaibudiu.github.io/work/ddlog.pdf
- Compiles to differential dataflow. Aggregation is a group-by over a key, with shortest-path `min` as the example.
- Negation is an antijoin, and stratified: "a relation cannot recursively depend on its own negation".
- The paper does not cover recursion through aggregation.
- State is DD's in-memory traces indexed by time.

**Soufflé** (Zhao, Raghothaman, Subotić, Scholz, "Towards Elastic Incrementalization for Datalog", PPDP 2021): https://davidwzhao.github.io/pdf/PPDP2021.pdf
- Uses sparse counting, not DRed. Each tuple stores two numbers: a derivation count and the iteration it was first derived in. That is O(|I|) space, against O(m|I|) for counts per iteration.
- A "re-discovery" step handles tuples that were deleted but have later derivations.
- It switches back to full recompute when an update takes more than about 20% of the last full run.
- Stratified negation is supported. Incremental aggregates are not.

**IncA / DRedL** (Szabó, Bergmann, Erdweg, Voelter, "Incrementalizing Lattice-Based Program Analyses in Datalog", PACMPL / OOPSLA 2018): https://voelter.de/data/pub/inca-oopsla2018.pdf
- Handles recursive lattice aggregation (lub and glb, which cover min and max).
- The problem it solves: "cyclic reinforcement of lattice values", where a stale value keeps supporting itself around a loop.
- Changes are split into monotonic and anti-monotonic ones, followed by over-delete, rederive, then monotonic insert.
- State: support counts per tuple, a multiset of aggregands per group, and an AVL tree per group whose nodes cache the subtree's aggregate. That gives O(log N) per aggregate update and O(N) memory.
- Requirements: "aggregations must be monotonic within a dependency component", no recursion through negation, and no infinite ascending chains.
- Memory measured 670MB to 5GB.

**Datalog over semirings** (Abo Khamis, Ngo, Pichler, Suciu, Wang, "Convergence of Datalog over (Pre-) Semirings", PODS 2022): https://arxiv.org/html/2105.14435v5
- Least-fixpoint semantics over partially ordered pre-semirings (POPS). A program converges if and only if the core semiring is "stable".
- The tropical semiring (min, +) gives shortest paths, and Trop⁺ is 0-stable.
- It gives semi-naive evaluation for the tropical case through a "minus" operator.
- No deletions and no maintenance under updates.

**Flix** (Madsen, Yee, Lhoták, PLDI 2016): https://plg.uwaterloo.ca/~olhotak/pubs/pldi16.pdf
- Lattice-valued last column, with monotone and strict functions and finite height.
- Semi-naive evaluation with a least upper bound per cell.
- No negation, no deletions.

### 9. Storage-side aggregation

**CouchDB**
- Before the FoundationDB rewrite, inner B+tree nodes held "aggregations of the results from their children". Source: RFC 012 at https://apache.googlesource.com/couchdb/+/refs/tags/3.2.2.post3/src/docs/rfcs/012-fdb-reduce.md
- The reduce function is called with rereduce=true on partial results. A result that grows is refused, since reduce must "converge rapidly to a single value". Source: https://github.com/apache/couchdb/blob/main/src/docs/src/ddocs/ddocs.rst
- Built-in reducers: `_sum`, `_count`, `_stats` (min/max/sum/count/sumsqr) and `_approx_count_distinct` (HyperLogLog, about 2% error).
- The RFC notes that `_sum` and `_count` can be updated atomically in upper levels. Other reducers must re-reduce every affected node from the level below.
- A min/max deletion is therefore a re-reduce along the root path over sibling summaries: O(fanout × depth).

**Finger trees** (Hinze and Paterson, JFP): https://www.staff.city.ac.uk/%7Eross/papers/FingerTree.pdf
- Each node caches "a cached reduction with some monoid".
- Splitting on a monotone predicate over the measure gives positional indexing (size), a max-priority queue and an ordered sequence.
- Cost is "proportional to the depth of the split point".

**Content-addressed, verified: GroveDB.** Source: https://dashpay.github.io/grovedb/aggregate-count-queries.html
- Provable count trees hash the count into the node hash (`node_hash_with_count`).
- A range count is O(log n) with "no per-element cost component". The proof holds boundary nodes plus whole-subtree (hash, count) pairs.
- Plain count trees store counts but leave them out of the hash.

**Content-defined-boundary trees:** I found no primary source on aggregates in their branch nodes. A replikativ persistent-sorted-set page describes the node hash as level, keys and addresses only.

### 10. Convergent, multi-writer and local-first settings

**Keep CALM and CRDT On** (Laddad, Power, Milano, Cheung, Crooks, Hellerstein; PVLDB 16, 2023): https://arxiv.org/pdf/2210.12605.pdf
- Queries over CRDT state are unconstrained. Monotone queries, such as thresholds on growing counts, are safe on one replica.
- Set difference (and with it retraction and negation) is non-monotone: "Only set difference is non-monotone" among relational algebra operators.
- It mentions IVM only as possible future work.

**Yanakieva, Bieniusa, Dumbrava, "A Datalog Framework for CRDTs"** (2026 preprint): https://arxiv.org/pdf/2605.31569
- When the visibility relation changes on each event, DDlog's incremental mode "effectively recomputes everything".

**Riffle / Rife** (Litt PhD thesis, MIT 2023): https://groups.csail.mit.edu/sdg/pubs/2023/litt_phd_thesis.pdf
- Expensive joins are SKDB (Skip) views, "incrementally maintained eagerly".
- Queries the UI is currently using are re-run when a table they depend on changes. Propagation stops when a result is unchanged.

**Skip Reducer API:** https://docs-site-sigma.vercel.app/docs/api/core/interfaces/Reducer
- `remove(accum, value)` may return null, which means: rebuild from `initial` by adding each of the key's remaining values. That is always valid.
- Non-null results must satisfy the invertibility laws.
- LazyCollection values are "computed only when queried".

**Zero (Rocicorp) ZQL:** https://www.mintlify.com/rocicorp/mono/advanced/zql-engine
- Push and fetch operators: Filter, Join, Take.
- Take buffers its result, and a remove can make it fetch more rows.
- The page shows no aggregates.

**Adapton** (Hammer, Phang, Hicks, Foster, PLDI 2014): https://people.mpi-sws.org/~hammer/adapton-pldi2014.pdf
- A set marks the dependency graph dirty, eagerly. A force re-evaluates only what is demanded and dirty.
- It used 3-98% of eager SAC's memory on lazy patterns.

**Jane Street Incremental:** https://blog.janestreet.com/introducing-incremental/
- Observers define what is "necessary". Changes apply only on `stabilize`. Firing a node costs about 30ns.

**Materialize** (local-first relevance is mine, the mechanism is theirs): https://materialize.com/robust-reductions-in-materialize/ and https://dev.materialize.com/api/rust/src/mz_compute_types/plan/reduce.rs.html
- Accumulable: invertible and associative, such as sum and count.
- Hierarchical: associative, such as min and max.
  - On non-monotonic input, the bucketed plan builds a reduction tree over (key, hash >> shift). It has 16 layers with no more than about 16 elements per group, so the work is "logarithmically bounded".
  - On append-only input, the monotonic plan keeps only the best value.
- Basic: everything else, including string_agg, array, jsonb_agg and window functions. These use differential dataflow's reduce, which recomputes the touched group.
- Materialize deduplicates before a DISTINCT aggregate.

---

## B. Comparison

Abbreviations: N = input size, M = size of the touched groups, G = number of groups, D = change size.

| Technique | State, and where | One change costs | Deletions | count/sum | min/max | distinct set | Joins feeding the fold | Recursion | Partial / on demand | Needs from substrate |
|---|---|---|---|---|---|---|---|---|---|---|
| Counting (GMS93) | Derivation count per tuple; base relation for non-incremental aggregates | O(Δ) for count/sum; O(group) otherwise | Decrement counts | Incremental | Recompute group from base | Counts per value | Delta rules against base | No (non-recursive only) | No | Signed deltas, base indexed by group |
| DRed (GMS93) | Materialized view | Over-delete plus rederive, can be large | Over-delete, then rederive | Yes | Via rederive | Set semantics | Yes | Stratified negation and aggregation | No | Base access for rederive |
| Data cube classes | Partial results | Distributive or algebraic: O(1); holistic: unbounded | MAX is holistic under delete | Algebraic for all ops | Delete-holistic | Holistic | n/a | n/a | n/a | n/a |
| DBToaster | Higher-order delta maps keyed by variables | Often O(1) per tuple | Negative multiplicity | Native (Sum) | Not covered | Via nested sums | Equi-joins removed from update path | No | View caches for input variables | Ring multiplicities |
| Differential dataflow | Indexed multiversioned input and output arrangements | O(M) per touched key and time | Negative diffs | Count trace only | Full input trace | Full trace | Arranged on both sides | Iterative, nested timestamps | No (full) | Timestamps, frontiers, compaction |
| DBSP / Feldera | Integrated input (and output) per operator | Linear: O(D); min/max: O(D log M); general: O(M) | Z-set weights | O(G) space | O(N) space | O(N) space, H function | Bilinear, both sides integrated | Nested streams, stratified | Waterline GC only | Z-sets, a clock |
| Noria | Only the keys that were read; base tables in RocksDB | Count: O(1); join: an upquery | Negative deltas, eviction | Yes | Unsupported | n/a | Upquery the other side | No | Yes (upqueries, eviction) | Indexed base tables |
| F-IVM | View tree of ring payloads | O(1) (q-hierarchical) to O(N) | Negative payload | Rings | Not a ring | Relational data ring (listings) | Factorized | No | No | Ring payloads, variable order |
| Soufflé elastic | Count and first-derivation iteration per tuple | Update, or full rerun past 20% | Sparse counting plus re-discovery | Not incremental | Not incremental | n/a | Yes | Yes, stratified negation | No | Per-tuple metadata |
| IncA / DRedL | Support counts, aggregand multisets, AVL tree per group | O(log N) per aggregate | Anti-monotone phase plus rederive | Lattice | Through recursion, if monotone | Powerset lattice | Yes | Yes, no negation in cycles | No | Lattice laws, ordered multiset |
| Semiring Datalog / Flix | Fixpoint values | Not maintained | None | Semiring | Tropical, through recursion | n/a | Yes | Yes | No | Stable or finite-height lattice |
| CouchDB / GroveDB / finger tree | Reductions or counts in inner nodes | O(depth) for invertible ones; O(fanout × depth) re-reduce otherwise | Re-reduce path | Yes | Re-reduce over sibling summaries | Approximate (HLL) or no | No | No | Any range on read | Changed node format, deterministic reducer |
| Materialize hierarchical | Tree of reduce layers per key | Up to 16 layers × 16 elements | Diffs | Accumulable | Logarithmic | Dedupe before aggregating | DD | DD | No | Hash bucketing |
| Adapton / Jane Street / Skip lazy | Dependency graph with memo tables | Proportional to the demanded dirty region | Re-evaluate, or reducer remove (null means rebuild) | Reducer | Rebuild from values | Rebuild | User code | No | Yes, by demand | Dependency tracking |

---

## C. What transfers to a system like Dialog, and what does not

1. **Retraction and supersession map onto signed deltas.** A row that stops holding is a -1 delta with its full old value. A superseding claim is -old then +new. Every IVM system above needs the removed value itself, not just a key (Z-sets; Skip `remove(accum, value)`; DBToaster's -=).
   - So a subscription needs, for each change, the exact row that stopped holding.
   - Because insert and delete commute over a ring (Olteanu), merged batches can be applied in any order for count/sum/avg.

2. **Count, sum and avg are cheap, with tiny state.** O(G) state and O(1) per change, kept as per-group counters plus a row count to detect empty groups (Feldera docs; Materialize accumulable; GMS93). This fits per-subscriber memory.
   - Float SUM is not truly invertible (Feldera treats it as non-linear).

3. **Distinct set and count-distinct need a multiplicity per (group, value).** That is O(N) state. Only values whose count crosses zero produce output (DBSP's H function; Feldera O(N) space and O(D log M) work).
   - The set-of-values aggregate in the relational data ring is the F-IVM version of this.

4. **Min and max under retraction need every value of the group, ordered.**
   - The only exception is append-only input (Data Cube: delete-holistic; GMS93: "not possible with respect to deletions"; Noria refuses them outright).
   - With an ordered multiset per group, a change costs O(log M) (Feldera; IncA's AVL trees; Materialize's 16×16 tree).
   - My inference, not from a source: if a group's values sit contiguously in value order in the store's ordered index, the store already is that ordered multiset. The new extremum is then the first entry that still holds in a range scan. The cost is that scan, which grows with the number of non-holding entries in front of the first one that holds.

5. **The universal fallback is to recompute the touched group.** DBSP for non-linear aggregates, differential dataflow's reduce, Materialize "basic", Skip `remove` returning null, and GMS93 all do this. Cost is O(M) per change, and it needs the group's inputs indexed by group key.
   - That is the step from today's "recompute the fold on each poll" to "recompute only the touched groups".

6. **Joins feeding a fold.** Bilinear deltas need indexed state for both sides (DBSP; differential dataflow arrangements). Noria gets the same effect by upquerying stored base tables, which is the stance that matches an embedded store with indexes.
   - Higher-order deltas (DBToaster, F-IVM) take joins off the update path by materializing partial aggregates. That costs extra views, and their payoff depends on query shape (q-hierarchical).

7. **Recursion with aggregation is the hard case.**
   - Options: DRed over-delete and rederive (GMS93; DRedL for lattices); per-iteration state (DBSP nested streams, growing with iterations; differential dataflow loop timestamps); sparse counting with an iteration number (Soufflé, 2 numbers per tuple).
   - None of these handles a cycle through negation. IncA also needs monotone aggregation inside a component.
   - Semiring Datalog and Flix give min/max-through-recursion semantics but no maintenance.

8. **Demand-driven, per-subscriber state has strong precedent.**
   - Noria keeps only the keys that were read, upqueries for the rest, and evicts downstream.
   - Adapton marks dirty eagerly but re-evaluates only on demand.
   - Jane Street Incremental limits work to the nodes observers need.
   - Skip has lazy collections.
   - Caveat (Noria): operators whose output depends on all of their input (top-k, and min/max after a deletion) cannot be partial without a scan.

9. **Aggregates in tree nodes (CouchDB, GroveDB, finger trees)** answer a fold over any key range in O(log n) without per-subscriber state. But they mean changing the stored node format, and a content-addressed tree must also hash the annotation (GroveDB) to stay verifiable.
   - My inference: if holding is decided causally rather than by which entries are present, a summary over raw entries would count rows that no longer hold. That would only work if retraction physically removes the entry.
   - Only invertible reducers (sum, count) update cheaply. Everything else is re-reduced along the root path (CouchDB RFC).

10. **Semantics under merge.** CALM: aggregates and retraction are non-monotone, so a replica's answer is provisional until merges settle. IVM keeps it current but does not make it final (Laddad et al.).
    - Yanakieva et al. warn that encoding causality as a relation that changes on every event defeats incrementality. So causal holding should arrive as row-level ±1 deltas, not be re-derived through a visibility join.

11. **Append-only fast paths exist** (Feldera O(G) min/max; Materialize monotonic plan; Olteanu's insert-only bounds). They apply only where a group's input provably never retracts.

---

## D. Not verified, from memory, or blocked

**Blocked fetches (no answer to the permission prompt):**
- usenix.org Noria PDF. I used the NSF copy instead.
- docs.couchdb.org views intro. I used the GitHub ddocs.rst and the RFC instead.
- The differential-dataflow mdbook page on reduce.
- A zenodo URL I guessed; this was my error.
- The code.obsidian.systems notes (access denied).

**Not read:**
- Mumick, Quass and Mumick SIGMOD 1997. From memory: it classifies aggregates as self-maintainable for insertions and deletions given COUNT(*) in the view (COUNT, SUM, AVG), and MIN/MAX as self-maintainable only for insertions. Unverified.
- Naiad (SOSP 2013): I cite it only by title.
- Electric SQL, Automerge, LiveStore: not researched. I found no "Pail" or "DBSP for local-first" primary source.

**From memory only:**
- Laddder is Szabó, Erdweg, Bergmann, PLDI 2021 (incremental lattice Datalog, a successor to DRedL). I did not read it.
- Reactive Aggregator / FlatFAT is Tangwongsan, Hirzel, Schneider, Wu, VLDB 2015, O(log n) for associative aggregates. DBSP cites "Reactive Aggregator" by name. I did not read the paper.
- Order-statistic trees as in CLRS.
- How Feldera achieves O(D log M) for min/max: the docs give the cost, not the mechanism.
- Jane Street's height ordering and cutoffs: the blog did not cover them.

**Relayed from tool summaries of long PDFs, worth re-checking before relying on them:**
- The GMS93 MIN/MAX deletion quote.
- IncA's AVL-tree-per-operator detail.
- The DBSP VLDB O(log |DB|) statement.
- Materialize's "16 layers × 16 elements".

**Not verifiable from the pages:** whether DBToaster handles min/max at all, and F-IVM's listing-payload details beyond the definition of the relational data ring.
