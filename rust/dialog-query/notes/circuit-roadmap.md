# Toward DBSP Circuits: Incremental Roadmap

## Where We Are

The query pipeline today:

```
Query (Term/Premise/Proposition)       names, typed
         │
    Planner::plan()                    orders by cost, tracks Environment
         │
    Conjunction { steps: Vec<Plan> }   ordered, costed
         │
    conjunction.evaluate()             folds over steps, streaming Answers
         │
    Stream<Answer>                     HashMap<String, Factors> with provenance
```

Key properties of the current system:

- **Term has no registers.** `Term<T>` is purely syntactic — variable name + type.
- **Parameter** is the type-erased dynamic counterpart (name + optional Type tag).
- **Answer** is `HashMap<String, Factors>` — name-indexed, with provenance tracking.
- **Environment** is `HashSet<String>` — just tracks which names are bound.
- **Evaluation** is a linear fold: each step transforms a `Stream<Answer>`.
- **Sub-circuits are async/on-demand.** `source.acquire(predicate)` loads rules
  from the store at evaluation time, not at plan time.
- **No registers, no compilation step, no circuit graph.**

## Where We Want To Be

```
Query → Plan → Circuit → Stream<ZSet>
```

Where:

- **Plan** = ordered execution graph from the Planner, with register assignments
- **Circuit** = live DBSP dataflow graph, operators connected by Z-set streams
- **Operators** have `step(input: ZSet) → ZSet` — same interface for initial load
  and incremental update (initial load = diff from empty tree)
- **Prolly tree IS the state store** — operators don't accumulate data in memory,
  they probe the tree at specific revisions
- **Subscription** holds a Circuit + revision cursor. Scheduler polls after changes.

## The Phases

Each phase is independently shippable, testable, and valuable. Later phases
build on earlier ones but each stands alone as an improvement.

---

### Phase 1: Register-based Answer (performance foundation)

**Goal**: Replace `HashMap<String, Factors>` with `Vec<Option<Factors>>` indexed
by register. Name→register mapping happens during planning.

**Why first**: Every later phase benefits from O(1) slot access instead of
HashMap lookups on every variable read/write. This is pure performance with
no semantic change.

**What changes**:

1. Add `Register` type (newtype over `usize`) and `RegisterAllocator` to
   the planner.

2. Extend `Parameter` (or introduce a `Cell` wrapper) to carry an assigned
   register after planning. The planner already traverses premises to estimate
   costs — during the same traversal, allocate registers for each named variable.

3. `Answer` gains a `slots: Vec<Option<Factors>>` field alongside `conclusions`.
   Initially both are maintained (dual-write). `assign()` writes to both.
   `resolve()` reads from slots first, falls back to conclusions.

4. Once all code paths use register-based access, remove `conclusions` HashMap.

5. The Plan carries register count. `Answer::with_capacity(n)` pre-allocates.

**What stays the same**: User API, Planner ordering logic, streaming evaluation,
provenance tracking via Factors. All existing tests pass.

**Measure**: Profile before/after on the existing benchmark suite. Expect
meaningful speedup on multi-join queries where the same variables are resolved
many times.

---

### Phase 2: Operator types (compilation boundary)

**Goal**: Introduce typed operators that premises compile into. This creates
the boundary between the planning layer (names) and the execution layer
(registers).

**Why**: Operators are the nodes of the future circuit graph. Even while we
still evaluate as a linear fold, having explicit operator types lets us
reason about the execution plan as data.

**What changes**:

1. Define operator types corresponding to each `Proposition` variant:

   ```rust
   enum Operator {
       Select(SelectOp),     // from Proposition::Relation
       Concept(ConceptOp),   // from Proposition::Concept
       Compute(ComputeOp),   // from Proposition::Formula
       Filter(FilterOp),     // from Proposition::Constraint
       Negate(NegateOp),     // from Premise::Unless
   }
   ```

   Each operator works with registers, not names. `SelectOp` has the
   pattern + output ports. `FilterOp` has two register addresses. Etc.

2. `Plan::compile() → Operator` — each plan step compiles its premise into
   an operator. This happens at the end of `Planner::plan()` as part of the
   same traversal (Phase 1 already does register assignment there).

3. `Conjunction` becomes:

   ```rust
   struct Conjunction {
       steps: Vec<Operator>,
       register_count: usize,
       cost: usize,
       // env/binds still tracked for replanning
   }
   ```

4. `Conjunction::evaluate()` folds over Operators instead of Plans. Each
   operator's `evaluate(answers, source)` uses register-based access.

5. **SelectOp replaces RelationQuery in the hot path.** `SelectOp` has:
   - `pattern: ArtifactSelector` — the constrained fields
   - `outputs: Vec<(Field, Register)>` — which fields bind to which registers
   - `cardinality: Cardinality`

   No more `resolve_from_answer` building a new `RelationQuery` per input row.
   Instead, SelectOp reads bound registers from the answer to build the
   selector directly.

6. **ConceptOp** is intentionally a thin wrapper — it holds the
   `ConceptDescriptor` and register mappings but loads the sub-circuit lazily
   at evaluation time via `source.acquire()`. This preserves the current
   async-on-demand behavior.

**What stays the same**: User API, planning logic, streaming evaluation model,
the `Source` trait.

---

### Phase 3: Circuit graph (topology)

**Goal**: Replace the linear `Vec<Operator>` with a directed graph. Operators
are nodes, edges carry data between output ports and input ports.

**Why**: A DBSP circuit is a graph, not a sequence. Even if the planner still
produces left-deep join trees (chains), the graph structure supports fan-out,
parallel scans, and (eventually) feedback loops for recursion.

**What changes**:

1. Introduce graph types:

   ```rust
   struct Circuit {
       nodes: Vec<Operator>,
       edges: Vec<Edge>,
       inputs: Vec<NodeId>,   // nodes with no predecessors (Select ops)
       outputs: Vec<NodeId>,  // terminal nodes
   }

   struct Edge {
       from: NodeId,
       to: NodeId,
   }
   ```

2. `Conjunction::compile() → Circuit` produces a graph. For the current
   linear plan, this is a chain: `Select₁ → Join₁ → Select₂ → Join₂ → ...`

   Each "step" in today's fold is really a Select + implicit Join. Making
   this explicit in the graph prepares for Phase 5 where Selects become
   independent input nodes.

3. Execution: topological sort → process nodes in order. For a chain this
   is equivalent to the current fold. The streaming model is preserved.

4. **ConceptOp sub-circuits** are nested Circuit instances. When a ConceptOp
   is first stepped, it loads rules from the store, compiles them into
   a sub-Circuit, and caches it. If rules change, the cache is invalidated
   and the sub-circuit is recompiled on next step. This captures the
   async-on-demand behavior in the circuit model.

**What stays the same**: User API, behavior, streaming, performance.

---

### Phase 4: Z-set streams (DBSP data model)

**Goal**: Make operators process `ZSet<Answer>` instead of `Stream<Answer>`.
This is the data model change that enables incremental maintenance.

**Why**: In DBSP, data flows as weighted multisets. An answer with weight +1
is asserted; weight -1 is retracted. Linear operators (Filter, Compute)
propagate weights unchanged. This is the foundation for incremental updates.

**What changes**:

1. Extend `Answer` with a weight field:

   ```rust
   struct Answer {
       slots: Vec<Option<Factors>>,
       weight: isize,  // +1 = asserted, -1 = retracted
   }
   ```

   For initial evaluation, all answers have weight +1. Operators propagate
   the weight. This is backward-compatible — existing behavior is the
   special case where all weights are +1.

2. Linear operators (Filter, Compute) pass weights through unchanged:
   if input has weight -1, output has weight -1.

3. Non-linear operators (Select with bound variables = dependent join) need
   more thought — deferred to Phase 5.

4. Wire the existing `dialog-dbsp` crate's `ZSet<T>` as the batch type.
   The `Operator` trait from `dialog-dbsp` (`process(ZSet<I>) → ZSet<O>`)
   becomes the target interface for circuit operators.

**What stays the same**: All behavior when weights are +1. Streaming still
works. No incremental maintenance yet — just the data model is in place.

---

### Phase 5: Subscription model (incremental pull)

**Goal**: Introduce `Subscription` — a Circuit + revision cursor that can be
polled to get incremental updates when the store changes.

**Why**: This is the payoff. Subscriptions turn one-shot queries into live
views that update incrementally as facts change.

**What changes**:

1. Extend `Source` with revision-aware pull:

   ```rust
   trait Source: ArtifactStore + ... {
       fn acquire(&self, predicate: &ConceptDescriptor) -> Result<ConceptRules, QueryError>;

       /// Current revision of the underlying store.
       fn revision(&self) -> Option<Revision>;

       /// Pull changes for a selector since a given revision.
       /// Returns a ZSet (assert = +1, retract = -1).
       fn pull(
           &self,
           selector: ArtifactSelector<Constrained>,
           since: Option<Revision>,
       ) -> impl Stream<Item = Result<(Artifact, isize), Error>>;
   }
   ```

2. SelectOp gains a `pull` mode: instead of scanning all matching facts,
   it pulls the diff since the subscription's last revision. First pull
   (since = None) returns all facts as +1. Subsequent pulls return deltas.

3. **Subscription** type:

   ```rust
   struct Subscription {
       circuit: Circuit,
       revision: Option<Revision>,
   }

   impl Subscription {
       async fn poll<S: Source>(&mut self, source: &S) -> Result<ZSet<Answer>, Error> {
           let output = self.circuit.step(source, self.revision.as_ref()).await?;
           self.revision = source.revision();
           Ok(output)
       }
   }
   ```

4. **Join as index probe** — when a delta arrives at a Select operator
   that has bound variables from upstream, it probes the prolly tree at
   the current revision. When a delta arrives at the upstream, the
   downstream Select re-probes for each new/changed upstream answer.

   The prolly tree IS the accumulated state. No in-memory accumulation
   needed. For the incremental join formula:

   ```
   Δ(A ⋈ B) = (Δa ⋈ B_old) + (A_new ⋈ Δb)
   ```

   - `Δa ⋈ B_old`: for each changed A, probe OLD revision's tree for B
   - `A_new ⋈ Δb`: for each changed B, probe NEW revision's tree for A

   Both old and new revisions are available (content-addressed blobs).

5. **Scheduler** manages active subscriptions:

   ```rust
   struct Scheduler {
       subscriptions: Vec<Subscription>,
   }

   impl Scheduler {
       async fn on_revision_change<S: Source>(&mut self, source: &S) {
           for sub in &mut self.subscriptions {
               sub.poll(source).await?;
           }
       }
   }
   ```

**What stays the same**: One-shot queries still work (create subscription,
poll once, drop). The streaming model is preserved for initial evaluation.

---

### Phase 6: Recursive rules (feedback loops)

**Goal**: Support recursive deductive rules (transitive closure, etc.) via
feedback edges in the circuit graph.

**Why**: Some queries require fixpoint iteration — repeatedly applying rules
until no new facts are derived. DBSP handles this with the Delay operator
(z⁻¹) and iteration.

**What changes**:

1. `Delay` operator: outputs previous step's input. Enables feedback edges.

2. `Distinct` operator: ensures each answer appears at most once (non-linear,
   needs the prolly tree as state).

3. The Planner detects recursive rule dependencies (the cycle analyzer
   already exists) and compiles them into circuits with feedback edges.

4. Iteration: step the circuit repeatedly until the output ZSet is empty
   (fixpoint reached). Each iteration only processes newly derived facts
   (semi-naive evaluation falls out naturally from the delta propagation).

**Prerequisites**: Phases 3-5 (graph structure, Z-sets, subscriptions).

---

## What's NOT in scope

- **Magic sets optimization** — transforming bottom-up evaluation to only
  process demand-relevant facts. The current top-down strategy with selective
  prolly tree loading already achieves this. Magic sets would be needed if
  we moved to pure bottom-up, but we're not.

- **Parallel evaluation** — processing independent circuit branches
  concurrently. The graph structure (Phase 3) enables this but the
  implementation can come later.

- **Arrangement sharing** — multiple circuits sharing the same prolly tree
  indexes. Already implicit in our model (all circuits read from the same
  Source).

## Dependencies

```
Phase 1 (registers) ──→ Phase 2 (operators) ──→ Phase 3 (graph)
                                                       │
                                                       ▼
                                                 Phase 4 (Z-sets)
                                                       │
                                                       ▼
                                                 Phase 5 (subscriptions)
                                                       │
                                                       ▼
                                                 Phase 6 (recursion)
```

Phases 1-3 are refactoring — they change internal structure without changing
behavior. All existing tests pass after each phase.

Phase 4 is a data model extension — weights are added but default to +1.

Phase 5 is the first new capability — live subscriptions with incremental
updates.

Phase 6 extends the system to handle recursive rules.
