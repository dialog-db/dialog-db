# Inductive Rules: Trigger-Indexed Commit-Time Dispatch

Design for native inductive-rule support in dialog. Generalizes what
tonk built in userland (`tonk-evaluator`, `dialog-reactor`,
`plan/effects.md` in the tonk tree): tonk gates rule firing on
*transient concepts* (commands) so that only rules handling an active
command are ever considered. Dialog keeps the cost model that makes
this viable — **a commit never enumerates, loads, or plans rules that
don't watch something it touched** — but does *not* require a transient
premise: inductive rules may trigger on durable facts too, so queues
and other stateful machines can be described over ordinary replicated
state. Transients remain first-class as the ephemerality tool
(commands, acks) and as the cheapest trigger tier.

The mental model is database triggers: rules are triggers, the
attributes named by their premises are the tables they watch, and a
stored reverse index is the trigger catalog. Dispatch is a point lookup
keyed by what the commit touched, never a scan over all rules.

## Decisions and build order

The prototype (see "Prototype" below) is the architecture: store as
the only state, stateless per-commit induction, dispatch structures
as head-keyed views of rule facts. Every design thread in this note
ends in "keep it, add X later." The X's, in order:

1. **`retract!` head polarity** — done: descriptor field beside
   `assert!` (exactly one required), `Polarity` on `InductiveRule`,
   dissociate-exact-triple emission, pinned by the mailbox-ack
   consumption test.
2. **Head-keyed dispatch caches** — done: `TriggerFootprint` (the
   O(1) gate) plus trigger/reads discovery keyed `(on, head)`,
   content-addressed inductive bodies, and transience verdicts, all
   on the branch's `RuleCache`; the overlay slice is scanned fresh
   per round, never head-cached. Head-advance invalidation pinned by
   `it_rescans_triggers_after_a_head_advance`.
3. **Delta-restricted body evaluation** — done: assert/replace
   stimulus rows bind into the positive premises naming their
   attribute and the body evaluates with those bindings fixed
   (planned under the seeded scope); the full-body fallback covers
   removal-enabled firings (`unless` over retracted or superseded
   facts) and candidates probed through the deductive closure.
4. **Event premises** (`asserted:`/`retracted:` over the round's
   stimulus, generalizing to version ranges under the watermark
   model) — monotone transition triggers, first-class. Next.
5. **`cardinality: sum`** — counters as contribution facts with a
   readout fold.
6. Only on demonstrated pain: alpha discrimination, the quiescence
   lint, `at: replica` placement / pull-induction, further lattice
   types.

Standing answers to recurring questions:

- *Cross-replica effects*: two rules — the event mints a durable
  obligation (replicates), a level-triggered consumer applies it
  against each replica's slice. Under the watermark model the
  consumer fires natively as the marker arrives by pull; until then a
  host bridges via subscription → dispatch.
- *Completion-by-merge* (P at replica A, Q at replica B, body needs
  both): resolved by the watermark model — induction follows a
  durable per-branch watermark and every head advance, commit or
  pull, is an instant.
- *Rules that keep matching*: quiescence is the novelty fixed point;
  no polling exists between commits; only value-generating or
  polarity-oscillating cascades diverge, and `MAX_ROUNDS` fails the
  commit atomically.
- *Derived premises*: closed over `db.rule/reads` at dispatch
  (implemented).
- *Negation*: `unless` stays for state absence; events cover
  transitions only.

## What exists today

**In dialog:** `InductiveRule` / `InductiveRuleDescriptor`
(`dialog-query/src/rule/inductive.rs`) compile through the shared
analysis pipeline; the `assert!` head marks transaction-time semantics
and the self-negation idempotence guard (Dedalus `P@next :- body, not
P@now`) is permitted, unlike deductive rules. But nothing evaluates
them: there is no reactor, no trigger index, no storage convention, no
transient facts. Deductive rules are stored as `db.rule/*` facts with
layered resolution and head-keyed discovery caching (`rules.rs`,
`layered-rule-resolution.md`).

**In tonk:** the full loop exists in userland.

- A *command* is a transient concept — literally `type Command =
  TransientConcept` — marked by one fact:
  `(concept, dialog.concept/transient, db:transient)`. Transience is a
  marker fact, **not** part of the concept's content address, so a
  command and a durable concept with the same shape share an entity.
- The browser tags event-derived assertions transient at the wire;
  `TransactionBuilder` buckets them into a separate `Changes` batch.
- Effects are stored as facts: `dialog.effect/source` (descriptor
  JSON), `dialog.effect/conclusion`, `dialog.effect/polarity`
  (assert/retract heads), and — the load-bearing one —
  `dialog.effect/on` `is` `on:<domain>/<name>`, cardinality many, one
  entry per attribute named by any concept premise (`when` *and*
  `unless`). The `on:` key is derivable from a runtime `Changes`
  instruction alone (the attribute name), with no schema lookup.
- The induce loop (`tonk-evaluator/src/effects.rs`) seeds its stimulus
  **exclusively from the transient bucket**. Durable-only commit ⇒ the
  loop body never runs ⇒ zero index probes, zero rules loaded. With
  commands present, the probe set is the handful of attribute names the
  commands touch; each is a one-hop `dialog.effect/on` lookup.
- Rounds are semi-naive with frozen input: all candidates in a round
  read identical state; outputs batch-integrate once. Transient heads
  become the next round's stimulus; termination is "no transients
  emitted". `MAX_ROUNDS = 16` guards runaway cascades.
- Command facts never persist: tonk integrates them into the
  transaction (so rule bodies can read them), then sweeps them by
  emitting inverse instructions that cancel at commit. They never reach
  the tree and never replicate.
- The `on` entries for a rule's *durable* premises exist but are dead
  weight: the probe set never contains a durable attribute, so tonk's
  tier-2/3 machinery (durable-change triggers) is indexed for but
  unimplemented. Tonk's design doc treats "every rule has a transient
  premise" as the performance requirement that collapses everything
  onto the cheap tier — but the check (`validate_effect`) is never
  wired in; the invariant holds by convention.

## Goals

1. Inductive rules evaluated natively at commit, no host reactor
   required.
2. DB-trigger cost model, preserved from tonk and generalized: dispatch
   cost is proportional to the commit's *touched trigger attributes*,
   never to the rule population. A commit touching nothing any rule
   watches does near-zero work — without requiring the trigger to be a
   command.
3. Transient premises are **optional**. Rules may trigger on durable
   facts (assertions *and* retractions), so queue promotion, cascading
   cleanup, and other durable-state machines are expressible without
   synthetic commands.
4. Transients as a first-class transaction citizen where they are used:
   no sweep trick, no phantom commits.

## Design

### Transience is a concept property, stored as a fact

```
db.concept/transient  of  concept:<hash>  is  true
```

Declared when a command concept is installed; not part of the concept's
content address (following tonk: same shape ⇒ same entity, transience
is a branch-level property). Keeping it out of the descriptor also
keeps `InductiveRuleDescriptor` self-contained and portable.

The marker now plays exactly one role: **head routing** — a rule whose
head concept is marked transient emits ephemeral facts (next-round
stimulus, never committed) instead of durable novelty. It no longer
gates which rules may exist or fire.

Lookup is one `AttributeQuery`, resolved against the transaction view
so a command declared in the same commit as its first use works.
Cacheable per `(concept, head Revision)` with the `RuleCache`
discipline; the overlay slice is read fresh.

### The transaction grows a transient bucket

```rust
pub struct Transaction<'a> {
    branch: &'a Branch,
    changes: Changes,     // durable, as today
    transients: Changes,  // commands: visible to reads, never committed
}
```

- `tx.assert(claim)` / `tx.retract(claim)` — durable, unchanged.
- `tx.dispatch(claim)` — routes into `transients`.
- `tx.query()` layers **branch ⊕ changes ⊕ transients**. `QueryEnv`
  already composes layered `Changes` overlays; the transient bucket is
  one more layer.

This replaces tonk's integrate-then-sweep: transients never enter the
durable batch, so nothing cancels at commit, and a command that derives
no durable novelty produces **no commit at all** (the existing
empty-commit short-circuit applies) — unlike tonk, where a
transient-only commit mints a revision with an identical tree hash.

### Storage: `db.rule/*` grows two attributes

An inductive rule is content-addressed like a deductive one
(`rule:<base58(blake3(dag-cbor(descriptor)))>`; the `assert!` /
`retract!` head field is in the encoding, so kinds and polarities get
distinct entities). Stored as:

- `db.rule/source` — canonical dag-cbor descriptor, shared attribute
  with deductive rules; hydration dispatches on the head field.
- `db.rule/induces` `is` head-concept-entity — the inductive sibling of
  `db.rule/conclusion`. **Deliberately a separate attribute**: deductive
  resolution scans `db.rule/conclusion` on every concept query, and
  sharing it would make every query hydrate-and-discard the inductive
  rules concluding that concept. Separate index ⇒ the deductive path is
  untouched by any number of installed effects.
- `db.rule/on` `is` `on:<domain>/<name>` — cardinality many, the
  trigger index. One entry per attribute named by any concept premise,
  `when` **and** `unless`, derived syntactically from the descriptor as
  tonk's `Effect::on_entities` does. With durable triggers these
  entries are all load-bearing — including the `unless` ones, because a
  *retraction* can newly enable a rule (retract `job/active` ⇒ the
  promotion rule's `unless active` guard clears).

`impl Statement for InductiveRule` (and `DeductiveRule`) writes these
facts synchronously — no schema lookup needed, since the index is
derived from the descriptor alone. (The layered-rule-resolution note
already claims `tx.assert(rule)` works; today callers stage the facts
by hand — that impl should land alongside.)

### The trigger footprint: restoring the near-zero gate

Tonk's zero-cost gate ("no commands ⇒ skip everything") came from the
transient bucket being empty. Once durable writes can trigger, every
commit has a non-empty stimulus — so the gate moves one level down:

Per branch head, maintain the **trigger footprint**: the set of `on:`
keys present in `db.rule/on` (equivalently: the set of attributes any
inductive rule watches). Rules are facts, so this is one range read
over the `on:` value space at head advance, cached like rule discovery
(head-keyed; overlay-staged rules folded in fresh). Represent it as a
small hash set (or bloom filter if rule populations grow large).

Dispatch then gates per attribute in memory:

- Commit touches attributes {a, b, c}; footprint ∩ {a, b, c} = ∅ ⇒
  skip the loop entirely. Zero I/O, a few set probes. This is the
  common case for the vast majority of commits and preserves tonk's
  property in spirit: cost is O(touched attributes), not O(rules).
- Non-empty intersection ⇒ probe `db.rule/on` only for the
  intersecting attributes, hydrate only those candidates.

The footprint is an over-approximation twice over (attribute
granularity; shared attributes between concepts), and that is fine: the
later funnel stages ("Keeping dispatch flat" below) discriminate
further before any store I/O, and the body evaluation is the precise
filter.

### The dispatch loop, in `Commit::perform`

`Transaction::commit()` carries both buckets into the `Commit` command.
`Commit::perform` gains a step 0, before the batch apply:

```rust
// Round 1 stimulus: everything the commit changes — durable asserts,
// durable retracts, and dispatched transients alike.
let mut stimulus = delta_of(&changes) + transients;
let mut round = 0;
while !stimulus.is_empty() {
    if round >= MAX_ROUNDS { return Err(CommitError::NonTerminatingInduction(round)); }
    round += 1;

    // 1. Probe keys straight off the instructions — no schema lookup.
    //    Gate on the in-memory trigger footprint first.
    let touched: BTreeSet<Attribute> = stimulus.attribute_names();
    let watched = footprint.intersect(&touched);
    if watched.is_empty() { break; }

    // 2. Trigger-indexed discovery: one db.rule/on lookup per watched
    //    attribute, against the tx view (committed slice via the
    //    head-keyed cache, overlay fresh). Nothing ever enumerates
    //    all rules.
    let candidates = probe_on_index(&watched, &view).await?;

    // 3. Hydrate (content-addressed cache) + plan (PlanCache) + fire
    //    each candidate against the FROZEN round view, delta-restricted:
    //    the premise(s) that matched the probe bind against the round's
    //    stimulus, remaining premises read the full view.
    let (novelty, next_transients) = fire(candidates, &stimulus, &view).await?;

    // 4. Fold durable novelty into changes. Next round's stimulus is
    //    the novelty's delta plus emitted transients; this round's
    //    transients are dropped (they never entered `changes`).
    changes.merge(&novelty);
    stimulus = delta_of(&novelty) + next_transients;
}
// ...existing perform: checkpoint, batch apply, revision, seal, publish.
```

Semantics:

- **Frozen rounds (semi-naive).** Sibling rules in a round read
  identical state; ordering among them is unobservable. Chaining
  happens across rounds.
- **Delta restriction.** A candidate fires with at least one triggering
  premise bound to the round's stimulus rather than re-joining the
  whole branch. This is what makes a durable trigger on a large
  relation affordable: the rule evaluates against the changed rows, not
  the table. The per-entity restriction machinery subscriptions use
  (`Application::restrict`, `affected_entities`) is the intended
  mechanism.
- **Novelty, not output, drives the next round.** `delta_of` keeps only
  instructions that change the view: asserting an already-present
  triple or retracting an absent one contributes nothing. This is the
  fixpoint's convergence test — a rule whose head re-derives existing
  state terminates for free, which makes `Replace`-headed
  (cardinality-one) rules and `assert-unless-present` rules naturally
  idempotent.
- **Termination = empty stimulus**, backstopped by `MAX_ROUNDS`. With
  durable triggers, "no transients emitted" is no longer sufficient —
  durable novelty can enable further rules, so the loop runs until
  novelty dries up. Genuinely self-feeding durable rules (`assert!
  counter{count: ?c + 1} when counter{count: ?c}` with no guard) are
  now expressible and will hit `MAX_ROUNDS` and fail the commit — the
  same posture as recursive triggers in SQL databases (Postgres errors
  at max recursion depth). The static tautology check (`assert! C when
  C`, no delta possible) rejects the trivial shape at analysis; the
  `unless`-own-head idempotence guard is the recommended idiom for
  everything else, and `retract! C when C`-shaped rules stay legal —
  that is the consumption pattern.
- **Head routing** consults `db.concept/transient` for the head concept
  (cached per head revision): marked ⇒ ephemeral, next-stimulus only;
  unmarked ⇒ durable novelty.
- **Atomicity and observability.** One commit ⇒ one revision ⇒ one
  subscription notification with settled state; intermediate rounds are
  invisible. Induction completes before the batch is applied and
  sealed, so a failed induction aborts with nothing persisted.

### Quiescence: rules that keep matching

A rule whose body matches the settled state — and whose firing does
not make the body stop matching — looks like it should run forever.
It doesn't, and the distinction between the shapes matters:

- **Idempotent re-match is the fixed point, not a loop.** A standing
  rule (`assert! open{desc} when task{desc} unless done`) matches
  forever once fired, but re-firing emits the same triples, the
  novelty check drops them, the stimulus empties, the loop exits.
  "Body matches and head holds" is exactly the state the system rests
  at. Matching is not what drives work — the *delta of the match set*
  is. Such rules behave as materialized invariants: re-checked at any
  commit that could disturb them, re-firing only when their conclusion
  actually stopped holding.
- **Between commits a matching rule doesn't run at all.** There is no
  daemon: a rule whose body holds costs nothing until a commit touches
  a watched attribute, and then one probe + one body evaluation + a
  not-novel verdict.
- **Genuine divergence requires firing to produce *fresh* state that
  re-enables the rule**, and only two shapes do: value generators
  (`?c + 1` — every firing novel by construction) and polarity
  oscillators (an `assert! X` rule and a `retract! X` rule flipping X's
  presence each round, each flip novel; neither rule is individually
  suspect). Excluding these statically is undecidable in general, so
  the guards layer: the analysis-time tautology check for the trivial
  shape, the `unless`-own-head idempotence idiom (which makes the
  *body* stop matching, not just the emission go stale), and
  `MAX_ROUNDS` failing the commit atomically as the hard backstop.

The instructive divergent example is the **unguarded durable
increment** — `assert! counter{?c + 1} when doc/edit{...},
counter{?c}` with a *durable* edit premise. In tonk's transient-only
model this shape is safe: the command expires after round 1, the body
fails, done. Durably triggered, the rule's own body reads
`counter/count`, so its head lands in its own watched set; the edit
fact is still in the view at round 2, the body re-matches, and the
formula makes every emission fresh — `MAX_ROUNDS`. Neither
delta-restriction nor the novelty check helps (each round's delta and
value are genuinely new). The lesson: an expiring premise is a
*termination device*, not just an optimization — a durably-triggered
rule deriving from its own output needs the `unless` guard instead.

**Across commits the engine cannot self-perpetuate.** Induction runs
inside a commit and nothing else: when the loop exits, no work is
scheduled. A rule fires in the next commit only if an external actor
makes that commit and it touches a watched attribute. "Fires on every
commit" across time is therefore either intended (an audit-log rule
watching what every commit writes — cost proportional to real
firings), or inert re-match (probe + not-novel, cost only). Oscillator
pairs cannot spread across commits either: the commit that enables the
second rule has both live in its own induce loop, so the ping-pong is
caught there by the round bound. The one true escape is a *host*
feedback loop — subscription delta → application write-back → trigger
— which no depth limit can see because each cycle is a genuine
external commit. The echo-shaped version starves for free: writing
back already-derived facts settles to a no-op batch, no revision is
minted, and the subscription never re-fires.

A future install-time **productivity lint** falls out of rules being
facts: `db.rule/induces` gives each rule's outputs, `db.rule/on` its
inputs, so the trigger graph is a join. Cycles carrying a formula
(value generator) or mixed assert/retract polarity — the two shapes
that defeat the novelty fixed point — can be flagged at install as
"needs an idempotence guard," the inductive analog of stratification
checking on the deductive side. The unguarded increment above is
precisely a trigger-graph self-loop through a formula. Necessarily
incomplete (it is the halting problem), but it catches the shapes
people actually write.

### Derived premises: closing the footprint over deduction

An inductive premise may name a concept that is itself *concluded by a
deductive rule* — `actor/status{on-duty}` derived from `shift/*` facts
rather than written. No commit ever touches `actor/status`, so the
authored `on:actor/status` entry alone would never match a probe and
the rule would silently never fire. Evaluation is not the problem —
once probed, the body resolves deductive rules through the standard
`QueryEnv` — the trigger is.

The fix is *where the expansion lives*: *not* in the stored index.
Expanding `db.rule/on` at install time (storing `on:shift/ended` on
the inbox rule) goes stale the moment a deductive rule is installed
later — a new way to become on-duty would silently miss every
inductive rule whose stored closure predates it, and repairing that
means rewriting other rules' facts on every deductive-rule change.

Instead every stored entry stays *per-rule and authored-level* — a
pure function of that one rule's immutable body, so never stale — and
the closure is **composed at dispatch time**. Deductive rules gain
their own reverse index: `db.rule/reads` `is` `on:<domain>/<name>`,
one entry per attribute the body names (written by the same
`Statement` install path, `Deduce`). Dispatch then chains per-rule
facts: touched attribute → `reads` probe → deductive rules whose
bodies read it → their conclusions' attributes join the touched set →
recurse. Monotone over a finite attribute set, so termination is
structural. A `shift/duty` commit expands to `actor.status/duty`,
probes the inbox rule, and the body's deductive resolution decides
whether the circumstance completed. A deductive rule installed *after*
the inductive one is picked up automatically — its `reads` entries are
in the next probe's path. The per-head footprint cache later absorbs
this walk exactly like the rest of discovery.

Conservative degradations across derived edges, both correct:

- **Polarity discrimination switches off.** Through negation, an
  assertion of `vacation/day` can *retract* derived on-duty status —
  base-change polarity no longer predicts derived-change polarity, so
  derived premises trigger on any change to their support attributes.
- **Constant discrimination weakens to attribute granularity** unless
  constants are pushed through rule bodies (magic-sets-style constant
  propagation — later, if ever).

One footgun to lint loudly: a premise transitively supported by the
built-in revision concepts is supported by `dialog.db/revision`, which
*every commit writes* — such a rule triggers on every commit. That is
semantically coherent ("react to any commit") but should be an
explicit opt-in, not a quiet cost.

Future precision: subscriptions already record **demand** — the base
ranges an evaluation actually read through deductive resolution. A
fired rule's demand could narrow the static closure (an
over-approximation) to the ranges that matter: static closure for
soundness before first firing, demand for precision after.

### Keeping dispatch flat in the number of rules

The footprint makes discovery O(touched attributes), but two places
still scale with rule population and need clamping:

1. **Per-attribute fan-out.** The `db.rule/on` probe returns *every*
   rule watching an attribute. Five hundred rules watching `job/status`
   — each pinning a different constant (`"done"`, `"failed"`, a
   specific entity) — would mean five hundred hydrations and body
   evaluations per matching commit, though at most a handful can fire.
2. **Head-advance maintenance.** Rebuilding the footprint by
   re-scanning the `on:` range at every head move is O(total rules),
   paid per commit on a busy branch.

The answer to (1) is a **dispatch funnel** — each stage strictly
cheaper than the next, with rule count reaching a stage only when its
rules genuinely might fire:

- *Stage 1 — footprint intersection.* O(touched attributes),
  in-memory; kills most commits outright.
- *Stage 2 — indexed probe*, only for intersecting attributes,
  head-cached: repeated commits on the same attributes at the same
  head never re-read the index.
- *Stage 3 — alpha discrimination.* At footprint-build time, compile
  per watched attribute a discrimination map over the hydrated
  candidates' *triggering premises*: candidates that pin a constant
  (value or entity) keyed by that constant in a hash map, wildcard
  candidates in a residual list. Matching a stimulus row is a hash
  probe plus the wildcard walk — O(delta rows), independent of how
  many constant-pinned rules watch the attribute. A rule watching
  `status = "done"` costs nothing when the delta writes `"failed"`.
  The map discriminates on **polarity** too: a positive premise is
  only newly enabled by an *assertion* of its matching value, an
  `unless` premise only by a *retraction* — half the probes on a
  watched attribute die on polarity alone. This is the alpha network
  of Rete, minus its stateful memories: nothing persists across
  commits, which is what fits dialog's branch model.
- *Stage 4 — the delta-join*, only for surviving (rule, row) pairs,
  and grouped: rules are content-addressed, so identical triggering
  premises are recognizable — the shared premise-against-delta match
  evaluates once and fans surviving bindings out to the individual
  bodies. Store I/O happens only for rules whose trigger matched a
  changed row.

Net cost per commit ≈ O(touched attributes + actual firings), with a
hash probe per (delta row, hot attribute) as the ceiling for
everything that doesn't fire.

**Worked example — inbox and duty status.** `assert! task when
inbox/message{actor: ?a}, actor/status{this: ?a, status: "on-duty"}`,
with a message present but the actor off duty — a half-satisfied join,
the shape where "waiting" could be expensive. What each commit costs:

- *Unrelated commit*: neither watched attribute touched — the rule is
  never probed. There is **no polling of pending conditions**; nothing
  records "waiting on status" as runtime state. The `on:actor/status`
  index entry *is* that knowledge, stored declaratively, free until
  relevant.
- *New message, still off duty*: probed via `on:inbox/message`, body
  evaluates, join dies on the status premise. One bounded failed
  evaluation, only on commits touching a watched attribute.
- *Actor goes on duty*: probed via `on:actor/status`, the join
  completes against the message already sitting in the store — the
  rule fires at exactly the commit that completes the circumstance,
  however much earlier the message arrived. No queue, no scheduler.
- *A different actor's status changes*: probed; the prototype re-runs
  the full join and the novelty check drops re-derivations. Alpha
  discrimination kills the non-`"on-duty"` writes before evaluation;
  delta restriction narrows the rest to the changed actor's messages.

The final notch — remembering the partial match itself so the status
change completes it without re-joining — is Rete's beta memory, and
the design declines it (state that must survive commits, branches,
and merges); re-derivation from indexes at the commits that matter is
the stateless answer, with the subscription fixpoint continuation as
the unification point if cross-commit join state ever proves out.

Note the semantics the example also fixes: when the actor goes *off*
duty, the asserted tasks stay — inductive heads are transitions, not
memberships. "Task exists only while on duty" is a deductive rule (a
view), or a paired `retract!` rule.

For (2): the head-advance tree diff (already computed for subscription
maintenance) names exactly which `db.rule/*` facts changed, so the
footprint and discrimination maps update incrementally in O(rule
churn) — a full rebuild happens only on cold open, and hydration is
content-address-cached, so even that is one-time per rule. The whole
structure is a pure function of the content-addressed rule set, so
branches with identical rule sets can share it.

Deliberately deferred: encoding constants into the storage-level index
key (`on:<attr>?is=<hash>`) — it bloats the index and forces
wildcard-plus-specific double probes for a discrimination the
in-memory map does better; revisit only if a single attribute
accumulates enough rules that hydrating them once per head is itself a
burden. Likewise Rete-style beta memories (persistent join state
across commits): dialog already has that shape in the subscription
fixpoint continuation, and if delta-joins ever warrant cross-commit
state, unifying with that machinery is the path rather than a parallel
structure.

### Firing locality: commit-time only, fire-forward only

Two deliberate restrictions, both inherited from tonk and kept even
though durable triggers would make the alternatives tempting:

- **Pull does not induce** *(v1 stance — superseded by the watermark
  model, below).* Rules fire at the peer that *commits* the
  triggering change; the derived facts then replicate as ordinary
  facts. This is the conservative simplification: it avoids
  double-firing and never evaluates a body over a slice the replica
  doesn't hold — but it is structurally blind to completion-by-merge,
  and the watermark model ("every head advance is an instant")
  resolves both concerns more precisely: novelty dedups deterministic
  heads across replicas, and the CALM/seal criterion — not the pull
  boundary — is what actually gates non-monotone bodies on partial
  slices.
- **Installation is not retroactive.** Installing a rule does not fire
  it against pre-existing matching state — triggers fire on changes,
  and installation changes only `db.rule/*`. Backfill is an explicit
  act: a one-shot transaction (or command-triggered rule) that touches
  the relevant state, or a deductive rule when the conclusion should
  simply *be* the view of existing facts. This is also what keeps
  branch-open cheap: no scan for a backlog of pending triggers.

The line between the two kinds stays sharp: **deductive rules answer
"what is true now" retroactively and everywhere; inductive rules answer
"what happens next" at the commit that causes it.**

### Caching

Same disciplines as deductive resolution, extended:

| Cache | Key | Invalidation |
|---|---|---|
| Trigger footprint | head Revision | head advance |
| Trigger discovery | `(on:<attr>, head Revision)` | head advance |
| Transience marker | `(concept, head Revision)` | head advance |
| Hydrated bodies | rule entity (content-addressed) | never stale |
| Plans | `(rule.this(), Adornment)` | never stale |

Overlay slices (rules, markers, or trigger entries staged in the open
transaction) are never head-cached and are read fresh — the same
structural exclusion of the masking bug that
`layered-rule-resolution.md` documents.

### Retract polarity

`InductiveRuleDescriptor` grows the `retract!` head as a sibling of
`assert!` (exactly one present). Same analysis pipeline, same
fully-bound-head requirement — a retract head must bind every field to
identify the cells to dissociate. Emission dispatches on polarity and
cardinality as tonk's `emit_head_facts_into` / `retract_head_facts_into`
do: assert heads use `Replace` for cardinality-one (supersede in place)
and `Assert` for many; retract heads dissociate exact `(the, of, is)`
triples. Without `retract!` there is no dequeue or cleanup, so this is
not optional.

### The host seam

External effects (network IO, keygen — tonk's typed-Rust
`CommandRegistry`) stay out of dialog core. The seam is the commit
receipt: `perform` returns the consumed transient batch, round count,
and derived novelty, so a host can run its post-commit dispatchers
against the same command facts without re-deriving them. Tonk's
`match_transients` maps onto this directly.

## Queues and friends

With durable triggers the patterns split into two families:

**Pure durable — no commands anywhere.** Queue promotion as a standing
machine over replicated state:

```yaml
rule!:
  assert!: job/active            # {this: ?j}
  when:
    - assert: job/pending
      where: { this: ?j, order: ?o }
  unless:
    - assert: job/active         # at most one active job
      where: { this: ?any }

rule!:
  retract!: job/pending          # activation consumes pending-ness
  when:
    - assert: job/active
      where: { this: ?j }
    - assert: job/pending
      where: { this: ?j, order: ?o }
```

The first rule fires when a pending job is asserted *or* when
`job/active` is retracted (the `unless` premise's `on:` entry catches
the retraction — this is why `unless` premises are indexed). Completing
a job = retracting `job/active`, which triggers promotion of the next
pending job in the same commit's cascade. No command, fully
replicated, works wherever the completing write commits.

**Command-driven — transients where ephemerality is the point.** The
tonk idioms carry over unchanged:

- *Mailbox with ack*: durable message (it must replicate), transient
  ack, consumption as `retract!: message when ack{target: ?m},
  message{this: ?m}`.
- *Order lens*: membership in an ordered collection is one
  cardinality-one attribute wrapped in its own narrow concept
  (`wiki/page-order`); enqueue/reposition are `assert!` on the lens,
  dequeue is `retract!` triggered by a delete command.
- *Staged pipelines*: a rule emits a transient head that another rule
  consumes next round; work queues within a commit, bounded by
  `MAX_ROUNDS`.

Choosing between the families is now a semantic decision, not a
performance one: use a command when the trigger should be ephemeral,
local, and non-replicating; use durable triggers when the machine
should run wherever the state change commits.

## Prior art

Every efficient system in this space **incrementally maintains derived
state**; this design re-derives lazily, and the reads-closure is the
tax for that. What each ancestor contributes:

- **Rete** (OPS5 → Drools): the alpha network is our discrimination
  stage, and the compiled network topology is the ahead-of-time form
  of "which rules can this change affect." Rete never faces the
  derived-premise problem because it materializes everything — Drools'
  `insertLogical` + truth maintenance turns a deductive rule into a
  production whose conclusions auto-retract when support fades, after
  which derived changes trigger like any others. The price is beta
  memories plus justification state. **TREAT** (Miranker) is the
  stateless counter-position we took — no join memories, recompute
  from indexed state on activation — and it frequently won under high
  churn.
- **Dedalus**: the semantics we already borrowed (`P@next :- body,
  ¬P@now`; commit = instant). An inductive premise on a derived
  predicate means "read at this instant's deductive fixpoint" — which
  legitimizes lazy resolution in the body — but the formalism has no
  efficiency story.
- **Bloom/Bud**: the operational answer under in-memory assumptions —
  each tick is stratified semi-naïve evaluation where *derived*
  collections' deltas fall out of the incremental fixpoint and flow to
  consumers. The trigger question dissolves into dataflow. Its
  full-strength modern form is **differential dataflow / DBSP**
  (persistent arrangements = industrialized beta memories); the
  `dialog-dbsp` crate is an unwired sketch of exactly this.
- **Active databases** — HiPAC, Starburst, and especially Hanson's
  **Ariel** (A-TREAT: a discrimination network over a persistent
  store with *selective* memory materialization) — are the closest
  problem statement: triggers whose conditions span joins over
  durable data.
- **Magic sets / demand transformation**: the established machinery
  for pushing constants through derived premises (the
  constant-discrimination gap across derived edges); dialog's
  adornment infrastructure is already half of it.

What none of them face: branching, merging, partial replication, and
rules as content-addressed facts. A beta memory or arrangement would
have to fork with a branch and reconcile on merge. Hence the outlier
position — stateless closure-based dispatch as the sound default —
with the pragmatic endgame the prior art suggests: when a specific
rule's re-join provably hurts, give *that rule* materialized support,
either Drools-style (materialize the derived concept) or DBSP-style
through the subscription fixpoint continuation, the arrangement-shaped
state dialog already maintains per head.

### The store is the arrangement

The DBSP comparison sharpens into a reframe rather than a rejection.
Of the two reasons not to adopt it, "the circuit is static and our
rules are data" is the weaker: Materialize instantiates differential
dataflows dynamically per view (backfill from a snapshot, then
stream), and because rules here are content-addressed facts, compiled
dispatch artifacts — plans, discrimination maps, closure edges — are
content-addressed too: recompilation on rule arrival is O(rule churn)
and the results are shareable across branches and replicas. The
objection that holds is **state**: an arrangement is a function of
complete input history, which partial replication falsifies (missing
≠ absent), demand-widening would have to backfill retroactively, and
fork/merge of arrangements is undefined.

But an arrangement is just an indexed collection of tuples — and the
EAV/AEV/VAE prolly trees *are* that, already CoW-forkable, already
demand-replicable, already the merge substrate. **Dialog is DBSP
where the only arrangements are the base indexes** and intermediate
results are recomputed per firing — TREAT's position exactly. The
gap opens only for deep multi-way joins and aggregates, where
selective per-rule materialization re-enters.

### Scaling roadmap, in order

1. **Bud's evaluation discipline**: semi-naive delta-joins — stimulus
   rows bind the triggering premise, remaining premises probe the
   tree indexes. Cost per firing ∝ delta join fan-out, never relation
   size; the prolly tree plays the role Bud's in-memory collections
   play.
2. **Dispatch caches** (footprint, discrimination, reads-closure),
   head-keyed, recompiled on rule churn, content-address-shared.
3. **Parallel candidates**: frozen-round semantics — chosen for
   determinism — make sibling evaluation within a round embarrassingly
   parallel for free.
4. **CALM as the replication-safety criterion**: a monotone body (no
   `unless`) evaluated over a partial replica is sound but incomplete;
   a non-monotone body needs the slice to be *authoritative* for its
   negated ranges. So: a rule is safely evaluable at a replica iff its
   demand covers the body's positive ranges and is authoritative for
   the negated ones. And a rule's body **is** a demand expression — a
   replica hosting a rule subscribes to its support ranges. Rules
   generate their own demand; demand-based replication is what makes a
   partial replica *sufficient* for the rules it hosts.
5. **Selective materialization** only where a specific re-join
   provably hurts, per rule, via the subscription continuation.

### The propagator lens

The system is a propagator network read off the database (Radul &
Sussman): cells = (entity, attribute) pairs; a propagator's
attachment list = `db.rule/on`; the alert queue drained to quiescence
= the induce loop; "did the cell gain information" = the novelty
check; quiescence = empty stimulus. Propagator networks need no round
bound because cell merges are monotone moves up a finite information
lattice; dialog's cells are not all monotone (`Replace` is an LWW
register, retraction is anti-monotone) — which is exactly why
`MAX_ROUNDS` exists. The divergence from the classic design: their
network is wired in memory ahead of time; ours is a **view of the
rule facts per head**, rewired by rule churn.

The lens points forward too: propagator cells hold *partial
information* merged monotonically — Bloom^L's lattice move. Typed
per-attribute merge semantics (sets, counters, causal LWW) would
widen both the CALM-safe zone for replication and the
guard-free-termination zone for rules; that converges with the
existing divergence-clock and merge work rather than adding a new
track.

### Monotonicity: exploit the layer that already has it

Dialog does not need to *become* monotone — the layering already is:
the history index is an append-only log of caused claim records and
revision records (a grow-only set), and the active EAV/AEV/VAE
indexes are a *fold* over it — the current frontier. Dropping
superseded values from active indexes is view maintenance, not
information loss; the supersession survives in history *as a fact*.
Likewise the non-monotone semantics are already lattices in disguise:
a cardinality-one attribute with causal supersession is a causal
register (monotone in version even as the readout flips); caused
retraction over cardinality-many is an OR-set. The opportunities are
about declaring and exploiting this, not restructuring:

1. **Let rules read the monotone face.** Every non-monotone state
   change has a monotone shadow: "X was retracted" is anti-monotone
   over active state but a positive, append-only fact in history —
   Dedalus's `del`-event move, with the event facts already
   materialized and indexed. A rule premised on a history/event
   concept (the built-in `Revision` projections are the precedent)
   instead of an `unless` over active state becomes monotone: it
   enters the CALM-safe zone for partial replicas, and polarity
   discrimination works again because only assertions of event facts
   exist.
2. **Declare per-attribute merge types** (Bloom^L): causal register
   and OR-set as the defaults dialog effectively has, plus counters,
   min/max, bounded enums, thresholds. Branch merge becomes pointwise
   lattice join computed from history causes — deterministic and
   coordination-free — and threshold guards (`when count >= 3` over a
   grow-only counter) are monotone where absence guards are not:
   prefer thresholds over absence wherever the quantity only accrues.
3. **Termination needs height, not just monotonicity.** An
   inflationary rule over an infinite-height lattice (`?c + 1`; a
   causal register's ever-advancing version) still diverges. Typed
   lattices therefore don't retire `MAX_ROUNDS`; they give the
   productivity lint a *sound green zone*: a rule concluding only
   finite-height cells (booleans, bounded enums, sets over bounded
   domains) is provably quiescent at install. Classify, don't force.

Unifying observation: `unless` is sound locally because the frozen
round view is a **seal** — the instant is known-complete. The
replication criterion ("authoritative for negated ranges") is the
same seal distributed: "range complete through context C," with the
divergence clock as the natural watermark carrier. Frozen rounds,
branch heads, and replication authority are one concept at three
scopes; making sealing first-class lets non-monotone rules run at
replicas exactly when their negated ranges are sealed.

### Event premises are instant-scoped, and evaluate against the stimulus

The mechanism for "rules read the monotone face" is *not* scanning
the history region: an event premise over all of history would
re-match every old event at every probe (novelty would suppress the
duplicates at O(history) cost). The right semantics is Dedalus's
`del@now`: **an event premise refers to the current instant's events
only** — and the instant's events are exactly the round's stimulus.
So the implementation reifies the round's instruction delta as one
more queryable overlay layer (the same mechanism the transient
overlay uses), with event-shaped facts per touched attribute; an
event premise queries that layer and never touches stored history.
History remains the durable record of the same events for audit and
retroactive deductive queries — a different consumer.

The dispatch entries for event premises carry polarity in the key
(`on:retract:<attr>` / `on:assert:<attr>`), which gives polarity
discrimination at the index level for free. Cascade cleanup phrases
positively — `retract! session/token when
retracted(user/active){of: ?u}, session/token{user: ?u}` — no
`unless`, so the body is monotone: CALM-safe and it fires exactly at
the commit containing the retraction. The unifying framing: **the
transaction's own delta is a system-provided transient relation** —
tonk turned DOM events into transient facts; this turns every
commit's instructions into them.

Event premises do **not** retire `unless`. The taxonomy: positive
premises are state *presence*, `unless` is state *absence* (a
standing condition — the queue's "nothing active," every idempotence
guard; no event formulation expresses it), events are *transitions*.
Events take over only the transition-shaped work absence was doing
badly; the non-monotone zone shrinks, its core remains.

Commands unify into this picture as sugar: tonk's sweep is a system
rule `retract! C when C` installed by the `transient` marker; the
engine's transient bucket is then an optimization (skip the
assert-and-cancel history noise), not a semantic primitive.

### The watermark model: every head advance is an instant

Commit-time-only induction has a structural blind spot:
**completion-by-merge is nobody's commit.** Replica A asserts P,
replica B asserts Q, and the body P ∧ Q first becomes true at the
merge — a pull, which does not induce, so the rule never fires
anywhere.

The fix generalizes "expose the revision": every datum is already
version-tagged, so induction keeps a **durable per-branch watermark**
— the last revision through which rules have evaluated — and the
stimulus is always *facts with version in (watermark, head]*. A
commit advances the head by its own delta; a pull advances it by the
merged-in novelty; both flow through the same induce and advance the
watermark. Consequences:

- Completion-by-merge fires exactly where the conjunction first
  exists. "Fires at the committing peer" becomes "fires at every
  replica as facts arrive," idempotently.
- `asserted:` / `retracted:` premises are version-range constraints
  against already-tagged data; the in-memory round delta is an
  optimization of the range read.
- The obligation pattern needs no host bridge: a marker arriving by
  pull advances the watermark and triggers its consumer natively.
- Crash-safety and resumability fall out: watermark behind head ⇒
  catch up at next open. (This is differential dataflow's *frontier*
  / a consumer offset — reconverging with DBSP vocabulary from the
  stateless side.)
- Cross-replica double-firing largely self-resolves: replicas derive
  the *same* head triples from the same bindings and the novelty
  check dedups on merge — deterministic heads are convergent.

Still gated, watermark or not: `unless`-rules at partial replicas
(the CALM/seal criterion decides who may evaluate absence), and
external side effects at the host seam need receipt-dedup, since two
replicas may both observe a firing.

This supersedes the blanket "pull does not induce" stance: the
principled statement is **induction follows the watermark, and every
head advance — commit or pull — is an instant**.

**Across replicas, edge-triggered effects do not self-heal.** An
event happens at one commit at one site; if that site's slice misses
join partners (tokens for the user held elsewhere), the effect is
partial *permanently* — no later commit re-presents the trigger.
Layered answer, weakest to strongest:

1. *Demand-fetch at fire time*: the commit path already reads through
   `NetworkedIndex`, and a body's evaluation is a demand expression —
   the join can fault in the missing range when connected, sound to
   the upstream's authority (the seal, again).
2. *Convert edge to level*: the event mints a small durable monotone
   **obligation** (`retracted(user/active){of: ?u}` ⇒ `assert!
   cleanup/pending{user: ?u}`), which replicates like any fact; a
   level-triggered consumer (`retract! session/token when
   cleanup/pending{user}, session/token{user}`) fires wherever
   obligation and token meet. Self-healing, idempotent (observed-
   remove: a concurrently asserted token survives add-wins and the
   standing obligation catches it later). Events are local;
   intentions are replicated state.
3. *…which is where pull-does-not-induce must grow an exception*: an
   obligation arriving by pull won't trigger the consumer under the
   blanket rule. Dedalus adjudicates — cross-site flow is `@async`;
   arrival is an instant at the receiver whose rules may run. The
   principled form: per-rule placement (`at: commit` default,
   `at: replica` opt-in) admitting **certified-idempotent rules
   only** to induce on pull — the green-zone certification's second
   job. Classify, don't force, again.

### Merge types extend `cardinality`; counters are contribution sets

`cardinality` is already the merge-type field: `one` is a causal LWW
register, `many` an OR-set — its first two lattices. Counters and
aggregators extend the same axis rather than overloading `as:` (the
value type — the readout stays an integer):

```yaml
attribute: &votes
  the: poll.option/votes
  as: UnsignedInteger
  cardinality: sum        # one | many | sum | max | min | count …
```

A `sum` attribute stores **contribution facts** (cardinality-many
under the hood); the value is a readout fold over them. Branch merge
is then set union of contributions — the existing many-merge, no
numeric merge path, PN-counter semantics with negative contributions.
A rule head "incrementing" emits a contribution whose entity is
content-derived from (rule, bindings): re-derivation produces the
same fact and novelty dedups it structurally — exactly-once per
distinct trigger. Threshold guards over sums are monotone. The
engineering lift is the readout: aggregation in the query engine
(fold over the contribution scan; incremental maintenance later).
`avg` is not a lattice but `(sum, count)` is, divided at readout.
Merge types live inside content addresses, so new types are for new
attributes.

### The quiescence green zone is a syntactic check

Monotone is not enough for guard-free termination — the lattice also
needs **finite height** (`?c + 1` is inflationary and never
arrives). But finite height reduces to Datalog's classic condition,
**no value invention**: a head that only recombines premise-bound
values has a finite reachable fact space, and inflationary
derivation over it must quiesce. Install-time certification is a
taint walk over the existing `AnalyzedRule` dependency graph:

- **proven** — durable, assert-polarity head (transient and retract
  heads excluded: expiry and removal are the anti-monotone devices)
  with no head variable tainted by a formula output. No guard
  needed; exempt from the round budget.
- **guarded** — carries the `unless`-own-head idempotence guard;
  quiescent by construction.
- **bounded** — everything else, under `MAX_ROUNDS`, with the lint
  naming the taint path ("`count` flows through `math/sum` and the
  rule reads `counter/count`").

The merge-type design feeds this: an increment written as a
`cardinality: sum` contribution has no formula-tainted head — the
arithmetic moved into the readout — so the counter migrates from
`bounded` to `proven` by changing where the addition lives.

## Divergences from tonk's implementation

| tonk | dialog native | why |
|---|---|---|
| Rules fire only from the transient bucket; transient premise required by convention | Full commit delta seeds dispatch; transient premise optional | durable-state machines (queues) expressible without synthetic commands |
| Zero-cost gate = empty transient bucket | In-memory trigger-footprint intersection | preserves O(touched attributes) cost now that durable writes can trigger |
| Termination = no transients emitted | Termination = no novelty (delta-empty), `MAX_ROUNDS` backstop | durable novelty can enable further rules |
| Durable `on` entries indexed but dead | Load-bearing, including `unless` (retraction-enables) | this *is* the durable trigger tier |
| Transients integrated then swept via inverse instructions | Separate bucket, never committed | no cancellation trick; true no-op commits; simpler crash story |
| Effects share discovery machinery but use `dialog.effect/*` | `db.rule/{source,induces,on}` | one rule store; `induces` kept separate from `conclusion` so deductive queries never touch effects |
| `dialog.effect/polarity` fact | polarity in the descriptor (`assert!`/`retract!` field) | source is decoded anyway; content address already distinguishes |
| Reactor in `dialog-reactor::Commit::perform` (host) | step 0 of dialog's `Commit::perform` | native means no host loop; atomic with the seal |
| `effect:system` well-known anchor | not adopted | tonk itself documents it as vestigial convention |

## Prototype

The core loop is implemented:

- `dialog-query`: `InductiveRule` gained `encode`/`decode`/`this`
  (content addressing, mirroring `DeductiveRule`).
- `dialog-repository/src/rules.rs`: the `db.rule/induces` and
  `db.rule/on` conventions, `on`-entity derivation from concept
  premises (`when` + `unless`), and the `Induct` / `Transient`
  statement wrappers.
- `dialog-repository/.../transaction.rs`: `Transaction` gained a
  transient bucket and `dispatch()`; `commit()` returns a
  `TransactionCommit` that runs induction before delegating to
  `Branch::commit`. Transients layer into `tx.query()`.
- `dialog-repository/.../transaction/induce.rs`: the round loop —
  touched-attribute probe of `db.rule/on` against the layered
  transaction view, content-addressed hydration, body evaluation via
  the standard planner/evaluator, head emission by cardinality,
  `db.concept/transient` head routing, per-instruction novelty check,
  `MAX_ROUNDS = 16`. End-to-end tests cover the command-triggered
  increment, durable triggering with an `unless` guard enabled by a
  retraction, a cascade through a transient intermediate, runaway
  divergence, dispatch selectivity, and the unconsumed-command no-op.

The deductive-support closure is implemented: `Deduce` installs
deductive rules with `db.rule/reads` reverse-index entries, and
dispatch closes the touched set over them
(`Dispatch::expand_through_deduction`) before probing — pinned by the
`it_triggers_through_a_deductive_premise` test, which runs the
inbox/duty scenario with the status derived and verifies the
`shift/duty` base write reaches the inductive rule.

Build-order items 1–3 are implemented (see "Decisions and build
order"): `retract!` polarity, the head-keyed dispatch caches behind a
`Dispatch` handle, and delta-restricted evaluation
(`fire_seeded` / `premise_attrs` with the full-body fallback).

Not yet implemented (documented above): event premises and the
watermark model, alpha discrimination, `cardinality: sum`, the
quiescence lint, `at: replica` placement, and the commit receipt.

## Open questions

- **Delta-restriction mechanics.** Reusing the subscription
  maintenance path (`Application::restrict` + `affected_entities`) vs.
  binding stimulus rows directly into the triggering premise. The
  latter is more precise (row-level, not entity-level) but new
  machinery; the former exists today.
- **Notation for `dispatch`.** The formal notation needs a way to mark
  a write transient (tonk tags at the wire: `{"kind": "transient"}`).
  Probably a `dispatch` sibling of `assert` in the transaction
  notation rather than anything on the concept reference.
- **Backfill API.** Is fire-forward-only + manual backfill enough, or
  should there be an explicit `induce(rule)` operation that runs one
  rule against current state once, transactionally?
- **Footprint representation.** Exact set vs. bloom filter; whether it
  should live beside `node_cache`/`RuleCache` on `Branch` (it should)
  and how it folds in overlay-staged rules (read fresh, like all
  overlay slices).
- **Receipt shape.** Minimum viable: `(transients, rounds, novelty)`.
  Whether novelty should be distinguishable from user-staged changes in
  the receipt depends on host needs.
- **`MAX_ROUNDS` value.** Tonk uses 16; a config knob on the branch (as
  with cache sizes) seems right.
