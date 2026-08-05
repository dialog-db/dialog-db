# Inductive Rules: Transient Triggers and Commit-Time Dispatch

Design for native inductive-rule support in dialog. Distills what tonk
built in userland (`tonk-evaluator`, `dialog-reactor`, `plan/effects.md`
in the tonk tree) into dialog primitives, keeps the performance property
that makes it viable — **a commit that dispatches no commands does zero
rule work** — and hardens tonk's convention into an invariant: **every
inductive rule must have at least one transient premise**.

The mental model is database triggers: rules are triggers, transient
concepts (commands) are the events, and a stored reverse index is the
trigger catalog. Dispatch is a point lookup keyed by the event, never a
scan over all rules.

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
  entry per attribute named by any concept premise. The `on:` key is
  derivable from a runtime `Changes` instruction alone (the attribute
  name), with no schema lookup.
- The induce loop (`tonk-evaluator/src/effects.rs`) seeds its stimulus
  **exclusively from the transient bucket**. Durable-only commit ⇒ the
  loop body never runs ⇒ zero index probes, zero rules loaded. With
  commands present, the probe set is the handful of attribute names the
  commands touch; each is a one-hop `dialog.effect/on` lookup. Rules
  not naming those attributes are never loaded, deserialized, planned,
  or evaluated.
- Rounds are semi-naive with frozen input: all candidates in a round
  read identical state; outputs batch-integrate once. Transient heads
  become the next round's stimulus; termination is "no transients
  emitted" (sharper than "delta empty", and sound precisely *because*
  every useful rule has a transient premise). `MAX_ROUNDS = 16` guards
  ping-pong and self-feeding cascades.
- Command facts never persist: tonk integrates them into the
  transaction (so rule bodies can read them), then sweeps them by
  emitting inverse instructions that cancel at commit. They never reach
  the tree and never replicate.
- The "rule must have a transient premise" check (`validate_effect`)
  exists but is **not wired into the install path** — it holds by
  convention, and structurally (a transient-less rule installs fine and
  simply never fires).

## Goals

1. Inductive rules evaluated natively at commit, no host reactor
   required.
2. Preserve the gate: zero rule work on commits with no active
   commands; dispatch cost proportional to the commands' attribute
   footprint, not the rule population. Rules behave like DB triggers —
   discovered by event, never bulk-loaded.
3. **Require** a transient premise in every inductive rule, enforced,
   so the gate is an invariant rather than a convention — while keeping
   queues, mailboxes, and ordered collections expressible.
4. Transients as a first-class transaction citizen: no sweep trick, no
   phantom commits.

## Design

### Transience is a concept property, stored as a fact

```
db.concept/transient  of  concept:<hash>  is  true
```

Declared when a command concept is installed; not part of the concept's
content address (following tonk: same shape ⇒ same entity, transience
is a branch-level property). Keeping it out of the descriptor also
keeps `InductiveRuleDescriptor` self-contained and portable — the same
rule source is valid on any branch that declares the trigger concept
transient.

Lookup is one `AttributeQuery`, resolved against the transaction view
so a command declared in the same commit as its first use works.
Cacheable per `(concept, head Revision)` with the `RuleCache`
discipline; the overlay slice is read fresh.

Granularity is per-concept, matching heads (a rule's head concept is
either transient or durable) and matching declarations (`command!` in
tonk notation). Attribute-level classification is *not* needed at
transaction time because bucketing is decided by how the write enters
(below), and the trigger index is an over-approximation that the body
match makes precise.

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
  one more layer. Rule bodies (and ordinary mid-transaction queries)
  see commands exactly as tonk's integrate step provided.

This replaces tonk's integrate-then-sweep: transients never enter the
durable batch, so nothing cancels at commit, nothing needs inverse
instructions, and a command that derives no durable novelty produces
**no commit at all** (the existing empty-commit short-circuit applies)
— unlike tonk, where a transient-only commit mints a revision with an
identical tree hash. Crash semantics are unchanged from tonk's: an
unprocessed command leaves no partial trigger; there is no cold-start
backlog at branch open.

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
  trigger index. One entry per attribute named by any concept premise
  (`when` **and** `unless`), derived syntactically from the descriptor
  exactly as tonk's `Effect::on_entities` does.

Deriving `on` entries from *all* concept premises (not just the
transient ones) is deliberate: it keeps `impl Statement for
InductiveRule` synchronous (no transience lookup at write time), and
the durable-premise entries — dead weight for dispatch today, since the
probe set only ever contains dispatched attributes — are exactly the
index a future durable-change trigger tier would need. The
mandatory-transient invariant is enforced separately (below), not by
the index shape.

`impl Statement for DeductiveRule` should land alongside (the
layered-rule-resolution note already claims `tx.assert(rule)` works;
today callers stage the two facts by hand).

### Enforcement: the mandatory transient premise

Two layers, both in dialog-repository (dialog-query cannot check this —
transience is branch data, not descriptor data):

1. **Commit-time validation.** A commit that installs an inductive rule
   (stages `db.rule/induces`) verifies, against the transaction view,
   that at least one *positive* `when` premise's concept carries
   `db.concept/transient`. Failure fails the commit with a dedicated
   error — this is tonk's `validate_effect`, actually wired. Checking
   at commit rather than at `tx.assert` keeps `Statement` synchronous
   and lets the command declaration ride in the same commit.
2. **Structural.** As in tonk, the dispatch loop seeds only from the
   transient bucket, so even a rule that slipped past validation
   (e.g. its trigger concept's marker was retracted later) cannot fire
   from durable writes — it degrades to inert, never to expensive.

Positive premises only: a transient in `unless` suppresses a firing but
cannot cause one, so it satisfies neither the performance argument nor
the semantic one.

The self-negation idempotence guard (`assert! P when body unless P`)
continues to compile and count as it does today; the `unless P` reads
the frozen round input (pre-state), the head asserts into the next
state. The trivial tautology `assert! C when C` with transient `C` is
rejected at analysis; the same shape under `retract!` is permitted — it
is the mailbox-consumption pattern.

### Retract polarity

`InductiveRuleDescriptor` grows the `retract!` head as a sibling of
`assert!` (exactly one present). Same analysis pipeline, same
fully-bound-head requirement — a retract head must bind every field to
identify the cells to dissociate. Emission dispatches on polarity and
cardinality as tonk's `emit_head_facts_into` / `retract_head_facts_into`
do: assert heads use `Replace` for cardinality-one (supersede in place)
and `Assert` for many; retract heads dissociate exact
`(the, of, is)` triples. Without `retract!` there is no dequeue, so
this is not optional for the queue goal.

### The dispatch loop, in `Commit::perform`

`Transaction::commit()` carries `transients` into the `Commit` command.
`Commit::perform` gains a step 0, before the batch apply:

```rust
let mut stimulus = transients;
let mut round = 0;
while !stimulus.is_empty() {
    if round >= MAX_ROUNDS { return Err(CommitError::NonTerminatingInduction(round)); }
    round += 1;

    // 1. Probe keys straight off the instructions — no schema lookup.
    let touched: BTreeSet<Attribute> = stimulus.attribute_names();

    // 2. Trigger-indexed discovery: one db.rule/on lookup per touched
    //    attribute, against the tx view (layered: committed slice via
    //    the head-keyed cache, overlay fresh). This is the only place
    //    rules are discovered — nothing ever enumerates all of them.
    let candidates = probe_on_index(&touched, &view).await?;

    // 3. Hydrate (content-addressed cache) + plan (PlanCache) + evaluate
    //    each candidate body against the FROZEN round view.
    let (novelty, next_transients) = fire(candidates, &view).await?;

    // 4. Fold durable novelty into changes; transient heads become the
    //    next stimulus. This round's stimulus is simply dropped —
    //    transients never entered `changes`, so there is no sweep.
    changes.merge(novelty);
    stimulus = next_transients;
}
// ...existing perform: checkpoint, batch apply, revision, seal, publish.
```

Properties carried over from tonk, now guaranteed by construction:

- **Zero-cost gate.** Empty transient bucket ⇒ the loop is skipped
  before any provider call. A 10,000-fact durable commit does no rule
  work whatsoever.
- **Frozen rounds (semi-naive).** Sibling rules in a round read
  identical state; ordering among them is unobservable. Cross-round
  chaining happens through transient heads.
- **Termination = no transients emitted.** Sound because of the
  mandatory transient premise: a round that emits only durable facts
  cannot enable anything. `MAX_ROUNDS` backstops parameterized
  self-feeding cascades.
- **Head routing** consults `db.concept/transient` for the head concept
  (cached per head revision): marked ⇒ next stimulus, unmarked ⇒
  durable novelty.
- **Atomicity and observability.** One commit ⇒ one revision ⇒ one
  subscription notification with settled state; intermediate rounds are
  invisible. Since induction completes before the batch is applied and
  sealed, a failed induction aborts cleanly with nothing persisted.
- **Pull does not induce.** The hook is commit-only. Replicated facts
  arrive as facts; commands are local to the committing session. This
  is what keeps partial replication sound.

The index probe is an over-approximation (attribute granularity, and a
durable concept may share an attribute with a command); the body
evaluation is the precise filter. A false candidate costs one hydration
(cached) and one body evaluation that fails on its first premise.

### Caching

Same disciplines as deductive resolution, extended:

| Cache | Key | Invalidation |
|---|---|---|
| Trigger discovery | `(on:<attr>, head Revision)` | head advance |
| Transience marker | `(concept, head Revision)` | head advance |
| Hydrated bodies | rule entity (content-addressed) | never stale |
| Plans | `(rule.this(), Adornment)` | never stale |

Overlay slices (rules or markers staged in the open transaction) are
never head-cached and are read fresh — the same structural exclusion of
the masking bug that `layered-rule-resolution.md` documents.

### The host seam

External effects (network IO, keygen — tonk's typed-Rust
`CommandRegistry`) stay out of dialog core. The seam is the commit
receipt: `perform` returns the consumed transient batch, round count,
and derived novelty, so a host can run its post-commit dispatchers
against the same command facts without re-deriving them. Tonk's
`match_transients` maps onto this directly.

## Queues and friends

The mandatory transient premise does not restrict these — each already
has a command in the trigger position. The recipes, from tonk's
library:

**Mailbox with ack** — durable message (it must replicate), transient
ack, consumption as a retract rule:

```yaml
rule!:
  retract!: message           # {this: ?m, body: ?b}
  when:
    - assert: ack             # transient — the trigger
      where: { target: ?m }
    - assert: message
      where: { this: ?m, body: ?b }
```

The shared `?m` scopes the dequeue to one message. This is a durable
work queue with explicit consumption.

**Order lens** — membership in an ordered collection is one
cardinality-one attribute wrapped in its own narrow concept
(`wiki/page-order`). Enqueue and reposition are `assert!` on the lens
(Replace supersedes in place); dequeue is `retract!` on it, triggered
by a delete command. The entity's other facts persist; only membership
is consumed.

**Staged pipelines** — a rule emits a transient head that another rule
consumes next round (`cmd_a → cmd_b → durable target`). Work queues
within a commit, bounded by `MAX_ROUNDS`; every stage trivially
satisfies the transient-premise requirement.

Cross-peer queues need commands that replicate, which transients by
definition do not. That is future work (a durable command tier with
consumed-markers), and nothing here forecloses it: the `on` index
already covers durable premises.

## Divergences from tonk's implementation

| tonk | dialog native | why |
|---|---|---|
| Transients integrated then swept via inverse instructions | Separate bucket, never committed | no cancellation trick; true no-op commits; simpler crash story |
| `validate_effect` defined but unwired | Commit-time hard gate | the invariant the whole cost model rests on should not be a convention |
| Effects share discovery machinery but use `dialog.effect/*` | `db.rule/{source,induces,on}` | one rule store; `induces` kept separate from `conclusion` so deductive queries never touch effects |
| `dialog.effect/polarity` fact | polarity in the descriptor (`assert!`/`retract!` field) | source is decoded anyway; one less fact, content address already distinguishes |
| Reactor in `dialog-reactor::Commit::perform` (host) | step 0 of dialog's `Commit::perform` | native means no host loop; atomic with the seal |
| `effect:system` well-known anchor | not adopted | tonk itself documents it as vestigial convention |

## Open questions

- **Notation for `dispatch`.** The formal notation needs a way to mark
  a write transient (tonk tags at the wire: `{"kind": "transient"}`).
  Probably a `dispatch` sibling of `assert` in the transaction
  notation rather than anything on the concept reference.
- **Marker retraction semantics.** Retracting `db.concept/transient`
  while rules trigger on the concept leaves those rules inert
  (structurally safe). Should the commit staging the retraction fail
  instead if dependent rules exist, symmetric with install validation?
- **Receipt shape.** Minimum viable: `(transients, rounds, novelty)`.
  Whether novelty should be distinguishable from user-staged changes in
  the receipt depends on host needs (tonk's sync-dirty check compares
  tree hashes and would not need it).
- **`MAX_ROUNDS` value.** Tonk uses 16; a config knob on the branch (as
  with cache sizes) seems right.
