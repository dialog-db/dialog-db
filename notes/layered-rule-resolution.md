# Layered Rule Resolution

A concept query reads from a stack of **layers**, each providing both
**facts** and **deductive rules**. Facts are unioned across a branch's
tree, any joined branches, and the per-query overlay; rules are resolved
the same way. This note records how that works and why.

## Layers

Each layer in the stack is a query source:

- **Durable layer** — one per branch in scope. Facts come from the
  branch's committed tree; rules come from `dialog.rule/*` facts on that
  tree.
- **Transient layer** — the per-query `Changes` overlay (`.with(...)`
  and a transaction's pending writes). Facts and rules both come from
  the in-memory batch.

`QueryEnv` (`repository/branch/session.rs`) composes the stack: it holds
the branches + overlay and implements `Provider<Select>` (facts) and
`Provider<SelectRules>` (rules). A transaction query is just a
single-branch `QueryEnv`, so committed and mid-transaction reads share
one implementation and cannot diverge.

## Rule storage (`dialog.rule/*`)

A deductive rule is stored as these facts (see `rules.rs` and
`dialog-query`'s `rule/statement.rs`):

- `dialog.rule/derives` `of` rule-entity `is` `on:<domain>/<name>` — one
  per head attribute; the discovery index. "Which rules derive attribute
  a", whatever the rest of their heads look like. This is what makes a
  query over any subset of a rule's head see the rule; see
  [`attribute-level-deduction.md`](./attribute-level-deduction.md).
- `dialog.rule/conclusion` `of` rule-entity `is` concept-entity — the
  exact-head index, kept for tooling and for rules committed before
  `derives` existed (the legacy path below).
- `dialog.rule/source` `of` rule-entity `is` the rule body as canonical
  dag-cbor `DeductiveRuleDescriptor` (a `Value::Bytes`), hydrated with
  `DeductiveRule::decode`.
- `dialog.rule/reads` `of` rule-entity `is` `on:<domain>/<name>` — one per
  attribute the body reads; commit-time dispatch's support index.

The rule-entity is content-addressed:
`rule:<base58(blake3(dag-cbor(descriptor))))>` (`DeductiveRule::this`).
dag-cbor canonicalizes map keys, so the encoding is a pure function of
the descriptor even though a premise's terms come from a `HashMap` — no
manual key sorting. (Stored as `Value::Bytes` rather than
`Value::Record`: Record isn't yet supported end-to-end through the
index; the bytes are opaque to the query layer either way.)

These attribute names are a dialog-repository convention, like
`dialog.session/*` and `dialog.meta/*`.

## Resolution

`QueryEnv`'s `Provider<SelectRules>::execute(concept_descriptor)`, via
`resolve_bundle`:

1. Build the **implicit** per-descriptor rule once (`ConceptRules::new`).
   It reads the concept's attributes directly; it is not stored and has
   no content identity.
2. Per attribute of the descriptor, gather every rule whose head derives
   it: the built-in rules (indexed by head attribute), each branch's
   **durable** rules (`dialog.rule/derives = on:<attribute>` against the
   tree, hydrated), and the **transient** rules staged in the overlay
   `Changes`. Each rule is taken once, by content address.
3. Project each of those rules onto the descriptor
   (`dialog_query::rule::project`): the head becomes the descriptor, the
   shared attributes' variables take the descriptor's field names, and
   the descriptor's remaining attributes are read through their own
   single-attribute concept when something derives them, scanned
   otherwise. A rule concluding the descriptor exactly is its own
   projection.
4. **Legacy**: look up `dialog.rule/conclusion = concept` against each
   tree and the overlay, and install as an exact-head rule any rule the
   attribute index did not already return. A rule committed before the
   `derives` index existed keeps resolving for its exact head this way,
   and joins the attribute index once re-asserted.
5. Install the projections and the legacy rules onto the implicit one
   and return the `ConceptRules`.

Rule demand records one narrow slice per attribute of the descriptor
plus the legacy slice, so a subscription is woken only by rules that
derive an attribute it reads.

The single consumer is `ConceptQuery::evaluate`
(`dialog-query/.../concept/query.rs`): it calls `SelectRules`, then
`ConceptRules::plan(terms, match)` to get a `Disjunction`. Everything —
composition, caches — sits behind that one call.

## Caches

Two caches with different correctness disciplines.

**Discovery + hydration** — per branch, on `Branch` (`RuleCache`,
alongside `node_cache`; configured once per opened handle):

- *Discovery* ("which rule entities derive attribute a, committed", and
  the legacy "which conclude concept X") is keyed by attribute (or
  concept) and tagged with the branch head (`Revision`). A head advance
  — commit or pull — re-scans that key. Read from the tree only.
- *Hydration* (compiled bodies) is keyed by the content-addressed rule
  entity, so an entry is never stale and is reused across concepts and
  head changes.
- *Projection* (a rule projected onto a descriptor) is keyed by the rule
  entity and the descriptor's canonical bytes — a pure function of the
  two, so never stale.

The **overlay is never head-cached**: it does not move the head, so a
head-keyed "skip the scan" cache would mask an uncommitted `.with(rule)`.
Overlay rules are read fresh every query (cheap — in-memory). Because the
durable cache only ever holds the committed slice and the overlay is a
separate layer, an overlay rule cannot be masked by a stale committed
entry — the failure is structurally excluded.

**Plan** — `PlanCache` (`concept/query/plan_cache.rs`), keyed by
`(rule.this(), Adornment)` → `Conjunction`. Planning a rule for a binding
pattern is a pure function of `(rule body, adornment)`, so a plan is
reusable across every query and concept that uses the rule, including
ones that re-assemble `ConceptRules` from layers each query (where the
per-instance plan map is cold every query). Content-addressed ⇒ never
stale; the cache only bounds memory (SIEVE eviction, the same
`sieve-cache` the node cache uses). The implicit and any attribute-bodied
rule have no content identity (`try_this` returns `None`) and are planned
directly, uncached.

The cache is **not a process global**: it is owned by the `Branch` (beside
`node_cache` and `RuleCache`) and handed to each assembled `ConceptRules`,
so its lifecycle follows the branch. Peer branches in a multi-branch query
share content-addressed plans, so `execute` rides the first branch's cache
(a branchless overlay-only query falls back to a private one). A
standalone `ConceptRules::new` gets a private `PlanCache::default`.

*Soundness:* `Adornment` is a bitmask over alphabetically-sorted
parameter slots — independent of caller variable names — so
`(rule, adornment)` keys plans correctly even though
`Adornment::into_environment` binds caller names into the scope. A rule's
plan depends only on which of *its* parameters are bound, not the
caller's names. Proven by `it_plans_independently_of_caller_variable_names`.

## Writes

`tx.assert(rule)`, `tx.retract(rule)`, and `.with(rule)` all go through
the existing `Statement` impl that writes/removes the `dialog.rule/*` facts.
There is no separate rule-write path: the layer holding the facts
(committed → durable, overlay → transient) surfaces them via resolution.

## Tests

Cache and invalidation invariants are covered in `session.rs` (`mod
rule_tests`): committed resolves, overlay resolves, overlay resolves
after a prior query at the same head, head-move re-scan adds, retract
re-scan removes, distinct rule bodies don't share a hydrated body, a
stale handle keeps its cached discovery, multi-branch unions, overlay
rules don't leak into a later plain query, and no rules → empty.
