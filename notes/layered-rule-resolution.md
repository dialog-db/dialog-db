# Layered Rule Resolution

A concept query reads from a stack of **layers**, each providing both
**facts** and **deductive rules**. Facts are unioned across a branch's
tree, any joined branches, and the per-query overlay; rules are resolved
the same way. This note records how that works and why.

## Layers

Each layer in the stack is a query source:

- **Durable layer** — one per branch in scope. Facts come from the
  branch's committed tree; rules come from `db.rule/*` facts on that
  tree.
- **Transient layer** — the per-query `Changes` overlay (`.with(...)`
  and a transaction's pending writes). Facts and rules both come from
  the in-memory batch.

`QueryEnv` (`repository/branch/session.rs`) composes the stack: it holds
the branches + overlay and implements `Provider<Select>` (facts) and
`Provider<SelectRules>` (rules). A transaction query is just a
single-branch `QueryEnv`, so committed and mid-transaction reads share
one implementation and cannot diverge.

## Rule storage (`db.rule/*`)

A deductive rule is stored as two facts (see `rules.rs`):

- `db.rule/conclusion` `of` rule-entity `is` concept-entity — the index;
  "which rules conclude concept X".
- `db.rule/source` `of` rule-entity `is` the rule body as canonical
  dag-cbor `DeductiveRuleDescriptor` (a `Value::Bytes`), hydrated with
  `DeductiveRule::decode`.

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

`QueryEnv`'s `Provider<SelectRules>::execute(concept_descriptor)`:

1. Build the **implicit** per-descriptor rule once (`ConceptRules::new`).
   It reads the concept's attributes directly; it is not stored and has
   no content identity.
2. For each branch, gather its **durable** rules: look up
   `db.rule/conclusion = concept` against the tree, hydrate each body.
3. Gather **transient** rules from the overlay `Changes`.
4. Install the durable + transient rules onto the implicit one and
   return the `ConceptRules`.

The single consumer is `ConceptQuery::evaluate`
(`dialog-query/.../concept/query.rs`): it calls `SelectRules`, then
`ConceptRules::plan(terms, match)` to get a `Disjunction`. Everything —
composition, caches — sits behind that one call.

## Caches

Two caches with different correctness disciplines.

**Discovery + hydration** — per branch, on `Branch` (`RuleCache`,
alongside `node_cache`; configured once per opened handle):

- *Discovery* ("which rule entities conclude concept X, committed") is
  keyed by concept and tagged with the branch head (`Revision`). A head
  advance — commit or pull — re-scans that concept. Read from the tree
  only.
- *Hydration* (compiled bodies) is keyed by the content-addressed rule
  entity, so an entry is never stale and is reused across concepts and
  head changes.

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
the existing `Statement` impl that writes/removes the `db.rule/*` facts.
There is no separate rule-write path: the layer holding the facts
(committed → durable, overlay → transient) surfaces them via resolution.

## Tests

Cache and invalidation invariants are covered in `session.rs` (`mod
rule_tests`): committed resolves, overlay resolves, overlay resolves
after a prior query at the same head, head-move re-scan adds, retract
re-scan removes, distinct rule bodies don't share a hydrated body, a
stale handle keeps its cached discovery, multi-branch unions, overlay
rules don't leak into a later plain query, and no rules → empty.
