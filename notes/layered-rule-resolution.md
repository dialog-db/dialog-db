# Layered Rule Resolution (query sources provide facts AND rules)

Target architecture for deductive-rule resolution, converged 2026-06-26. Supersedes the `RuleSource`/`RuleClaims`/`Arc<dyn>` injection seam currently on `feat/rule-source` (which works but is the wrong shape — see "Why" below).

## The model

A query is a stack of **layers**. Each layer answers two questions:
- **facts**: `select(selector) -> stream of artifacts` (already exists — branch tree scan, Changes overlay).
- **rules**: `rules(concept_entity) -> Vec<DeductiveRule>` — the INSTALLED deductive rules this layer holds concluding that concept. NEW, symmetric with facts.

Today only facts are transposed/merged across sources (`QueryEnv` iterates `branches` + `changes`, unions via `merge_grouped`). Rules should transpose the SAME way: union each layer's `rules()`.

## The seam: revive `Source`, make it async

`dialog_query::query::source::Source` already = `ArtifactStore` (facts) + `acquire(&ConceptDescriptor) -> ConceptRules` (rules). It is currently dead (no impls; the Session/QuerySession that implemented it are gone). It is the layer trait's ancestor. Revive it:
- make `acquire` ASYNC (a durable layer reads the branch to find rules).
- (refinement) the per-layer method should be keyed by concept ENTITY and return INSTALLED rules only (`Vec<DeductiveRule>`), not a full `ConceptRules`. The implicit per-descriptor rule is built ONCE at the composition top (it's a pure fn of the descriptor; a layer keyed by entity can't and shouldn't build it). So either evolve `Source::acquire` to that shape, or keep `acquire(descriptor)->ConceptRules` at the COMPOSITION level and add a lower `rules(&Entity)->Vec<DeductiveRule>` on the layer trait.

## Typed layers (no `dyn`)

User wants concrete types, not `Box<dyn>`:
```
struct DurableLayer<'a> { branch: &'a Branch }   // facts: tree scan; rules: db.rule/conclusion from TREE, head-cached
struct TransientLayer    { changes: Changes }      // facts: Changes select; rules: db.rule/* from its own Changes
struct LayerComposition<'a> { durable: Vec<DurableLayer<'a>>, transient: TransientLayer }
```
`QueryEnv` becomes (or holds) a `LayerComposition`. Its `Provider<Select>` unions `select` across layers (as today via merge_grouped). Its rule resolution unions each layer's installed rules + builds the implicit once.

## Two caches, two disciplines

1. **Discovery** ("which rules conclude concept X") — PER-LAYER, owned by the layer.
   - DurableLayer: reads db.rule/conclusion from the TREE only (committed slice), cacheable by branch HEAD (evict on head move).
   - TransientLayer: reads from its Changes (cheap/in-mem), cache as it likes.
   - CRITICAL: overlay rules must NOT be cached against branch head — that was the (b) bug (overlay rule masked by a head-keyed cached-empty). Per-layer caches make this structurally impossible (the durable layer only ever caches the committed slice; the transient layer is the overlay).
2. **Plan** ("how rule R plans for adornment A") — GLOBAL, content-addressed.
   - Key `(rule identity, Adornment)`; value `Conjunction`. Adornment is name-independent (u64 bitmask over sorted param slots — confirmed in adornment.rs), so the key is sound and reusable across queries/concepts.
   - Replaces `ConceptRules`' per-instance `plans` map (which `install` clears — defeating caching once layers reassemble ConceptRules per query).
   - PREREQ: `DeductiveRule` has NO identity in dialog-query today (it lives in tonk-schema's storage wrapper as `rule:<base58(blake3(canonical-json(descriptor)))>`). The plan cache needs a stable identity INSIDE dialog-query. Move the canonical-JSON identity into dialog-query (comes naturally with moving the db.rule/* read path into dialog). NOTE: descriptor does NOT dag-cbor encode and serde_json is non-deterministic (HashMap term order) — must canonicalize (sort keys) before hashing. See tonk-schema deductive_rule.rs `content_entity`/`source_string` for the working impl to port.
   - Content-addressed ⇒ never stale; LRU evict only (memory, not correctness).
   - SOUNDNESS RESOLVED (2026-06-26): the `(rule.this(), adornment)` key IS sound. The concern was that `Adornment::into_environment` binds CALLER variable names into the scope, but the empirical probe `it_plans_independently_of_caller_variable_names` (concept/query/rules.rs tests) PROVES `rule.plan` produces identical `Conjunction`s for the same adornment regardless of caller var names. (Why: the rule's premises use the rule's OWN internal var names; the planner orders by which rule PARAMETERS are bound = the adornment; caller names are just bound-slot markers, not plan inputs.) So the global cache is safe to implement.

## Implicit rule boundary

Layers return installed rules EXCLUDING the implicit. Composition builds `implicit = DeductiveRule::from(descriptor)` once, then `[implicit] ++ union(layers' installed)`. Each looked up in the global plan cache (implicit has its own identity, caches uniformly).

## Writes (unchanged)

`tx.assert(rule)` / `tx.retract(rule)` / `.with(rule)` all ride the existing `Statement` impl on the tonk-schema DeductiveRule writer. The layer that holds those facts (committed → durable layer; overlay → transient layer) surfaces them via its rules(). No special write path.

## Why replace the current dyn seam

The shipped `RuleSource`/`RuleClaims`/`Arc<dyn>` + per-call `with_rules` works but: (a) it's dyn (user dislikes); (b) it's injected per-call so paths can diverge (caused the query-vs-evaluate inconsistency, fixed by unifying envs + Transaction::with_rules); (c) the cache asymmetry caused the overlay-rule bug. The layer model makes resolution always-on, dyn-free (typed layers), divergence-impossible (one composition), and the overlay-cache bug structurally impossible.

## Integration point (tiny)

`ConceptQuery::evaluate` (dialog-query concept/query.rs:~276) does `Provider::<SelectRules>::execute(env, descriptor)` → `rules.plan(&terms, &input)` → Disjunction. The ONLY use is `.plan(...)`. Everything (composition, discovery cache, plan cache) sits behind this one call. Replace `Provider<SelectRules>` with the composition's async `acquire`.

## Build order + STATUS (2026-06-26)

DONE (committed on this branch, all green, read-counts unchanged):
- [x] async `Source` (commit dd8eedf3) — trait is async, still no impls.
- [x] canonical rule identity in dialog-query: `DeductiveRule::try_this()`/`try_canonical_source()` (Option — None for implicit/attribute-bodied rules) + panicking `this()`/`canonical_source()` for the storage path (commit 77bc6ce3). Soundness of (rule,adornment) proven (it_plans_independently_of_caller_variable_names).
- [x] global per-(rule,adornment) plan cache: `concept/query/plan_cache.rs`, wired into `ConceptRules::plan` (commit 6f50a16d). Implicit rule planned directly (no identity). 692 tests pass.

REMAINING (the live-path rewire — left for review, browser-verify needed):
1. Typed DurableLayer{&Branch}/TransientLayer{Changes}/LayerComposition in dialog-repository; impl async Source / rule-resolution on them. (task #7 cutover)
2. Move db.rule/* read path into the DurableLayer (lookup db.rule/conclusion on the TREE + hydrate via DeductiveRuleDescriptor::compile, which is pure dialog-query). Identity already moved. Per-layer discovery cache: durable=by-head, transient=overlay (never head-cached). (task #9)
3. Rewire QueryEnv's Provider<SelectRules> + ConceptQuery::evaluate (dialog-query concept/query.rs:~276) to route through the composition instead of Arc<dyn> SelectRules. (task #7)
4. Delete RuleSource/RuleClaims/Arc<dyn>/with_rules/ReactorRuleSource + reactor wiring; tonk-schema keeps only the Statement writer + notation. tonk-schema's content_entity/source_string can now delegate to dialog's DeductiveRule::this/canonical_source. (task #10)

Then: full workspace build + tests + benches (read-count is the reliable signal; wall-clock noisy on a loaded machine) + browser verify search/candidate.

NOTE: the (b) fix (committed, tonk 8bf0b9f3) already makes the feature work correctly via the Arc<dyn> seam — the remaining (a) work is pure architecture (no behavior change), replacing the dyn seam with the layer model.
