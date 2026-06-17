# Storing Deductive Rules in the Database

## The Problem

Deductive rules ([rules.md](rules.md)) derive new concepts from existing data, Datalog-style. Today they exist only in memory: a Rust client opens a session and installs rule functions via `RuleRegistry::register`. The rules are compiled, kept in a `HashMap<Entity, ConceptRules>` keyed by the conclusion concept entity, and consulted at query time.

This does not work for a live, multi-client system where rules are authored as data (YAML `rule!` declarations) and need to be visible to any client querying the branch. We want rules stored in the branch like any other fact, found by the concept they conclude, and hydrated on demand.

The balance to thread: a client that does not care about rules must not pay a continuous lookup tax. The resolution must be cheap when there are no rules, amortized when there are, and entirely absent from the pure embedder that never opted in.

## Context: How Resolution Works Today

The query engine never reaches for rules directly. `ConceptQuery::evaluate` (`dialog-query/src/concept/query.rs`) does:

```rs
let rules = Provider::<SelectRules>::execute(env, app.predicate.clone()).await?;
```

`SelectRules` (`dialog-query/src/source.rs`) is a capability: `Command { Input = ConceptDescriptor, Output = ConceptRules }`. Whoever builds the `env` decides where rules come from. The engine is already source-agnostic and does **not** need to change.

Two production impls of `Provider<SelectRules>` exist in `dialog-repository`, and both ignore the DB:

- `QueryEnv` (`repository/branch/session.rs:325`) returns `Ok(ConceptRules::new(&input))` — the implicit per-descriptor rule only. Comment: *"the overlay holds facts only."*
- `TransactionEnv` (`transaction_query.rs:180` and `repository/branch/transaction/query.rs:215`) — same.

`QueryEnv` is built fresh per `.perform(env)` (`session.rs:216`) and already holds everything needed to query the branch for rules: `branches`, the per-query `changes` overlay, and the underlying `env`. Its `Provider<Select>` impl (`session.rs:292`) unions branch streams + the overlay. **The rule lookup must go through this same union** so that `with(rule)` (overlay-only, uncommitted) rules are visible alongside committed ones.

`ConceptRules::extend` (`concept/query/rules.rs:65`) already exists *"to combine two rule sources (a primary registry and an overlay)"* — merges only the `installed` set, since the implicit rule derives from the descriptor. Built for exactly this overlay.

## Design

### Storage shape

Mirror how inductive rules persist (`tonk-schema/src/rule.rs`: `dialog.effect/source` JSON + `/conclusion` reverse index + `/on` index, rehydrated by `EffectByEntity::resolve`). For deductive rules, index by the concept they conclude so a single lookup answers "are there rules for this concept?":

```
the: db.rule/conclusion   of: <rule entity>     is: <concept entity>   # reverse index, cardinality-many on concept? see below
the: db.rule/source       of: <rule entity>     is: <RULE_CBOR>        # the compiled body
the: dialog.meta/rule      of: <rule entity>     is: db:rule           # marker, enumerate all rules
```

**Single fact, keyed by concept:**

```
the: db.rule/<rule.this()>   of: <concept entity>   is: <RULE_CBOR>
```

The attribute name carries the rule's identity; `of` is the conclusion concept; `is` is the compiled body. One `of`-keyed select returns every rule for a concept *with its body inline* — no secondary fetch.

This collapses an earlier draft that split the index (`db.rule/conclusion`) from the payload (`db.rule/source`). That split reintroduced an N+1 (scan index for rule entities, then fetch a body per entity) for no benefit: there is no path where you learn rules exist and then *don't* want their bodies — if the scan returns N rules, you wanted those N bodies. The split would only pay off for "enumerate rule names without bodies," which is speculative. The single fact also makes identity, idempotent re-assert, and content-addressing fall out of the `the`-carries-`rule.this()` trick. (The inductive-effect shape splits because effects are looked up by *entity* and need a reverse `/on` index for the reactor; deductive rules are looked up by *concept*, which this serves directly — so the kinds legitimately differ.)

Lookup at query time: one `of: <concept entity>` select. For a concept with no rules it returns empty — a single indexed miss. This is the SQLite-view analogy: a catalog lookup at plan time, not a scan per row.

### Rule identity (`rule.this()`)

`DeductiveRule` has **no identity today** — only `conclusion().this()` (the concept it concludes). The user's scheme needs `rule.this()`. Define it as the content hash of the rule's `DeductiveRuleDescriptor` (the serializable form already used by `Serialize`/`Deserialize` in `deductive.rs:95-106`), parallel to `ConceptDescriptor::this()` (`descriptor.rs:151`: `concept:<base58(blake3(cbor))>`). So:

```
rule:<base58(blake3(cbor(descriptor)))>
```

Content-addressed identity means asserting the same rule twice is idempotent (same entity, `associate_unique` collapses) and a YAML author can re-run bootstrap without duplicating rules. This must be added in `dialog-query` (new `DeductiveRule::this()`).

### `DeductiveRule: Statement` (replaces `register`/`install`)

Implement `Statement` for `DeductiveRule` in `dialog-query` (see the pattern in `rule.rs:311` for `Person`, and the inductive `Rule` impl in `tonk-schema/src/rule.rs:150`):

```rs
impl Statement for DeductiveRule {
    fn assert(self, update: &mut impl Update) {
        let this = self.this();
        let concept = self.conclusion().this();
        let cbor = self.to_cbor_bytes();          // via existing Serialize -> descriptor
        // Attribute name carries rule identity; `of` is the conclusion concept;
        // `is` is the body. One claim — `of`-select returns rule + body inline.
        update.associate_unique(rule_attr(&this), concept, Value::Record(cbor));  // Record vs String: see Open Questions
    }
    fn retract(self, update: &mut impl Update) { /* dissociate the same claim */ }
}
// rule_attr builds the attribute `db.rule/<rule.this()>` (or dialog.rule/*; see Open Questions).
```

A separate `dialog.meta/rule` marker claim can be added if we ever need to enumerate all rules regardless of concept; not required for the query path.

This gives the user-requested ergonomics, replacing both `register` and `install`:

- `tx.assert(rule)` / `branch.with(stmt).commit()` — **persist** the rule (writes claims into the branch).
- `query_layer.with(rule)` — **session-local, not persisted** (rule lives in the per-query `changes` overlay; visible to this query only).

`RuleRegistry::register`/`install` and `ConceptRules::install` can then be removed once callers migrate. The in-memory `RuleRegistry` stays available as a `Provider<SelectRules>` for pure embedders who never touch a branch.

### The load-bearing change: `QueryEnv::Provider<SelectRules>`

Rewrite the impl at `session.rs:325` (and the two `TransactionEnv` ones) to:

1. Check the cache for `input.this()` (see below). On a fresh hit, iterate the cached rules and `extend` them onto `ConceptRules::new(&input)` — **no tree scan**.
2. On miss or stale, run one overlay-aware `of: input.this()` select for `db.rule/*`, **through the same union path as `Provider<Select>`** so `with(rule)` overlay rules are seen. Each returned claim carries `rule.this()` (in `the`) + the CBOR body (in `is`).
3. Hydrate each rule (decode `is` → `DeductiveRuleDescriptor` → `compile`), reusing the inner per-rule cache so a `rule.this()` already hydrated skips decode+compile. Refresh the cache entry and record the tree root scanned at.
4. `extend` onto `ConceptRules::new(&input)` and return.

The empty-select case is the short-circuit: rule-indifferent concepts cost one indexed select (when uncached) or one cache lookup (when cached), and nothing else.

### Cache (one structure, freshness folded into the per-concept entry)

The freshness marker and the rules live in the same entry, so the first-level lookup tells you *both* what the rules are and whether they are current — no second map to keep in sync:

```rs
struct RuleCache {
    tree: TreeReference,                       // root this entry was scanned at
    rules: HashMap<Blake3Hash, DeductiveRule>, // keyed by rule.this()
}

struct ConceptCache(HashMap<Entity, RuleCache>); // keyed by concept.this()
```

Query flow: look up the concept. If present **and** `entry.tree == current branch head` → iterate `entry.rules` and `extend` onto `ConceptRules::new(&input)`, **zero tree scans, zero decode**. If absent or `entry.tree` differs → scan by `of: concept`, rebuild `rules` (reusing any inner `Blake3Hash` already hydrated so a body shared across concepts or surviving an unrelated head change isn't re-decoded), and set `tree` to the current head.

Why fold freshness into the entry: a single lookup yields the verdict (fresh vs. needs-refresh) alongside the data, and the tree reference can never drift from the rule set it describes — they're written together.

Why per-concept tree reference beats a global head key: a head advance touching unrelated facts does not invalidate a concept's cached rules — you rescan a concept only when *its own* `tree` no longer matches the current head, and the rescan is a single cheap `of`-select.

**Decision — store `DeductiveRule` (compiled), not `RuleDescriptor`.** The user's sketch had `HashMap<Blake3Hash, RuleDescriptor>`. Storing the descriptor still pays `compile()` (`Planner::plan`) on every hit. The cache exists to make repeat queries near-free, so store the fully compiled `DeductiveRule` and a hit skips both decode *and* planning. (Revisit only if profiling shows compiled rules are too heavy to retain and `compile()` is cheap relative to decode.)

Coarseness note: comparing `entry.tree` against the whole branch head means *any* commit marks *every* cached concept stale on its next access (one rescan apiece). If too coarse, derive the dirty signal from whether a commit's changeset touched `db.rule/*` claims (and which concepts) rather than head equality. Head-comparison is the simple, correct-but-conservative v1; rescans are cheap.

Where the cache lives: it must outlive a single `QueryEnv` (per-`perform`). Natural home is on `Branch` / the repository handle, or threaded through `env`. Inner entries are content-addressed (keyed by `rule.this()`) so never go stale — only LRU eviction if memory is a concern.

## Recursion (deferred)

A rule body can reference other concepts that themselves have rules — a Datalog evaluation graph. v1 assumes non-recursive bodies (reference data concepts / the implicit rule only). Before lifting: add cycle detection in the `SelectRules` resolver (track the concept-resolution stack) and decide on fixpoint semantics. The storage shape above does not need to change to support recursion later.

## Plan

1. **`dialog-query`**: add `DeductiveRule::this()` (content hash of descriptor); add `to_cbor_bytes`/`from_cbor_bytes` if not already reachable; `impl Statement for DeductiveRule`. Verify `DeductiveRuleDescriptor`, premise, and `ConceptDescriptor` constructors are `pub` enough to hydrate from outside the crate.
2. **`dialog-repository`**: define the `db.rule/*` attribute scheme; rewrite `QueryEnv` + both `TransactionEnv` `Provider<SelectRules>` impls to do the cache-checked, overlay-aware `of: concept` lookup + hydrate + `extend`; add the two-level cache with per-concept dirty tracking.
3. **Migrate callers** off `RuleRegistry::register` / `ConceptRules::install` to `assert`/`with`; keep `RuleRegistry` as the pure-embedder `Provider<SelectRules>`.
4. **tonk side**: YAML `rule!` → `DeductiveRule` → `tx.assert(rule)` during bootstrap/seed; validate with `board-view`/`column-view`/`tile-view` projected over flat `tile`/`board`/`column`.

## Open Questions

- **`is` value type.** `Value::Record(bytes)` (opaque CBOR, see [record-type-design-decision.md](record-type-design-decision.md)) vs `Value::String` (base64/JSON, as inductive `dialog.effect/source` does). Record is the right model (the query layer never looks inside a rule body); String matches existing effect precedent. Pick one — leaning Record if the variant is ready, else String for parity.
- **Attribute name carries `rule.this()`.** `the = db.rule/<rule.this()>` means each distinct rule is its own attribute. Confirm: (a) the attribute namespace tolerates a content-hash suffix (it's just a `domain/name` string — `name` = the base58 hash), and (b) an `of: concept`-keyed select returns *all* `db.rule/*` claims on that entity regardless of the varying `name` suffix (i.e. the select can be by `of` alone, not pinned to a single `the`). If a select must pin `the`, fall back to a fixed attribute (`the: db.rule/source, of: concept, is: rule`) and put `rule.this()` only in cache keys — still one `of`-select, cardinality-many. **This is the one storage detail to verify against the index API before coding.**
- **Where the cache lives** — on `Branch`/repo handle vs threaded through `env`. Settled that the two-level + dirty-tracking structure is wanted; open is the host.
- **Namespace.** `db.rule/*` vs `dialog.rule/*`. Inductive uses `dialog.effect/*` and `dialog.meta/*`; prefer `dialog.rule/*` for consistency unless `db.` is the intended public-facing namespace.
