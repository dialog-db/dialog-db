# Definitions on the branch: what is kept today, and what it would take

A reading of `origin/feat/notation-poc` at `fa5fa26`, of the engine on the same branch, and of tonk at `c006b50` (paths under tonk's `rust/`), made on 2026-11-19 by a research thread. Nothing was built or run. Every statement is tagged READ, TESTED (a named test pins it) or INFERRED. Sections 1 to 6 are facts; section 7 is options and nothing in it is decided.

The maintainer, 2026-11-19: a query's definition is kept and synced "just like concepts, rules and macros that is the whole point".

## 1. What reaches the branch today

The only writer is the example `Notebook` (`rust/dialog-notation/examples/notebook/session.rs:41-128`). It runs `check`, maps each `Step` onto one transaction (`session.rs:93-117`), and commits only when something wrote (`:121-128`). By form:

- **`concept`: nothing.** READ, TESTED.
- `Expander::install` keeps the concept as a `Definition` and returns `Ok(None)` (`src/expand.rs:1059-1069`), so it yields no kernel rule.
- TESTED by `it_gives_an_analyzed_tree_a_form_and_keeps_a_definition` (`src/analyze.rs:146`): the concept gives no analyzed tree.
- `design/macros/kernel.md:151` states it: "a definition has no entity until syntax is kept on a branch".
- `claims.md:193` marks "a concept is found by what the comment above it says" as *does not hold*.
- **`rule`: `dialog.rule/*` facts.** READ.
- `Step::Deduce`/`Step::Induce` are passed to `transaction.assert(rule)` (`session.rs:97-100`).
- A deductive rule asserts `dialog.rule/conclusion` (the concept entity), `dialog.rule/source` (`Value::Bytes` of the body), `dialog.rule/reads` (`on:<attr>` per premise attribute) and `dialog.rule/derives` (`on:<attr>` per head attribute) (`rust/dialog-query/src/rule/statement.rs:266-297`).
- An inductive rule asserts `source`, `induces` and `on` (`statement.rs:335-358`).
- Identity is the content address of the canonical body (`statement.rs:18-21`, `deductive.rs:455-470`).
- The body is format 4, a canonical spelling. "What the author wrote is not stored" (`rule/body.rs:1-16`).
- **A rule's description (the comment above it).** READ, TESTED.
- The comment becomes `description:` on the analyzed tree (`src/check.rs:208-223`).
- `compile` then adds `Step::Assert` of `syntax.meta/description` (text, `last`) on the rule's entity (`src/compile.rs:366-369`, `:434-457`).
- TESTED by `it_gives_a_rule_the_comment_above_it_as_its_description` (`check.rs:236`) and `it_finds_a_rule_by_what_the_comment_above_it_says` (`tests/notation.rs:1281`).
- **`(rule (syntax …))` macros and `(rule (warning …))`: nothing.** READ.
- Both are kept in `Expander.syntax` and `install` returns `None` (`expand.rs:923-1070`).
- **`query`: nothing.** READ.
- `query_forms` calls `define`, which inserts into `Expander.queries` and returns an empty rule list (`src/expand/query.rs:149-151`, `:287`).
- `design-queries.md:277`: "kept by the analyzer and compiles to nothing".
- `(select …)` of a query compiles to `Step::Select`, and a markup query to `Step::View`. Neither is written (`compile.rs:378-400`; `session.rs:106-116`).
- **A command concept: the concept writes nothing.**
- Its domain goes into `Expander.commands`, in memory (`expand.rs:1014-1024`). READ
- A top-level `(assert (cmd …))` compiles to `Step::Issue`, which goes to `transaction.dispatch`, so it holds one round and is never committed (`compile.rs:865-869`, `session.rs:96`). READ
- A rule that issues a command also emits `Step::Transient(concept entity)` (`compile.rs:417-423`). That asserts `dialog.concept/transient = true` on the concept entity (`rust/dialog-repository/src/rules.rs:100-136`). READ
- **The description of a concept or query: nothing.**
- It is carried in `Checked.declarations` (`declare.rs:24-31`, `:36-52`), which `Notebook::submit` never reads. READ, INFERRED from `session.rs` not touching `declarations`.
- The declarations also carry each concept's `ConceptDescriptor`s (`declare.rs:50-51`), and those are not written either.
- **Ground writes** are `Step::Assert`/`Step::Retract` of a `ConceptStatement`. READ

## 2. What the analyzer holds, and whether anything loads it from a branch

- **What it holds.** `Analyzer` wraps one `Expander` (`analyze.rs:44-55`). The `Expander` holds, all in memory (`expand.rs:716-753`):
- `syntax: Vec<Definition>`;
- `names`, `concepts`, `commands`;
- `queries: BTreeMap<String, Syntax>`, each query as written;
- `entries`;
- `reads`, the domains installed rules read;
- a `declared` counter.
- **Where it comes from.** `Expander::new()` submits the compiled-in `PRELUDE` (`include_str!`, `expand.rs:50`, `:757-760`). After that it grows only by `check`, which analyzes a clone and keeps the clone only when nothing is refused (`check.rs:105-147`; `analyze.rs:65-82`). READ
- **Nothing loads from a branch.** READ
- The crate's runtime dependencies are only `dialog-corpus`, `dialog-query`, `merkle-reference`, `base58`, `serde_json` and `thiserror`. `dialog-repository` is a dev-dependency (`rust/dialog-notation/Cargo.toml`).
- The module doc states the intent: "The definitions are held here, in memory. They are to be read from a replica" (`analyze.rs:9-11`).
- `design-forms.md:384` lists "The walker reading the branch" under Not built.
- **No test opens a second analyzer on a branch.** Each `Notebook::open` makes a new repository and a new `Analyzer` (`session.rs:27-36`). READ
- **A consequence for a fresh analyzer.** INFERRED
- The prelude's warning rule `(unless (dialog.rule _ on: ?domain))` (`src/prelude.clj:42-45`) is answered from `self.reads` (`expand.rs:1275-1278`), filled only by rules expanded in this session (`expand.rs:943-948`).
- So a fresh analyzer on a branch that already has the reading rules would warn "no rule reads this command" falsely.

## 3. Keeping a form as data

- **How a tree becomes a value.** `data.rs` stores a tree as dialog's `Composite`: a `Value::Record` holding a map or list, with a version byte, named by a merkle reference that does not depend on layout (`src/data.rs:1-24`). READ
- `Syntax::value()` makes every node a map with a `type` (`src/syntax.rs:349-407`).
- `encode`/`decode` give the record bytes (`:495-505`).
- `Syntax::entity()` is `form:<base58 ref>` (`:484-491`).
- `Signature { name, subject, parameters }` has `value()` and `entity()`, which is `name:<ref>` (`:722-786`).
- Decoding an unknown node type gives `Error::Read` (`syntax.rs:713-716`). An unknown record version is refused (TESTED `it_refuses_a_version_it_does_not_know`, `data.rs:192`).
- **The three `syntax.*` attributes in the tests.**
- `syntax.form/is` (record) on a `form:` entity. TESTED `it_keeps_a_form_on_a_branch_under_the_entity_that_names_it` (`tests/notation.rs:1965`) and `it_finds_a_kept_form_by_a_pattern_over_its_tree` (`:2016`), which select by a struct pattern over the tree.
- `syntax.name/is` (text) and `syntax.name/definition` (record) on the signature's `name:` entity. TESTED `it_keeps_definitions_on_a_branch_under_their_signatures` (`tests/notation.rs:1614-1689`): three `todo` definitions go in, the third a correction under the first's signature, and two come back by name alone.
- `syntax.meta/description`, on a rule's entity.
- **Who writes them.** The first two are written only by those tests, by hand-built `ConceptStatement`s (`:1636-1642`, `:1976-1979`). No code in `src/` names `syntax.form` or `syntax.name` (grep). Only `syntax.meta` is written by the build (`compile.rs:369`). READ
- **Missing between that and "a definition is stored and found by name":**
- A `Step` (or extra output of `check`) that keeps each definition's form. Definitions produce no analyzed tree today, so `check` would have to emit them from the trees themselves.
- An identity per kind:
  - a concept has `Signature::of(head, "concept")`;
  - a query has none (subject `"query"` is unused);
  - a macro whose pattern applies a variable has no signature: `Signature::of(&one("(?name ?t)"), "concept") == None` (`syntax.rs:1076`), so it would need its `form:` entity.
- A reader: a query of `syntax.name/*` (plus macros) on open, fed into the `Expander`.
- A concept's and a query's description as `syntax.meta/description` on the name entity.
- A way to remove a definition (no retract form for one).
- A decision on which form to keep: the written `(concept …)`, or the `Definition` it becomes after the prelude. The test keeps the written one.

## 4. How the engine already stores things

- **Rules.** As in section 1, with identity = blake3 content address of the canonical body (`deductive.rs:455-470`, `:543-555`; `statement.rs:18-21`). Older body formats are read, a later one is refused by name, and `Branch::upgrade_rules` rewrites old formats (`rule/body.rs:1-34`). READ
- **Concepts.** The engine stores no concept descriptor and no name. The only concept fact is the `dialog.concept/transient` marker. It is deliberately not part of the concept's content address (`rules.rs:100-136`). READ
- `claims.md:7`: "concepts and names are not stored; a query carries descriptors inline and the engine never sees a name".
- **How a second replica runs a rule it pulled.** The rule is plain facts, so a pull carries it. READ
- Deductive rules are found at query time through `dialog.rule/derives` keyed by an `on:` entity (`rust/dialog-repository/src/repository/branch/session.rs:1785-1795`).
- Inductive rules fire from the `dialog.rule/on` trigger index probed at commit (`statement.rs:51-56`; `transaction.rs:254`).
- A body that does not hash to its entity is inert (`session.rs:1918-1922`). TESTED `it_keeps_forged_bytes_inert_after_the_genuine_body_is_cached` (`session.rs:4165`).
- Rule-set caching keys on "rule facts" only: `dialog.rule/*` and the transient marker (`rules.rs:90-98`). INFERRED: a `syntax.*` write would not touch that cache.
- **Write gate.** `dialog.` is reserved against application writes, with carve-outs for `dialog.rule/`, `dialog.concept/`, `dialog.layer/`, `dialog.command/`, `dialog.provider/` and two `dialog.branch/*` attributes (`rust/dialog-corpus/src/tree.rs`, the gate around lines 1526-1560). READ
- So `dialog.concept/*` is writable from a transaction today, and `dialog.meta/*` is not (`compile.rs:366-368`).
- **What could carry each definition kind:**
- **A concept's lines.** `declare::Concept.descriptors` already holds `ConceptDescriptor`s, which have an entity. But the name and the field labels are not part of a descriptor's identity (`claims.md` "a concept's entity is its attributes"). So the signature still has to be stored beside it, as `syntax.name/*` or a `dialog.concept/*` vocabulary. INFERRED
- **A macro.** It has no engine counterpart. "a rule about syntax can be a stored rule Dialog-DB evaluates" is *open; the crate holds syntax in memory* (`claims.md:30`; `design-forms.md:36-43`). READ
- **A query.** It is not a rule:
  - Today it lowers to a `Selection`: `ConceptQuery`s plus an in-memory fold (`compile.rs:19-20`, `:396-400`).
  - fa5fa26 (`claims.md`, 2026-11-18) records the maintainer's opinion that a query should become a deductive rule plus a reducing rule over an anonymous head. That is gated: `TypeError::ReduceInOpenRule` refuses an authored rule with `reduce`. It also needs a set aggregator.
  - So `dialog.rule/*` cannot carry a query today. Even once the gate opens, it would carry the lowered rules, not the query under its name. READ, then INFERRED.

## 5. Identity and redefinition

- **Design.** `research/notation/design.md:258` (Identity): "A name binds a concept to a signature, meaning the head's labels. Redefining a name with a different body rebinds the name. Stored facts belong to attributes, not names." `design.md:111` says that in a transaction "a top-level `rule` or `concept` is installed and runs from then on", which the build does not do for `concept`. READ
- **Concepts in the build.**
- A later definition under the same signature (name + `concept` + label set) replaces the earlier one, and a definition under other labels stands beside it (`expand.rs:981-1013`). TESTED `it_replaces_a_definition_by_a_later_one_of_the_same_signature` (`expand.rs:2410`).
- A read fitting several definitions expands once per definition (TESTED `it_reads_through_every_definition_the_labels_fit`).
- **Macros.** A macro without a concept-shaped signature is pushed with `signature: None` and never replaced (`expand.rs:984-994`, `:1059`). INFERRED: an edited macro stands beside the old one. An identical resubmission is collapsed by `ask` treating answers that differ only in variable names as one (`expand.rs:1286-1287`).
- **Queries.** Keyed by name alone, and a redefinition replaces (`query.rs:287`). A query named like a concept is refused (`query.rs:273-277`). INFERRED: nothing in `install` refuses a concept defined later under a query's name.
- **Rules.** A changed rule is a new content-addressed entity. INFERRED (no retract path in `session.rs` or `compile.rs`): the notation never retracts the old one, so both stay installed.
- **Across sessions.** Nothing carries over, since the analyzer loads nothing (section 2).
- In the hand-written branch test, `last` under the signature entity replaces in order (TESTED above).
- Two writes of one round both stand and are elected by hash (TESTED `it_tells_a_name_that_was_repointed_from_one_claimed_twice`, `tests/notation.rs:1874`). That test runs on one replica only, never across two (`claims.md:71`).
- Authority is not built (`design-forms.md:383`).

## 6. What tonk does

- **Install.** A library document (YAML) is analyzed with `tonk_analyzer::analyzer::analyze_local`. Every top-level statement is planned to claims; commands go to a separate transient set (`tonk-worker/src/router/repository.rs:7054-7146`). READ
- **What is stored: compiled definitions as facts, not forms.** READ
- A concept is an entity derived from its content, carrying the marker `db.meta/concept = db:concept` and one `db.concept.with/<field>` per field, pointing to the attribute entity (`tonk-schema/src/concept.rs:1-20`, `:847-853`, `:1088-1106`).
- Attributes are `db.attribute/{id,as,pick,among}` facts.
- A name is `id:<n>` with `db.name/referent`, cardinality one (`tonk-core/src/meta.rs:21-25`; `tonk-analyzer/src/analyzer.rs:961`, `:1057`).
- Descriptions are `db.meta/description`.
- Views are facts of the builtin `xyz.tonk.view` concept: a `show` dictionary of templates, plus `bindings` as a dag-cbor `Record` resolved at lowering (`tonk-schema/src/builtin.rs:120-170`).
- `rule!:` becomes an install effect, which ends up as dialog rules (`analyzer.rs:21-23`).
- The source text is not stored: `db.meta/source` "has no real EAV backing" (`concept.rs:1093`).
- **Read-back is lazy, per reference.** `analyze(syntax, Source)` resolves each name against the branch or transaction through `tonk_schema::resolution` (`ConceptReference`/`NamedReference` to `db.name/referent`) (`analyzer.rs:126-135`; `tonk-schema/src/resolution.rs:1-30`). READ
- **Seeds and upgrades.** The library itself is re-installed on mount when the seed hash changes. A seed version is the entity `seed:{hash}` with `SeedAvailable`/`SeedInstalled` records. An upgrade uninstalls the old claims and installs the new ones (`upgrade_seed` `:4720`, `install_seed` `:4886`, `seed_record_facts` `:5560`). READ
- **A discrepancy.** `kernel.md:146` says tonk keeps descriptions as `dialog.meta/description`. The tonk source I read uses `db.meta/description`. READ

## 7. What it would take (options, not facts)

### Option A: forms as records under `syntax.*`; the analyzer reads them on open

Changes by file:
- `src/compile.rs`: a new `Step::Define { entity, kind, name, form }`.
- `src/check.rs`: emit `Define` for each declaring tree. Reuse the `declares` test of `expand.rs:796-803`, and extend `Checked.declarations` to cover macros.
- `src/syntax.rs`: identities per kind:
- concepts: `Signature(name, "concept", labels).entity()`;
- queries: `Signature(name, "query", labels)`;
- macros: `form:` entity of the rule.
- Attributes: `syntax.name/is`, `syntax.name/definition` (last), `syntax.name/kind`; `syntax.form/is` for macros; `syntax.meta/description` on the name entity.
- `src/analyze.rs` / `src/expand.rs`: `Analyzer::with_definitions(Vec<Syntax>)`, which feeds them through `check` as one document. That keeps the crate free of `dialog-repository`, and the caller queries the branch. Alternatively, an async `Analyzer::open(branch)`, which adds the dependency in `Cargo.toml`.
- Fill `reads` from the branch's `dialog.rule/on`/`reads`, or answer that premise by a branch query.
- `examples/notebook/session.rs`: write `Define`, build the analyzer from the branch on open, and rebuild on refresh when `syntax.*` changed.

What it costs, and what it leaves open:
- **Ordering** is mostly solved. `Expander::check` already puts declarations first and re-expands until nothing new is declared (`expand.rs:793-847`). That includes a macro that defines forms other definitions use, provided all stored definitions load as one batch.
- **Macros cannot be replaced**, only retracted, because they are keyed by content.
- **An older release** syncs the records as plain data. It decodes them only if their node types and composite version are known; otherwise it gets `Error::Read` or a version refusal.
- **Concurrent definitions** of one signature both stand and `last` elects by hash.
- **Hot reload** is the notation's job. The engine's rule cache ignores `syntax.*`.
- **Rebinding a concept does not re-lower rules** already installed from it. That is consistent with design.md:258.

### Option B: engine-level statements, as rules are

Changes:
- A `dialog.concept/*` (or new `dialog.definition/*`) vocabulary in `dialog-query` and `dialog-repository`: a `Statement` impl storing the `ConceptDescriptor` plus the signature, content-addressed and verified like rule bodies.
- A query stored as its lowered reducing rule once the `ReduceInOpenRule` gate opens for a head no rule reads.
- The `dialog-corpus` write gate, if the prefix is new.
- `CHANGELOG.md` with a Compatibility entry, because it is a storage change.

What it costs, and what it leaves open:
- **Gains:** a second replica's engine can answer and subscribe without the notation, and storage reuses rule verification.
- **Costs:** the engine learns names, which it deliberately does not have today. Macros still need option A.
- **Undetermined:** whether a pulled fact under a prefix an older release reserves passes that release's gate (section 8).

### Option C: definitions as rules dialog evaluates

This is `design-forms.md` columns B/C:
- A macro becomes a deductive rule over `syntax.form` records.
- Expansion becomes a query.
- The analyzer holds nothing, because the branch is its state.

It is the largest change. It needs record building in rule heads and several heads. H3, the cost of expansion as a query, is unmeasured (`design-forms.md` H3). Ordering, older peers and hot reload become the engine's existing behaviour.

Option A and B or C are not exclusive: A works now, and the in-memory `Expander` becomes a cache of the branch.

## 8. The test that would prove it

1. Session 1, a `Notebook` on branch `main`, submits one document containing:
 - a concept `todo`;
 - a command `todo/add` and the rule that reads it;
 - a macro `(rule (syntax …) (expand (pin ?t)))` that expands to a `todo` write;
 - a query `todo/card` that collects tags;
 - comments above each.
2. It asserts a to-do through the macro.
3. A new `Analyzer` is built from the branch alone, with no submission (same storage, new process state).
 - `(select (todo/card ?t is: ?card))` answers the card.
 - `(pin todo:2)` expands.
 - `(select (todo ?t …))` reads.
 - The command warning is absent.
 - `(select (syntax.meta ?e description: ?d))` finds the concept and the query by their comments.
 - Assert the specific error for a name never defined (`Error::Unknown`) and that nothing was written on that refusal.
4. Redefine `todo` under the same signature in session 1. The fresh analyzer, refreshed, uses the new body. A second definition under other labels stands beside it.
5. On a second replica, a branch with `set_upstream(main)` then `pull(Download::MinimalRanges)` as in `dialog-peer/src/peer/branch.rs:1046-1069`, or a remote through the S3 provider for a true second peer:
 - a fresh analyzer gives the same answers;
 - a definition added on the first replica after the pull appears after a second pull and refresh (hot reload).
6. Run on the filesystem provider as well as the volatile one, per the authority-tests rule.

## What I could not determine

- Whether a pull from a newer release passes `WriteScope::Application`'s reserved-prefix gate on an older release. This decides whether a new `dialog.*` prefix in option B would be refused or kept.
- The cardinality `syntax.name/definition` gets when it is declared with only `the` and `as` (`tests/notation.rs:1622-1625`). The test shows replacement, so I assume `one`/`last` by default, but I did not read where the default is set.
- Whether `InductiveRule::decode` or `DeductiveRule::decode` treats an unknown head kind in `dialog.rule/source` as inert or as an error.
- Whether tonk's analyzer ever stores or re-reads a view's template source beyond the `xyz.tonk.view/show` facts. I read only the builtin descriptor.
- Which pull helper the dialog test suites use for two genuinely separate replicas. I found only the same-repository upstream pattern.
