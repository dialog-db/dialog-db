# The claims a library stores, from YAML and from the notation

Measured 2026-11-18 on tonk's local branch `poc/dialog-notation`, not pushed. The test is `rust/tonk-analyzer/tests/notation_claims.rs`, from commits `7151e7c` and `b21f1d0`. Dialog is the notation checkout at `2b5a5d8`, and `dialog-notation` was built from its working tree. That tree had uncommitted edits to `expand.rs` and `tests/notation.rs` at the time.

## How it was measured

Four definitions were taken from `notebook.yaml` and from `design/macros/sketches/notebook.clj`:

- the concept `notebook/block` (YAML 137, sketch 89);
- the command `block/edit` (YAML 610, sketch 462);
- the rule that persists a block edit (YAML 851, sketch 589);
- the route `/notebook`, an assert of core's `route` (YAML 390, sketch 309).

The YAML side is lowered as `library_claims_after` (`tonk-worker/src/router/repository.rs:7069`) lowers a component: core's YAML and the definitions are analyzed as one document, and each statement from the first definition on is planned into a `dialog_corpus::Changes`.

The notation side is checked one definition at a time with `dialog_notation::check`, after a prelude that declares `route` as core's YAML does. Its `Step`s are asserted into a `Changes` exactly as `examples/notebook/session.rs` submits them. The route's `Step::Write` is run against an empty branch.

A branch `Transaction` is a `Changes` (for assert) plus another `Changes` (for dispatch), so both sets are what a commit would be handed. Each claim is printed as a sorted `(the, of, is)` triple and each rule body is decoded.

`tonk-analyzer` is the lowest crate that can produce a library's claims from YAML: parse, `analyze_local`, `Application::plan` and `Changes` are all there or below. `dialog-notation` is a dev-dependency of it, resolved through the same `[patch]` as the other dialog crates. The patch works even though the `v0.5.0-rc.6` tag has no `dialog-notation`.

## Headline

The YAML path writes 39 claims for the four definitions.

| definition | YAML claims | notation as it is: identical / different / not written | with tonk's declaration claims mapped around it | mapped, command lines read `cardinality: :one` |
|---|---|---|---|---|
| concept `notebook/block` | 13 | 0 / 0 / 13 | 13 / 0 / 0 | 13 / 0 / 0 |
| command `block/edit` | 19 | 0 / 0 / 19 | 0 / 19 / 0 | 18 / 1 / 0 |
| rule, persist a block edit | 5 | 0 / 5 / 0 | 0 / 5 / 0 | 0 / 5 / 0 |
| assert route `/notebook` | 2 | 0 / 2 / 0 | 0 / 2 / 0 | 0 / 2 / 0 |
| all | 39 | 0 / 7 / 32 | 13 / 26 / 0 | 31 / 8 / 0 |
| claims only the notation writes | | 8 | 27 | 9 |

"Different" means the notation writes the same attribute with a different entity or value. Logs: `claims-before.log` and `claims-after.log` beside this note.

## Differences by kind

| kind | claims | definitions | reader that notices |
|---|---|---|---|
| a declaration the notation does not write: `db.meta/concept`, `db.concept.with/<field>`, `db.attribute/{id,as,pick}`, `db.meta/description` | 29 | concept, command | the display and render resolve a model through `ConceptByEntity::resolve` (`tonk-schema/src/concept.rs:252`, `MissingAttribute` at `:128`) and the phase 1 query (`tonk-template/src/resolve.rs:601` to `:624`, used at `tonk-display/src/element.rs:1362` and `tonk-render/src/page/orchestrate.rs:368`). Without these claims no concept resolves and no page mounts |
| a name only tonk writes: `db.name/referent` of `id:<name>` | 2 | concept, command | `name_query` (`tonk-template/src/resolve.rs:583`), elements by tag (`tonk-display/src/registry.rs:17`), the CLI (`tonk-cli/src/views.rs:274`) |
| a command marked transient only by tonk: `dialog.concept/transient` | 1 | command | the phase 1 query reads `transient` (`resolve.rs:620`). The display posts a claim of a transient concept to `/transact` as a command (`tonk-worker/src/router/transact.rs:306` to `:309`), and the analyzer reads the marker off the branch (`tonk-schema/src/concept.rs:980`). The notation writes the marker (`Step::Transient`) only for a command a rule issues, not for one a rule reads |
| a command's lines picked differently: the notation's default pick for a line with no `cardinality:` is `all:`, tonk's is `last:` | 19 after mapping | command | every attribute entity and the command's concept entity move (`concept:D3BK…` against `concept:AzCQ…`). A `/transact` post names the concept by the entity the display resolved, so a page written against one would not fire rules written against the other. With `cardinality: :one` on each line, 18 of 19 claims are identical |
| a rule stored under a different identity | 5 | rule | dialog's induction reads whatever is stored, so a lone install fires. tonk notices on upgrade and protection: `uninstall_claims` (`repository.rs:5259`) retracts by stored bytes, so a revert still works, but `declared_by` (`repository.rs:5311`) recomputes other libraries' claims from their text. A rule declared by a YAML library and by a notation library would be two rules |
| a claim entity derived differently: `id:<base58>` by `entity/derive` against tonk's `Entity::of({assert, where})`, a `did:key` | 2 | route | the router reads routes by path and concept and never by entity (`tonk-schema/src/domain.rs:521`, `tonk-worker/src/router/session.rs:866` to `:917`), so it does not notice. A YAML route and a notation route for one path would be two rows, ordered as a tie |
| a description only the notation writes: `syntax.meta/description` on the rule | 1 | rule | nothing in tonk reads `syntax.meta/*` (grep). The YAML path stores no rule description |
| a description that differs as text | 1 in the last column | command | none reads it for behaviour. A YAML `|` block keeps its line breaks and trailing newline, and a comment cannot. I also wrote that comment shorter than the sketch's |

The rule's identity moves for two reasons, both read from the decoded bodies:

1. The command premise reads its fields under `pick: all:`, from the default above.
2. The command premise's own entity is a named local on the notation side, `"this": {"?": {"name": "~0"}}`, and a blank on the YAML side, `"this": {"?": {}}`.

With the first removed (the last column) the body is 366 bytes against 358, and the only difference left is the second, with the labels it shifts. That contradicts `rule/body.rs`, which says every spelling of one rule stores the same bytes: a variable used once and `_` mean the same thing and store differently.

The concept identities agree. The rule's conclusion is `concept:CxQH…` on both sides, and the command is `concept:AzCQ…` on both sides once its lines read `cardinality: :one`. So `in: :control` is not part of a concept's identity.

Contrary to `tonk-integration.md`, `db.meta/source` is not stored. The phase 1 query synthesizes it from `db.concept.with/*` and `db.attribute/*` (`tonk-schema/src/concept.rs:1090` to `:1113`).

## What a tonk back end at column C has to add

Measured with the mapping in `b21f1d0`, `declaration_claims` in the test, about 80 lines. For each concept and command:

- it compiles a read of the concept and takes the read's `select.predicate` as the descriptor;
- it asserts tonk's own `attribute_statements` for each field, then `AnonymousConcept`, or `TransientConcept` for a command;
- it writes `db.name/referent` of `id:<name>`;
- it takes descriptions from the comment above the form and above each line.

That brings the concept to 13 of 13 identical, and the command to 18 of 19 once its lines are read `cardinality: :one`. What it still lacks:

1. **The pick on command lines.** The back end either writes `cardinality: :one` for a command line that does not say, or the notation's default changes.
2. **Rule identity.** The rule needs the blank-against-named-local spelling settled on the Dialog side, or an upgrade from the YAML install accepts a new rule entity.
3. **Entities of asserts with `when (id …)`.** They can stay `id:` entities, since the router does not read them, or the back end derives them as tonk does.
4. **Not measured: views, events and elements.** The notebook's views and events do not compile today (`tonk-port.md`), so no claims were compared for them. These are the `xyz.tonk.view/*` rows with the `bindings` record and the element's dictionaries.

## Does column C still look cheaper than A and B

On this evidence, yes, for concepts, commands, rules and asserts:

- The notation's descriptors and rules land on tonk's entities wherever the two mean the same thing.
- The mapping reuses tonk-schema's own builders rather than reproducing them.
- Against column B, the mapping writes the claims B's `concept` built-in application would produce, without building an `Application`.
- Against column A, there is no tree to print back to YAML.

The remaining differences are three choices of default or identity, not missing machinery.

What would change my mind:

- A view back end that needs the analyzer's view checks and its compiled `bindings` record (`tonk-analyzer/src/analyzer/view.rs:223` to `:330`) badly enough that rebuilding them is dearer than handing over at B.
- The rule identity difference turning out to be wanted, so that every rule moving from YAML to the notation is a new rule.

## Decisions needed

1. **Command line default pick.** Should the notation read a command line with no `cardinality:` as `last:` like tonk, or should every command line say it?
2. **A variable used once in a rule.** Should Dialog's canonical form label it as `_`, or should `dialog-notation` write `_` for a command premise's entity? Reproduction: the two bodies `#1f405a02f9c90376` and `#a2f315a64ae43262` in `claims-after.log`, which differ only in `this`.
3. **Asserts that derive their entity.** Should they keep `entity/derive`'s `id:` entities, or should the tonk back end derive them as `Entity::of` does?
4. **Descriptions from comments.** Is a field's description the comment above its line, and a concept's the comment above the form, as the mapping assumes?
5. **Missing public API in `dialog-notation`.** It has no public way to get a concept's descriptor. The mapping compiles `(query (notebook/block ?b notebook: ?n source: ?s))` and takes `Step::Query`'s `select.predicate`. A declaration also compiles to no `Step` at all. Does `dialog-notation` give a declaration as data, or does the tonk back end keep reading it through a query?
6. **Where the back end lives.** `tonk-analyzer` takes `dialog-notation` as a dev-dependency today. A real back end needs it as a dependency there or in a crate of its own, reached from `install_claims_for` (`repository.rs:7054`) keyed on the source.
