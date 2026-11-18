# The notebook's claims, from YAML and from the notation

Measured 2026-11-18 on tonk branch `poc/dialog-notation`, commit `1763087`.

- **Test:** `rust/tonk-analyzer/tests/notation_claims.rs`, test `it_measures_the_notebook_in_two_notations`.
- **YAML side:** tonk's `notebook.yaml`, lowered after core's YAML as `library_claims_after` lowers a component.
- **Notation side:** `rust/dialog-notation/sketches/notebook.clj`, lowered after `rust/dialog-notation/sketches/core.clj` by `notation_library_claims`. Both files are read from the dialog checkout at run time.
- **Dialog state:** dialog at `5a5cdf2`. Its `dialog-notation` sources were clean at the last run; uncommitted changes were in its examples and tests only.

## Whole library

Of the 376 claims the YAML path stores:

| | identical | differently | not at all | stored only by the notation |
|---|---|---|---|---|
| the notation as it is | 87 | 206 | 83 | 263 |
| ADJUSTED: lines with no `cardinality:` read as `:one` | 202 | 113 | 61 | 148 |

The adjustment is made in memory on the sketch's text, before the notation reads it. It is not the notation's behaviour. Both rows are pinned in the test.

"Differently" means the notation stores a claim of the same attribute with the same entity or the same value. "Not at all" means it stores no such claim.

## By kind of form, as it is

YAML forms, the YAML forms whose notation counterpart passes, the YAML's claims, then their fates. The last column is claims only the notation stores.

| kind | forms | counterpart passes | claims | identical | differently | not at all | only notation |
|---|---|---|---|---|---|---|---|
| concept | 9 | 9 | 78 | 65 | 11 | 2 | 13 |
| command | 6 | 6 | 118 | 0 | 93 | 25 | 119 |
| attribute | 3 | none (no form) | 16 | 7 | 5 | 4 | 0 |
| rule | 16 | 15 | 112 | 4 | 91 | 17 | 116 |
| assert of a route | 2 | 2 | 4 | 0 | 4 | 0 | 4 |
| assert of an element | 1 | 1 (as 11 asserts) | 11 | 0 | 2 | 9 | 11 |
| assert of other data | 7 | 7 | 11 | 11 | 0 | 0 | 0 |
| view | 5 | 0 (5 described, none lowered) | 7 | 0 | 0 | 7 | 0 |
| event | 4 | none (inline in views) | 19 | 0 | 0 | 19 | 0 |

## By kind of form, ADJUSTED

| kind | claims | identical | differently | not at all | only notation |
|---|---|---|---|---|---|
| concept | 78 | 65 | 11 | 2 | 13 |
| command | 118 | 111 | 4 | 3 | 4 |
| attribute | 16 | 11 | 1 | 4 | 0 |
| rule | 112 | 4 | 91 | 17 | 116 |
| assert of a route | 4 | 0 | 4 | 0 | 4 |
| assert of an element | 11 | 0 | 2 | 9 | 11 |
| assert of other data | 11 | 11 | 0 | 0 | 0 |
| view | 7 | 0 | 0 | 7 | 0 |
| event | 19 | 0 | 0 | 19 | 0 |

The sketch has 56 forms: 9 concepts, 6 commands, 16 rules, 5 views and 20 asserts. Two of them are refused:

- **Rule 8**, the YAML's line 829 rule over `block/insert`, is refused by the compiler: "not supported yet: (id ?b next: ?x notebook: ?n prev: ?p source: ?s): ?b is the entity of no head". The YAML reads the command's own entity here.
- **The five views** are refused by the compiler for the reason it gives every view: a view is a description that Dialog does not run. All five reach a `View` description.

## By kind of difference

Each row gives counts of claims the notation stores differently, then not at all.

| kind | as it is | ADJUSTED | who would notice |
|---|---|---|---|
| a rule stored under another identity | 86 / 15 | 86 / 15 | upgrade protection, `declared_by` (`tonk-worker/src/router/repository.rs:5311`). Induction fires either rule |
| a command's lines picked `all:` by default where tonk picks `last:`, so the attribute and concept entities move ("declaration under another identity", "description on another entity", "transient marker on another concept", "name points at another entity") | 60 + 21 + 6 + 6 | 0 | the display and `/transact` name a command by its concept entity (`tonk-template/src/resolve.rs:601`, `tonk-worker/src/router/transact.rs:306`) |
| declaration not written, through the same pick | 0 / 19 | 0 / 0 | as above |
| an event inline in a view, with no back end yet: `xyz.tonk.event/type`, `xyz.tonk.event.where/*` | 0 / 15 | 0 / 15 | the analyzer inlines events into a view's `bindings`, and the display posts by them (`tonk-display/src/element.rs:2261`) |
| an event's own name, `db.name/referent` of `id:on/<name>` | 0 / 4 | 0 / 4 | a binding a document does not make is read by name at render time (`tonk-analyzer/src/analyzer/view.rs:285`) |
| a view described, with no back end yet: `xyz.tonk.view/<facet>`, `xyz.tonk.view/bindings` | 0 / 7 | 0 / 7 | the display renders nothing (`tonk-display/src/element.rs:1778`) |
| an element's method text: the YAML's block scalar ends in a newline, and the entity differs (the sketch pins `tonk:element/notebook-save`, the YAML derives a `did:key`) | 0 / 9 | 0 / 9 | elements are found by tag through `db.name/referent` (`tonk-display/src/registry.rs:17`), so the entity is not noticed. The trailing newline reaches the method's source and is harmless JavaScript |
| an entity derived differently: route rows, the element's description | 5 / 0 | 5 / 0 | the router reads routes by path and concept (`tonk-schema/src/domain.rs:521`), so it does not notice |
| a concept pinned to an entity in the YAML (`this: tonk:notebook/route`, `tonk:notebook/index-route`), derived in the notation | 7 / 0 | 7 / 0 | the router writes a site's `concept` as the pinned entity (`tonk-worker/src/router/session.rs:866`), and a route names it, so the notation's route concepts would render nothing |
| a pinned concept's description not written | 0 / 2 | 0 / 2 | none for behaviour |
| an attribute form: the YAML's three `attribute!:` forms name their attribute (`id:notebook/title`) and hold its description and `role:` | 5 / 4 | 1 / 4 | name: the CLI and the inspector (`tonk-cli/src/schema.rs:381`). `role:` goes to the palette |
| palette metadata (`action:`, `role:`) with no spelling | 0 / 3 | 0 / 3 | the palette's intent rules |
| description text differs (comments are not the YAML's descriptions; they include `GAP:` lines, and a YAML block keeps its line breaks) | 2 / 0 | 6 / 0 | none for behaviour |
| the refused rule 8 | 5 / 2 | 5 / 2 | as for any rule |

### What moves a rule's identity

Read off the decoded bodies, adjusted:

| rules | what tells the two bodies apart |
|---|---|
| 1 | nothing: identical (`rule:CMzC…`) |
| 9 to 15 (7 rules) | only a variable used once where the YAML writes `_` |
| 3, 5, 6, 7 | the sketch derives with `id` what the YAML reads as the command's `this`, and a variable used once |
| 4 | the YAML binds a key with `==` where the notation binds it directly |
| 2 | a variable used once, the head attribute with no type and pick `all:`, and the command named as a symbol `notebook/retitle` where the YAML gives its concept entity |
| 16 | a variable used once, and a retract by entity alone that types its premise differently |
| 8 | refused |

So a canonical form that writes a variable used once as `_` would make 8 of the 16 rules identical. The same goes for a back end that writes it that way.

## The back end and `Checked::declarations`

`notation_library_claims(core, library)` returns what `library_claims_after` returns for a YAML component: durable and transient instructions, plus per-form detail for measuring. It writes:

- for a concept or command: tonk's declaration claims (`attribute_statements` per field, `AnonymousConcept` or `TransientConcept`, `db.name/referent`);
- for a rule: dialog's `dialog.rule/*` facts;
- for an assert: its facts, with a write that has a `when` run once against a branch;
- for a view: nothing.

The query workaround is gone. `Checked::declarations` gives enough to write tonk's declaration claims. With the joins below, concepts store 65 of 78 claims identically and commands 111 of 118 (adjusted). Every concept and command claim left differs for a reason of the design (pinned concepts, descriptions, palette metadata), not for missing data.

What I still worked around, with the field I wanted:

1. **Field labels.** `Concept::descriptors[0]` names a field by its attribute (`stray-at`, `entry`), and tonk by the head's label (`at`, `block`), which is what a view's `{field}` and a posted command name. The back end joins `Concept::fields` (label to variable) with `Line::is`. Wanted: the descriptor labelled by the head, or a `Line::label`.
2. **Lines and fields.** A collection line (`xyz.tonk.notebook/?k`) is matched to its descriptor field by domain. Wanted: `Line` carrying its `AttributeDescriptor`, or the descriptor field's label.
3. **Steps per form.** `Checked::steps` is not attributed to forms, and one refusal empties it. The back end walks the document itself:
   - it removes the forms the walker refuses until none is refused (`Analyzer::analyze`);
   - it compiles each analyzed rule with `compile`, and copies the private `described` so a rule keeps the comment above it as `syntax.meta/description`.
   Wanted: steps by origin, and a refusal that removes its form only.
4. **Declaration origins.** A declaration does not say which form it came of, so concepts are matched by name. Wanted: an origin index on `Declaration`.
5. **Attribute descriptions.** An attribute's description is set through a serde round trip, because `AttributeDescriptor` has no setter (dialog-query, not dialog-notation).

## Views

tonk stores the following for the notebook's views. No `xyz.tonk.view/embeds` record is stored for any of them.

| view | stored | in the YAML |
|---|---|---|
| 1, `notebook` `ui` | `xyz.tonk.view/ui` text (538 bytes), `xyz.tonk.view/bindings` record (767 bytes) | `<notebook-save>` wrapping `<tonk-notebook>` with `data-subject={this}`, `title={title}`, four `on:<name>=<command>` attributes, a `hidden` div with one `span data-block={block} data-key={block/key}` per entry, and `<tonk-display model=notebook/block>` |
| 2, `notebook` `directory` | `xyz.tonk.view/directory` text | a link `href="/notebook/{this}"` and `{title}`, with an HTML comment |
| 3, `notebook/block` `directory` | `xyz.tonk.view/directory` text | a row with `subject={this} data-block={this} data-notebook={notebook} data-source={source}` |
| 4, `notebook/index-route` `ui` | `ui` text, `bindings` record (the same 767 bytes as view 1) | `draft`, `with="{branch}@{repo}"`, four `on:` attributes, `<tonk-display with=… model=notebook>` |
| 5, `notebook/route` `ui` | `ui` text | `<tonk-display with="{branch}@{repo}" entity={entity} model=notebook>` |
| events 1 to 4 | per event: `db.name/referent` of `id:on/<name>`, `xyz.tonk.event/type`, one `xyz.tonk.event.where/<field>` path each | `bindings` holds the same, keyed by name, with `prevent-default` and `stop-propagation` false |

All five sketch views reach a `View` description. For each stored thing, whether the description holds enough to produce it:

- **Facet text** (`ui`, `directory`). There is enough for the markup in all five. `View::name` is the facet. Elements, `Text`, `Variable`, `Present` (`hidden`, `draft`) and `Parts` (`/notebook/{this}`, `{branch}@{repo}`) cover every attribute. A `Nested` with `model`, `of` and `with` covers each `<tonk-display>`. What is missing:
  - a variable is not a slot name. `?title` has to become `{title}`, `?n` `{this}` and `?k` `{block/key}`, through the view's conditions and the concept's head labels;
  - an event becomes an `on:<name>=<command>` attribute, and its name has to be made up;
  - the stored text will not be byte for byte the YAML's: layout, the HTML comment of view 2, attribute order. Whether that matters is a decision below.
- **`bindings` record** (views 1, 4). There is enough except the name and the flags. `Event::kind` is `event-type`, `Event::fields` with `Input::Path` are `sources`, and `Event::command` is the command. Missing:
  - the event's name (`on/block-edit`), the key of the record. In this file it is `on/` plus the command with `/` turned into `-`, a convention to decide;
  - `prevent-default` and `stop-propagation`, which have no spelling. None of the notebook's events needs them, but 9 of the library's 38 do (`tonk.md`).
- **Event rows.** There is enough for `type` and the `where` paths, so the back end can print `.detail.notebook` from `Path`. The name is missing as above, and the entity, which the YAML derives from the event's body.
- **The view's entity.** A view is stored on its model concept's entity. For views 4 and 5 that is the pinned `tonk:notebook/index-route` and `tonk:notebook/route`, which the notation's concepts do not have.

## Decisions needed

1. **The pick default on command lines** (already with the maintainer). It is the largest single move: 115 claims go from different or missing to identical.
2. **A variable used once against `_`** (already with the maintainer). It is the only difference in 7 of 16 rules and a part of 6 more.
3. **The command's own entity in a rule.** Rules 3, 5, 6 and 7 recompute it with `id`, and rule 8 is refused for it. The notation can now read the entity of the command that fired (`4d4eb1a`), but the sketch does not use it yet. Should the sketch move to it?
4. **Concepts pinned to an entity** (`this:`). The route concepts need it for the router and for their views.
5. **Event names and flags.** Should an event's name follow `on/<command with - for />`, or be spelled? What spells `prevent-default` and `stop-propagation`?
6. **What counts as the same view.** A back end printing HTML from a `View` stores different text bytes than the YAML for the same template. Is template equivalence the bar for views, rather than identical claims?
7. **Descriptions from comments.** Should a back end drop `GAP:` lines from a comment before storing it as a description?
8. **Elements.** Should the element's entity be pinned as the sketch does or derived as the YAML does? And should a method's text keep the YAML's trailing newline?
9. **Palette metadata** (`action:`, `role:`). It has no spelling, and an attribute's name and `role:` have no form.
10. **Public API in `dialog-notation`.** The five items under "What I still worked around" above.
