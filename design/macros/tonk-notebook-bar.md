# The notebook's claims, from YAML and from the notation

Measured 2026-11-18 on tonk branch `poc/dialog-notation`, commit `aa0299e`.

- **Test:** `rust/tonk-analyzer/tests/notation_claims.rs`, test `it_measures_the_notebook_in_two_notations`.
- **YAML side:** tonk's `notebook.yaml`, lowered after core's YAML as `library_claims_after` lowers a component.
- **Notation side:** `rust/dialog-notation/sketches/notebook.clj`, lowered after `sketches/core.clj` by `notation_library_claims`, now including views and events. Both sketches are read from the dialog checkout at run time.
- **Dialog state:** dialog at `023df09`. At the last run the crate's `expand.rs` had uncommitted changes, and the numbers matched the run before them.

## Whole library

Of the 376 claims the YAML path stores:

| | identical | differently | not at all | stored only by the notation |
|---|---|---|---|---|
| the notation as it is, last report | 87 | 206 | 83 | 263 |
| the notation as it is, now | 107 | 211 | 58 | 277 |
| ADJUSTED, last report | 202 | 113 | 61 | 148 |
| ADJUSTED, now | 231 | 111 | 34 | 153 |

ADJUSTED means lines with no `cardinality:` are read as `:one`. The adjustment is made in memory on the sketch's text before the notation reads it; it is not the notation's behaviour. The "now" rows are pinned in the test, and so is the view comparison below: of 5 views, 0 have equal facet claims, 5 have equivalent templates and 5 have equal bindings.

"Differently" means the notation stores a claim of the same attribute with the same entity or the same value. "Not at all" means it stores no such claim.

### What moved the numbers since the last report

Each step was measured with the step before it held fixed. Values are changes in identical / differently / not at all / only notation.

| step | as it is | ADJUSTED | how it was told apart |
|---|---|---|---|
| U: an expansion's variable used once is written `_` (dialog `c6f99e7`) | 0 / 0 / 0 / 0 | +9 / −7 / −2 / −9 | the same sketch and no views, before and after the commit. Rules 9 and 10 became identical. Nothing moved as it is, because every rule over a command already differs there by the command's pick |
| the view and event back end (this work) | +20 / +4 / −24 / +6 | +20 / +4 / −24 / +6 | the same build with the view back end on and off |
| V: the sketch reads an inserted block's entity with `of: ?c` (dialog `50d8f3a`) | 0 / +1 / −1 / +8 | 0 / +1 / −1 / +8 | the committed sketch before V against the sketch after it. Rule 8 now compiles (it was refused) and is stored under another identity. Rules 3, 5, 6 and 7 no longer recompute the entity, but still differ, see below |
| W: labels, descriptors, origins and `check_forms` (dialog `023df09`) | 0 | 0 | the same numbers with the earlier workarounds and with the new API, at the same checkout. I could not build the crate between V and W, so a change W makes in `expand.rs` and `kernel.rs` (two lines) is counted with V |

## By kind of form, now

Columns: YAML forms, the forms whose notation counterpart passes, the YAML's claims, then identical / different / not at all for as it is and for ADJUSTED, then claims only the notation stores (as it is).

| kind | forms | passes | claims | as it is | ADJUSTED | only notation |
|---|---|---|---|---|---|---|
| concept | 9 | 9 | 78 | 65 / 11 / 2 | 65 / 11 / 2 | 13 |
| command | 6 | 6 | 118 | 0 / 93 / 25 | 111 / 4 / 3 | 119 |
| attribute | 3 | none, no form | 16 | 7 / 5 / 4 | 11 / 1 / 4 | 0 |
| rule | 16 | 16 | 112 | 4 / 92 / 16 | 13 / 85 / 14 | 124 |
| assert of a route | 2 | 2 | 4 | 0 / 4 / 0 | 0 / 4 / 0 | 4 |
| assert of an element | 1 | 1, as 11 asserts | 11 | 0 / 2 / 9 | 0 / 2 / 9 | 11 |
| assert of other data | 7 | 7 | 11 | 11 / 0 / 0 | 11 / 0 / 0 | 0 |
| view | 5 | 5 described (the Dialog compiler refuses every view) | 7 | 1 / 4 / 2 | 1 / 4 / 2 | 6 |
| event | 4 | 4, inline in views | 19 | 19 / 0 / 0 | 19 / 0 / 0 | 0 |

No form of the sketch is refused now, except the five views by the Dialog compiler. tonk's back end lowers those from their descriptions.

## By kind of difference, now

Each cell is counts of claims the notation stores differently, then not at all.

| kind | as it is | ADJUSTED | who would notice |
|---|---|---|---|
| a rule stored under another identity | 92 / 16 | 85 / 14 | upgrade protection, `declared_by` (`tonk-worker/src/router/repository.rs:5311`). Induction fires either rule |
| a command's lines picked `all:` where tonk picks `last:` ("declaration under another identity", "declaration not written", "description on another entity", "transient marker on another concept", part of "name points at another entity") | 60 + 0, 0 + 19, 21 + 0, 6 + 0, 6 + 0 | 0 | the display and `/transact` name a command by its concept entity (`tonk-template/src/resolve.rs:601`, `tonk-worker/src/router/transact.rs:306`) |
| a view stored on the pinned concept entity in the YAML (views 4 and 5) | 1 / 2 | 1 / 2 | the router names the pinned entity as a site's `concept` (`tonk-worker/src/router/session.rs:866`), so these views would not be found |
| a view's text differs as bytes, with the templates equivalent (views 1 to 3) | 3 / 0 | 3 / 0 | none: the display parses the text (`tonk-display/src/element.rs:1778`) |
| an element's method text: the YAML block ends in a newline, and the entity differs | 0 / 9 | 0 / 9 | none: elements are found by tag through `db.name/referent` (`tonk-display/src/registry.rs:17`) |
| an entity derived differently: route rows, the element's description | 5 / 0 | 5 / 0 | none: the router reads routes by path and concept (`tonk-schema/src/domain.rs:521`) |
| a concept pinned to an entity in the YAML | 7 / 0 | 7 / 0 | the router, as above |
| an attribute form's name, description and `role:` | 5 / 4 | 1 / 4 | name: the CLI (`tonk-cli/src/schema.rs:381`); `role:` goes to the palette |
| palette metadata (`action:`, `role:`) with no spelling | 0 / 3 | 0 / 3 | the palette's intent rules |
| descriptions: not written for pinned concepts, or other text (comments include `GAP:` lines) | 2 / 5 | 6 / 2 | none for behaviour |

Rules 1, 9 and 10 are identical (adjusted). What tells the other bodies apart, read off the decoded bodies:

| rules | cause |
|---|---|
| 3, 5, 6, 7 | variables the author writes and uses once (`next: ?x prev: ?p` on `block/insert`), where the YAML writes `_`. U covers only variables the expansion introduces |
| 8 | as 3, and a collection key bound with `==` |
| 11 to 15 | the notation binds a collection's key with `==` (`"assert":"=="` on `~1/key`) where the YAML binds the key variable in the premise |
| 4 | the reverse: the YAML binds with `==` what the notation binds directly |
| 2 | a variable used once, the head attribute with no type and pick `all:`, and the command named as a symbol where the YAML gives its concept entity |
| 16 | a retract by entity alone that types its premise differently |

## The back end

`notation_library_claims(core, library, views)` returns what `library_claims_after` returns for a YAML component (durable and transient instructions), plus per-form detail for measuring. It covers concepts, commands, rules, asserts and now views and events. It reads the notation through `check_forms`, `Checked::declarations` with `declaration_forms`, and `steps` with `step_forms`.

### Workarounds

Dropped with W:

- the reimplemented check loop;
- the copied private `described`;
- the join of head labels to lines (now `Line::label`);
- matching a collection line by domain (now `Line::descriptor`);
- matching declarations by name (now `declaration_forms`).

Remaining:

1. **An attribute's description** is set through a serde round trip. `AttributeDescriptor` has no setter; this is dialog-query's, not the notation's.
2. **A view's conditions come expanded to reads of attributes**, `(xyz.tonk.notebook ?n title: (last (text ?title)) entry: (entry … key: ?k))`, not as the concept read they were written as. Slot names are recovered by finding each read attribute among the concept's lines and taking `Line::label`; a collection's entry is found by its domain. Wanted: the condition as written (`(notebook ?n title: ?title key: ?k block: ?b)`), or on `View` the label each variable fills.
3. **`check_forms`' doc comment** says a refused form gives no declaration. The code declares before the compiler refuses, so a view still comes back. I rely on the code, and the comment should say so.

## Views and events

What is built, each piece a small function in the test:

- **Facet text.** The `View`'s template becomes a `tonk_render::Node` tree and is printed with `tonk_render::serialize_nodes`, tonk's own serializer. It is stored as `xyz.tonk.view/<name>` on the model concept's entity. A nested view becomes `<tonk-display>` with `model`, `entity` from its `of` and each entry (`with`).
- **Bindings record.** `tonk_template::bindings::Bindings::new` of one `EventDescriptor` per event (`Input::Path` as `Source::Property`), encoded by `Bindings::encode`.
- **Event rows.** `xyz.tonk.event/type`, one `xyz.tonk.event.where/<field>` per field, and `db.name/referent` of `id:on/<name>`. They are stored on the entity `tonk_schema::transact::derive_this` gives for the built-in `event` concept and the payload `{type, where.<field>}`, the recipe the YAML analyzer uses (`tonk-analyzer/src/analyzer/assertion.rs:793`, `:850`). All 19 event claims are identical.

Calls marked as the managing session's opinion, each one an isolated function:

- **The bar for a view is template equivalence** (`normalized`, `equivalence`). Both texts are parsed with `tonk_render::parse_fragment`, then compared as trees with attributes sorted, comments dropped (the renderer drops them) and text trimmed (whitespace alone dropped). Bindings are compared as decoded data. Result: 0 of 5 facet claims equal as bytes, 5 of 5 templates equivalent, 5 of 5 bindings equal. View 1's bindings record is equal as bytes too.
- **A variable's slot name** (`slots`). The rule tonk uses is in `tonk-template/src/fields.rs:26` to `:33` and `:47` to `:57`, checked at `tonk-analyzer/src/analyzer/view.rs:442`:
  - `{this}` is the subject;
  - `{<field>}` is a field the model declares, by its head label;
  - `{<field>/key}` is a keyed collection entry's key;
  - `{dom.host/<attr>}` is a host attribute.
  The description lacks the concept read each variable comes from, which this rule needs (workaround 2 above).
- **An event's name** (`event_name`) is `on/` and the command with `/` turned into `-`. Checked against every `on:<name>=<command>` binding in tonk's library YAML:
  - 41 bindings, 26 distinct;
  - 17 bindings (13 distinct) follow it as written;
  - 19 more (9 distinct) follow it once a command named by URI drops its scheme (`on:join=tonk:join`, 7 times);
  - 5 (4 distinct) do not: `on:branch-create=space/create-branch`, `on:invite=tonk:agent-handoff`, `on:prose-change=prose/edit` (2), `on:repository-rename=tonk/rename-repository`.
- **No event flags are read.** `prevent-default` and `stop-propagation` are written false, as all four of the notebook's events have them.
- **A view of a pinned concept** (views 4 and 5) is written on the entity the notation gives and counted as different.

## Installing from notation in tonk-worker, measured by reading

The install path, with what takes YAML text or tonk's `Syntax`:

| function | file and line | takes | can `notation_library_claims` stand in |
|---|---|---|---|
| `fetch_library_document` / `embedded_standard_library` | `tonk-worker/src/router/repository.rs:5704`, `:5722` | a URL, matched to an `include_str!` of a YAML file; in a browser the service worker's `/library/` (`tonk-ui/assets/service_worker.js:1561`) | no change needed beyond a new arm for the sketch's URL and the file shipped beside the YAML |
| `is_component_url` | `tonk-worker/src/router/library.rs:170` | requires the `.yaml` suffix | needs a change: a `.clj` URL reads as a main library, so `install_claims_for` would not put core in front |
| `install_claims_for` | `repository.rs:7054` | the library text, dispatching main against component | yes, keyed on the source: the one place to branch |
| `library_claims_after` | `repository.rs:7069` (parse at `:7079`, `analyze_local` at `:7092`) | YAML text joined with core's YAML text | yes: it returns the same durable and transient sets. One gap: it needs core in the notation (`core.clj`, four concepts), while the space's core is installed from YAML. Notation rules read core concepts by the notation's descriptors (`site` without its optional `selection`) |
| `seed_version` | `repository.rs:5604` | any text | yes, as it is |
| `install_fresh_from`, `install_seed` (upgrade), `upgrade_seed`, `upgrade_components` | `:4985`, `:4876`, `:4720`, `:4750` | call `install_claims_for` and `seed_version` | yes, through `install_claims_for` |
| `declared_by` (protection of other libraries on upgrade and uninstall) | `:5311` | re-reads each installed library's text through `install_claims_for` | yes, through `install_claims_for`, so a notation library is protected the same way |
| `uninstall_claims`, `assertions_at_version` | `:5259`, `:5483` | stored claims at the recorded version | no text read: works as it is |
| `prepare_seed`, `seed::prepare` (a space created from a seed URL) | `:870`, `tonk-worker/src/router/seed.rs:28` | YAML `Syntax`, checked against core's | not on the component path. It needs a notation form of `check` |
| `library_assertions` (profile library) | `:7027` | YAML text | not needed for a component |

Not on the install path and not replaced by it:

- the language server and the display's highlighter read YAML documents;
- the inspector's live `dialog-yaml` cells and the `/evaluate` route take YAML at run time;
- `tonk-library`'s `build.rs` parses YAML to find what to bundle.

The smallest end-to-end demonstration in tonk's suite is a `tonk-worker` router test beside `a_component_installs_beside_the_seed` (`repository.rs:16205`):

1. create a space from core;
2. install the notebook from the sketch's URL (with the two changes above, and `install_claims_for` calling the notation back end);
3. issue `block/edit` through the evaluate or transact path;
4. read `notebook/block` back.

It was not built. The cost in disk was not measured either. Estimated from this work, a `tonk-analyzer` test build took about 1 GB the first time, with 16 direct dependencies; `tonk-worker` has 77, so roughly 2 to 3 GB. That is more than the 2 GB left above the 3.5 GB floor at 5.4 GB free.

## Decisions needed

1. **The pick default on command lines** (with the maintainer). It is still the largest move: ADJUSTED stores 124 more claims identically.
2. **Variables the author writes and uses once.** Should the notation write them `_` too, or should the sketch spell `_`? This is the only cause left for rules 3, 5, 6 and 7 (U covers only the expansion's).
3. **A collection's key: bound with `==` or bound directly?** The two sides disagree in opposite directions on rule 4 and on rules 8 and 11 to 15. One spelling on both sides would make rules 4 and 11 to 15 identical, and rule 8 once 2 is settled.
4. **Concepts pinned to an entity.** Views 4 and 5, both route concepts, and their routes depend on it.
5. **The event name convention.** 36 of 41 bindings follow it once a command URI's scheme is dropped. The 5 that do not need a spelling, or a rename in tonk.
6. **A view's conditions as written, or each variable's label, on `View`.** This would end workaround 2.
7. **Whether tonk's install path branches on the source's suffix,** and which core a notation component reads: `core.clj` or core's YAML.
8. **Element entity and trailing newline, palette metadata, `GAP:` lines in descriptions.** These are unchanged from the last report.
