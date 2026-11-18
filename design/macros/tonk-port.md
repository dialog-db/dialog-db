# What porting notebook.yaml found

`rust/tonk-core/assets/library/notebook.yaml` at tonk `c006b50`, written in the notation as `research/notation/design.md` has it at `origin/docs/notation` (R46), with a struct's keys as keywords: [sketches/notebook.clj](../../rust/dialog-notation/sketches/notebook.clj). How the result would reach tonk is in [tonk-integration.md](tonk-integration.md).

The YAML holds 53 top-level forms: 3 attributes, 9 concepts, 16 rules, 1 element, 5 views, 2 routes, 6 commands, 4 events and 7 asserts of data. 24 of them hit at least one gap; 29 port as the design says.

## Gaps

Each kind is a `; GAP:` comment in the port, right above the form or the part of a template that needs it. "Forms" counts top-level YAML forms.

| gap | forms | YAML lines | in the design's Open list |
|---|---|---|---|
| a rule reads a command's own entity (`this: ?this` on a command premise) to make it the entity of what the command creates. The design derives a command's entity and gives a rule no way to read it; the port recomputes it with `id` over every field, and whether that is the same entity is not said | 5 | 704, 738, 773, 811, 835 | no. Nearest: "Whether `id` names a domain or a concept, so two concepts with the same labels and values do not share an entity." |
| a nested view of a named concept, with no entity, or read from another space and branch (`<tonk-display model=… [entity=…] with="{branch}@{repo}">`). The design's reference, `{view: :row of: ?t}`, names the view and the entity only | 3 | 327, 434, 463 | in part: "Whether templates name models by concept name rather than by pinned URI." |
| a concept named by an entity where a value stands: a route's `concept:`, a site's `concept:` | 3 | 392, 439, 604 | no |
| text spanning lines, or holding `"` and `\` | 3 | 214 to 301, 990 to 993, 998 to 1001 | yes: "How text spans lines or holds `"`. The sketches write templates as `"""…"""`, which is not part of the notation." |
| a retract of one entry of a collection | 3 | 885, 916, 937 | yes: "What retract takes for a collection concept: `(retract (notebook-entry notebook:scratch))` names every entry; it likely needs the entity and the key." |
| a view with no entity: a `directory` view renders every instance, its markup outside the per-entity element once | 2 | 343 to 356, 359 to 369 | no |
| a concept pinned to an entity (`this: tonk:notebook/route`), so two route concepts over the same site attributes stay two concepts with two views | 2 | 395, 442 | no. The design says the opposite: "A concept's identity is its set of attributes." |
| an attribute value built of several parts (`href="/notebook/{this}"`, `with="{branch}@{repo}"`) | 2 | 353, 426, 434 | no |
| a valueless HTML attribute (`hidden`, `draft`): the design has no boolean term | 2 | 323, 425 | no |
| metadata the palette reads: `role:` on an attribute, `action:` on a command | 2 | 571, 575, 582 | yes: "Whether metadata (`description:`, `action:`, `role:`) is labeled entries kept as data." |
| a comparison: the design lists "less than, greater than" among constraints and gives no spelling. The port writes the build's, `(< ?p : 10)` | 1 | 129 to 132 | no |
| naming an entity so the runtime finds it by name: the element's tag is its anchor, `&notebook-save` | 1 | 211 | yes: "How an entity is named across the rules and commands of a document, and what a bare symbol such as `scratch` means." |
| an element is four collections keyed `by: :name` over one entity, and a concept holds one collection, so `core.yaml`'s `element` cannot be asserted through | 1 | 211 to 301 | no. The design states the rule: "a concept holds one collection only" |
| a concept as a value: an intent names the command it would run, `command: notebook/retitle` | 1 | 595 | in part: the naming line above |
| an HTML comment in a template | 1 | 348 to 352 | no |

Ten of the fifteen kinds are not in the Open list. Seven of those are about views and routes, the part the design marks Proposed.

What needed no gap: the four `event!` forms are inline in the two views that bind them, and none needs a flag (none prevents a default). The three `attribute!` forms have no form of their own, as the design says. The YAML's `unless: ==` is `!=`, and its `==` that binds a key is the key bound directly, since a constraint binds nothing. The two seeds that assert `notebook` without its title go through `notebook/blocks`, the concept that holds only the sequence.

## Design features the port uses

Counted over the port's forms. "Built" is from [alignment.md](alignment.md), and from `rust/dialog-notation/src` where alignment.md says nothing.

| feature | forms | built in dialog-notation |
|---|---|---|
| attribute lines by full name, `of:` `is:` `as:` | 15 (9 concepts, 6 commands; 33 lines) | yes |
| `cardinality:` | 9 concepts, `:many` in 2 | yes, on main's picks `last` and `all` |
| a command premise in a rule | 13 | yes |
| a command: no entity in the head, lines `in: :control` | 6 | yes, since 2026-11-18, each command in one domain as here |
| a collection, `by: :position` (declared, read or written) | 15: 2 concepts, 8 rules, 2 views, 3 seeds | no (alignment.md item 6) |
| a collection `by: :name`, written by attribute | 1 (the element, 9 entries) | no |
| `id` as a premise | 7: 5 rules, 2 top-level writes | no (alignment.md item 2). It compiles only as a head's entity, to `entity/derive` (`src/compile.rs:15`) |
| top-level assert through a concept, ground | 7 | yes |
| a deductive rule concluding a concept | 5 (recursive in 2) | yes |
| a rule asserting through a concept | 6 | yes |
| a rule retracting | 4 | yes: `(retract A)` with premises is an inductive rule (`src/compile.rs:13`). A retract of a concept by its entity alone, `(retract (notebook/block ?b))`, is not looked at |
| `view` with a Hiccup template | 5 | no (alignment.md item 8) |
| inline events, `:on/<type>` with paths | 2 views, 8 bindings | no |
| a parameter left out of a read | 4 (a view, 3 rules) | no: refused since 2026-11-13, open again, not built |
| `dialog/position` | 2 | no: the build's formulas are `+ - * / < <= > >=`, `value` and the types (`src/compile.rs:829` to `:832`) |
| `!=` | 2 | no: the build spells equality `value` and has no inequality |
| top-level write with `when` | 2 | no (alignment.md item 7) |
| a deductive rule concluding an attribute, terms unlabeled | 1 | yes (alignment.md item 4) |
| `unless` | 1 | yes (`src/kernel.rs:449`) |
| a comparison | 1 | yes, in the build's spelling only |
| a top-level attribute write with `as:`, `cardinality:`, `by:` | 1 form, 11 writes | yes without `by:` |
| electors, derived attributes that fold, layers other than control, variants, `fresh` | 0 | |

So the order the notebook asks for: collections (15 forms), `id` as a premise (7), and views with inline events (5 views, every one with a gap), then a parameter left out of a read (4), then `dialog/position`, `!=` and `when` (2 each). The notebook uses no fold anywhere: the YAML says why at lines 105 to 107 and 756 to 758, a fold in the recursion is unstratified. So the wall alignment.md names, a fold in a rule that derives, does not stand between the notebook and the engine.

## What the YAML does not say plainly

- Whether `pick: :all` on a keyed field (line 55) means every claim of each entry or every entry. The comment above it (lines 50 to 54) says it is per entry and that its effect is the renderer iterating. `notebook/blocks` over the same attributes has no pick (line 78). The port writes `cardinality: :many` on the one and `:one` on the other, as written.
- Whether the `block` collection over the domain `xyz.tonk.notebook` (line 49) also reads `xyz.tonk.notebook/title` (line 25) as an entry keyed `title`, or whether `as: {[position]: entity}` leaves it out. The design does not say whether `by: :position` filters keys that are not positions either.
- What the index view posts on a block edit: its `<tonk-notebook>` has no `data-subject` (lines 424 to 431), and the events read `.currentTarget.dataset.subject` (line 540).
- Which variable is the value of the attribute concept `notebook/retitle-subject` when a rule concludes it (line 590). The port takes `?subject`, the name of the field the attribute's name ends in.
- The comment at lines 510 to 515 describes a create command that is not in the file.
- `block/move`'s `from` (line 669) is written by the element and read by no rule; the comment says so.

## Where the design disagrees with itself

- **What a top-level form is.** Documents (design.md lines 95 to 100) lists four: a question, a write, a `rule`, a `concept`. Views (line 424) adds `(view …)`, used at the top level in every example.
- **Where a view concludes.** "Folds conclude only into a concept's domain, choosing or computing: `count`, `all`, a view" (line 201). "`(view todo :row TEMPLATE CONDITIONS…)` is a deductive rule concluding the built-in relation `view`" (line 449), which is no concept's domain.
- **Whose layer a conclusion is on.** "`in:` names the layer a line's claims are on" (line 219), and an attribute's identity is "its domain, its name, its type and its cardinality" (line 265), with no layer. "A rule's conclusion is a claim of the attribute it concludes, on that attribute's layer" (line 228). The port's palette rule (YAML 588) concludes a command's attribute, which is on the control layer only through the command's line.
- **What `concept/name` names.** "`list/count-open` is an attribute in the domain of the `list` concept" (line 199). `todo/add` is a command, a concept of its own (line 272). `(todo/state ?s)` is a variant's relation (line 214). tonk names concepts this way throughout: six of the notebook's concepts are `notebook/…` beside a declared `notebook`, and `intent/expression` sits beside core's `intent`. The Open list has the case where the concept's name is ambiguous (line 527), not this one.
- **How a map is written.** A record is `{"N1" id:notebook/scratch/1}` (line 68), a nested view `{view: :row of: ?t}` (line 461), a record template `{name: ?name open: ?count}` (line 470). The keyword-key decision settles the last two, `{:view :row :of ?t}`; a collection's record keeps text keys.
- **The examples' `todo`.** The concept is declared with `title:` and `state:` (line 128); writes pass `text:` (lines 98, 276, 291, 331) and a view reads `done:` (line 431). Line 291 says leaving a parameter out of such an assert is a compile error, so the examples do not compile against the declaration.
