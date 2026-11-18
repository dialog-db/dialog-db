# Where the build stands against the notation's design

The notation is designed on #728: `research/notation/design.md`, read here at `31929a6f` (reader rounds to R46). This branch builds it. The build took its spellings from that design as of R25 and then settled things of its own, so the two have drifted both ways.

## What moved in the design since it was last read here

Read at `45b0fdd1` on 2026-11-16, and again at `31929a6f` on 2026-11-18. Eight commits between them. What each changes for the build, in the order it is to be closed:

| | the design now | built here | gap |
|---|---|---|---|
| 1. a command | a concept whose head names no entity, declared, with attributes of its own on a control layer: `(concept (todo/add list: ?l text: ?x) (tonk.todo.add/list of: ?c is: ?l as: :entity in: :control) …)`. Its entity is derived from what is written | built 2026-11-18, as the design has it. The kernel tells a command by its fields on the control layer, `(:control …)`, and derives the entity a head does not name with `:id`. A misspelled command is no command | its lines in one domain only, as this build derives the entity from one domain's claims. Unsupported, as not decided: a concept with no entity whose lines are not all `in: :control`, one that mixes the two, one that names its entity and holds control lines, one about two entities it does not name |
| 2. issuing one | `(assert (todo/add …))`, at the top and in a rule's head, lands for the next tick and holds that tick only. A command in a head with no word is a deduction: it holds in this tick | built 2026-11-18: `assert` issues, at the top and in a head; no word deduces, read by other rules in the tick and never stored. A call alone at the top level is refused | retracting a command is not decided, and unsupported. `(id ?t …)` as a premise is not built |
| 3. a derived attribute | a rule concludes an attribute in a concept's own domain: `(rule (list/count-open ?l (count ?t)) …)`, read `(list/count-open list:home ?n)`. Concluded only: asserting it is an error. The getter is gone | built 2026-11-18: a name under a declared concept, `list/count-open`, is a relation of its own, `concept.list.count-open`, unlabeled under `is:` or with the fields its labels name. One that folds nothing runs. A write of one is refused. The getter, `:getter` and `:said` are gone | one that folds still waits on the engine (below). The relation is named by the concept's name, not its hash: the build has no hash of a concept. A concept's name read two ways (`list` declared twice) is not looked at |
| 4. an attribute's two terms | unlabeled, the entity then the value: `(xyz.todo/title ?t "Buy milk")`. `of:` and `is:` are the labeled spelling | built 2026-11-18, in a premise, a head, a line and a write | with `as:`, `cardinality:` or `in:` the terms are labelled: the reader refuses two parts with no label beside a labelled one |
| 5. a variant | the relation of its cases: `(todo/state ?s)` ranges over `open:` and `done:` | read as `:entity`, cases unchecked | this branch for the relation; a type for it in the engine |
| 6. a collection | the key a variable in the attribute's name, `by: :name` or `:position`: `(xyz.tonk.notebook/?p of: ?b is: ?x … by: :position)` | nothing | dialog's `Keyed`; not looked at yet |
| 7. a write with `when` | any top-level write may take one; it runs once and is not installed | nothing | this branch |
| 8. a view | proposed: `(view concept name template condition…)`, a deductive rule concluding `view` with the template folded into its value; events inline as `:on/<type>` | nothing | a fold in a rule that derives, a markup value, a renderer |

Two of the eight meet the same wall: 3 and 8 are folds in a rule that derives, which main still refuses (`ReduceInOpenRule`). The refusal has a design topic of its own now, #830 (`design/named-fold`). A spike there (2026-11-18, `spike/named-fold`) admitted the fold with a cycle through it quarantined, and it runs. What keeps the refusal is the reason #827 gave when it took the same change back: a folded value concluded into an attribute competes with the claims stored there. On the spike a stored `5` is read over a folded `150`. So what 3 and 8 wait on is a decision about where a computed value lives, not on work in dialog-query.


## What tonk's documents need, in the order they need it

Ordered by what `notebook.yaml` cannot be written without ([tonk.md](tonk.md)), from 2026-11-18. The bar is [sketches/notebook.clj](sketches/notebook.clj), measured by `cargo run -p dialog-notation --example bar` and pinned in `it_measures_how_far_the_notebook_gets`: of 56 forms, 56 read, 34 expand and 32 compile before step J.

| | the design now | built here | gap |
|---|---|---|---|
| E. a document's order | "The order of its forms does not matter" | built 2026-11-18: concepts and rules about syntax are expanded first, and what failed is expanded again once a macro's expansion declares a name | a name two expansions need of each other is refused as whatever the first met |
| F. creating things | `(id ?t list: ?l text: ?x)` derives an entity from labels and values, `(fresh ?n)` mints one, both as a rule's premises | built 2026-11-18 for `id`: the entity `(:id domain …)` names, the domain the head's that `?t` is the entity of. The to-do example creates with it | `fresh` unsupported: Dialog-DB has no formula that mints. Whether `id` names a domain or a concept stays open; an entity of no head or of two domains is unsupported |
| G. collections | a line over one leaves its key a variable in the name, `xyz.tonk.notebook/?p`, and says `by: :name` or `:position`. The key is a head field; one collection to a concept | built 2026-11-18, both kinds: read a row a key, one entry asserted by its key, rules that read and write entries. A key that is no head field and a second collection are refused | a retract of a collection is unsupported, not decided. An entry cannot be read by its attribute written out when its key is a position: Dialog-DB refuses the uppercase name |
| H. a text over lines | open: "How text spans lines or holds `\"`" | built 2026-11-18, the managing agent's choice: a text spans lines as written, `\"` and `\\` as in Clojure, printed back to the same text | no file included, no indentation taken off; an escape other than `\" \\ \n \t \r` is refused |
| I. a write with `when` | any top-level write may take one; it runs once and is not installed; every variable is bound by it | built 2026-11-18 for `assert` and `retract`: a query of the `when` over the branch before the document, each match making the write. Under `:one` the write succeeds the cell's claim, as main's `last` does | the `when` does not see the document's own writes or rules. A command with a `when` is unsupported, not decided. Premises after a top-level write with no `when` are refused |


## What the design says and the build does not do yet

| | the design, at R45 | built here | needs |
|---|---|---|---|
| a concept's body | a line an attribute, by full name: `(xyz.todo/title of: ?i is: ?t as: :text cardinality: :one)` | the same since 2026-11-16, in a concept and wherever a read stands. Under it a line is the read it was: `(xyz.todo ?i title: (:last (:text ?t)))`, which is still read | done |
| which claim is read | `cardinality: :one` or `:many` on the line; picks are gone | `:one` reads the newest and `:many` every claim, on main's picks `last` and `all`. What a write under `:one` retires is the engine's, as it was for `last` | the engine's cardinality work (#793) for what a write does |
| a variant, `as: [:open :done]` | a type: on a read it filters, on an assert it refuses | read as `:entity`. That the value is one of the cases is not checked, either way | a type for it in the engine |
| a sized number, `as: (:float 64)` | a type | refused: not a type | this branch |
| a layer, `in:` on a line | a constant or a field | `:control` only, a command's, since 2026-11-18; any other refused as unsupported | the layers work (#795 on) |
| choosing among claims | an elector in the head: `(account ?a status: (top ?s))`, ordered by the line's `as:` list | nothing | #793 |
| a variable of the body | is in the head, so a concept runs both ways | checked, for lines as for reads | done |
| `!` | no form carries one: `assert`, `retract` | the same since 2026-11-16; `assert!` and `retract!` are refused with the word to write | done |
| a parameter left out of a read | matches anything | refused since 2026-11-13; open again on 2026-11-15, not built | this branch |
| a derived attribute | a rule concludes an attribute in a concept's own domain: `(rule (list/count-open ?l (count ?t)) …)`, read as `(list/count-open list:home ?n)`. It replaced the getter | built 2026-11-18, to a relation of its own, `concept.list.count-open`. One that folds nothing runs. One that folds is refused as unsupported | a decision: the engine refuses a fold in a rule that derives (below) |
| a layer | `in:` on a line, a constant or a field | nothing | the layers work (#795 on) |
| a view | proposed: a template in Hiccup over one concept, kept as data | nothing | after the above |

## What the build settled and the design does not say

Each of these was decided in this branch's sittings (`journal.md`) and is in no file of #728. Until it is, it is decided in one place only.

| | here |
|---|---|
| `[ ]` | a tuple as syntax; where a value stands, a list |
| `'( )` | a quoted list, the same list |
| `{ }` | a struct: a value, and with a variable in it a pattern, open where it is read |
| `...` | `... p` takes what is not named; in a head it captures, in a body it spreads. The design's prelude writes `… ?params`, proposed |
| a call | `(todo ?t list: ?l)` is sugar for `(todo {:of ?t :list ?l})`, the maintainer's on 2026-11-18. A struct's keys are keywords; `{of: ?t}` is refused with the spelling to write |
| a macro | a rule: `(expand …)`; of the rules that apply, the one that leaves least to `...` wins |
| the reader | gives `...` no meaning; `'x` is `(quote x)` |
| a comment | right above a form it describes the form |

## Two things that cannot both be true

The design's derived attribute, as its getter was, is a rule that derives a fold, installed, and read wherever a read stands. The engine has the fold (`reduce`, `notes/aggregation.md`) and refuses it in a rule that derives (`ReduceInOpenRule`, argued in `notes/attribute-heads.md`):

> `reduce` does not: a fold has no reading over a set still growing, and unlike a choice it cannot be deferred to the component's exit without giving the rules inside a different relation than the readers outside. A fold belongs to the closed places, a query, a subscription, an inductive rule.

```
 a derived attribute, designed    (rule (list/count-open ?l (count ?t)) …)            installed; any rule may read it
 a fold, as the engine has it     refused in a rule that derives                      admitted in a query, a subscription, a rule that writes
```

Both are the maintainer's. Which gives is his. What each way would mean for the notation, as columns and not as a recommendation:

| | a derived attribute stays a rule | a derived attribute is a name for its body |
|---|---|---|
| what `(rule (list/count-open …) …)` is | a rule that derives, which the engine has to admit with a fold, outside a cycle at least | not installed: where it is read, its premises and its fold stand in its place |
| where it may be read | anywhere a read stands | where the engine admits a fold: a query, a view, a rule that writes |
| read in a rule that derives | allowed, so a fold feeds a derivation | refused, with the engine's reason |
| what the engine needs | a fold in a rule that derives, stratified as `unless` is | a fold a query or a subscription can state, which main does not expose as a way to ask yet |

The left column was spiked on 2026-11-18 and runs (#830's journal). It does not settle the row the table lacks: what a read gives when a claim is stored under the derived attribute's name, which a merge can bring whatever a write refuses.

Not checked: whether a query on main can state a fold at all. `DeductiveRule::with_reduce` is the one constructor found, and it is the one refused.

## One thing that may be one thing

The design counts three meanings of `[ ]`: a variant in `as:`, markup in a view, and (in conduct) a list. Read as this branch reads it, it is one: a list. `as: [:open :done]` is a list of cases, in the place a type goes. `[:li ?title]` is a list whose first item is a keyword, in the place a template goes. A hypothesis, not tested: readers given "`[ ]` is a list" and the two places read both as they do now. A reader round would say.

## Order

In [synthesis.md](synthesis.md), which puts the two tables above together with the layers, the cardinality work and the record.
