# Where the build stands against the notation's design

The notation is designed on #728: `research/notation/design.md`, read here at `31929a6f` (reader rounds to R46). This branch builds it. The build took its spellings from that design as of R25 and then settled things of its own, so the two have drifted both ways.

## What moved in the design since it was last read here

Read at `45b0fdd1` on 2026-11-16, and again at `31929a6f` on 2026-11-18. Eight commits between them. What each changes for the build, in the order it is to be closed:

| | the design now | built here | gap |
|---|---|---|---|
| 1. a command | a concept whose head names no entity, declared, with attributes of its own on a control layer: `(concept (todo/add list: ?l text: ?x) (tonk.todo.add/list of: ?c is: ?l as: :entity in: :control) …)`. Its entity is derived from what is written | built 2026-11-18, as the design has it. The kernel tells a command by its fields on the control layer, `(:control …)`, and derives the entity a head does not name with `:id`. A misspelled command is no command | its lines in one domain only, as this build derives the entity from one domain's claims. Unsupported, as not decided: a concept with no entity whose lines are not all `in: :control`, one that mixes the two, one that names its entity and holds control lines, one about two entities it does not name |
| 2. issuing one | `(assert (todo/add …))`, at the top and in a rule's head, lands for the next tick and holds that tick only. A command in a head with no word is a deduction: it holds in this tick | built 2026-11-18: `assert` issues, at the top and in a head; no word deduces, read by other rules in the tick and never stored. A call alone at the top level is refused | retracting a command is not decided, and unsupported. `(id ?t …)` as a premise is not built |
| 3. a derived attribute | a rule concludes an attribute in a concept's own domain: `(rule (list/count-open ?l (count ?t)) …)`, read `(list/count-open list:home ?n)`. Concluded only: asserting it is an error. The getter is gone | the getter: `(open-count (list ?l) is: …)`, in `getter.open-count.list` | the spelling and the refusal of an assert. One that folds still waits on the engine (below) |
| 4. an attribute's two terms | unlabeled, the entity then the value: `(xyz.todo/title ?t "Buy milk")`. `of:` and `is:` are the labeled spelling | `of:` and `is:` only | this branch |
| 5. a variant | the relation of its cases: `(todo/state ?s)` ranges over `open:` and `done:` | read as `:entity`, cases unchecked | this branch for the relation; a type for it in the engine |
| 6. a collection | the key a variable in the attribute's name, `by: :name` or `:position`: `(xyz.tonk.notebook/?p of: ?b is: ?x … by: :position)` | nothing | dialog's `Keyed`; not looked at yet |
| 7. a write with `when` | any top-level write may take one; it runs once and is not installed | nothing | this branch |
| 8. a view | proposed: `(view concept name template condition…)`, a deductive rule concluding `view` with the template folded into its value; events inline as `:on/<type>` | nothing | a fold in a rule that derives, a markup value, a renderer |

Three of the eight meet the same wall: 3 and 8 are folds in a rule that derives, which main still refuses (`ReduceInOpenRule`), and the design's own Open list says the engine and the notation have to agree (#793, now #827). The contradiction below stands, with the getter's name changed.


## What the design says and the build does not do yet

| | the design, at R45 | built here | needs |
|---|---|---|---|
| a concept's body | a line an attribute, by full name: `(xyz.todo/title of: ?i is: ?t as: :text cardinality: :one)` | the same since 2026-11-16, in a concept and wherever a read stands. Under it a line is the read it was: `(xyz.todo ?i title: (:last (:text ?t)))`, which is still read | done |
| which claim is read | `cardinality: :one` or `:many` on the line; picks are gone | `:one` reads the newest and `:many` every claim, on main's picks `last` and `all`. What a write under `:one` retires is the engine's, as it was for `last` | the engine's cardinality work (#793) for what a write does |
| a variant, `as: [:open :done]` | a type: on a read it filters, on an assert it refuses | read as `:entity`. That the value is one of the cases is not checked, either way | a type for it in the engine |
| a sized number, `as: (:float 64)` | a type | refused: not a type | this branch |
| a layer, `in:` on a line | a constant or a field | refused as unsupported | the layers work (#795 on) |
| choosing among claims | an elector in the head: `(account ?a status: (top ?s))`, ordered by the line's `as:` list | nothing | #793 |
| a variable of the body | is in the head, so a concept runs both ways | checked, for lines as for reads | done |
| `!` | no form carries one: `assert`, `retract` | the same since 2026-11-16; `assert!` and `retract!` are refused with the word to write | done |
| a parameter left out of a read | matches anything | refused since 2026-11-13; open again on 2026-11-15, not built | this branch |
| a getter | a rule whose conclusion wraps a concept and gives a value in `is:`: `(rule (open-count (list ?l) is: (count ?t)) …)`, read as `(open-count (list list:home) is: ?n)` | read and expanded since 2026-11-16, to a relation of its own, `getter.open-count.list`. One that folds nothing runs. One that folds is refused as unsupported | a decision: the engine refuses a fold in a rule that derives (below) |
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
| a call | `(todo ?t list: ?l)` is sugar for `(todo {of: ?t list: ?l})` |
| a macro | a rule: `(expand …)`; of the rules that apply, the one that leaves least to `...` wins |
| the reader | gives `...` no meaning; `'x` is `(quote x)` |
| a comment | right above a form it describes the form |

## Two things that cannot both be true

The design's getter is a rule that derives a fold, installed, and read wherever a read stands. The engine has the fold (`reduce`, `notes/aggregation.md`) and refuses it in a rule that derives (`ReduceInOpenRule`, argued in `notes/attribute-heads.md`):

> `reduce` does not: a fold has no reading over a set still growing, and unlike a choice it cannot be deferred to the component's exit without giving the rules inside a different relation than the readers outside. A fold belongs to the closed places, a query, a subscription, an inductive rule.

```
 a getter, as designed            (rule (open-count (list ?l) is: (count ?t)) …)      installed; any rule may read it
 a fold, as the engine has it     refused in a rule that derives                      admitted in a query, a subscription, a rule that writes
```

Both are the maintainer's. Which gives is his. What each way would mean for the notation, as columns and not as a recommendation:

| | a getter stays a rule | a getter is a name for its body |
|---|---|---|
| what `(rule (open-count …) …)` is | a rule that derives, which the engine has to admit with a fold, outside a cycle at least | not installed: where it is read, its premises and its fold stand in its place |
| where it may be read | anywhere a read stands | where the engine admits a fold: a query, a view, a rule that writes |
| read in a rule that derives | allowed, so a fold feeds a derivation | refused, with the engine's reason |
| what the engine needs | a fold in a rule that derives, stratified as `unless` is | a fold a query or a subscription can state, which main does not expose as a way to ask yet |

Not checked: whether a query on main can state a fold at all. `DeductiveRule::with_reduce` is the one constructor found, and it is the one refused.

## One thing that may be one thing

The design counts three meanings of `[ ]`: a variant in `as:`, markup in a view, and (in conduct) a list. Read as this branch reads it, it is one: a list. `as: [:open :done]` is a list of cases, in the place a type goes. `[:li ?title]` is a list whose first item is a keyword, in the place a template goes. A hypothesis, not tested: readers given "`[ ]` is a list" and the two places read both as they do now. A reader round would say.

## Order

In [synthesis.md](synthesis.md), which puts the two tables above together with the layers, the cardinality work and the record.
