# Where the build stands against the notation's design

The notation is designed on #728: `research/notation/design.md`, read here at `45b0fdd1` (reader rounds to R45). This branch builds it. The build took its spellings from that design as of R25 and then settled things of its own, so the two have drifted both ways.

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
