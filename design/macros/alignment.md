# Where the build stands against the notation's design

The notation is designed on #728: `research/notation/design.md`, read here at `45b0fdd1` (reader rounds to R45). This branch builds it. The build took its spellings from that design as of R25 and then settled things of its own, so the two have drifted both ways.

## What the design says and the build does not do yet

| | the design, at R45 | built here | needs |
|---|---|---|---|
| a concept's body | a line an attribute, by full name: `(xyz.todo/title of: ?i is: ?t as: :text cardinality: :one)` | one call on the domain, each field in a wrapper: `(xyz.todo ?t text: (:last (:text ?x)))` | this branch |
| which claim is read | `cardinality: :one` or `:many` on the line; picks are gone | the wrapper's pick, `:last` | this branch for `:one` and `:many`; the engine's cardinality work (#793) for the rest |
| choosing among claims | an elector in the head: `(account ?a status: (top ?s))`, ordered by the line's `as:` list | nothing | #793 |
| a variable of the body | is in the head, so a concept runs both ways | not checked | this branch |
| `!` | no form carries one: `assert`, `retract` | `assert!`, `retract!`, decided here on 2026-11-12 as a departure | this branch |
| a parameter left out of a read | matches anything | refused since 2026-11-13; open again on 2026-11-15, not built | this branch |
| a computing fold | only in a getter: `(rule (open-count (list ?l) is: (count ?t)) …)` | nothing | this branch, over the engine's folds |
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

## One thing that may be one thing

The design counts three meanings of `[ ]`: a variant in `as:`, markup in a view, and (in conduct) a list. Read as this branch reads it, it is one: a list. `as: [:open :done]` is a list of cases, in the place a type goes. `[:li ?title]` is a list whose first item is a keyword, in the place a template goes. A hypothesis, not tested: readers given "`[ ]` is a list" and the two places read both as they do now. A reader round would say.

## Order

In [synthesis.md](synthesis.md), which puts the two tables above together with the layers, the cardinality work and the record.
