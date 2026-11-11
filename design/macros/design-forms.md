# What a form is to dialog

**Decision:** what a form is in dialog, so that a rule about syntax is a rule dialog stores and evaluates.

**For:** the direction, macros as rules. The [proof of concept](poc.md) runs the kernel on dialog, and holds the rules about syntax in memory and matches patterns itself. Nothing about syntax is stored, named by hash, or synced.

Not decided. This is the first sitting: the approaches, what separates them, and what would settle the open cells.

## What a rule about syntax needs

```
(rule (syntax (xyz.todo ?t list: ?l text: ?x state: ?s))      ; builds a form
  (when (expand (todo ?t list: ?l text: ?x state: ?s))))      ; takes one apart
```

1. A form is something a variable can be bound to.
2. `expand` takes a bound form apart by shape, and binds the parts. A part may be a run: `...` and `...?rest`.
3. `syntax` builds a form from the parts, and names apart every variable the pattern did not bind.
4. The walker asks dialog what a form expands to, a form at a time.

## The approaches

```
A  facts      form ── entity ──┬─ form/head  todo          each subform an entity of its own
                               ├─ N1         ?t
                               └─ text       ?x

B  bytes      form ── record:<bytes only form/* formulas read>

C  record     form ── { form/head: todo, N1: ?t, label/text: ?x }      a record as #748 has it:
                                                                       pairs, sorted, nested

D  data       pattern and template stored; a native expander applies them; no rule takes part
```

| criterion \ approach | Status quo: the crate holds syntax | A, a form is facts | B, a form is opaque bytes, read by formulas | C, a form is a record as #748 has it | D, syntax is data, expanding is native |
|---|---|---|---|---|---|
| a rule about syntax is a stored rule dialog evaluates | 🔴 it is a Rust struct in a walker | 🟢 a rule over `form/*` facts whose head asserts the facts of new forms | 🟢 a deductive rule: `expand` lowers to one `form/match` premise a variable, `syntax` to a chain that fills one variable a link, as `(:id …)` lowers to `entity/derive` | 🟢 a deductive rule: `expand` is destructuring a bound record, `syntax` a term that builds one | 🔴 a pattern and a template are stored; what applies them is not a rule, and a guard such as `(unless (dialog.name _ is: ?command))` is a second language |
| a form is whole: no part of it can be retracted from under it (decided 2026-11-11, full inlining) | holds, in memory | 🔴 a subform is an entity with facts of its own, which is the shared subform that was ruled out | 🟢 one value | 🟢 one value, written and replaced whole | 🟢 one value |
| `...` and `...?rest` | matched natively | 🟡 a run is a scan of a sequence, and the rest of a dictionary is "every entry but these", which is a negation a field | 🟢 a run and a rest are forms too, so a variable holds them | ? #748 leaves the surface of destructuring open, and says nothing of binding what a pattern did not name | matched natively |
| what dialog has to gain | nothing | heads that write several entities; a formula that makes each one (there now: `entity/derive`) | three or four formulas of fixed cells; a layout for a form's bytes, canonical, that nothing else reads | #748 built, plus what it has not decided: destructuring by pattern, a rest, and a term that builds a record in a head | an index of syntax by head name; nothing in the engine |
| one layout for compound values | not a question | not a question | 🟡 a second layout beside #748's, for forms alone | 🟢 the one #748 defines; a form's atoms and attributes are the tokens claims use | 🟡 as B |
| named by hash, with names as facts | 🔴 nothing is stored | 🟡 by an entity derived from its parts | 🟢 by its merkle reference, as a rule is today | 🟢 by its canonical form | 🟢 as B |
| cost of one expansion | a match in memory | ? a join a pattern node | ? one query, in which every rule deriving the expansion is tried | ? as B | 🟢 a lookup by head name and a match in memory |

Sorted by how much a row separates the columns. The last row is unknown for every column that keeps the direction.

## Hypotheses

Each is the agent's, to be decided on what it shows.

| hypothesis | confirmed by | refuted by |
|---|---|---|
| **H1.** A form is a #748 record with three conventions: a head attribute, position-named members for the bare parts, and a domain for labels. | Every form of `prelude.clj` and `examples/todo.clj` round-trips through such a record, with atoms as values dialog has. | An atom with no value to be: a variable, `_`, `...`. Then a form needs a value type #748 does not give it. |
| **H2.** A pattern is a destructuring. | #748's destructuring, as its surface is decided, binds a rest and a run. | It names fields by attributes only, with no way to say "every other field". Then C needs its own premise, and is B with #748's layout. |
| **H3.** Asking dialog for an expansion is cheap enough that expansion can be a query a form. | With 10, 100 and 1000 rules deriving one relation, each guarded by a constant head, a query bound to one form costs about the same. | The cost grows with the number of rules. Then syntax needs to be found by head name before any rule is tried, which is D's index under B or C. |
| **H4.** An expansion is a function of the form, where it stands and the syntax in force, so it is kept by the form's hash. | Submitting `examples/todo.clj` a second time asks dialog nothing. | A guard reads state that changes without syntax changing. `(dialog.rule _ on: ?domain)` in the warning rules does; it is a diagnostic, not an expansion. |

H3 does not depend on what a form is. It can be measured now, with rules over plain facts, and it decides whether B and C need D's index.

## Where this leaves the decision

- A is out by a decision already made, unless full inlining is reopened.
- D gives up the direction. Its index may still be what makes B or C fast.
- B can be built today and is a second layout. C is one layout and waits on #748, where nothing is built and the three things forms need are open.

So the question is not which encoding. It is whether forms are a use case #748 has to carry: patterns, a rest, and building a record in a head. If they are, they belong in that topic's requirements before its surface is decided. If they are not, B is the honest choice and the second layout is its cost.
