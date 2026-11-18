# How a label is spelled

**Decision:** how a call says which parameter a value is for.
**For:** one notation a reader reads right cold, and one reader for it (#728).

Raised by the maintainer on 2026-11-18: "instead of my special labels we should just use keyword based labeling". Decided by him the same day: "I think we should just treat (foo ?bar baz: ?beep) as sugar for (foo {:of ?bar :baz ?beep})". That is the fourth column, keywords in a map only, with a call read as sugar for the struct. Its yellow cell, one spelling in a map and another in a call, is answered by the sugar: a call's label is the struct's key. Built 2026-11-18 ([journal](journal.md)).

Today a token that ends in a colon is a label and one that starts with a colon is a keyword, a value. The design says why the keyword has its colon in front: "`:done` is `done:`, with the colon in front so it is not read as a label."

| criterion \ approach | Status quo: `list: ?l` | Keywords, paired by place: `:list ?l` | Keywords, paired by what is called | Keywords in a map only: `{:view :row}`, calls keep `list:` |
|---|---|---|---|---|
| telling a label from a value | by the token alone, with nothing known about what is called | after the entity, terms go in pairs, and the first of a pair is the label. 🔴 A keyword that is a value and has no label is read as one: `(view todo :row [:li …])` gives the template the label `row`, and `(xyz.state/name :open "Open")` gives `"Open"` the label `open` | 🟡 the reader gives a flat list and what is called says which keywords are labels. A form whose head is not known cannot be split, and a macro's pattern matches places, not labels | as the status quo in a call; in a map every key is a keyword |
| one spelling of a key | 🟡 two in the design today, in one template: `{:type "checkbox" :checked ?done}` and `{view: :row of: ?t}` | one | one | 🟡 one in a map and another in a call, and the build reads a call as a struct: `(todo ?t list: ?l)` is `(todo {of: ?t list: ?l})` |
| two keywords side by side | does not happen: `state: :open` | `:state :open`, told apart by place only | the same | does not happen in a call |
| read by tools there are | ? `list:` is a symbol Clojure reserves; not checked that an EDN reader refuses it | ? every form in the design would be EDN; not checked | the same, and it is how Clojure reads keyword arguments | ? maps are EDN, calls are not |
| the YAML spelling to come | a label is a YAML key as written | mapped: `:list` to `list:` | mapped | mapped inside maps |
| what readers have been shown | every round, R1 to R46 | none | none | none |
| what the build changes | nothing | the reader's token rule, the printer, and the text of every example and test | those, and the expander's patterns, which match labelled parts today | the reader inside `{ }` and the printer |

No column is all one colour. The second column's red cell is in the design's own newest form, `view`, so the pairing rule cannot be the whole of it: either `view` takes labels for its name and template, or a keyword value with no label is written some other way.

The agent's hypothesis, to be decided by a round and not by argument: readers read `:label value` as well as `label: value` wherever labels and values alternate cleanly, and misread or miswrite where a keyword value stands with no label. See [claims.md](claims.md).
