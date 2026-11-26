# The canonical form of a read

**Decision:** what one read of an attribute is in its canonical form: where expansion stops, what is shown back, and what a rule about syntax would match.

**For:** a line says everything about the claims it reads, and whoever reads the canonical form should meet nothing the design retired.

**Not decided.** The maintainer, 2026-11-18: the expansion built today is confusing and misleading, and the line should be canonical. He also considered `claim`, below.

```clj
; A. as built: a read of a domain, the pick and the type wrapped round the term
(xyz.tonk.notebook ?n title: (:last (:text ?title))
                      entry: (:entry (:all (:entity ?b)) by: :position key: ?k))

; B. the line, as the design writes it
(xyz.tonk.notebook/title of: ?n is: ?title as: :text cardinality: :one)
(xyz.tonk.notebook/?k of: ?n is: ?b as: :entity cardinality: :many by: :position)

; C. claim, every part a labeled term
(claim domain: "xyz.tonk.notebook" name: "title"
       of: ?n is: (term/text ?title) cardinality: :one)
(claim domain: "xyz.tonk.notebook" name: (term/position ?k)
       of: ?n is: (term/entity ?b) cardinality: :many)
```

| criterion \ approach | A. domain read: what `primitive.rs` reads today | B. the line: the surface is the canonical form | C. `claim`: one operator, each part a term |
|---|---|---|---|
| says what the design says | 🔴 `:last` and `:all` are picks, which `cardinality:` replaced (design.md, Cardinality; #759). It has the shape of a concept applied, and is not one. `entry:` is a field nobody wrote | 🟢 it is the design's line; nothing is translated | says what the line says, part for part. The design already calls a line a claim |
| one role to a part | the operator is a domain, lines of one entity fold into one read, pick and type nest | 🟡 the attribute's name holds the domain, the name and, over a collection, a variable (`/?k`). `by:` is there because the key sits in the name. A blank key is `/_` | 🟢 domain, name, key and type are each a part. A key is a term like any other, typed by `term/position`, so `by:` goes |
| read by people | ? never put to readers | 🟢 R40 to R46: 100 of 100 read `/?k` as a row for each key, 20 of 20 told a sequence from a dictionary with `by:` | ? never put to readers. Five lines to a read |
| matched by a rule about syntax | a pattern binds a domain's fields | 🟡 a pattern cannot take the name apart. Nothing does today: no rule in the prelude looks inside a line, and `expand.rs` splits the name in native code | 🟢 every part is a term a pattern binds |
| forms for one read | 🟡 two, and the second renames what the first said | 🟢 one | two if the line stays the surface, though one for one; one if it replaces the line |
| what it lets one write that dialog cannot read | nothing | nothing: a variable in the name is refused without `by:` | ? `name: ?x` bare reads as any attribute of the domain. Whether dialog can read that is not checked |
| cost here | none | `primitive.rs` reads lines; 26 tests written by hand in A move; whether lines of one entity run as one application becomes the compiler's, out of sight | B's, and a reader for `claim` and `term/…` |

A is the engine's vocabulary shown as syntax: a transliteration of the descriptor dialog-query runs. Nothing in B or C needs the engine to change. The descriptor is what a read compiles to, and need not be a form.

B is easy and C is simple. The line was fitted to readers and they read it right; `claim` gives each part one role and has met no reader. Which matters turns on who reads the canonical form: a person (an error, a view's source, a description), or a rule that takes a read apart.

**Hypothesis, the agent's.** A person. No rule takes a line apart today, so B is enough, with the descriptor as compile output. Refuted by the first rule about syntax that must build or match an attribute's name from its parts; then C is the primitive forms and the line a one-for-one spelling of it.
