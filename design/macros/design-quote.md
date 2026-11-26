# Quote and syntax-quote

**Decided, by the maintainer, 2026-11-26.** Both quotes, as a Lisp user expects them:

- `'form` is the form as data, and a `?x` in it is a literal variable.
- A backtick is the form as data, with `~?x` replaced by the value `?x` is bound to, and `~@` splicing if it comes for free.
- `[ ]` is the only spelling of a list.

The type, the coordinating session's call, which he delegated: a quoted form is the value `notation/read` gives for that text. Not built; this note is what building it has to settle.

## What changes from today

2026-11-14 decided that a quoted `( )` where a value stands is a list: `'(1 2 3)` is the list of three numbers (`walk` in `compile.rs`). A variable in it binds, as a list pattern does: `'(1 ?x)` matches a list whose second item is anything. Under the decision, both readings go:

- **`'(1 2 3)` is a form,** the application of `1` to `2 3`, as data. It is not the list `[1 2 3]`, and the two are not equal: the form is a map node, `{:type :application :operator … :operand …}` (`Syntax::value`), and the list is a list. A Lisp has them equal because its code is lists. Here code is a tree of maps, so they cannot be.
- **`'(1 ?x)` matches nothing it matched.** `?x` in it is a variable node, a literal, so it equals only a form that holds that variable by that name.

To migrate, every list written `'( )` becomes `[ ]`. There are 16 in `tests/notation.rs` and none in the fixtures or the prelude. A quoted form where a list is expected is refused, naming `[ ]`.

## The value a quoted form is

It is what `notation/read` gives under `tree` for the same text: `Syntax::value` of the tree, a composite. It is a plain value, not an entity, and nothing of it is stored by quoting.

- **Equal by content.** Two composites are equal when their trees are. A tree holds no source positions: spans, the order labels were written in, whether `of:` was written, and comments are kept beside it, in `Written`. So `'(todo ?t)` equals the read of `"(todo ?t)"` wherever that text stands, and `'(todo of: ?t)` equals `'(todo ?t)`.
- **Its type in a query** is the composite's, a map. No line can say so today: the eight types a line takes (`primitive::TYPES`) are scalars, so a field that holds a form is read untyped. Whether a line can say "a composite", or "a form", is open.
- **What takes it.** `notation/analyze` takes a form, so a quoted form can be analyzed without parsing, and later evaluated. Evaluating one from a rule waits on rules holding the supplied formulas (`crate::supplied` in dialog-query).

## Syntax-quote

A backtick builds the form from the row: `` `(todo ~?t title: "x") `` is the form with `?t`'s value in its place. In a query this is a composite built from bound values, which the engine already does for a pattern a head builds (`Pattern::build`). `~@?items` splices a list's items into the items of an application. That needs a list's items placed among a node's, which a pattern does not do today: it does not come for free, and is left out until asked for.

## What the reader needs

The reader reads `'` alone (`form.rs`). A backtick, `~` and `~@` are not tokens today, and are taken by nothing else in the notation, so adding them collides with nothing written.
