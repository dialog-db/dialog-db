# A constraint per type

**Decision:** what a document writes to say a variable's type where no line says it, now that a document writes no wrapper (journal, 2026-11-25, "Decided: a document writes no wrapper"); what it compiles to, what it stores, and what it does at compile time and at run time.

**For:** a document that passes analysis must not fail at evaluation on the types of the values it meets, and compile time and run time must never disagree on a type: a wrong answer is worse than a refusal (the maintainer, relayed 2026-11-26).

**Not decided.** Nothing is built. Open: the matrix below, and hypotheses H1 to H4.

## What the maintainer asked for

> "something like formula `(integer of: ?n is: ?m)` where `is` is of `integer` type and just like `==` two are unified. Which would mean that for wider `?n` it gets narrower and for incompatible `?n` it produces error."

And a short form, a constraint standing in a body, for a concept or an attribute that says no type:

```clj
(concept (total ?i is: ?t)
  (demo.a/total ?i is: ?t cardinality: :one))
(concept (stuff ?i total: ?t name: ?n)
  (total ?i is: ?t)
  (named ?i is: ?n)
  (integer ?t))
```

On what it does where inference cannot place it:

> "Intention here is that inference can recognize it and narrow down scans to specific type. I think it already does that, but in cases where it does not I would pretend it does at runtime by which I mean it filters out other types."

## What the code does today

Each is a fact, read in the code at `d4d8e2b02` (main `7e9f621ff` merged); none was run for this note unless it says so.

- **F1. A type is a lens on a scan.** An attribute premise whose value term carries a kind reads each claim through `Type::view` (`attribute/query/all.rs`, `admit`): a number of a type the kind does not list is read through the number type it does list, by `Value::viewed_as` (`dialog-corpus/src/model/value.rs`). An integer view reads 2.5 as 2 (the floor of the decimal it prints as), a natural view reads 2.5 as 2 and does not see -3, a float view reads every number. Anything that is not a number is admitted as it is or not at all.
- **F2. The type constraint filters by kind, not by lens.** `Constraint::TypeOf` (`constraint/type_of.rs`) yields a row when `kind.admits(value)`: the value's own data type is in the kind. It does not view: a float 2.5 is not admitted under integer, and an integer 5 is not admitted under natural, though the natural lens reads it as 5.
- **F3. Inference carries a constraint's type into the attribute scans of the same rule.** `TypeOf`'s schema gives its slot the kind; `TypeEnv::infer` unifies it with every slot of the variable; `Planner::narrow` rewrites each premise with `plan::apply_types`, and an attribute premise is retyped (`narrow_attribute`). So in one rule body, a scan that binds `?t` beside `(integer ?t)` reads through the integer lens (F1).
- **F4. Not into a concept query.** For a concept premise, `apply_types` projects the kinds onto its parameter terms, "consumed by diagnostics today and by checked execution later" (`planner/plan.rs`); the concept's own scans are not narrowed.
- **F5. The notation inlines concepts.** A read of a concept is analyzed into the primitive reads of its body, in the rule that reads it. Run for this note: the select of `stuff` above, without `(integer ?t)`, is analyzed to `(rule (report (demo.a ?i total: (:one ?t))) (demo.a ?i total: (:one ?t)) (demo.a ?i name: (:one ?n)))`, and a second rule like it for `name:`. No concept query is left for F4 to apply to.
- **F6. Number kinds meet as sets.** Unification intersects the primitive bit sets (`type_system.rs`, `Primitive::intersect`): integer and natural are two bits, and their meet is empty, so a variable a line reads `as: :integer` and a constraint `(natural ?t)` is a conflict today, not a narrowing.
- **F7. The rule grammar takes the constraint; its kind has no stable spelling.** A premise's `"assert"` is tried as a `Constraint` first (`proposition.rs`, `Deserialize`), and `Constraint` has `"type"`. `TypeOf.is` is a `type_system::Type` with derived serde, the bit-set form `type_system/wire.rs` calls unstable; that module's stable form, `{"type": {"int": {}}}`, is not what `TypeOf` writes. The notation wrote `"is": "integer:"` (`compile.rs`), which neither form reads, and Dialog-DB refused every rule that held one (run 2026-11-25: "unknown variant `type`").
- **F8. A stored body is its descriptor's JSON.** Format 4 for a rule of terms, format 5 for a rule holding a pattern, the lowest that can say it (`rule/body.rs`). Format 5 is under `## Unreleased` in `CHANGELOG.md`.

## The example, walked

`(select (stuff ?i total: ?t name: ?n))` with `(integer ?t)` in `stuff`'s body would be analyzed (F5) to a rule whose premises are `(demo.a ?i total: (:one ?t))`, `(demo.a ?i name: (:one ?n))` and the constraint on `?t`. The constraint and the `demo.a/total` scan are premises of one rule, so inference gives `?t` the integer kind and the scan is retyped (F3): the cardinality-one read elects among the claims the integer lens reads, as `as: :integer` on the line does. So the constraint reaches the scan through the concept, because the notation leaves no concept between them; it would not through a concept query the engine runs (F4). That this is what happens, rather than what the code reads as, is H1.

## (a) One form per type

One form per name `as:` takes: `text`, `natural`, `integer`, `float`, `boolean`, `entity`, `bytes`, `symbol`. `number` is not one of them; the engine's wire form has no single name for it either (`int`, `uint` and `float` are three). Each joins `syntax::FORMS`, so none may be declared as a concept, query, command or macro; in this repository no declared name is one (checked 2026-11-25), and tonk's library and the tour are not in this repository to check. A field label, a line's attribute name and a derived attribute's name (`todo/text`) are not declared names and are not affected. A listed type, `as: [:open :done]`, has no form of this kind: it stays `as:`, or needs a form of its own, which is not proposed here.

## (b) What narrower does to 2.5

Under the integer lens 2.5 is 2 (F1). Where the constraint is placed on a scan (F3), that is what it does, because it is the lens: `(integer ?t)` over a claim of 2.5 binds `?t` to 2. This matches what `natural:` does (it floors) and for the same reason: number types are views over one domain of numbers, which coerce only where precision is lost and filter what is out of range (`notes/identity-storage-notation.md`, Views read and write, cited by `viewed_as`). A constraint that dropped 2.5 instead would answer otherwise than `as: :integer` on the same line, for the same claims: two spellings of one type would disagree, which is the wrong answer the constraint above rules out.

The long form says this outright: `(integer of: ?n is: ?m)` binds `?m` to `?n` read through the lens, so 2.5 is 2, -3 is -3, and `"x"` is no match.

## (b2) A cast, at the lossy edge

The maintainer, 2026-11-26, of #895 (one variable through a `natural:` field and a `float:` field stays a compile error): "if there should be way to cast though". The long form is that cast: `(float of: ?q is: ?f)` binds `?f` to `?q` read through the float lens, written where the author means the conversion, so the loss is chosen, not met. What it does at each edge, by the lenses as they are (F1) and #892's float arithmetic:

| cast | of | gives | why |
|---|---|---|---|
| `float` | 3 | 3.0 | exact |
| `float` | 2^53 + 1 | 2^53 as a float | a float holds 53 bits: the nearest float, ties to even, as #892 reads a whole number mixed with a float |
| `integer` | 2.5 | 2 | the integer lens floors |
| `integer` | -2.5 | -3 | floors, toward negative infinity, not toward zero |
| `natural` | -1 | no match | the natural lens drops what is out of its range |
| `integer` | `"x"` | no match | not a number |

A cast never fails: it rounds, floors or gives no match, as the lens does. An author who wants the cast exact writes the round trip, `(float of: ?q is: ?f) (integer of: ?f is: ?q)`, which matches only where nothing was lost. Not decided: whether a cast that loses precision warns at compile time where the analyzer knows the value, a constant past 2^53 say. Not built.

## (c) Incompatible

- **Known at compile time:** a compile error. A variable the analyzer has typed text, by a line `as: :text` say, meeting `(integer ?t)` is an inference conflict (`InferenceError::Conflict`), refused when the rule is compiled.
- **Not known:** a non-match, never a failure (the maintainer, relayed 2026-11-26).
- **Wider:** "for wider `?n` it gets narrower" does not hold between number types today: integer and natural conflict (F6). Natural is a narrower view of integer, so by the maintainer's words `(natural ?t)` on a variable read `as: :integer` should narrow it, not refuse. That needs the unifier to meet number views as views (H3).

## (d) How it lowers, and what is stored

- **Short form**, `(integer ?t)`: the `type` constraint, `{"assert": "type", "where": {"of": {"?": {"name": "t"}}, "is": {"type": {"int": {}}}}}`. The engine change: `TypeOf.is` read and written in the stable wire form of `type_system/wire.rs` (F7), so that a stored body holds a spelling that does not move when a value type is added. Nothing else in the grammar changes.
- **Long form**, `(integer of: ?n is: ?m)`: no engine formula reads a value through a type and binds the result. It needs one, a formula of `of:` and `is:` that binds `is` to `Type::view(of)` and is no match where the view sees nothing; its name in the descriptor (`type/int`, or one name with the type as a parameter) is open.
- **Format:** a body that holds either is written in format 5. A format 4 reader cannot read the wire form, so such a body is not one format 4 can say, as a pattern was not; format 5 is unreleased (F8), so no new format number is needed, provided no released body holds a `TypeOf` already (H2).

## (e) What the analyzer infers from `(integer ?t)`

`?t` is integer, required, in the whole rule: every premise reading it sees that kind (F3), so a formula or a comparison over it is checked against integer when the rule is compiled (`text/like` over `?t` is a conflict; `<` and `math/sum` are not); a fold over it takes integer as its input type (`try_listed`, `rule/analyzer.rs`); a head field it feeds is checked against integer. A derived attribute's labelled field fed by `?t` would be concluded as integer, which is what `as: {:status :text}` says today; for the notation to declare the derived field's type from the constraint instead of the map, the walker has to carry the constraint's type to the head field it feeds (H4). `?t`'s value is what the lens read (b), so the type the analyzer infers is the type of the value at run time, which is the agreement the decision is for.

## The decision left: a short form where no scan binds the variable

Where `?t` is bound by a formula, `(+ ?a : 1 is: ?t)`, or by any premise inference does not retype, the constraint is evaluated as itself (F2), and F2 is not the lens.

| Decision: what `(integer ?t)` does where inference places it on no scan, for: compile time and run time never disagree on a type |
|---|---|---|---|---|
| criterion \ approach | A. `TypeOf` as it is: keep the rows whose value is of the kind | B. keep the rows the type reads: `Type::reads` | C. accept the short form only where it reaches a scan; elsewhere refuse it, naming the long form | D. lower every short form to the long form, a fresh variable for what the lens read |
| agrees with `as:` and with itself on a scan | 🔴 2.5 is dropped here and read as 2 on a scan; an integer 5 under `natural` is dropped here and kept on a scan | 🟡 2.5 is kept, as the scan keeps it, but kept as 2.5 | 🟢 only the scan case exists | 🟢 2.5 is 2 wherever it stands |
| the inferred type is the value's type | 🟢 a kept value is of the kind | 🔴 `?t` is inferred integer and holds the float 2.5: downstream arithmetic computes on a float | 🟢 | 🟢 `?m` is what the lens gave |
| what the author writes | the short form anywhere | the short form anywhere | 🟡 the long form where no scan binds the variable; the refusal names it | the short form anywhere; the head shows the narrowed value |
| how the notation tells the two cases apart | no need | no need | 🟡 it asks the engine's analysis whether `?t`'s binder was retyped, when the rule is compiled | no need |
| engine change | the wire form only (d) | the wire form, and `TypeOf` evaluated by `reads` | the wire form, and a way to ask where a constraint was placed | 🟡 the wire form and the new view formula (d) |
| meets the maintainer's "filters out other types" | 🟢 as said | 🟢 by the lens's reading of "other" | 🟡 refuses where he would filter | filters what the lens does not see, and narrows what it does |

A and B are the maintainer's words taken two ways; each fails a row the decision is for. C keeps them by refusing; D keeps them by rewriting. No recommendation is made here.

## Hypotheses

| | claim | would be confirmed by | refuted by |
|---|---|---|---|
| H1 | the constraint reaches the `demo.a/total` scan in the example (F3, F5) | a test: `demo.a/total` of `i:1` holds 3 then 2.5 under `cardinality: :one`; `stuff` with `(integer ?t)` answers `total: 2`, as `as: :integer` on the line does | it answers nothing, or 2.5 |
| H2 | no body a released build stored holds a `TypeOf` | the engine inserts its own (`rule/deductive/head.rs`) only into the rules it plans, never into a stored descriptor; a search of the stored-body tests and of 0.5.0's encoder | a stored body that holds one |
| H3 | the unifier can meet number views as views, integer with natural giving natural, without changing what any rule today infers | a unifier change and the analyzer's test suite unchanged | a rule today whose inference changes |
| H4 | the walker can carry a constraint's type to the derived attribute's head field it feeds, replacing the `as:` map | a derived attribute concluded with `(text ?s)` in its body declares `status` text, as the map does | a head field the walker cannot trace to one constraint |
