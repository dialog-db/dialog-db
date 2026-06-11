# Optionality in the query engine

This guide explains how optional values work in `dialog-query`: what
`Absent` means, where it can appear, how the planner and the type
inference treat it, and why the design is layered the way it is. It is
written around one running example.

## The running example

Three facts in the store:

```text
the                of      is
person/name        alice   "Alice"
person/nickname    alice   "Ali"
person/name        bob     "Bob"
```

Alice has a nickname. Bob does not. Nothing in storage says "Bob has
no nickname": storage holds only facts that exist. There is no null,
no `None`, nothing is ever persisted for an absent value.

## Two layers: facts are scalar, concepts may widen

The engine has two layers with different vocabularies:

The **associative layer** is the raw fact lookup: a premise of the
shape `the(of, is)` that scans the EAV indexes. It is *scalar*. A fact
either matches or it does not, and a row that finds no fact is simply
filtered out. Asking the associative layer for nicknames:

```rust
the!("person/nickname").of(person).is(nickname)
```

yields one row (Alice). Bob produces no row, the same way a SQL inner
join drops him. There is no way to express "give me the nickname or
tell me it is missing" at this layer, by design.

The **semantic layer** is where concepts live, and it is where
optionality is expressed. A concept field declared `Option<T>`:

```rust
#[derive(Concept)]
struct Person {
    this: Entity,
    name: Name,                 // required
    nickname: Option<Nickname>, // optional ("maybe")
}
```

queries as an *optional lookup* (in SQL terms, a left join; this
guide says "optional lookup" from here on): every entity matching the
required fields produces a row, and the optional slot reports what
the lookup found.

```text
?person   ?name     ?nickname
alice     "Alice"   Present("Ali")
bob       "Bob"     Absent
```

Bob is not dropped. His `?nickname` is bound to `Absent`.

## `Absent` is a claim, not a hole

A variable in a row is in one of three states:

| state        | meaning                                  |
|--------------|------------------------------------------|
| unbound      | not yet known; nothing has looked        |
| `Present(v)` | known to be `v`                          |
| `Absent`     | *we looked, and there is no value*       |

The difference between unbound and `Absent` carries real information.
`Absent` is a positive claim about the store ("this entity has no
such fact"), produced by exactly one construct: the optional lookup
behind an optional concept field (`OptionalAttributeQuery` internally). Nothing
else in the engine manufactures `Absent`, and nothing ever stores it.

This is why the optional lookup *requires the entity to be bound*
before it runs. "Absent" answers the question "absent for whom?", which is
meaningless without a concrete entity. The planner enforces this
structurally: an optional field's lookup can never lead an unbound
scan; some required premise must bind the entity first.

If the planner did not enforce this, an optional field's lookup could
end up leading the scan whenever it happened to sort first among the
concept's fields. With no concrete entity there is nothing sound to
report `Absent` *about*, so the fallback would have to be suppressed,
and the lookup would silently degrade to an inner join: every entity
lacking the optional fact would vanish from the result, based on
nothing more than the alphabetical position of a field name. A
correctness property must not depend on what you happened to name
your fields.

### Why a concept must have at least one required attribute

A concept whose fields are *all* optional is rejected at compile time
(`#[derive(Concept)]` fails to build it, and the dynamic constructors
return `TypeError::EmptyConcept`). Two independent reasons, both
falling out of the rules above:

1. **It would match everything.** Each optional field widens rather
   than constrains: any entity either has the fact (`Present`) or
   does not (`Absent`), so every entity in the store satisfies every
   optional field vacuously. An all-optional concept would be the
   concept of "anything at all", and a query for it would enumerate
   the universe.
2. **Nothing would bind the entity.** Every optional field's lookup
   requires `this` already bound, and in an all-optional concept
   there is no required field to bind it. The body would be
   unplannable by its own rules.

The required fields therefore do double duty: they are what gives the
concept a meaning (they constrain which entities match), and they are
what binds `this` so the optional fields have a concrete entity to
report absence about.

The optional lookup itself has four behaviors, all consequences of
reading `Absent` as a claim:

| input row state for `?nickname` | facts for the entity | output |
|---------------------------------|----------------------|--------|
| unbound                         | `"Ali"`              | row with `Present("Ali")` |
| unbound                         | none                 | one row with `Absent` |
| `Present("Al")` (pinned)        | `"Ali"` (mismatch)   | no row; a mismatch is *not* absence |
| `Absent` (claimed upstream)     | `"Ali"`              | no row; the fact contradicts the claim |
| `Absent` (claimed upstream)     | none                 | row passes; the store confirms the claim |

## Consuming an optional value: filters by default

What happens when `?nickname` flows into a context that demands a
present value, say a formula?

```rust
// person: required name, optional age
// then: math/sum { of: ?age, with: 1, is: ?next }
```

The rule-level type inference sees that `math/sum` demands a present
number for `?age`. It *narrows* the variable: rows where `?age` is
`Absent` cannot satisfy the formula, so they are excluded. People
without an age simply do not appear in the result. No error, no
`Absent` reaching the formula.

This is ordinary relational semantics. A premise is a predicate on
the row set, and a predicate that demands presence filters rows
lacking it, exactly the way the scalar nickname lookup filtered Bob.
Think of it as occurrence typing: using `?age` in a context that
requires a number *is* the evidence of presence, the same way a
future `?x.text()` type predicate would narrow `?x` to strings.

The planner exploits the narrowing: when inference proves a sibling
premise guarantees the value is present, the optional lookup can
never take its `Absent` branch, so it is *demoted* to a plain scalar
scan. Same semantics, less work.

If filtering is not what you want, say so explicitly with a default:

```rust
let nickname: Term<Option<String>> = Term::var("nickname");
let display: Term<String> = Term::var("display");
nickname.unwrap_or("Anon".to_string()).is(display)
```

`unwrap_or` (the `Coalesce` constraint) is the one operator that
*consumes* an `Absent` and produces a present value from it:

```text
?person   ?nickname        ?display
alice     Present("Ali")   "Ali"
bob       Absent           "Anon"
```

The choice between "filter rows missing the value" and "substitute a
default" is the one intent the engine cannot infer, so the default is
the relationally natural filter, and `Coalesce` is the explicit
opt-in for defaults.

### Coalesce is ordered after its source

The coalesce's `source` slot is a *hard dependency*: the planner
schedules the constraint only after the premise that resolves
`?nickname` (to `Present` or to `Absent`) has run. This matters
because the constraint is very cheap, so a greedy planner would
otherwise love to schedule it first, and if it ran before the lookup,
an unbound source would be indistinguishable from an absent one:
`"Anon"` would shadow Alice's real nickname on every row. For the
same reason, an unbound source at evaluation time is an error, never
a silent fallback; silently substituting the default is precisely the
failure mode the ordering rule exists to prevent.

## Producing values: heads are contracts

Rule bodies filter; rule heads promise. A conclusion's required field
is a promise that every derived row carries a present value, so
binding it from a variable that may be `Absent` is rejected at
compile time:

```rust
// REJECTED: RequiredHeadFromOptional
// deduce Greeting { text: String }   <- required head field
// when   Person { nickname: ?text }  <- ?text may be Absent
```

The fix is to discharge the optionality explicitly before the head:

```rust
// OK
// when Person { nickname: ?nick }
//      ?text.is(?nick.unwrap_or("friend"))
```

The same contract holds across concept boundaries: an outer rule that
feeds an inner concept's *optional* field into its own *required*
head is rejected, because the inner concept's schema declares that
the slot can deliver `Absent`.

## Negation and absence

`unless` filters a row when its inner query has at least one match.
With one more fact in the store:

```text
the           of     is
club/banned   club   "Ali"
```

and the rule body:

```rust
// Person { name: ?name, nickname: ?nickname }
// unless club/banned(_, ?nickname)
```

| row   | `?nickname` | inner query                  | verdict |
|-------|-------------|------------------------------|---------|
| alice | `"Ali"`     | finds `club/banned _ "Ali"`  | filtered (banned) |
| bob   | `Absent`    | matches nothing              | passes |

Alice's nickname is banned, so she is filtered. Bob passes because an
`Absent` binding matches nothing: `club/banned` is a scalar fact
lookup, its value slot demands a present value, and Bob's row carries
the claim that there is no value. A person with no nickname cannot
have a banned one. (The same holds in positive position: a scalar
premise receiving an `Absent` binding filters the row, the
filter-by-default semantics from the previous section arriving by
another road.)

### You cannot negate an optional field

An optional field's lookup always yields a row once its entity is
bound: `Present` when the fact exists, `Absent` when it does not.
Negating something that always matches filters every row, making the
rule vacuously false, so the analyzer rejects it at compile time
(`NegatedOptional`). What you almost certainly meant is one of:

```rust
// "the entity has no nickname fact": negate the scalar lookup
// unless person/nickname(?person, _)

// "the entity is not a Person": negate the concept
// unless Person { this: ?person, .. }
```

### Negation does not narrow types

The banned-nicknames rule also shows why a negated premise must stay
out of the rule's type narrowing. The inner `club/banned` lookup
demands a present `?nickname`. If that demand counted as evidence
about the rule's rows (the way a formula's demand does), inference
would conclude that every row has a present nickname, the optional
lookup would tighten into a required one, and Bob would be dropped
before the negation ever ran. The rule says "unless the nickname is
banned", not "must have a nickname, and it must not be banned"; if
you want the second meaning, write it explicitly with a required
field.

So narrowing is computed from positive premises only: what a negated
subquery demands says nothing about the rows that survive it. The
finer points, including the reverse direction (whether positive
narrowing should flow *into* a negated subquery), are design-note
territory rather than user-facing behavior: see
`notes/polarity-and-negation.md`.

## Where errors surface

The engine has exactly two error surfaces, and only one of them is
reachable from a rule that compiled.

**Compile time** (rule construction, whether authored locally or
hydrated from the wire) rejects every misalignment between *known*
types: a variable demanded as `String` by one slot and as a number by
another (an empty meet), a required head fed by an optional source, a
negated optional lookup, a malformed `Coalesce`, a literal that
cannot inhabit a formula's cell. If the knowledge exists to catch a
mistake, it is caught here.

**Evaluation time has no type errors — only membership.** A row
either inhabits the types a premise demands or it is a non-match: a
`String` fact under a numeric slot is filtered the same way a wrong
*value* under a pinned constant is, an `Absent` in a scalar slot is
filtered, and a row whose values cannot share a formula's type
variable is filtered. Nothing throws and nothing is coerced. The few
runtime errors that remain in the engine are *contract* violations
(an optional lookup scheduled with an unbound entity, a bind outside
a variable's kind): they indicate an engine or construction-path bug,
are unreachable from any rule that compiled, and exist as defense in
depth.

For comparison, the three corners of this design space: PostgreSQL
*errors* at runtime (a bad cast kills the query); SQLite *coerces*
(`'abc' + 1 = 1`, no error, occasionally nonsense — its `STRICT`
tables exist because of how that felt in practice); dialog *filters*.
Filtering keeps SQLite's ergonomic guarantee — a query never dies
mid-stream on data — without ever fabricating a value.

## Inference in an open world

No annotation is ever required. Inference reconstructs everything
reconstructable — slot kinds, concept schemas, formula schemes — and
an untyped wire concept compiles fine; its fields are simply wide.
The guarantee a compiled rule carries:

> A rule that compiles never produces a runtime type error, and every
> row it yields inhabits its inferred types.

This is the closed-world (Elm-style) soundness property restated for
an open world. The data layer is schema-on-query: an attribute may
hold values of several types across facts, and wire concepts need not
declare field types, so no amount of inference can make unknown data
known at compile time. Where inference can only establish a *bound*
(say NUMERIC, or "any present value"), the bound's runtime meaning is
the filter semantics above: types narrow on use, and rows outside the
narrowed type are non-matches. One worked example, with `person/age`
undeclared on the wire:

```rust
// person/age(?p, ?age)
// math/sum { of: ?age, with: 1, is: ?next }
```

Analysis puts `?age` and `?next` in one type variable bounded NUMERIC
(the formula's scheme) and stamps that bound onto the age scan. Then,
per row: an entity with `age = 30` (unsigned) instantiates the
variable to unsigned, the literal `1` follows losslessly, and
`?next = 31`; an entity with `age = 2.5` instantiates to float and
yields `3.5`; an entity whose `age` fact is the *string* `"old"` is
filtered at the scan by the stamped bound, before the formula ever
runs. And if some other premise in the same rule demanded `?age` as a
`String`, the rule would not compile: there the types were known, and
they misalign.

Two consequences worth naming. There is no implicit numeric
promotion: a row mixing unsigned and float inputs in one scheme
variable is a non-match, not a lossy widening (dialog's value lattice
has no arbitrary-precision type to promote into, so promotion would
trade "row excluded" for "row matched with a quietly wrong value").
Conversions are explicit formulas, parallel to `Coalesce` being the
explicit form of defaulting. And because exclusion is silent by
design — the excluded fact may be someone else's perfectly valid
data — the inference must be *inspectable*: the planned diagnostics
surface reports what each variable narrowed to and which premise
narrowed it.

## Why it is layered this way

Keeping the associative layer scalar and pushing all optionality into
one semantic-layer construct is not just tidiness. It buys three
things:

1. **One place to be correct.** The optional lookup's contract
   (entity bound, mismatch is not absence, claims are checked) lives
   in one operator instead of being distributed across runtime guards
   inside every scan, where plan reordering could break it.
2. **Types that tell the truth.** Set-widening appears in exactly the
   schemas that can deliver `Absent` (the optional lookup, a
   concept's optional fields), so the inference's verdict about a
   variable is also a fact about runtime. Derivability ("this premise
   produces the binding") and absence ("the value may not exist") are
   kept apart; only the latter is ever expressed in types.
3. **A future hook.** "We looked at range R for entity E and found
   nothing" is precisely the event an incremental subscription must
   record so a later fact in R can flip `Absent` to `Present`. With
   the optional lookup compiled as a single operator, the future
   demand-tracking work attaches to one node instead of
   reverse-engineering scattered guards.

The deeper principle, which also governs negation and the planned
replication work: **absence is a claim about a completely examined
range**. The engine only ever asserts `Absent` relative to a bound
entity (a finite, checkable range), only consumes it through explicit
operators (`Coalesce`), and treats it as matching nothing everywhere
else.

For the design history see [`scalar-associative-layer.md`](./scalar-associative-layer.md),
[`optional-fields.md`](./optional-fields.md), and
[`query-engine-design.md`](./query-engine-design.md).
