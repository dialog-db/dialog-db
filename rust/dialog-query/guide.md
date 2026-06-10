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

queries as a *left join*: every entity matching the required fields
produces a row, and the optional slot reports what the lookup found.

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
such fact"), produced by exactly one construct: the left join behind
an optional concept field (`MaybeQuery` internally). Nothing else in
the engine manufactures `Absent`, and nothing ever stores it.

This is why the left join *requires the entity to be bound* before it
runs. "Absent" answers the question "absent for whom?", which is
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
2. **Nothing would bind the entity.** Every optional field's left
   join requires `this` already bound, and in an all-optional concept
   there is no required field to bind it. The body would be
   unplannable by its own rules.

The required fields therefore do double duty: they are what gives the
concept a meaning (they constrain which entities match), and they are
what binds `this` so the optional fields have a concrete entity to
report absence about.

The left join itself has four behaviors, all consequences of reading
`Absent` as a claim:

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
premise guarantees the value is present, the left join can never take
its `Absent` branch, so it is *demoted* to a plain scalar scan. Same
semantics, less work.

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
//      ?nick.unwrap_or("friend") = ?text
```

The same contract holds across concept boundaries: an outer rule that
feeds an inner concept's *optional* field into its own *required*
head is rejected, because the inner concept's schema declares that
the slot can deliver `Absent`.

## Negation and absence

`unless` filters a row when its inner query has at least one match.
Two rules govern how it interacts with optionality.

**`Absent` matches nothing.** A scalar slot demands a present value,
so a row that arrives with `?nickname = Absent` cannot match any fact
through it. Concretely, with one more fact in the store:

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

Bob has no nickname, so he cannot have a banned one. If `Absent` were
instead treated like *unbound* inside the negation, the inner scan
would run unconstrained, find the `"Ali"` fact (anyone's banned
nickname), and filter Bob too, treating a person with no nickname as
if they had every banned nickname in the store. The claim "Bob has no
nickname" would silently turn into the question "does any banned
nickname exist?".

The same rule applies in positive position for symmetry: a scalar
premise receiving an `Absent` binding filters the row (instead of
scanning unconstrained or crashing), which is exactly the
filter-by-default semantics from the previous section arriving by
another road.

**You cannot negate a left join.** A left join always yields a row
for a bound entity (`Present` or the `Absent` fallback), so negating
it would filter every row; the rule would be vacuously false. The
analyzer rejects it at compile time (`NegatedOptional`). What you
almost certainly meant is one of:

```rust
// "the entity has no nickname fact": negate the scalar lookup
// unless person/nickname(?person, _)

// "the entity is not a Person": negate the concept
// unless Person { this: ?person, .. }
```

**Narrowing does not cross into negation.** The types inference
derives describe rows that *survive* the positive premises ("in every
surviving row, `?x` is present"). A negated subquery asks a
hypothetical question about rows that must *not* match, so it is
typed in its own context and never receives the positive premises'
narrowing. This polarity discipline is what keeps "the formula
narrows `?age`" from silently changing what an unrelated `unless`
means.

## Why it is layered this way

Keeping the associative layer scalar and pushing all optionality into
one semantic-layer construct is not just tidiness. It buys three
things:

1. **One place to be correct.** The left join's contract (entity
   bound, mismatch is not absence, claims are checked) lives in one
   operator instead of being distributed across runtime guards inside
   every scan, where plan reordering used to break it.
2. **Types that tell the truth.** Set-widening appears in exactly the
   schemas that can deliver `Absent` (the left join, a concept's
   optional fields), so the inference's verdict about a variable is
   also a fact about runtime. Derivability ("this premise produces
   the binding") and absence ("the value may not exist") are kept
   apart; only the latter is ever expressed in types.
3. **A future hook.** "We looked at range R for entity E and found
   nothing" is precisely the event an incremental subscription must
   record so a later fact in R can flip `Absent` to `Present`. With
   the left join as a single operator, the future demand-tracking
   work attaches to one node instead of reverse-engineering scattered
   guards.

The deeper principle, which also governs negation and the planned
replication work: **absence is a claim about a completely examined
range**. The engine only ever asserts `Absent` relative to a bound
entity (a finite, checkable range), only consumes it through explicit
operators (`Coalesce`), and treats it as matching nothing everywhere
else.

For the design history see `notes/scalar-associative-layer.md`,
`notes/optional-fields.md`, and `notes/query-engine-design.md` at the
repository root.
