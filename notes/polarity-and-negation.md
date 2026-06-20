# Polarity: negation and the type narrowing

> Design note. Records the polarity discipline adopted for rule-level
> type narrowing, the reasoning behind each direction, and the part
> that remains an open judgment call. User-facing behavior is
> summarized in `rust/dialog-query/guide.md`; this note is the
> rationale and the uncertainty.

Rule-level inference computes, per variable, the meet of the kinds of
every slot the variable appears in. The result is read as an
occurrence-typing fact: "in every row that survives the positive
premises, `?x` inhabits this type." Two questions follow, one per
direction of flow between the positive body and a negated premise.

## Direction 1: negated premises do not contribute to inference

Settled, and semantics-bearing. The running counterexample:

```text
Person { name: ?name, nickname: ?nickname }   // nickname is maybe
unless club/banned(_, ?nickname)
```

The negated `club/banned` lookup demands a present `?nickname` in its
value slot. If that demand contributed to the rule-wide meet, the
inference would strip `Nothing` from `?nickname`, the optional lookup
would demote to a required scan, and people without nicknames would
be dropped from the result before the negation ever ran. The rule
says "unless the nickname is banned"; the narrowed reading would be
"must have a nickname, and it must not be banned", which the author
did not write and can write explicitly if wanted.

The general statement: a negated premise constrains which rows are
*rejected*, so its demands are facts about rows that do not survive.
Inference describes rows that do survive. Mixing the two changes the
meaning of rules.

`TypeEnv::infer` therefore skips `Premise::Unless` entirely.

## Direction 2: positive narrowing does not flow into negated subqueries

Implemented (`apply_types` rewrites positive premises only), but this
direction is a judgment call rather than a theorem, and is worth
revisiting when checked execution lands.

The case for keeping it out (the current choice):

- A negated subquery is a hypothetical question evaluated against its
  own slots; typing it from the surrounding rule makes its meaning
  depend on context that its author cannot see at the premise level.
- Today, narrowed kinds influence behavior only through the optional
  lookup demotion, and a `Maybe` cannot appear under `unless` (the
  analyzer rejects it), so stamping positive kinds into negations has
  no behavioral effect. Inert rewrites are pure hygiene risk: they
  create the appearance that something depends on them.
- Under future *checked* execution (kinds enforced at bind time,
  dialog-db-48 and beyond), a stamped kind inside a negation would
  become load-bearing: "no matching row" could turn into a type
  error path inside the inner query depending on what the positive
  body happened to prove. Keeping negations self-typed keeps their
  failure mode singular.

The case for letting it flow in (the road not taken, for now):

- Matching is by equality. If the positive body proves `?x : String`
  in every surviving row, then any fact the negated lookup matches
  against `?x` necessarily holds a `String` value, so stamping
  `String` onto the negated slot is sound and could let a future
  optimizer pick a tighter index range for the inner probe.
- The asymmetry ("the negation sees the variable's *bindings* at
  evaluation time but not its *type* at analysis time") is real and
  slightly odd. Evaluation already leaks the positive context into
  the negation through the row itself.

Resolution for now: soundness does not require the flow, hygiene
mildly argues against it, and the optimization it would enable does
not exist yet. Revisit when checked binds or index-range narrowing
make the inner typing matter; the change is localized to
`apply_types`.

## Related settled points

- `Absent` matches nothing in any scalar slot, in both polarities. In
  positive position this is the filter-by-default semantics; under
  negation it makes "Bob has no nickname" pass "unless the nickname
  is banned" instead of being treated as having every banned nickname
  in the store.
- `unless` over a `maybe` premise is rejected at analysis
  (`NegatedOptional`): the optional lookup always yields a row for a
  bound entity, so its negation is vacuously false.
- Negation placement is structural, not a cost accident: a negation's
  named variables are hard requirements in its schema, so it is
  feasible only after its binders have run.
