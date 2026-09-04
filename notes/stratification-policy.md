# Stratification policy: reject at install, quarantine at merge

> Design note for how an ill-stratified rule set is handled. Records the
> definition the two mechanisms share, the order that makes quarantine
> deterministic, and the alternatives considered. Companion to
> [`attribute-level-deduction.md`](./attribute-level-deduction.md) (which
> makes the dependency graph attribute-granular) and
> [`aggregation.md`](./aggregation.md) (whose aggregating edges are one
> of the three polarities this note orders).

## The problem

Stratification is a property of a whole rule set, not of one rule. A
rule that negates, aggregates over, or optionally reads a relation is
fine on its own and becomes ill-stratified only when some other rule
closes a cycle back to it. Rules are installed concurrently on
replicas that cannot see each other, so no install-time check can
guarantee the merged program is stratified. Today registration is
therefore unconditional, and the analysis runs when a query resolves
rules: an ill-stratified region fails exactly the queries that touch
it, with `NegationThroughRecursion` or
`AggregationThroughRecursion`.

That is sound but wrong for the person querying. The error names a
rule they did not write and cannot fix from where they are, and it
turns a modelling conflict between two authors into an outage for
every reader of the affected concept. Two things are wanted instead:

- a rule that would make the *local* program ill-stratified is
  refused at install, with an error the author can act on;
- a merge that produces an ill-stratified program disables the rules
  that introduce the cycle, deterministically, so every replica
  evaluates the same active set and no query fails.

Both must agree: a rule the install path refuses is exactly a rule
the merge path would disable.

## Definitions

A **violation** is a negative, aggregating or optional edge whose two
endpoints lie in the same strongly connected component of the
dependency graph. Under attribute-level deduction the graph's nodes
are attribute concepts and the concepts that read them, and the unit
that carries an edge is a rule.

A **per-rule** property is anything decidable from one rule and the
descriptors it embeds: type inference, a required head bound from an
optional source, `unless` over a `maybe`, and self-reference — a rule
that negates or optionally reads a concept containing an attribute its
own head derives. Per-rule properties are compile errors and stay so.
They are never quarantined; a malformed rule is a bug, not a
conflict.

The **order** on rules is by the cause of the rule's `dialog.rule/source`
fact, then by content address for causes that are concurrent. Causes
are replicated data and content addresses are pure functions of the
body, so every replica computes the same order from the same facts.
The tie-break is arbitrary and deterministic, which is the most a
concurrent conflict admits: two replicas that each installed one half
of a cycle share no fact that makes one of them earlier.

## The definition both mechanisms share

```
quarantined(rules):
    accepted = {}
    for rule in rules sorted by order:
        if stratified(accepted ∪ {rule}):
            accepted.insert(rule)
        else:
            rejected.insert(rule)
    return rejected
```

The active rule set of a program is `rules \ quarantined(rules)`.
Read it as: the merged program behaves as if the rules had been
installed one at a time in causal order, each one refused if it would
have made the program ill-stratified given those accepted before it.
The later rule loses, because that is what sequential installs would
have done.

Install-time rejection is the same function with the new rule last:
`install(rule)` fails when `quarantined(current ∪ {rule})` contains
`rule`. Since the new rule is last in order it can only be rejected
on its own account, never displace an existing rule, and the error it
carries is the violation the analysis found: the attribute or concept
closing the cycle, and the rules on it.

Quarantine is a pure function of the rule facts, so it is never
stored. There is no quarantine fact to conflict over, and retracting a
rule that closed a cycle frees the rules it had quarantined on the
next resolution with no bookkeeping.

## Where it runs

Rule resolution already walks the dependency closure of a queried
concept, cached per head. The simulation runs over that closure before
it is used: sort the closure's rules, accept them one by one against
the analysis, and drop the rejected ones from their bundles. Queries
then evaluate against the active set and succeed. The closure is the
right scope because a rule outside it cannot be on a cycle through the
queried concept.

`validate` keeps reporting every violation, now naming the quarantined
rule beside the cycle it broke. tonk surfaces that in the editor and
in `tonk schema`, and a rule's status is worth exposing as a derived
concept so a page can show it. A quarantined rule is silent by
construction; the status is what makes it visible.

The install path is a checked variant of assertion: resolve the
program the transaction sees, run the simulation with the new rule
last, and refuse the transaction with the violation if the rule is
rejected. The raw `assert` of a rule's facts stays available for
replication and import, which must accept any rule set and rely on
quarantine.

## Overriding

An author who wants their rule to win over a conflicting one does not
need a policy knob: they retract the conflicting rule in the same
transaction that installs theirs. The retraction is a fact, replicated
and auditable, and the install error names the rules to retract. That
is the whole override story, and it is why the install error lists
the cycle's members rather than just the concept.

## Properties, and the tests that pin them

- **Order independence.** For any set of rules and any arrival
  order, the active set after every merge equals `rules \
  quarantined(rules)`. This is the property a non-deterministic
  tie-break would break.
- **Agreement.** A rule refused by `install` against a program is
  quarantined by the simulation over that program plus the rule, and
  conversely.
- **Monotone growth.** Adding a rule never un-quarantines an existing
  one; removing a rule never quarantines a new one. The active set
  moves in the direction the change points.
- **No displacement at install.** The new rule is last in order, so
  installing it never changes the status of an existing rule.

All four are property tests over small generated rule sets.

## Considered and not adopted

**Precedence flags on rules.** A flag such as `supersede` that makes a
newer rule quarantine an older one on conflict. It keeps determinism
if the flag joins the order (rank, then cause, then address), but it
adds a knob whose interaction with concurrency is confusing: two
superseding rules in one cycle fall back to the hash tie-break, and
the effect on the losing rule is derived and silent where a retraction
would have been a visible fact. Everything the flag buys, an explicit
retraction buys better. Not adopted; the order stays cause then
address.

**Origin-biased evaluation.** Each querier ignores conflicting rules
according to its own origin, preferring local rules. This is not a
policy but the absence of one: two replicas with the same facts return
different answers, subscriptions diverge, and an inductive rule
premised on the disagreeing concept writes different facts on each
side, turning a view disagreement into a storage divergence. It also
hides the conflict from the one person who could resolve it. Not
adopted as semantics. An explicit query option that evaluates as if a
given set of rules won is a legitimate tool for previewing a change,
and is a different thing.

**Failing the query.** Today's behavior. Sound, deterministic, and
the wrong party pays. Replaced by quarantine.

**Rejecting at install only.** Cannot cover the concurrent case, so
merges would still produce programs some query has to fail on. Kept
as one half of the policy, not the whole.
