# A value computed over many facts cannot be given a name

**Phase: Delimiting.** Back from Directing on 2026-11-18: a spike showed the cause stated below is not what keeps the refusal in place. See the [journal](named-fold/journal.md) for where the topic stands.

## Description

The notation's design (#728, `research/notation/design.md`) has two forms that compute one value from many rows and give it a name a rule can read:

```clj
; a derived attribute: how many to-dos of a list are open
(rule (list/count-open ?l (count ?t))
  (list ?l)
  (todo ?t list: ?l state: :open))

; a view: the markup of a list, which reads the count
(view list :card
  [:article [:h2 ?name] [:p ?count " open"] [:ul {:view :row :of ?t}]]
  (list ?l name: ?name)
  (list/count-open ?l ?count)
  (todo ?t list: ?l))
```

Both are deductive rules that fold. dialog-query refuses a fold in a deductive rule when the rule is compiled (`TypeError::ReduceInOpenRule`, `open_rule_error` in `rust/dialog-query/src/rule.rs`), and [`notes/attribute-heads.md`](../notes/attribute-heads.md) says why:

> `reduce` does not: a fold has no reading over a set still growing, and unlike a choice it cannot be deferred to the component's exit without giving the rules inside a different relation than the readers outside. A fold belongs to the closed places, a query, a subscription, an inductive rule, and is refused in a deductive rule at compile time (`ReduceInOpenRule`), which the author sees at once and a merge never does.

The cardinality work (#827) leaves it there: "A deductive rule still takes no authored `reduce`, as on main. Where a computed value belongs (a getter, per the notation design) is left for its own change."

It was met building the notation (#773): a derived attribute that folds is read, expanded and then refused as unsupported. tonk's library has 74 views, and a page of tonk shows counts.

## Problem

An author cannot name a value computed over many facts and read it where a read stands: in another rule's condition, in a view, in a query. The count of a list's open to-dos can be asked for but not referred to.

The cause is one property dialog keeps on purpose. A deductive rule is open: it is installed as facts, replicas merge their rules, and a query reads whatever program exists when it runs. A fold inside a dependency cycle has no answer, and today the analysis reports it as an error that fails the evaluation (`AggregationViolation`, `EvaluationError::AggregationThroughRecursion` in `rust/dialog-query/src/session/dependencies.rs`). Two rule sets, each sound alone, could merge into a cycle through a fold. Refusing every fold when a rule is compiled is what keeps a merge from ever failing a query.

```
 an absence test in a cycle   unless, an optional field, a ranked pick   one rule of the cycle is quarantined; the program evaluates
 a fold in a cycle            reduce                                     a violation; the evaluation fails
                                                                         so no deductive rule may fold at all
```

A spike (`spike/named-fold`, 2026-11-18) lifted the refusal and made a cycle through a fold a quarantine, in two lines of the analysis. Merged in either order, replicas set the same rule aside. So the cause above is answerable, and it was answered once before: #827 carried the same change (`3293f0f`) and took it back (`411c3ff`) for another reason, now in its `notes/attribute-heads.md`:

> A computing fold (`count`, `sum`, `avg`) yields a value none of its inputs holds: concluded into an attribute it would compete with the claims stored there, and nothing could be written through it, so where it belongs is not settled.

The spike shows the competition: with a `5` stored under `org.dept/total` and a rule that sums it to `150`, a read gives `5`.

So the problem statement above names the wrong cause, and is to be written again around this one: a folded value has no place of its own. A name that can be folded into can today also be written to, and a read cannot tell the two apart.

## Approach

Not decided. The [direction](named-fold/direction.md) lists five, drawn up for the cause as first stated; its second column is now measured.
