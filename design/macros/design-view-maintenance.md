# A view is a template over rows, not a value

**Decision:** what a view is, so that a change repaints the smallest part of it.
**For:** the design has a view be a rule that folds its template into one markup value. A folded value is opaque: when a fact changes the whole value is made again. And a fold in a rule that derives is what Dialog-DB refuses (#830).

The maintainer, spoken on 2026-11-18 and put down here in substance: a view is not a concept. A concept competes with stored values and a view does not have to. Queries and getters are the same separate kind of thing: they derive, and what they derive is not stored, so nothing competes with it. They could be something with no domain, which is why it cannot be stored. And a view should not be made whole again at every change: it is maintained, as tonk maintains its views.

What follows is the agent's reading of that, a hypothesis to prototype, not a decision.

## Three things with no domain

```
 a query     (query head premise…)                     asked once, or subscribed to
 a getter    (rule (list/count-open ?l (count ?t)) …)  a query with a name, read where a query may stand
 a view      (view concept :name template condition…)  a template and the queries that fill it
```

A claim is an attribute, an entity and a value, and an attribute has a domain. A name with no domain has no claim that could hold it: nothing asserts it, no merge brings it, no read of it looks at the store. What is stored is the definition, as a rule's body is. What it derives never is.

## A view

A template is a tree of elements. A view's conditions are not one query whose rows are folded into the tree. Each element that holds variables has the conditions that bind them, given what the elements above it bound:

```
 (view list :card
   [:article [:h2 ?name] [:ul [:li ?title]]]
   (list ?l name: ?name)
   (todo ?t list: ?l title: ?title))

 element      holds      its conditions                       an instance of it
 article      ?l         the view's concept on its entity     one, while the list is one
 ├ h2         ?name      (list ?l name: ?name)                one a name
 └ ul                                                         one, while the article is
   └ li       ?title     (todo ?t list: ?l title: ?title)     one a title; none when the list is empty
```

- An element has one instance for each distinct combination of the variables it holds, under one instance of its parent. An element that holds none has one instance while its parent has.
- A condition belongs to the highest element that needs a variable it binds. A condition no element needs belongs to the root: the view is there only while it holds.
- So a list with no to-dos keeps its card. One query over both conditions would have no row, and no card.
- Each element's conditions are a subscription. A row that comes or goes is an instance that comes or goes, and nothing else in the tree is touched. A renamed list is one text in one `h2`.
- A nested view is a reference with its own subscriptions, as the design has it.

No fold is in it. A view needs nothing of Dialog-DB that a subscription does not already give.

## What a getter is then

A fold with a name. This section first said a view's element could read one in its conditions because those are a subscription, "one of the places Dialog-DB already lets a fold stand". That was wrong, corrected on 2026-11-18 from a reading of the code (#830, `design/named-fold/design-no-domain.md`): no query, subscription or inductive rule can state a fold. Every fold goes through `DeductiveRule::with_reduce`, and that is the one refused. The notes' "closed places" are places a fold would not be refused, not places one can be written.

So a view that shows a count needs one of two things of Dialog-DB, and neither is there:

```
 a query that folds          one new kind of query, folding over an inner one     a getter is read in a view, a query, a rule that writes; not in a rule that derives
 a relation only rules       a reserved name answered from the rules and never    a getter is read anywhere; a fold is admitted into those heads only
 conclude                    from the store, as dialog.rule/quarantined is
```

A view with no count in it needs neither.

## Open

- An instance with no row behind it: whether the root is there for an entity that is no instance of the concept.
- The order of instances.
- What is stored for a view, so another replica's renderer finds it.
- Whether "the highest element that needs it" is the rule an author expects, or a condition is written where it applies.

## As built

Built 2026-11-18 in `rust/dialog-notation`: `Plan::of` (`src/view/plan.rs`), `Maintainer` and `render` (`src/view/tree.rs`), run on a branch in `tests/view.rs`. What differs from the above:

- An instance is told apart by what the values its element holds are fields of, not by the values. `[:li ?title]` has one item a to-do, so two to-dos of one title are two items and a retitle is one text. Read literally, the rule above merges them.
- The entity rendered is bound above the root, and the root takes the concept applied to it alone. Without that, every condition binds `?l` and goes to the root.
- Siblings that both need a condition share it at their common ancestor. A join on a variable no element holds is pulled into the element whose condition it ties, or is a filter where its variables are bound.
- A layer of several reads compiles to a rule and a query of it. `Branch::subscribe` takes no rule, so the rule is put in the branch's session overlay.

## The maintainer's form, 2026-11-18

Spoken, after the prototype. Put down in substance, his words where quoted.

The layered reading keeps a card for an empty list, but which part of a template stays when nothing matches is implied by where the variables sit, and is not said. Move one variable up and the frame is gone. "I think we need something there."

His answer: "define a container as a separate view", and within a view everything repeats. A nested view is written as a call, which he liked "because then you can have maybe constraints on other fields too", and the call says `view`: "it's `view notebook row` instead of just `notebook row`". "That works for me."

```clj
(view notebook :directory
  [:div {:class "notebook-index"}
    (view notebook :row)])                  ; every notebook, a row each

(view notebook :row
  [:a {:href ["/notebook/" ?n]} [:span ?title]]
  (notebook ?n title: ?title))

(view list :card
  [:article [:h2 ?name]
    [:ul (view todo :item list: ?l state: :open)]]   ; the open items of this list
  (list ?l name: ?name))

(view todo :item
  [:li ?title]
  (todo ?t title: ?title ...))
```

```
 a view with no condition      stands once, always                       the frame
 a view with conditions        its whole template, once a match          nothing in it repeats on its own
 (view concept :name …)        a read of the concept with a view's name  where repetition happens; its labels narrow the match
 in a template
```

- A directory is a view like any other. `:directory`, `:card`, `:row` are names. Several views of one concept are wanted: directory and ui today, an embed for a message thread and one for a terminal considered.
- No notebooks: the index's `div`, empty. No to-dos: the article, its heading, an empty `ul`. Both are written, not inferred.
- A view that is not found is shown as its source: the entity as the notation writes it. "It's more like a view source." That is the renderer's, and it takes the place of tonk's view of any model, which is where the host was read ([design-view-context.md](design-view-context.md)).
- Every reference names its concept, so no template reads which model it was asked for.

Open, and his: whether a view and a concept are two things at all. "Concept is kind of the view."

Not decided by him and the agent's to show: what a view with two conditions and no nested view does. By "everything repeats" it is one instance of the whole template a match, the one-query reading the prototype already has, which shows nothing for an empty list. The layered reading stays in the build beside it until both have been seen on the real views.

