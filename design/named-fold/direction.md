# Direction

**Decision:** where a fold may stand so that its value has a name.
**For:** an author cannot name a value computed over many facts and read it where a read stands, because a merge of rule sets must never fail a query.

| criterion \ approach | Status quo: a fold only in the closed places | A fold in a deductive rule, quarantined in a cycle | A fold only into a concept's own domain | A name for a body, not a rule | A rule that writes the value |
|---|---|---|---|---|---|
| in one line | refused in a deductive rule; a query, a subscription or an inductive rule may fold | admitted; a cycle through a fold sets one of its rules aside, as a cycle through `unless` does | the notation's rule: a fold concludes an attribute named under a concept, which is concluded only and never asserted | the derived attribute is not installed; where it is read, its conditions and its fold stand in its place | an inductive rule folds and asserts the value as a claim |
| read in another rule's condition, as the view reads `(list/count-open ?l ?count)` | 🔴 there is nothing to read | read as any concept | read as any concept | 🔴 the reader is then a deductive rule with a fold in it, refused | read as any claim |
| what a merge of two sound rule sets can do | nothing new | quarantine a rule; the query answers without it | 🟡 says where a folded value goes, not whether a cycle closes through it: a rule may still derive the folded rows from the fold | nothing new | nothing new |
| a folded value beside a stored one | cannot happen | 🔴 the stored claim is read over the folded value: stored `5`, folded `150`, read `5` (spike). The reason #827 took an authored fold back | ? cannot happen while nothing writes there. A replica without the rule can write there and a merge brings the claim; not tested | cannot happen | 🟡 it is a stored claim, and the notation's design rules that out: "a stored `5` can never beat a count" |
| when the facts under it change | asked again | derived again | derived again | asked again | 🟡 stale until the rule fires again, and each firing is a write that syncs |
| what dialog-query has for it | everything | two lines of `dependencies.rs` and the removal of `open_rule_error` (spike, `0c41ee8`). #827 had it at `3293f0f` and took it back | as the second column, and a check that the head is in a concept's domain | ? no way was found for a query to state a fold on main: `DeductiveRule::with_reduce` is the one constructor, and it is the one refused | ? not checked that an authored inductive rule takes a `reduce` |
| under a subscription | ? | 🟡 right rows, by asking the whole query again on every change (spike). `e9712a9` on #827 maintained it per entity | ? | ? | as any write |
| serves the notation's `view` | 🔴 no | yes | yes; a view's head would be in its concept's domain | 🔴 a view reads derived attributes in its conditions | 🟡 markup stored and synced as claims |

The second and third columns are not alternatives: the third says where a folded value may go, the second what happens when a cycle closes through one. The notation's design asks for the third and needs the second under it.

The second column is measured (2026-11-18, [journal](journal.md)): it is a small change and it works. It fails the fourth row, which is the reason the maintainer gave for taking the same change back on #827. This matrix was drawn for the cycle as the cause. With the cause moved to where a folded value lives, its rows are to be chosen again.
