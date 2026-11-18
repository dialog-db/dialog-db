# Direction

**Decision:** where a fold may stand so that its value has a name.
**For:** an author cannot name a value computed over many facts and read it where a read stands, because a merge of rule sets must never fail a query.

| criterion \ approach | Status quo: a fold only in the closed places | A fold in a deductive rule, quarantined in a cycle | A fold only into a concept's own domain | A name for a body, not a rule | A rule that writes the value |
|---|---|---|---|---|---|
| in one line | refused in a deductive rule; a query, a subscription or an inductive rule may fold | admitted; a cycle through a fold sets one of its rules aside, as a cycle through `unless` does | the notation's rule: a fold concludes an attribute named under a concept, which is concluded only and never asserted | the derived attribute is not installed; where it is read, its conditions and its fold stand in its place | an inductive rule folds and asserts the value as a claim |
| read in another rule's condition, as the view reads `(list/count-open ?l ?count)` | 🔴 there is nothing to read | read as any concept | read as any concept | 🔴 the reader is then a deductive rule with a fold in it, refused | read as any claim |
| what a merge of two sound rule sets can do | nothing new | quarantine a rule; the query answers without it | 🟡 says where a folded value goes, not whether a cycle closes through it: a rule may still derive the folded rows from the fold | nothing new | nothing new |
| a folded value beside a stored one | cannot happen | ? #793 ranked a fold's value against stored claims; not looked at on #827 | cannot happen: the domain holds no stored claims | cannot happen | 🟡 it is a stored claim, and the notation's design rules that out: "a stored `5` can never beat a count" |
| when the facts under it change | asked again | derived again | derived again | asked again | 🟡 stale until the rule fires again, and each firing is a write that syncs |
| what dialog-query has for it | everything | `Polarity::Aggregating`, the stratification check and `reduce_rows` are there; the cycle is a violation where it would have to be a quarantine, and `open_rule_error` refuses first | as the second column, and a check that the head is in a concept's domain | ? no way was found for a query to state a fold on main: `DeductiveRule::with_reduce` is the one constructor, and it is the one refused | ? not checked that an authored inductive rule takes a `reduce` |
| under a subscription | ? | ? `notes/aggregation.md` lists incremental maintenance of a fold as later work | ? | ? | as any write |
| serves the notation's `view` | 🔴 no | yes | yes; a view's head would be in its concept's domain | 🔴 a view reads derived attributes in its conditions | 🟡 markup stored and synced as claims |

The second and third columns are not alternatives: the third says where a folded value may go, the second what happens when a cycle closes through one. The notation's design asks for the third and needs the second under it.

The agent's hypothesis is in the [ledger](claims.md): the second column is a small change, because the analysis already knows an aggregating edge and already quarantines. It is a hypothesis until a spike runs.
