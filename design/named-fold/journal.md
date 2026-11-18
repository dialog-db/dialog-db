# A value computed over many facts cannot be given a name: journal

## Reflective inquiry

|  | Understanding | Activity |
|---|---|---|
| **Status** | A fold in a deductive rule can be admitted with a two-line change to the analysis: a cycle through it sets one rule aside, the same rule on every replica. That was never what kept the refusal. #827 made the same change and took it back because a folded value concluded into an attribute competes with the claims stored there, and nothing can be written through it. On the spike a stored `5` is read over a folded `150`. | **Delimiting**, back from Directing. The problem statement names the cycle as the cause; the cause the maintainer holds is that a computed value has no place of its own. |
| **Agenda** | Whether the notation's rule (a derived attribute is named under a concept, concluded only, never asserted) is the place a computed value belongs. What a read does with a claim stored under such a name, which a merge can always bring. Whether a subscription over a fold is maintained or asked again (the spike asks again; `e9712a9` on #827 maintained it per entity). | Ask the maintainer the first question. Then state the problem again and redraw the matrix for it. |

## 2026-11-18, Directing: from a refused getter to a problem

**Asked.** Building the notation (#773), a derived attribute that folds was refused by dialog-query. The maintainer was asked whether a view should compile to what tonk renders today or to the notation's design, which is a rule that folds. He answered: "Not sure what engine, but seems like design would be a right approach."

**Learned.** "The engine" was the wrong word for it: the refusal is one check in dialog-query, `open_rule_error`, in front of machinery that is there. The analysis already marks an edge from a reducing rule as aggregating and already checks stratification; `reduce_rows` already folds. What differs from `unless` is what a cycle does: a cycle through an absence test quarantines a rule, a cycle through a fold fails the evaluation. The refusal when a rule is compiled exists to keep a merge from reaching that failure.

The notation's rule that a fold concludes only into a concept's own domain answers a different question: it keeps a folded value from standing beside a stored one. It does not stop a cycle.

**Decided.** Not decided. The direction rests on one open hypothesis, the agent's: that a cycle through a fold can be quarantined as a cycle through `unless` is. A spike will say.

## 2026-11-18, Delimiting: the spike ran, and the cause was another one

**Asked.** Can a cycle through a fold be a quarantine, chosen alike on every replica (the first hypothesis in the ledger)? Does a subscription over a folded concept give the right rows (the second)? A spike on main at `54b1fc4`, branch `spike/named-fold`, commits `0c41ee8` and `de5ee74`.

**Learned.** Both hold.

- The change is `Polarity::Aggregating` counted as a test of absence with the strength of `unless`, and `open_rule_error` removed. Nothing in the fixpoint or in what is stored moved. dialog-query 989 tests pass, dialog-repository 818, 15 of them new.
- Two rules that close a cycle through a fold leave one set aside and the query answering. Replicas merging them in either order set aside the same one. Commit order decides which, as for `unless`.
- The objection in `notes/attribute-heads.md` (rules inside a cycle would read a different relation than readers outside) is stepped around, not answered: no fold is ever run over what its own component derives.
- A subscription over a fold delivers the right retractions and assertions, by asking the whole query again on every change it reads. Not maintained.

What the spike found that the ledger did not ask:

- **The experiment was not new.** #827 carried the same change (`3293f0f`, 2026-11-14) with per-entity maintenance of a subscription over a fold (`e9712a9`), and took the authored fold back two days later (`411c3ff`): "Where a computed value belongs is not settled." The agent read #827's body and not its history before proposing the spike. The hypothesis it called its own was the maintainer's text in that branch's `notes/attribute-heads.md`.
- **A stored claim is read over a folded value.** `5` stored under `org.dept/total`, a rule summing to `150`: the read gives `5`. The notation's design says this must not happen.
- A sum is narrowed per group (`UnsignedInt` under a field declared `integer:`), and a sum of naturals cannot conclude a `natural:` head. `3293f0f` fixed the second.
- A rule with several heads is set aside one head at a time, under an identity nothing is stored under.

Two things said before cannot both be true: the problem statement says the refusal is there for the cycle, and #827's note says it is there because a computed value has no place. The second is the maintainer's and the newer.

**Decided.** Not decided. The spike stays a branch; it does not go into the stack.

**Backtracked.** From Directing to Delimiting. The matrix was drawn for a cause that is answerable and was answered. The cause that stands is where a folded value lives, which only the third column of the matrix speaks to. The problem is to be stated again before any column is chosen.
