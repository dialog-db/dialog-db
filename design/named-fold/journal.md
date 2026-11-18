# A value computed over many facts cannot be given a name: journal

## Reflective inquiry

|  | Understanding | Activity |
|---|---|---|
| **Status** | A folded value concluded into an attribute is read together with what is stored there, and the spike shows the stored value winning. The maintainer's new direction: a getter, a query and a view are a kind apart from a concept, deriving what is never stored, with no domain. On the stack no query, subscription or inductive rule can state a fold at all; the "closed places" are places a fold is not refused, not places it can be written. One attribute, `dialog.rule/quarantined`, is already answered without the store. | **Delimiting.** The problem is restated in [design-no-domain](design-no-domain.md) around where a computed value lives, with four alternatives drawn against the code. |
| **Agenda** | Whether a getter must be readable in a rule that derives (tonk's library would say). Whether a reserved prefix answered at `QueryEnv::select` hides a stored claim from every read. Whether inline needs only a fold query type. | Ask the maintainer whether the restated problem is his, and whether a getter read by a rule that derives is wanted. Then the two hypotheses that split inline from a reserved namespace. |

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

## 2026-11-18, Delimiting: what a derivation with no domain could be

**Asked.** The maintainer, spoken: a query, a getter and a view are a kind apart from a concept; they derive, what they derive is not stored, so nothing competes with it; they could have no domain, which is why it could not be stored. A first attempt at what that is in the engine, read from the stack at `62b1f10`, the spike and #827. Nothing was built or run.

**Learned.**

- No fold can be stated outside a deductive rule. Every read is an `Application` and none folds; a query of several premises is itself a deductive rule laid over the query; `InductiveRule` takes no `reduce`, and its descriptor would drop one without a word. `DeductiveRule::with_reduce` is the one constructor, and it is the one refused. The "closed places" of `notes/attribute-heads.md` do not exist as places to write a fold, and `notes/attribute-heads.md:445` says as much for inductive rules. The view note's getter, read in a subscription, stands on this.
- A read of a derived attribute always plans the stored scan first; only the exact path skips it, and a fold rules that out.
- The precedent for a name nothing stored can answer is not `dialog.rule/*`, which is written freely and kept inert by content address. It is `dialog.rule/quarantined`, answered at `QueryEnv::select` from the analysis, and `dialog.revision/*`, which the write gate refuses. A merge screens no attribute.
- The notation's `concept.list.count-open/is` is an ordinary attribute to the engine; only the notation refuses a write of it.
- "No domain" taken literally is a conclusion with no `Relation`, since a relation must be `domain/name`. That is the one alternative in which no claim can exist under the name, and by far the largest.

**Decided.** Not decided. Two questions decide between inline and a reserved namespace: whether a getter must be read by a rule that derives (N5), and whether answering a prefix at `QueryEnv::select` hides a stored claim from every read (N2). The maintainer is to say whether the restated problem is his.

