# A branch commit is refused when the head has moved: journal

## Reflective inquiry

|  | Understanding | Activity |
|---|---|---|
| **Status** | *What do we know?* How a branch commit publishes on main: compare-and-set of the head cell against the head it was built on, refused with `VersionMismatch` when the head moved, recovery left to the caller. That a commit can ask to merge with the winner instead. That tonk's callers refresh and run again up to four times. The maintainer's statement: the refusal gives a UI application no good handling strategy. | *Where are we at?* Describing. The topic arrived as a remedy (an approach inspired by Software Transactional Memory); the maintainer has since given the symptom. What the symptom looks like to a person using a UI is not written down yet. |
| **Agenda** | *What do we need to know?* **Open question, asked 2026-11-10, not answered:** when a UI's commit is refused today, what does the application do and what does the person using it see; and what is wrong with each of the two strategies that exist (run again on the fresh head, `merge()`). Then: what moves a head under a UI's commit (a pull, another tab or worker on the same storage, something else), and how often. What "Software Transactional Memory" stands for in the request: the word names several separable things (checking what was read rather than whether anything changed, running the body again without the caller, composing transactions, waiting until a read changes), and which is meant is not said. | *Where are we going?* Finish Describing with the symptom as a person meets it, then Diagnosing: from the remedy to the problem it answers, with more than one candidate cause written down. |

## 2026-11-10, Describing: the situation, and the remedy it arrived as

**Asked.** The topic was opened as "Software Transactional Memory inspired
approach to handling commits". Before a question was put, the maintainer
added: "Problem is that commit on the branch can be refused if branch head
has moved, which provides no good handling strategy to a UI application."

**Learned.** Read on main (`4fd67af`) and in tonk (`bfb2d72`), all in the
ledger: a publish is a compare-and-set of the head cell; a lost race is
`VersionMismatch` and the staged batch is stale; the caller recovers by
running the transaction again; `merge()` publishes a merge with the winner
instead of failing; tonk runs again up to four times in two places and
matches the error by its text in both.

The maintainer's sentence holds two things. One is an observation the code
confirms (a commit is refused when the head moved). The other is a
judgement ("no good handling strategy"), which does not yet say compared
to what: two strategies exist and one of them, `merge()`, does not refuse.
What is wrong with each for a UI is the open question.

One tension to put to the maintainer, entered in the ledger as the
agent's hypothesis: Software Transactional Memory as usually built answers
a conflict by discarding the attempt and running the transaction's body
again. That is tonk's retry loop, moved from the caller into the runtime.
If being refused and run again is what a UI cannot handle, the remedy as
named keeps it.

**Decided.** Not decided. The title moved from the remedy to the symptom.
Nothing is diagnosed; the next sitting starts from the open question in
the agenda.
