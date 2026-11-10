# A branch commit is refused when the head has moved: journal

## Reflective inquiry

|  | Understanding | Activity |
|---|---|---|
| **Status** | *What do we know?* How a branch commit publishes on main: compare-and-set of the head cell and nothing else (confirmed: no reads are compared), refused with `VersionMismatch` when the head moved, recovery left to the caller; `merge()` publishes a merge with the winner instead. Three callers in tonk handle the refusal three ways. A transaction can hold commands, whose rules read and decide at commit, so dialog holds what it would need to do such a transaction again. The maintainer accepts three problems: the refusal has no answer in the application's terms; a caller that runs again through an agent is slower than the head; a correct loop for running again is not obvious and applications get it wrong. | *Where are we at?* Delimiting. A problem statement is drafted in the top story, not accepted. The maintainer's approach (capture reads and writes; apply without running again when they do not overlap what moved the head, assert the command again on the latest revision when they do) is recorded for Direction and not examined. |
| **Agenda** | *What do we need to know?* **Open question, asked 2026-11-10, not answered:** which of the four ways a transaction's writes come about (table in the last entry) the problem is about, given that for two of them dialog cannot do the deciding again. Then: whether the drafted problem statement is accepted. Not answered from the sitting before: what the agent or person gets back when a command's rule finds nothing to do, and so what "fail" means. Open hypotheses: what bounds the running again, since a commit applied or run again still publishes by compare-and-set; which refusal the agents met; where the web app's errors come from. Unsettled terms: conflict, fail, overlap. | *Where are we going?* Accept the problem statement and its scope, then Direction: use cases, a second and third approach beside the maintainer's and the status quo, and the matrix. |

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

## 2026-11-10, Diagnosing: what a refusal looks like to a person and to an agent

**Asked.** When a UI's commit is refused today, what does the application
do, what does the person see or lose, and what is wrong with the two
existing strategies? Answer as given: "Today if commit is refused in the
web app user sees an error in the console or somewhere in the user
interface if author bothered to handle an error. If two agents use tonk
cli and work on the same branch they often run into conflicts and get
confused by it and by the time they go and retry they often run into
conflict again as head moved again, worse they don't necessarily consider
that query they're working of maybe stale. Ideally commit never fails and
instead it receives an operation that dialog can rerun on latest state and
consider latest state."

What is wrong with `merge()` was not answered directly.

**Learned.** Read in tonk (`bfb2d72`): the command line pulls before a
write, evaluates and publishes in one process with no `merge()` and no
second try, and after the write pushes with up to five pull-and-push
rounds. So an agent on the command line can meet two different refusals,
the local publish's and the push's, and which one the report is about is
not known.

The report holds more than one thing. Written as candidate problems, the
agent's reading, each a hypothesis in the ledger:

1. **A refusal with no answer in the application's terms.** The head
   moving is not something the application's author or its user did or
   can act on, yet it arrives as the commit's error, in every caller.
   Each caller writes its own loop or writes none.
2. **The one who runs again is slower than the head.** When the reading,
   deciding and writing pass through an agent, one round takes longer
   than the gap between the other writer's commits, so running again
   from the caller loses again.
3. **A write decided from a read is applied without that read being
   looked at again.** This is not the refusal. The head check refuses
   when the head moved for reasons that touch nothing the writer read,
   and it does not refuse when the read was made earlier (another
   invocation) and the write lands on a fresh head. `merge()` and the
   pull before a push carry such a write onto state it never saw. A
   commit that is never refused would leave this as it is.

Two things do not sit together yet. "A commit never fails" and "considers
the latest state": an operation that looks at the latest state can find
that what it was decided from is gone, and then something other than "it
was applied as meant" has to come back. Which goes, or what "fails" means,
is the open question.

A second tension, not yet put: in the agent case the deciding happens in
the agent, between two invocations. An operation dialog can run again has
to hold the decision, and that one is not code dialog has.

"Conflict" is used for the head having moved. In the literature the
remedy comes from it means that two transactions' reads and writes
overlap. Both are in the glossary, unresolved.

**Decided.** Not decided. Phase moved from Describing to Diagnosing: the
symptoms are written, and the remedy and the ideal are recorded as the
maintainer's, not as the problem.

## 2026-11-10, Delimiting: three problems accepted, a statement drafted

**Asked.** The three candidate problems were put, and then: agent A
decides to mark todo T done, agent B deletes or reassigns T before A's
write lands, A's operation runs on the latest state; what should A get
back, and is that a failure? Answers as given: "1. Exactly. 2. Correct.
3. My point is correct retry loop is not obvious and applications get it
wrong. What we should have is rule and a command to mark todo completed.
Command is asserted which reads whatever state it needs and update it
accordingly e.g. only update todo if one with matching entity still
exists. Yeah so solution I'm reaching for is to capture reads & writes
performed and if there is no overlap commit can be applied without rerun.
If there is an overlap dialog could reassert command over latest
revision."

**Learned.** Candidates 1 and 2 are accepted. Candidate 3 is the
maintainer's restated: the fault is that a correct loop is not obvious and
applications get it wrong. The agent's version (a write lands without its
read being looked at again, refusal or not) is narrower in one way and
wider in another; what is left of it is kind 4 in the table below.

The todo answer says how the deciding is to be held: as a command, whose
rule reads what it needs when it runs. That exists on main
(`Transaction::dispatch`, commit-time induction). So the hypothesis that
an agent's decision cannot be held by dialog is refuted as far as the
agent states it as a command. The answer does not say what A gets back
when the rule finds T gone and writes nothing; that part of the question
stands.

The hypothesis that a remedy from Software Transactional Memory only moves
the refusal is refuted on its own terms: the maintainer wants the running
again, and objects to callers doing it.

Confirmed by reading `TransactionBatch::publish`: a publish compares the
head cell and nothing else. `ReadObservation` is the key of a memo for
read settlements, not a check at publish. The hypothesis is now a fact.

A transaction's writes come about in one of four ways, which differ in
what dialog holds when the head has moved:

| | how the writes come about | the reads | the deciding |
|---|---|---|---|
| 1 | asserted or retracted without reading | none | none |
| 2 | written by rules firing on a dispatched command | made by dialog, at commit | in the rule, which dialog holds |
| 3 | asserted by the caller after reading through the transaction (`Transaction::query`) | made through dialog, which could record them | in the caller's code, which dialog cannot run |
| 4 | asserted by the caller after a query made elsewhere, an earlier invocation say | unknown to the commit | outside dialog |

For 1 and 2 dialog holds everything needed to land the change on another
head. For 3 it could know that a read went stale and cannot decide again.
For 4 it knows nothing, and today such a write is accepted. "A commit
never fails" can be made true of 1 and 2 only.

**The maintainer's approach, recorded for Direction, not examined.**
Capture the reads and writes a commit performed. If what moved the head
does not overlap them, apply the commit without running it again. If it
does, assert the command again over the latest revision.

Two observations on it for when it is examined, both the agent's and both
in the ledger as hypotheses. It joins two things, dialog doing the work
again and telling when that is needed; the first alone answers the three
accepted problems, and what the second adds has not been said in terms of
a problem (a cost nobody has reported yet, or telling kind 3 apart).
And a commit applied or run again still publishes by compare-and-set, so
it can lose again; what makes it never fail is whatever bounds that, which
the approach does not name.

**Decided.** Not decided. The problem statement is a draft; its scope
waits on the open question. Phase moved from Diagnosing to Delimiting on
the maintainer's acceptance of the three problems.
