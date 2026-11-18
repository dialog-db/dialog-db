# How a query learns whether a block is held

Decision: how a consumer that can only query learns whether a node's
block is held by the replica, without fetching it, for: the problem in
the [top story](../tree-exploration.md), under the maintainer's
constraint that a tree read stays idempotent (see the
[glossary](glossary.md)).

**Decided by the maintainer, 2026-11-12: the sixth column.** The
built-in handlers assert nothing. Each answers with a second command,
and a rule in the space decides whether the answer is kept and where.
This replaces the same day's earlier decision for the fifth column
(handlers assert into the overlay), which the maintainer made first and
then improved on.

It rests on two things not yet true of dialog, and the maintainer has
said what to do about each (2026-11-13):

1. A built-in reaction to a command that performs an effect and
   dispatches a command in answer. To be built; the maintainer expects
   it to be easy.
2. A way for a rule to write to the overlay. Wanted, and not required:
   until it exists the answer is exposed as a command and a rule that
   keeps it commits it to the branch. The maintainer states the want
   more widely than this decision needs: a way to assert, retract,
   deduce and induce into the overlay.

**What dialog-db/dialog-db#795 does for these two (read 2026-11-14;
open, not merged, not run here).**

The second is answered, by a different route than a new kind of rule
head. A space's schema declares a layer kept in memory
(`dialog.layer/store`) and places an attribute on it
(`dialog.attribute/in`); a session opened on the branch holds that
layer, and "a rule head whose attribute is placed on a memory layer
lands there". So a rule that keeps a command's answer keeps it in the
session by placing the answer's attribute, and the fact is gone when
the session is. `Session::subscribe` sees it. Nothing is committed, so
the consequence below does not arise.

The first is not in it. #795 dispatches a command
(`Session::invoke`) and runs the rules over it; nothing in it performs
an effect in reaction to a command or answers one. Its note
(`notes/layers.md`, #721) describes that as open and later work: a
command "answered with a result", where "the reply is the stimulus of
the caller's next tick", under channels (its point 8a and follow-up 5).

It does supply what a handler can be built from without a new hook in
dispatch, which is the agent's reading and is untested: a rule turns
the request command into a fact on a memory layer; whoever handles it
subscribes to that attribute through the session, performs the effect,
and invokes the answer as a command, which starts the next tick; a rule
retracts the request.

It applies only through a session: "a branch read, committed or
subscribed directly, with no session, behaves exactly as before."

A consequence of committing in the meantime, for whoever writes the
rule: a committed fact syncs, and what one replica holds is not what
another holds, so the fact has to say whose holding it is (the replica
is already an entity) or it is false wherever it arrives.

| criterion \ approach | Status quo: tonk's reader probes the local archive while answering | A resolver that takes a time (the maintainer's) | A scan that probes the archive when the query runs (the agent's earlier) | Recorded answers: a command probes, the answer is a fact in the branch's ephemeral line | Two commands whose handlers assert (the maintainer's): one asks whether a block is local and asserts the answer; one fetches a block and asserts it is local | Two commands whose handlers answer with a command (the maintainer's, later the same day): the built-in handler asserts nothing; it dispatches a second command carrying the answer, and a rule in the space decides what becomes of it |
|---|---|---|---|---|---|---|
| the same query over the same inputs answers the same | 🔴 the answer changes when a block arrives and nothing the query names changed | 🟡 holds only if the archive can say when each block arrived; it keeps no such record today | 🔴 two reads of one node inside one evaluation can disagree if the block arrives between them | 🟢 the query reads a fact; the line it reads is pinned for the evaluation, and its revision is already part of what a query's caches are keyed by | 🟢 as the column to the left: the query reads a fact | 🟢 the query reads whatever fact a rule made of the answer |
| asked again as of an earlier moment | 🔴 answers for now | 🟡 needs the arrival record above | 🔴 answers for now | the fact as it stood, for as long as the line's ring of 1024 instants reaches; past that a reader recomputes at the present, as it does for every session fact | as the column to the left | depends on where the rule put it: a committed fact is in the revision; an overlay fact as in the two columns to the left |
| a subscriber is told when a block arrives | 🔴 no; tonk's reads are one-shot | 🟡 something must change the time and something must read it into every rule | 🔴 nothing in a poll looks at the archive: a poll re-runs when the head or the ephemeral line moved | 🟢 the arrival replaces the fact, which mints an instant; a poll already reads the instants since its pin and maintains per touched entity | told when the fetch command's handler asserts; 🟡 a block that lands another way (a tree read that fetched it, a pull) leaves "not local" standing until something asks again | told when the rule's fact lands; the stale case as the column to the left |
| a block is fetched without being asked for | does not fetch: reads with `Load::locally` | does not fetch | does not fetch | does not fetch: the probe is a local read, and the fetch is a second, separate command | does not fetch: only the fetch command does | does not fetch: only the fetch command does |
| what a consumer does to see a node's children as held or not | one request | reads the time, passes it to the resolver | one query | issues the probe for the children, then queries; a node never probed has no fact, which reads as not known | as the column to the left | issues the probe, then queries; with no rule installed for the answer, nothing is recorded and nothing is seen |
| what has to be built | exists, in the embedder | a clock fact, commands that move it, an arrival record per block, the resolver | a new kind of scan, and a way for a poll to notice the archive changed | a probe command; a hook where a hydrated block is written back; one reserved attribute | two commands and their handlers; nothing in the hydration path | two commands, their handlers, two answer commands; ? a rule's head is committed or transient today (`transaction/induce.rs`), so a rule cannot put anything in the overlay until a head can be marked for it |
| where the fact lives | - | - | - | ? the ephemeral line is per branch and a block belongs to a space's archive, shared by its branches; which lines a hydration writes to is not worked out | 🟢 a command is issued against a branch, so its handler writes to that branch's line | wherever the space's rule says. 🟡 committed is the only place a rule can say today, and a committed fact about what one replica holds syncs to replicas that hold something else |
| a replica drops a block | - | ? | ? | ? no effect that removes a block from an archive was found (`Get`, `Put`, `Import`, `Load`); if one exists it needs the same hook | the fact stands until something asks again | as the column to the left |
| where the probing and fetching code lives | 🔴 in the embedder, woven into its worker | in dialog | in dialog | ? a command and its handler are the embedder's mechanism in tonk (`impl Provider<C> for CommandEnv`); the hook is dialog's | in dialog, built in (the maintainer). ? dialog dispatches a command as a transient fact and reacts to it with inductive rules at commit (`Transaction::dispatch`, `transaction/induce.rs`); a built-in reaction that performs an effect and writes the overlay was not found and would be new | in dialog, built in; the naming and placing of the fact is the space's |
