# Journal

## Reflective inquiry

|  | Understanding | Activity |
|---|---|---|
| **Status** | We know what the maintainer asked for (an inversion: the tree performs effects a handler resumes, without naming its environment), what exists (`Provider<C>::execute` returning `Resume`, statically bound), what one boxed future per command costs (2 to 4% on point reads, measured in the tree-node-reads topic), and that the maintainer's bar is "performs as good or better". | Describing. A branch and a topic file exist; nothing is diagnosed. |
| **Agenda** | What the maintainer is trying to accomplish that the current binding prevents: which of decoupling, handler composition, batching across suspended requests, or testability is the objective, and what concretely is in the way today. Whether "effect" and "handler" mean the same thing to the maintainer as in the two libraries named. | Diagnose: from the feature ("proper effects") to the problem, one question at a time. |

## 2026-11-08, Describing: opening the topic

**Asked.** Nothing yet of the maintainer beyond what they volunteered
while closing dialog-db/dialog-db#755: that `Resume` is a compromise and
that they want a proper inversion, decoupled from the environment type,
on another branch, if it performs as well or better.

**Learned.** Written into the description: what exists on the data and
dispatch sides, what was measured, and the two hand-written things an
inversion might subsume. All of it is observation or attribution; no
cause is claimed.

**Decided.** The topic exists and sits on its own branch
(`claude/design-effect-handlers`), off the #755 branch so a prototype can
be compared with `Resume`. Not decided: anything about the problem.
