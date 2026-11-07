# The tree reads nodes beside the capability system: journal

## Reflective inquiry

|  | Understanding | Activity |
|---|---|---|
| **Status** | *What do we know?* How a tree reads a node on main and on #733 (see the description and the [claims](claims.md)); that the maintainer wants no side channels and the tree to read through capabilities; that `Holds` is used for more than the node cache. | *Where are we at?* Describing: the situation is written down; nothing is diagnosed yet. |
| **Agenda** | *What do we need to know?* What the tree's callers and the stores are trying to accomplish that the current shape is in the way of, beyond the stated remedy ("read through capabilities"); whether `Holds` as a whole is in scope or only the tree's node reads; whether in-place reads are worth an interface at all (an open hypothesis). | *Where are we going?* Diagnosing: from the feature as asked ("no side channels") to the problem underneath it. |

## 2026-11-07, Describing: what reading a node involves today

**Asked.** Is a `Load` that returns a buffer the right abstraction for the
tree, rather than asking for a node and leaving reading and decoding to the
provider? Answer as given: the maintainer wants a correct design with no
side channels; caching should not be the tree's, and was expected to have
moved into the environment on main already, to be verified.

**Learned.** Checked on main: the cache's ownership moved into the
environment, the cache's use did not; the tree still looks it up through
`Holds` and answers hits itself. The `NodeSource` on #733 copied that
pattern, and its per-read lookup was the branch's regression. `Holds` keeps
a formula cache, the repository's caches and network routes as well.

**Decided.** Not decided. The request arrived as a remedy ("read through
capabilities, no side channels"); the next sitting asks what problem it
answers.
