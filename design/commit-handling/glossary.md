# A branch commit is refused when the head has moved: glossary

Terms this topic needs. A term marked *unsettled* is in use with more than
one meaning, or has no definition yet; settling it is on the agenda.

- **head**: the signed revision published in a branch's head cell.
- **the head moved**: the head cell holds a different revision than the
  one a commit was built on. It says nothing about what the other
  revision changed.
- **refusal**: a publish that fails with `VersionMismatch` because the
  head moved. What happens today.
- **conflict** (*unsettled*): the maintainer uses it for a refusal. In
  the Software Transactional Memory literature it means two transactions
  whose reads and writes overlap, which is narrower: the head can move
  without any overlap. Until settled, the files say "refusal" or "the
  head moved" and keep "conflict" for quotes.
- **stale** (of a query's result): the state has changed, since the query
  ran, in a way that changes the result. A head that moved does not by
  that alone make a result stale.
- **fail** (of a commit, *unsettled*): in "a commit never fails" it may
  mean "is never refused because the head moved" or "always leaves the
  writes the caller meant". The two differ when what the caller decided
  from is no longer so.
- **operation**: the maintainer's word, answered 2026-11-10: a command
  together with the rules that fire on it.
- **command**: a claim dispatched into a transaction as a transient fact.
  Inductive rules that watch for it fire at commit against the
  transaction's view; what they write joins the commit; the command is
  not stored.
- **overlap** (*unsettled*): the maintainer's word for what a commit read
  or wrote meeting what the writer that moved the head wrote. Not
  defined: at what grain (a fact, an entity, an attribute, a range a
  query scanned), and whether a read of something absent counts.
- **run again**: to evaluate a transaction on another head, its reads and
  its deciding included. Distinct from **apply again**: to put the same
  writes on another head without reading or deciding.
