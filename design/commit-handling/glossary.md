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
- **operation** (*unsettled*): the maintainer's word for what a commit
  would be given in place of writes, such that dialog can run it again
  on the latest state. Not defined: what it holds, and who writes it.
