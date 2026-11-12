# Design decision: what the corpus asks for when a value spilled out of its key

**Problem it serves:** the search tree's concerns are not expressed as
capabilities (the topic's [problem](../tree-node-reads.md)). #755 settled
the tree's nodes: a tree performs `nodes::Read<N>` and the environment
answers however it keeps nodes. The corpus has one more read beside its
nodes: a value too large for its key is stored apart, as a blob of the
catalog's archive, and every scan, record read, merge and export that
meets a `Spilled { hash }` payload reads those bytes. Today that read is
`dialog_effects::blob::Load`, performed by the corpus in one place
(`load_spill`, which checks the bytes against the reference) and named in
about forty-five `Provider<blob::Load>` bounds, of which this pull request
found only the ones on a path that reaches `load_spill` to be real.

**Decision:** does a request of the corpus's own stand in front of
`blob::Load`, as `nodes::Read<CorpusNode>` stands in front of
`archive::Load`, and if so, now or with the next change to these call
sites?

## Facts the matrix rests on

- `blob::Load` is already a command, not an effect: it carries the
  archive, the digest, a `Priority` and `local`, exactly the fields
  `nodes::Read` carries, and its doc says it reads "a spilled value
  wherever the environment can reach it"
  (`rust/dialog-effects/src/blob/load.rs`). The peer answers it from its
  store, the legacy catalog, then a hydration; the query environment per
  line; `DeltaOverlay` from its staged lane.
- A request defined in `dialog-corpus` cannot be answered by the stores in
  `dialog-storage`: `dialog-corpus` depends on `dialog-search-tree`, which
  depends on `dialog-storage`, so `dialog-storage` cannot name the request;
  and a blanket `impl<Env: Provider<blob::Load>> Provider<spill::Read> for
  Env` is refused by the orphan rule (the uncovered `Env` comes before the
  local type). Only wrapper environments (the peer, the access and query
  environments, `DeltaOverlay`, the test environments) could answer it,
  each with its own impl or a macro line. This is the assumption in the
  brief ("a block store answers it through a translation like
  `reads_nodes_through_blocks!`") that does not hold: the tree crate can
  write those impls for the stores because it sits above `dialog-storage`;
  the corpus crate cannot without the dependency this pull request removes.
- What the node request changed beside its name was *what comes back* (a
  decoded, checked node) and *who caches* (the environment, through
  `NodeCache::of`). For a spilled value there is nothing to decode; the
  only content a corpus request could add is the hash check (already in
  `load_spill`) and, later, answering from a spill cache the environment
  holds. The spill cache is still handed in by the caller today
  (`CorpusExt::scan(store, cache, selector)`, `TreeHistory`'s own cache),
  shared from `HeldCaches` by #742.
- The follow-up that dissolves `dialog_search_tree::Accessor` decides
  whether the environment or the request names the subject. A new request
  would carry a `catalog` (or `archive`) field, which is that question.

| Decision: the spilled-value read, for: the corpus's concerns expressed as requests on its environment |
|---|---|---|---|
| criterion \ approach | Status quo: `blob::Load`, performed through one corpus function | B: a corpus request `spill::Read` in front of `blob::Load`, cache still handed in | C: B, and the spill cache moves into the environment, as the node cache did |
| the corpus asks in its own vocabulary | 🟡 the bound names a blob by digest; the corpus's function (`load_spill`) is where "spilled value by reference, checked" is said | 🟢 the bound names the spilled value by reference, the translation checks it | 🟢 as B |
| who answers for a bare store | 🟢 every store in `dialog-storage` already does (`loads_locally!`) | 🟡 no store can; a wrapper per environment (peer, access, query, overlay, each test environment), by a macro line, because of the dependency direction and the orphan rule | 🟡 as B |
| what the request adds beyond a name | the hash check and the missing-is-an-error rule, in one function | 🟡 the same two, moved into the translation | 🟢 the environment answers a value it holds at once (`Resume::now`), the caller stops threading a cache |
| call sites touched | none beyond the bounds this pull request drops | 🔴 about a hundred and ten `Provider<blob::Load>` bounds in the repository, query and peer crates that forward to corpus operations, each twice if the subject decision moves the field again | 🔴 as B, and `scan`'s signature, `HeldCaches`, `Source`, `Branch` and `TreeHistory` |
| the perf gate | at 0% by construction | a boxed future per miss where the provider macro already boxes one; a hit never performs the request | ? a hit answered with `Resume::now` costs a `Holds` lookup per read that the handed-in cache does not pay today; to measure |
| coupling to the subject decision | none | 🟡 the field is chosen now and may move | 🟡 as B |

## Scope decision for this pull request

Status quo, with the bounds audited: `blob::Load` stays the request the
corpus performs, in one function, and `Provider<blob::Load>` is named only
where a path reaches a spilled value. B and C are one decision, to take
with the subject decision, since B alone is a rename of `blob::Load` that
touches every site the subject decision will touch again, and C is where
the request earns its place. The maintainer can overrule this here: B is
a day's mechanical work on top of this branch, and nothing in it is
blocked by what landed.
