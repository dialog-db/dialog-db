# The search tree reads through capabilities

Status: taken. Step two after the store-held node cache (#730); lands
in #735.

## Context

The search tree was written before the capability model. Its storage
contract was `LoadBlock { hash }`, a `Command` whose input was itself: no
subject, no catalog, no `Capability`, nothing to attenuate or fork. The
capability-native read of a block is `Get`, an `Effect` under
`Archive → Catalog → Block`, built by `CatalogScope::get(digest)` and
performed on the session env, where the storage router dispatches it by
subject and catalog.

`LocalIndex` was the bridge between the two: it held the `(env, catalog)`
pair the subject-less command lacked and turned `LoadBlock` into `Get`.
Everything else stacked on that bridge as a `Provider<LoadBlock>`:
`NetworkedIndex` added hydration from the branch's tracked remote,
worked out per read by `Branch::fallback`; `DeltaOverlay` added a batch's
staged blocks; `QueryEnv` added a loader across several lines; and test
envs in dialog-query, dialog-repository and dialog-diagnose forwarded.
`ArchiveReader` and `Reads` were alias traits over the same split. Step
one (the `Nodes` trait) put the node cache on the bridge and routed every
read through it, so no reader could skip the cache, and left the bridge
in place.

Step two removes the bridge: the tree performs capabilities on the env,
the env holds the cache and the hydration routes, and the wrappers go.

## Decision

### The hydrating read is a command, `Load`, and `Get` does not change

`dialog_effects::archive::Load { catalog, digest, priority, local }` is a
`Command` whose input is itself and whose output is
`Result<Option<Buffer>, ArchiveError>`. Its meaning: the block a `Get` of
`digest` in `catalog` names (`Load::capability`), from the local archive,
or else from wherever the env knows the subject's blocks live, written
back locally before it is returned. It names the catalog and the digest
rather than carrying the built `Capability<Get>`, so a hydrating provider
reads where to hydrate from without unpicking the capability, and it
carries the priority a hydration it causes ranks at, so a speculative
walk never takes a site's slot from a demand read.
`dialog_effects::blob::Load` is its spilled-value counterpart.

It is a command and not an effect because nothing can answer it on
another's behalf: it is never forked to a remote, and a remote's own
hydration is no answer to it. `Get` stays the one attenuated, signable,
forkable read of a block; its wire shape does not change, and it never
hydrates.

A read that must see only what the local archive holds performs `Get`
(the differential's `Boundary` side), or, where it reads through code
that loads (a tree's walk), a load made local with `Load::locally`, which
every provider answers exactly as the `Get` it names. That flag is what
the design's "local policy" became: it is carried by the read, not by
`Get`, so the effect stays unchanged.

### Who answers `Load`

The storage backends (`Volatile`, `FileSystem`, `IndexedDb`) and
`Storage<S>` answer a load as the read: they know no remote. The peer
(and `AccessEnv`) answers through `dialog_repository::load_or_hydrate`:
the local `Get`, then, on a miss, the routes registered for the subject,
hydrating through the peer's existing `Provider<Hydrate>` (one shared,
prioritised job per digest in the `HydrationScheduler`), and failing with
the same `Unavailable` error text as before when the only route is down.
With no route registered, a miss is a miss.

### Routes are registered per subject

Blocks are content-addressed and the catalog is per repository, so any
remote holding the subject can serve any block. A `Routes` handle held on
the env (`Routes::of(env)`, through `dialog_common::Holds`) maps a subject
to the routes its open branches track, in order, pulls before pushes, as
`Tracking::first_remote_route` orders them, with routes lent for a pull's
prepare, its novelty walk and a downloading export ahead of them. The
registry lives in dialog-network rather than dialog-repository: the peer
links the repository's library build while the repository's tests use
its test build, and two builds of one crate would hold two registries.
`OpenBranch` registers each branch, and a registration re-reads the
branch's tracking cell only when its version or the connection count
changes, so every tracking write is picked up. `Branch::fallback` is off
the read path; it remains for push's "could not confirm" error, a
download, and the asset blob read, which streams a whole asset through a
digest-verified import rather than loading a block.

### A tree is a root under a catalog

`PersistentTree` (and so `Index` and `ArtifactTree`) carries the
`CatalogScope` its blocks live in next to its root:
`PersistentTree::at(catalog, root)` and `PersistentTree::empty_in(catalog)`.
Every node read performs `Load` on the env, in one function in the tree
crate (`fetch_node`, behind `load_node` and the `Accessor` an operation
opens), and the env's bound is `Provider<Load> + Holds + ConditionalSync`
(and `Provider<Get>` where a difference is computed). A tree also carries
how it reads (`Reading`): `ranked(priority)` for a warming walk, and
`locally()` for a reader whose answer is what the replica holds, such as
an export that does not download.

### The env holds the checked-node cache

`NodeCache::of(env, subject)` is a `Holds` lookup under the tree crate's
key, scoped by the catalog's subject, made once per operation by the
`Accessor`. An env that keeps handles and holds no cache yet is given one
with the default budget; an env that holds nothing (a bare store) has no
cache. `HeldCaches` holds its node cache under that same key, adopting
one a tree already made, so the repository's budget, `release` and
`clear` govern the cache every tree read through the env uses. A persist
keeps the nodes it seals there, and a flush that fails forgets them
through the same lookup (`archive::persist`).

### What a batch stages

`DeltaOverlay` stays, as an env over an env that keeps both lanes: the
nodes a batch persisted and the values it spilled, both unflushed. A
batch that persists and then reads again before its flush (the
canonical apply over a root an earlier batch persisted, a record after an
apply) reads its sealed nodes back from the staged blocks when the env
holds no node cache, or the cache let them go under its budget.

### The resolver's loader

A resolver premise (`tree/node` and co) asks for a content address
anywhere the query reads: it performs `Load` against the wildcard
subject's archive (`dialog_query::resolver::anywhere`). `QueryEnv` answers
such a load by trying each line's catalog in turn, through the node cache
the env holds for that line's repository, and answers a load of one
catalog's block as that catalog's, through the env.

### One copy of the tree code

The repository reads every tree through `&dyn TreeEnv`, the env itself as
a trait object, so the generic tree code is instantiated once rather
than once per env type. A borrowed env provides what the env provides
(`impl Provider<C> for &T` in dialog-capability, `impl Holds for &T` in
dialog-common), which is what lets code that takes its env by value (a
scan's stream, a history) be handed `&env` instead of a wrapper.

## What it removed

`LoadBlock`, `LoadBlob`, `ArchiveReader`, `Reads`, `LocalIndex`,
`NetworkedIndex`, `Nodes`, `Cached`, `ArtifactNodes`, `MemoryBlocks`,
`WithoutSpills`, the forwarding `Blocks` of dialog-diagnose and the
forwarding loaders of dialog-query's test envs, and `Branch::fallback` on
the read path. Tests read through dialog-storage's `Volatile` under a
test catalog (`dialog_search_tree::helpers::Blocks`); a counting test
backend implements the loads and counts them.

## Consequences

Easier: a new read path cannot bypass the cache or the hydration policy,
because it has no way to get a node except `Load` on the env. A test env
is any `Provider<Load> + Holds`. The route registry gives one answer to
"where does this subject hydrate from" that pull, push, select and
subscriptions all share, and a branch can hydrate a block only a sibling
branch's remote holds.

Harder: the tree crate depends on dialog-effects for `Load`, `Get` and
`CatalogScope`. Every tree constructor takes a catalog. A reader's env
must now hold (`Holds`) for its reads to be cached. Any read of a subject
with a tracked remote can hydrate, snapshots and a transaction's staged
view included; a non-downloading export reads locally.

## Open questions, as answered

1. One route per subject or per branch? Per subject, the union of the
   open branches' tracked remotes, tried in order.
2. Local-only as a policy on `Get`, or a separate effect? Neither: a
   reader that must not hydrate performs `Get`, or a `Load` made local,
   which every provider answers as `Get`. `Get` is unchanged.
3. Does the peer hydrate for every caller, or only when a route is
   registered? Only when a route is registered; with none a load is
   today's local read.
