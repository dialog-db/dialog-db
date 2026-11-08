# The tree talks in nodes: implementation plan

For whoever builds the [approach](../tree-node-reads.md#approach) decided in
this topic, or reviews whether what was built builds it. The plan was
written against a survey of main (`9b98395`) with dialog-db/dialog-db#733
merged in; it was then built on main alone, and each step below says what
landed and where it departed from the plan. The
[journal](journal.md) has the reasons.

## Why

Three things were wrong, and each step below answers one of them.

1. **The tree asked indirect questions.** It needs nodes: one from anywhere,
   one from this replica only, some ahead of a scan. What it asked instead
   were questions about how nodes are kept: where the cache is
   (`NodeCache::of`), whether a node is in memory (`Accessor::cached`),
   whether there is anywhere to put one (`Accessor::keeps`), and on the
   single-file branch whether anyone holds it as rows (`NodeSource::of`).
   The answers belong to whoever serves the node, so the questions are now
   requests the tree makes and providers answer.
2. **How a node is kept is the environment's business, and the tree reached
   into it.** The environment owned the node cache, but the tree found it
   through `Holds` (a map of type-erased values under string keys), answered
   hits itself, and filled it on reads and on persist; other code filled it
   again its own way. So an environment could lack a cache without anything
   failing, a read path could skip it by loading for itself, and every new
   environment or read path reopened one of these.
3. **The layering assumed every store holds a node as the same bytes.**
   `archive::Load` answers with a block's bytes and the tree decoded them.
   A store that keeps nodes differently on disk, such as the single file
   (dialog-db/dialog-db#733), which reads a node's rows in place through a
   memory map, would have to transcode them into those bytes on every read.
   So a translation layer now sits between blocks and nodes, and a store
   with its own node form answers above it. Blocks stay: sync, push,
   serving a peer and hydrating from a remote move bytes.

## The shape

Three layers:

- **Block layer:** reads and writes blocks as bytes by digest. `archive::Get`,
  `Load`, `Put`, `Import`, answered by `Volatile`, `FileSystem`,
  `IndexedDb`, remotes. Unchanged.
- **Translation layer:** turns a block into a node (decode) and a node into a
  block (encode). One implementation, in `dialog-search-tree`
  (`nodes::Read::through_blocks`, `nodes::Write::through_blocks`,
  `nodes::decode`), used by every environment whose store keeps bytes.
- **Node layer:** what the tree talks to, through three commands in
  `dialog_search_tree::nodes`, generic over the tree's key and value types:

  | request | means | replaced |
  |---|---|---|
  | `Read { catalog, name, priority, local }` | the node a name names, from anywhere (may hydrate), or from this replica only when `local`; a node the environment holds is answered at once (`Resume::now`) | `Accessor::load_node` / `get_node`, the free `load_node`, `Reading { local: true }` (`Load::locally`), the `Get` a difference's boundary side performed, `Accessor::cached` |
  | `Preload { catalog, name, priority }` | make this node cheap to read soon; returns nothing | `Accessor::warm`, `Accessor::keeps` |
  | `Write { catalog, nodes }` | the nodes a persist made; the provider stores them and keeps them readable | the `NodeCache::of(..).insert` in persist, `Import` and `forget` in `archive::persist` |

  `Nodes<Key, Value>` is the bound a tree operation puts on its environment:
  `Provider<Read> + Provider<Preload> + ConditionalSync`. `Holds`,
  `Load` and `Get` have left every tree bound.

**Who answers.** A store answers at the layer it keeps nodes in:
`Volatile`, `FileSystem` and `IndexedDb` through the translation layer, in
`dialog-search-tree` (the orphan rule allows it: the request type is the
tree crate's). `Space` hands a request to its archive; `Storage` routes it
to the space the catalog's subject is mounted as. A store with its own node
form (the single file) implements the three commands itself, and the rest of
the chain reaches it unchanged. This settles the plan's open question of how
a node request reaches such a store: through the storage, as every other
request does. The option of making the requests `Effect`s was dropped; the
reason `Load` is a command (a hydrating read is nobody's to answer on
another's behalf, and a node is never forked to a remote as itself) applies
to them too.

An environment above the storage adds what the store cannot: the peer
(`Peer`, `AccessEnv`) answers a read from the node cache it holds, else from
its storage, else hydrated through its `Hydrate` from the subject's routes,
and keeps what it read (`dialog_repository::read_node_or_hydrate`,
`preload_node_or_hydrate`, `write_nodes`). The query session (`QueryEnv`)
tries each line's catalog in turn for a read against the wildcard subject,
and no longer fills the cache itself. A batch's `DeltaOverlay` answers a
read of a node the batch sealed and has not flushed with the staged node,
decoding nothing. Test and measuring environments (`Counting`, `Metered`,
the tree crate's `Blocks` and friends) answer through their own loads, so
every block a tree reads is still tallied.

**Caching** is the environment's. The cache a tree used to look up is found
by the environment's provider (`NodeCache::of(env, subject)`, a `Holds`
lookup the environment makes of itself), once per node read rather than
once per operation. Nothing outside the environment names a cache, so an
environment without one reads uncached in exactly one place, its own
provider, and the tree cannot tell.

**Dispatch.** `Provider::execute` answers with a `Resume`: the output at
once (`Resume::now`, nothing allocated) or a future that produces it
(`Resume::later`, one boxed future, as `async_trait` always made). This is
a change to the capability crate, not to the tree: every provider in the
workspace answers this way, and `#[dialog_capability::provider]` writes
the common case (an `async fn execute` whose body is the future). A
provider that holds the answer returns it at once, and one standing in
front of another hands back what it was handed, so an answer at hand stays
at hand through the storage, the overlay and the query session. The tree
awaits either the same way. The journal's Building entries have the
measurement at each step: with every answer boxed, the point-read
scenarios paid 2 to 4% for one boxed future and one cache lookup per node
read; with a hit answered at once they are within 0.4% of main. A
side-door trait on the request (`nodes::Ready`) did this first and was
replaced: it answered outside the capability model, and the cost it
worked around was the model's, not the tree's.

**A node's hash** is still the hash of its block, and on main the two
types the plan weighed (a served node and a verified node) are not needed:
a node read from bytes is checked against the name it was asked under
before it exists. The distinction waits for #733, where a node read from
rows has no bytes until it encodes them.

**The commit** keeps ordering its own writes: spilled values, then nodes
(one `Write`), then the head. Crash safety does not move.

## Steps, as built

### 1. The node requests, as commands in dialog-search-tree

Built as planned: `nodes::Read`, `nodes::Preload`, `nodes::Write`, each a
`Command` whose input is itself, generic over `Key` and `Value`, and the
`Nodes` alias. `Read` and `Preload` keep `Load`'s `priority` for hydration
ranking. Two macros, `reads_nodes_through_blocks!` and
`writes_nodes_through_blocks!`, give a type that answers `Load` (and
`Import`) and holds handles the requests through the translation layer,
for the many test environments.

### 2. A node's name and its hash

Deferred to #733's rebase onto this: on main `PersistentNode` is its
block, so nothing here changes. What #733 has to do when it lands on top:
make the node an enum of a block or rows, give a rows node a `hash()` that
encodes once and keeps the result, and run the `hash()` inventory the plan
describes (the tree diff compares `link.node` with `node.hash()` at the
lines listed in the journal's sketch). The node cache already keys by the
requested name.

### 3. The translation layer, and how each environment answers

Built, with the open decision settled as described under "Who answers".
Beyond the plan: `Read::through_loader` answers a read through a caller's
own loading function rather than an environment's `Load`, for the counting
environment that answers loads through its own hydration. `Counting` and
`Metered` recognize the three requests by `Any` downcast inside their
blanket providers, as they recognized `Load`, so the perf counters still see
every block.

### 4. Reads move onto the requests

Built as planned. The two memory-only reads went as the plan said:

- the compressed forced-run check hands its piece summaries back
  (`piece_summaries`, then `compressed_verdict` over them), and the
  new-minimum check reads the previous piece's last key from them, or
  reads the piece when the check summarized nothing;
- pass 1 of the check defers every stored piece to pass 2's ordinary read,
  which takes the summary the node already carries when it has one.

The integration's read-ahead (`open_pending`) could no longer ask which
nodes were in memory, so it no longer opens a window only when a change's
path misses: it opens every window of pending changes, reading the first
stored node on each path and then each path's next node as its read lands,
deduplicating by hash. A node the environment holds answers at once, so
the cost of a warm window is reads that hit. The widening heuristic
(`OPEN_WIDTH`, `OPEN_LOOKAHEAD_MAX`) stays, counting the distinct stored
nodes the window names rather than the ones not in memory: the replication
gate showed what a fixed window costs (a seeded device's replay went from
17 round trips to 37, one per leaf), and the count of stored nodes needs
no peek. The difference from main is a warm window that names 32 stored
nodes of which few miss, which goes out with fewer misses than main would
have gathered; no gated scenario has that shape.

### 5. Writes move onto the write request

Built, with one addition the plan's review asked for: the `Delta` a
persist fills holds nodes (`Delta<Blake3Hash, PersistentNode<Key, Value>>`),
not blocks, and `ArchiveDelta` stages nodes (`nodes()`, `node(hash)`,
`flush_nodes()`), still handing out blocks to callers that ship them
(`block(hash)`, `flush_blocks()`). So a batch that reads a node it sealed
before the flush gets the node back from the overlay with everything the
seal derived (the hashes it adopted), rather than decoding its block; and
`persist` takes no environment, since it keeps nothing.

### 6. Other code that decodes or caches nodes

- The query session's `Load` provider no longer decodes and caches: a node
  read reaches the peer's cache through the session's `Read`.
- The difference's `Boundary` side became a local `Read`. Its old reason for
  bypassing the cache (a node in memory is not proof the archive holds it)
  no longer holds: a scope's cache keeps only what its archive produced or
  what a successful write stored. A test environment that hydrated every
  load had to learn to honor a local one, as every real provider does.
- Push's forwarding of a remote's block, the resolver's node inspection and
  the baseline's walks still read blocks on purpose; they are block readers
  and stay on `Load` and `Get`.

### 7. Bounds

Built. In the tree crate every `Env: Provider<Load> + Holds +
ConditionalSync` (and the `+ Provider<Get>` variants) is
`Env: Nodes<Key, Value>`. `dialog-artifacts` bounds on `ArtifactNodes`
(and `ArtifactWrites` where it persists); `TreeEnv`, `Capabilities`,
`BranchEnv`, `PeerSpace` and `Scope` carry them. The repository's own
bounds keep `Provider<ArchiveLoad>` and `Holds` where they still load
blocks or hold routes; those were not pruned.

### 8. Remove

`Accessor::cache`, `cached`, `keeps`, the cache field, every `NodeCache::of`
in tree code, the `Holds` bound on tree code, `Node::into_link`'s cache
argument, and `archive::persist`'s `forget`. `NodeCache::of` stays as the
environment's way to find its own cache.

## How it was checked

- `cargo clippy --workspace --all-targets --all-features -D warnings`, the
  tests of every crate touched, and the wasm build of the tree, storage,
  artifacts, repository, peer and query crates.
- `dialog-perf` on main (`9b98395`) and on this branch with one toolchain,
  under callgrind: the journal has the numbers.

## Open, for #733 and after

- Step 2, when #733 rebases: the enum, the lazy hash, the `hash()`
  inventory.
- Whether the per-read cache lookup (about 200 instructions, what a hit
  costs now) should become a field of the peer, found once at
  construction, rather than a `Holds` lookup. `HeldCaches::hold` can
  replace the held cache after the peer is built, so the field would have
  to be the source of truth.
- A mapped file's page cache is a cache the environment cannot own or
  release; out of scope here and noted in the [claims](claims.md).
