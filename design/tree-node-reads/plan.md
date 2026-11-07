# The tree talks in nodes: implementation plan

For whoever builds the [approach](../tree-node-reads.md#approach) decided in
this topic, or reviews whether this plan builds it. Line numbers come from a survey
of dialog-db/dialog-db#733's branch with main (`9b98395`) merged in, so in
the tree crate they are off where that branch changed it, and all will
drift; the names will not.

## Why

Three things are wrong today, and each step below answers one of them.

1. **The tree asks indirect questions.** It needs nodes: one from anywhere,
   one from this replica only, some ahead of a scan. What it asks instead
   are questions about how nodes are kept: where the cache is
   (`NodeCache::of`), whether a node is in memory (`Accessor::cached`),
   whether there is anywhere to put one (`Accessor::keeps`), and on the
   single-file branch whether anyone holds it as rows (`NodeSource::of`).
   The answers belong to whoever serves the node, so the questions should be
   requests the tree makes and providers answer.
2. **How a node is kept is the environment's business, and the tree reaches
   into it.** The environment owns the node cache, but the tree finds it
   through `Holds` (a map of type-erased values under string keys), answers
   hits itself, and fills it on reads and on persist; other code fills it
   again its own way. So an environment can lack a cache without anything
   failing, a read path can skip it by loading for itself, and every new
   environment or read path has reopened one of these. The environment is
   where caching lives; nothing outside it should have to know a cache
   exists.
3. **The layering assumes every store holds a node as the same bytes.**
   `archive::Load` answers with a block's bytes and the tree decodes them.
   A store that keeps nodes differently on disk, such as the single file
   (dialog-db/dialog-db#733), which reads a node's rows in place through a
   memory map, would have to transcode them into those bytes on every read:
   overhead that defeats the point of the representation. So a translation
   layer goes between blocks and nodes, and a store with its own node form
   answers above it, skipping it. Blocks stay: sync, push, serving a peer and
   hydrating from a remote move bytes.

## The shape

Three layers:

- **Block layer:** reads and writes blocks as bytes by digest. `archive::Get`,
  `Load`, `Put`, `Import`, answered by `Volatile`, `FileSystem`,
  `IndexedDb`, remotes. Unchanged.
- **Translation layer:** turns a block into a node (decode) and a node into a
  block (encode). One implementation, in dialog-search-tree, used by every
  environment whose store keeps bytes.
- **Node layer:** what the tree talks to, through four requests:

  | request | means | replaces |
  |---|---|---|
  | read a node | the node a name names, from anywhere: may hydrate | `Accessor::load_node` / `get_node`, the free `load_node` |
  | read a node locally | from this replica only, never hydrated | `Accessor::reading(Reading { local: true })` (`Load::locally`) |
  | preload nodes | make these nodes cheap to read soon; returns nothing | `Accessor::warm`, `Accessor::keeps` |
  | write nodes | the nodes a persist made; the provider stores them and keeps them readable, forgetting them if storing fails | `NodeCache::of(..).insert` in persist, `forget` in `archive::persist` |

  A store with its own node form (the single file) answers these directly
  and still answers block requests by encoding; a node it lacks arrives from
  a remote as a block, through translation, and it stores it as a node.

**Caching** is a provider concern behind the node layer: whatever answers a
read in an environment consults the environment's node cache first and fills
it; whatever answers a write keeps the nodes. The tree performs no `Holds`
lookup.

**A node's hash** is computed from its content and never supplied (for a
node read from rows, by encoding it once when first asked, then remembered).
A node read is known by the name it was requested under; a consumer that
wants to verify compares that with `hash()` and pays for it. Whether the name
and the hash live on one type or on a served node and a verified one is an
implementation decision (see step 2).

**The commit** keeps ordering its own writes: spilled values, then nodes,
then the head. Crash safety does not move.

## Steps

Each step should land green on its own: tests, clippy, and `dialog-perf`
within the gate against main. Steps 1-3 add; 4-7 move callers; 8 removes.

### 1. The node requests, as commands in dialog-search-tree

Define them as `Command`s (not `Effect`s) in dialog-search-tree, generic
over the tree's key and value types, as dialog-network defines `Hydrate`
(`dialog-network/src/hydrate.rs:45`). A command defined outside
dialog-effects and generic over its types is coherent as long as it is not
also an `Effect` (dialog-capability `command.rs:7-22`).

- `node::Read<Key, Value> { catalog, name, local }` with output
  `Result<Option<PersistentNode<Key, Value>>, DialogSearchTreeError>`.
- `node::Preload { catalog, names }`, output `()`.
- `node::Write<Key, Value> { catalog, nodes }`, output
  `Result<(), DialogSearchTreeError>`.

Names are placeholders. Keep `priority` from `Load` on `Read` if hydration
ranking still needs it (`load.rs`).

### 2. A node's name and its hash

On #733 a node read from rows takes its hash from the store
(`NodeRows::hash`); undo that. `PersistentNode` becomes an enum of a block
(exactly main's fields: `buffer`, `root`) or rows, so a block node is main's
size, and `hash()` on a rows node encodes it once into the hasher and keeps
the result. Decide between: one type carrying the requested name beside
`hash()`, or a served node and a verified node the second obtained only by
checking (`verify() -> Result<Verified<..>>`). Before choosing, inventory
`hash()` calls on loaded nodes in non-test tree code: the tree diff calls it
to compare with a parent's link (`differential.rs`, around lines 372, 589,
596, 690, 1522, 1952, 2764); each should use the requested name, or be kept
as a deliberate verification. Otherwise trusting stops being cheap for rows
nodes.

### 3. The translation layer, and how each environment answers

dialog-search-tree provides the translation as functions an environment's
impl calls, the way `load_or_hydrate` serves `Load` today
(`dialog-repository/src/repository/archive/hydrate.rs:110`):

- read: consult the node cache the environment owns; on a miss perform
  `archive::Load` (local or not, as asked), check the bytes against the
  name, `PersistentNode::try_from`, keep it in the cache;
- write: encode each node's block, `Import` them, keep the nodes in the
  cache, and forget them if the import fails (today's
  `archive/persist.rs:33-52`);
- preload: what the walker's read-ahead does today (`walker.rs:496-500`),
  landing in the environment's cache, or nothing where there is none.

**The open decision of this step: how a node request reaches a store with
its own node form.** `Space` and the generic `Router` never see `Load`: the
router routes only `Effect`s by subject (`dialog-storage/src/storage/router.rs:41`),
`Space` derives only `Get`, `Put`, `Import` onto its archive
(`dialog-storage/src/storage/provider/space.rs:103-105`), and `Peer`
answers `Load` itself (`dialog-peer/src/peer/hydrate.rs:89`). So the single
file (`FileArchive`, the `A` of `Space<A, ..>`) cannot be reached by a
command today. Two options to weigh:

- make the node requests `Effect`s under the archive's catalog, and teach
  `Space` to hand any archive effect it does not name to its archive by a
  marker trait in dialog-effects, so dialog-storage needs no dependency on
  the tree; the store answers natively or with the translation;
- or keep them commands, and give the peer a way to reach the mounted
  space's archive for a subject (a `NodeLayer` trait in dialog-search-tree,
  implemented for `Volatile`, `FileSystem`, `IndexedDb` by translation and
  for `FileArchive` natively; the orphan rule allows both, since the trait
  is local to the tree crate).

Either way, every environment that answers `Load` today answers the node
requests: `Peer` (`dialog-peer/src/peer/hydrate.rs:89`), `AccessEnv`
(`dialog-peer/src/peer/access.rs:214`), `QueryEnv`
(`dialog-repository/src/repository/branch/session.rs:1097`), `DeltaOverlay`
(`dialog-artifacts/src/archive.rs:179`, answering staged nodes by name),
`Storage` and the stores (`dialog-storage/src/storage/provider/load.rs:80-112`,
`storage.rs:89`), `Replica` (`dialog-baseline/src/replica.rs:83`), and the
test envs (search-tree `helpers/blocks.rs` `Blocks`, `Holding`,
`JournaledBlocks`, `helpers.rs` `ObservingBlocks`, `benches/sync.rs`, the
`differential.rs` and `accessor.rs` test backends; artifacts
`helpers/counting.rs`, `history/tests.rs`, `tree/distribution.rs`; query
`source.rs` `TestEnv`, `helpers.rs` `CountingStore`, `JoinEnv`; repository
`fetch.rs`, `archive/hydrate.rs` `Recording`, and the macro-forwarded ones
in `caches.rs`, `branch/integration_tests.rs`, `branch/subscription.rs`).

`Counting` (`dialog-repository/src/helpers.rs:465`) and `Metered`
(`dialog-peer/src/helpers/metered.rs:106`) recognize `ArchiveLoad` by `Any`
downcast: teach them the node requests, or the perf counters stop seeing
reads (`archive.get` counters are gated).

### 4. Reads move onto the requests

- `Accessor` keeps the env, the catalog and the reading policy; its cache
  field goes. `load_node` / `get_node` perform the read request; `reading`
  sets `local`; `warm` performs preload; `keeps` goes (a provider with
  nowhere to keep nodes ignores preload).
- `accessor.cached()` in `tree/transient.rs`:
  - in `forced_run_quiet` (around line 2536): `compressed_run_quiet`
    returns the summaries it computed, and the new-minimum check reads the
    previous piece's last key from them; no read;
  - in `compressed_run_quiet` pass 1 (around line 2822): defer every stored
    piece to pass 2's ordinary read.
  Verdicts must not change: the debug-build check against the full stream
  and the forced-run tests pin them. If this costs the write path past the
  gate, add a batch read ("these nodes") for pass 2 rather than restoring a
  memory-only read.
- `tree/transient.rs:708-1025` (`unopened_on_path`, `unopened_below`) and
  `tree/transient.rs:914` read the cache directly (`get_cached`, `cache()`):
  move them onto the read request, or onto preload where they only warm.

### 5. Writes move onto the write request

- `TransientTree::persist` (`tree/transient.rs:647`) and
  `HitchhikerTree::persist` / `persist_mut` (`hitchhiker.rs:967`, `1014`)
  stop inserting into the cache (`node/transient.rs:1762`,
  `hitchhiker.rs:983, 1001, 1032, 1043`); they seal and return the nodes,
  with their blocks still in the `Delta` for callers that ship blocks.
- `archive::persist` (`dialog-repository/src/repository/archive/persist.rs:33`)
  writes spilled values as today, then performs the write request in place
  of `Import` and `forget`.

### 6. Other code that decodes or caches nodes

Decide for each whether it is a node read (move it onto the request) or a
genuine block read that decodes on purpose (route it through the
translation's decode, so there is one decoder):

- `QueryEnv`'s `Load` answering from and filling the node cache
  (`session.rs:1104-1126`): becomes its node read, answered by the cache.
- `differential.rs:125-152` `MissingBlocks::Boundary`: `Get`, check,
  decode, skipping cache and hydration on purpose.
- `push.rs:1253`: decodes a remote's bytes with no digest check, uncached
  on purpose (a block streamed through); add the check.
- `dialog-repository/src/helpers.rs` `reachable_blocks`;
  `dialog-query/src/resolver.rs:891` with `inspect::inspect_node`
  (`dialog-artifacts/src/inspect.rs:194`); `read_amplification.rs:1164,
  1346, 1420`; `dialog-baseline/src/nodes.rs:37-42`;
  `dialog-archive-file/src/node.rs:36`; `dialog-migration/src/archive_file.rs:41`.

### 7. Bounds

The canonical tree bound `Env: Provider<Load> + Holds + ConditionalSync`
(64 uses in dialog-search-tree, 14 in dialog-artifacts, 3 in
dialog-repository) becomes the node requests' providers. Trait aliases that
bundle it (`TreeEnv` in `dialog-repository/src/repository/archive.rs:45`,
`Capabilities` in `branch/session.rs:405`, `CountedLoads` in
`dialog-repository/src/helpers.rs:307`) change in one place each. `Holds`
leaves every tree bound; it stays where something else is held (routes,
other caches).

### 8. Remove

`NodeCache::of` from the tree crate's public use, `Accessor`'s cache,
`NodeSource` and `serve` (if #733 landed first), `Reading`'s use outside the
read request, and any `Holds` bound left on tree code.

## How to check it

- The tests of every crate touched, and the forced-run tests in particular.
- `dialog-perf` against main, every scenario within the gate's 3% on
  instructions, every counter (blocks read and written, engine steps)
  unchanged. Run it on main and on the branch with one toolchain
  (`perf sweep`, then `perf compare`); the recorded baseline was taken with
  another.
- The replication gate (`dialog-soak`) unchanged.
- The single file (#733), if it has landed: nodes served in place with no
  per-read lookup; `dialog-archive-file`'s repository tests.

## Open, to settle while building

- The `hash()` inventory (step 2).
- The cost of the ordinary read in the forced-run check (step 4).
- How a store with its own node form is reached (step 3).
- A mapped file's page cache is a cache the environment cannot own or
  release; it is out of scope here and noted in the
  [claims](claims.md).
