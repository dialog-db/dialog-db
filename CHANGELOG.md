# Changelog

What changed, what it may break, how to move across, and why we did it.
Entries are grouped by the change a reader has to understand, not by
commit; each links the pull request that landed it and the note that
argues it in full. Newest first.

Versions follow [semver](https://semver.org) over two surfaces: the Rust
API, and what a replica stores and syncs. While dialog is 0.x, a change
that breaks either one bumps the minor version, and anything else bumps
the patch. The whole workspace shares one version. `0.1.0` is the commit
tagged `tonk-2026-10-03`, the last of the date tags. [RELEASING.md](./RELEASING.md)
says how to cut a release.

## Unreleased

### The corpus crate is `dialog-corpus`, and it asks its environment for everything it reads

[#768](https://github.com/dialog-db/dialog-db/pull/768), decided in
[design/tree-node-reads](./design/tree-node-reads.md); the spilled-value
read is argued in
[design-spilled-values.md](./design/tree-node-reads/design-spilled-values.md).

The crate that held the data model and its indexes, `dialog-artifacts`, is
`dialog-corpus`: the corpus is the body of claims a branch holds, in the
search tree. Its storage side is named after it, and the crate
no longer depends on a store crate. What it reads beside its nodes, a
value spilled out of its key, it asks its environment for through
`blob::Load`, performed in one place and checked against the reference;
nothing in the crate reads a store by type. The word "artifact" is
retired: what the model called an artifact is a fact, a claim with its
cause.

What this changes for you:

- The crate is `dialog-corpus`; paths that began `dialog_artifacts::`
  begin `dialog_corpus::`. `tree::ArtifactTree` is `tree::Corpus`,
  `ArtifactTreeExt` is `CorpusExt`, `ArtifactNodeCache` is `CorpusCache`,
  `BufferedArtifactTree` is `BufferedCorpus`, `ArtifactWriter` is
  `CorpusWriter` and `DialogArtifactsError` is `DialogCorpusError`.
- The model side is renamed the same way. `Artifact` is `Fact`,
  `ArtifactView` is `FactView`, `ArtifactViewStream` is `FactViewStream`,
  `ArtifactSelector` is `Selector`, `artifact_index_keys` and
  `artifact_index_keys_with` are `fact_index_keys` and
  `fact_index_keys_with`, and every `from_artifact`, `to_artifact` and
  `for_artifact` is `from_fact`, `to_fact` and `for_fact`. The module that
  held them, `artifacts`, is `model`. In `dialog-query` the re-export
  module `artifact` is `corpus`, and its aliases `ArtifactsRelation` and
  `ArtifactTypeError` are `CorpusRelation` and `CorpusTypeError`. `Fact`
  prints as `Fact` in its `Debug` and `Display` forms.
- The nightly fuzz soak is `soak:facts`, run by `--test fact_fuzz`, and
  reads `DIALOG_FACT_FUZZ_SEEDS` and `DIALOG_FACT_FUZZ_OPS` where it read
  `DIALOG_ARTIFACT_FUZZ_*`.
- `dialog-corpus` does not depend on `dialog-storage`. `Blake3Hash` is
  still `[u8; 32]` and is defined in the crate; the `HashType` re-export
  is gone (it is `dialog_storage::HashType`). `IndexRoot`, a CBOR wrapper
  nothing used, is removed. `From<DialogStorageError> for
  DialogCorpusError` is gone: a storage error reaches the corpus as an
  `ArchiveError`, a `BlobError` or the tree's error, each of which still
  converts. `DialogSearchTreeError` implements `AuthorizationFailure` and
  has `rejection()`, so a decision made beneath the tree is read without
  naming the store's error type.
- `ArchiveDelta::block` and `ArchiveDelta::flush_blocks` are gone, and
  `DeltaOverlay` no longer answers `archive::Load`, `archive::Get` or
  `Holds`: it answers the node layer's `Read`, `Preload` and `Write` for
  `CorpusNode`, and `blob::Load` for a value the batch staged. A caller
  that shipped a batch's nodes as blocks takes them with `flush_nodes()`
  and writes them with `nodes::Write`, as a commit does.
- `Provider<blob::Load>` is no longer asked of an environment by an
  operation that never reaches a spilled value: `CorpusExt::select_data`,
  `format_manifest`, `record` and `estimate`; `BlobIndexExt::get_blob`,
  `has_blob` and `list_blobs`, and `blob_changes`; a buffered batch's
  `record` and `seal`; `rekey_tree`; `history::context_in` and
  `region_entry`; `spilled_refs`; `merge::screen_history`. It stays on
  `apply`, `select_record`, `scan`, `write_instructions`, the buffered
  applies, `observe_revisions`, `TreeHistory` and, through
  `select_record`, `content_size` and `asset_size`. A caller that carried
  the bound only to satisfy one of the former can drop it.

Why: a crate named for artifacts no longer said what it held, and a crate
that embodies the model should not depend on a store. #755 made the tree
ask its environment for nodes; this makes the layer above it ask for
everything else the same way, so what a store is and where it is stay the
environment's business.

### A tree reads a node from its block or from rows a store keeps it in

[#733](https://github.com/dialog-db/dialog-db/pull/733),
[design](./research/single-file-format/design.md)

A node's body is now read through views, `IndexView` and `SegmentView`, that
either a block or a store's rows back. A store that keeps nodes as rows
implements `NodeRows`. An environment that holds a `NodeSource` for a
subject (`NodeSource::serve`) makes every tree read of that subject ask it
first. The node is then read in place: keys compare part by part, values
are read as archives where the store keeps them, and no block is built,
hashed or validated. `FileArchive` is such a source.

What this changes for you:

- `NodeBody::Index` and `NodeBody::Segment` hold `IndexView` and
  `SegmentView` instead of `&ArchivedIndex` and `&ArchivedSegment`;
  `PersistentNode::as_index` and `as_segment` return the views. The views
  have the archived types' methods. `index.novelty` is `index.buffers()`,
  a buffer's `child` is `child()`, its `count` is `count()`, and its
  `polarity` is `is_assert(at)`.
- `PersistentNode::buffer` returns `Result<&Buffer, _>`: a node read from
  rows builds its block the first time it is asked, and that can fail.
- A segment's `keys()` returns a `KeyCursor`, which has `next_key` as
  `StreamingLeaf` does.
- Nothing changes for a tree read through an environment with no node
  source.

Why: a store that keeps nodes compactly need not rebuild a block for every
read. Read in place, the single-file archive's cold reads match the `fs`
archive's, where rebuilding made them about 2× slower, and its cached nodes
take about a third of the memory of cached blocks.

### A transaction that loses its branch's race runs again over the head that won

[#733](https://github.com/dialog-db/dialog-db/pull/733),
[design](./design/file-writers.md)

`Branch::transact(|transaction| async { ... })` takes the code that builds
a transaction instead of the transaction. Each try hands it a fresh
transaction on the branch's head as it is then, commits what it returns,
and publishes that compare-and-swapped against the head it read. When
another writer moved the head first, the head that won is read and the
code runs again: its queries read over the winner, so a write computed
from a read is computed again, as in a software transactional memory. It
tries as many times as a commit does (64) before returning
`VersionMismatch`.

What this changes for you:

- New: `Branch::transact(build).perform(&env)`, with `allow_empty()` and
  `canonicalize()`; `build` returns `Result<Transaction, E>` where
  `E: From<CommitError>`, and an error from it ends the transaction with
  nothing written.
- Deprecated: `branch.transaction()...commit().publish().perform(&env)`.
  It keeps failing with `CommitError::Publish(PublishError::VersionMismatch)`
  on a lost race, as it did. Move to `Branch::transact`, which builds the
  same transaction inside the closure.
- `Branch::induce` runs its rules again over the head that won.
- A `TransactionBatch` its caller staged (`.commit().perform(..)`, then
  `.publish()`) is still refused on a lost race: its caller read what it
  staged on. The repository's own commits (delegations, migrations) apply
  their instructions again over the winner, as before.
- `dialog_corpus::Instruction` is `Clone`.

Why: processes writing one branch concurrently should not each catch and
retry every commit; and retrying the instructions a transaction computed
reapplies writes derived from reads of a head that lost, where running the
transaction again computes them over the head that won.

### A space can keep its memory cells in its file, branch heads included

[#733](https://github.com/dialog-db/dialog-db/pull/733),
[design](./design/file-writers.md)

`FileArchive` provides the memory effects (`Resolve`, `Publish`,
`Retract`, `List`), so a `Space` can name it as its memory as well as its
archive: `Space<FileArchive, FileArchive, ...>` keeps a space's blocks and
its cells, branch heads among them, in one file. A cell is a record
written over in place, in two halves taken in turn; its capacity is its
first value's size and a fifth, and a value past it spills to a block. An
update takes a lock on that cell alone, never the writer's turn, so
writers of two branches never meet; creating a cell takes the turn once.

What this changes for you:

- A `Space` whose memory is a `FileArchive` keeps no `memory` directory:
  its cells are in `archive.dialog`.
- A cell's edition is the BLAKE3 hash of its value, as under `fs`, so a
  `Publish` names the same version either way, and publishing the value a
  cell already holds succeeds whatever edition the writer expected, as on
  every memory store.
- A `Resolve` reads a value the cell held at one moment: never torn, and
  never older than one a read before it returned, while other processes
  write it.
- `List` walks the cells' own index, not the blocks'.

Why: a space that is one file is one thing to copy, back up and keep
consistent, and a head kept apart from the blocks it names was a second
thing to keep in step with them.

### Writers to one space's file do not wait on one another

[#733](https://github.com/dialog-db/dialog-db/pull/733),
[design](./design/file-writers.md)

A writer to a `FileArchive` tries the file's turn without waiting. With
it, it appends as before. Without it, it writes its blocks whole into an
extent of its own, space it claimed in an earlier turn, and they read back
at once, in its process and in others. The next writer to get the turn,
usually the same one after its commit, turns those blocks into frames and
tokens, and the extent is freed once a sync point makes that durable. A
process that holds the turn keeps an extent ready for itself; a process
that opens the file with the turn free leaves one no process owns, and
takes one back as it closes, so there is one for each process that has the
file open. A process with none takes one of those, or one whose writer
died, so a process that has never held the turn writes without waiting for
it too. A writer waits for the turn only to claim an extent when there is
none to take, or for a catalog the file has not numbered.

What this changes for you:

- Processes writing one space (separate branches, or the same one) no
  longer queue behind each other's appends, as they did not on the `fs`
  store either.
- The writer's turn is a lock on one byte of the file rather than on the
  whole file, on every platform.
- `Options::extent` sets how many bytes of blocks an extent holds (1 MiB
  by default). Each process that writes keeps one extent claimed while it
  has the file open, and each process that has it open leaves one for
  another; the last process to close the file retires them and cuts free
  extents that end the file off it. One left before blocks were appended
  stays free mid-file and is the next one claimed.
- Claiming an extent writes its header and grows the file, without
  writing the extent's bytes: an unused extent is a hole on file systems
  that keep sparse files.
- `Options::leave_extent` (on by default) is whether opening leaves an
  extent for another process; a process that only reads can turn it off
  and leave the file's length alone.
- Opening the file no longer waits for another process's turn: the slot
  it opens from was written in this boot, so there is nothing to repair.
  After a reboot, or for a new file, opening still takes the turn.
- A file reached by two names in one process (a hard link, a symbolic
  link) is one handle, as a path reached two ways already was: two would
  both hold the writer's turn.
- The file's format is version 12: a file written by an earlier build of
  `dialog-archive-file` does not open.

Why: many processes work on separate branches of one space at once, and on
main they never wait on one another to write. A file whose every append
took one turn made them wait, which would rule the file out wherever such
processes run.

### A space can keep its blocks in one file

[#733](https://github.com/dialog-db/dialog-db/pull/733),
[design](./research/single-file-format/design.md)

The new `dialog-archive-file` crate adds `FileArchive`, an archive
provider that keeps a space's blocks in one append-only file instead of a
file per block. It answers `archive::Get`, `Put` and `Import` like the
`fs` provider, and opens from a space's `Location` (`Resource<Location>`),
so a `Space` can take it as its archive and keep memory, credentials and
blobs where they were. A node of the artifact tree is kept as a frame:
rows of ids into the file's tokens (each distinct entity, attribute, value
and datum once, and each revision's version once, so a history record
shares its fact's tokens), written against the node it supersedes. A frame is only
written once it realizes to the exact block; anything else is kept whole.
A read realizes the block. Native only.

What this changes for you:

- Nothing, unless you choose it: `Space<FileArchive, FileSystem,
  FileSystem, FileSystem, FileSystem>` is a space whose blocks live in
  `archive.dialog` in the space's directory. The tree, queries and sync
  work through it unchanged.
- Any number of processes can open one file. Writers take turns on an
  exclusive lock on it and read what the others appended first; readers
  take no lock. A file this process already has open is shared, not
  opened twice.
- The file syncs itself every 64 appends or 2 seconds, and on
  `FileArchive::sync`. A power loss loses at most the appends since, and
  never leaves the file damaged: opening goes back to the newest state
  that is whole.
- Opening reads small slots at the head of the file and nothing else.
  The file's index is in the file, so a reader holds a few tens of KB,
  not an index that grows with the file; its pages are written over once
  nothing reads them. The format is still changing, and a file written
  now may not open in the release.
- `Options::frames = false` keeps every block whole.
- The new `dialog-migration` crate holds migrations an application runs
  before it opens a replica, each behind a feature of its own, and the
  `dialog-migrate` binary that runs them. A migration is a `Step` that finds
  its work by looking at the replica, so it is safe to run on every start:
  `Migrations::plan` says what the steps would do, `Migrations::run` does
  it, and an interrupted run picks up where it stopped. The `archive-file`
  step (`archive_file::ArchiveFile`) moves every space of a replica the
  `fs` provider wrote into the `archive.dialog` a file-archive space reads,
  writing it under another name and renaming it once complete; with
  `remove_source` it deletes the `fs` archive after. It is a conversion an
  application opts into, not an upgrade every replica needs. Every block
  reads back as the bytes it was; nodes in an older layout are kept whole.
  `dialog-migrate [--dry-run] [--step <id>] [--remove-source] <replica>`
  runs the steps; `--source`, `--target` and `--catalog` move one store.

Why: a replica's history costs a file per block per version, which is
most of what a replica weighs. Replaying 4096 Stack Exchange commits, the
`fs` archive takes about 1.2 GiB and the file 13 MiB.
### A tree asks its environment for nodes, and the environment keeps them

[design/tree-node-reads.md](./design/tree-node-reads.md), planned in
[#753](https://github.com/dialog-db/dialog-db/pull/753).

A search tree no longer reads blocks and decodes them, nor finds the node
cache for itself. It performs three commands on the environment it is
given, defined in `dialog_search_tree::nodes`: `Read` (the node a name
names, from anywhere or from this replica alone), `Preload` (make a node
cheap to read soon) and `Write` (the nodes a persist sealed, to store and
keep readable). The environment answers however it keeps nodes. A store
that keeps blocks (`Volatile`, `FileSystem`, `IndexedDb`) answers through
the translation layer in the tree crate, which loads, checks and decodes;
`Storage` routes a request to the space the catalog's subject is mounted
as, and `Space` hands it to its archive; a peer answers from the node cache
it holds, then its storage, then a hydration from the subject's routes, and
keeps what it read or wrote. A store that keeps nodes in a form of its own
implements the three commands and is reached the same way.

What this changes for you:

- A request is written at the node it is about: `nodes::Read<N>`,
  `nodes::Preload<N>` and `nodes::Write<N>` for an `N: nodes::Node`, a
  sealed trait `PersistentNode<Key, Value>` implements. A tree operation's
  environment bound is spelled out, `Env:
  Provider<nodes::Read<PersistentNode<Key, Value>>> +
  Provider<nodes::Preload<PersistentNode<Key, Value>>> + ConditionalSync`,
  with no alias trait in between, instead of `Provider<archive::Load> + Holds + ConditionalSync` (and `+
  Provider<archive::Get>` where a difference was computed). `Holds`, `Load`
  and `Get` are no longer asked of a tree's environment.
- `dialog_artifacts::tree::CorpusNode` names the node of the corpus, the
  tree over claims (`PersistentNode<Key, State<Datum>>`), and an
  environment a corpus is read through is bounded as
  `Provider<nodes::Read<CorpusNode>> + Provider<nodes::Preload<CorpusNode>>`,
  one that is written through as `Provider<nodes::Write<CorpusNode>>`.
  There is no alias trait for either: a signature says which requests it
  performs. "Corpus" is the name the crate's storage side will carry when
  `dialog-artifacts` is renamed; everything new here uses it.
- `TransientTree::persist(delta)`, `HitchhikerTree::persist(delta)`,
  `HitchhikerTree::persist_mut(delta)` and `Node::into_link(delta,
  manifest)` take no environment and no cache, and the delta they fill is
  a `Delta<Blake3Hash, PersistentNode<Key, Value>>`: the sealed nodes, not
  their blocks. A persist keeps nothing in any cache; the environment keeps
  the nodes when the commit performs `Write`. `ArchiveDelta::nodes()`
  replaces `blocks()`, `node(hash)` and `flush_nodes()` are new, and
  `block(hash)` and `flush_blocks()` still hand out blocks to a caller that
  ships them. A batch that reads a node it sealed before the flush gets
  the staged node back from its `DeltaOverlay`.
- `Accessor::new(env, catalog)` asks nothing of the environment;
  `Accessor::cache`, `cached` and `keeps` are gone, and `load_node` needs
  `Provider<nodes::Read<PersistentNode<Key, Value>>>` and
  `Provider<nodes::Preload<PersistentNode<Key, Value>>>` of its environment.
- An environment that answers `Load` and holds handles answers the node
  requests through `dialog_search_tree::reads_nodes_through_blocks!` and
  `writes_nodes_through_blocks!`; one with a store beneath it and a
  hydration uses `dialog_repository::read_node_or_hydrate`,
  `preload_node_or_hydrate` and `write_nodes`. `Peer`, `AccessEnv`, the
  query session, `DeltaOverlay`, `Counting`, `Metered`, `Storage`, `Space`
  and the three stores answer them; `PeerSpace` requires them of a space.
- A difference's boundary side reads locally through the node layer
  instead of performing `Get`: the environment's cache is consulted, which
  changes no answer, since a scope's cache holds only what its archive has
  produced or a successful write stored.
- The query session no longer decodes and caches the blocks it loads; a
  node read through it reaches the peer's cache. An integration opens its
  read-ahead window once per batch of pending changes instead of when a
  path misses the cache, since nothing outside the environment can tell.
- A node the environment holds is answered at once, with nothing
  allocated: the translation layer, `read_node_or_hydrate`, the overlay
  and the query session return `Resume::now` on a hit (see the entry
  below). Under `dialog-perf` the point-read scenarios are within 0.4% of
  before and the commit scenarios 0.6% to 1.9% cheaper; the design topic's
  journal has the table.

Why: the tree was written before the capability model and only partly
moved onto it, so what it needed from the world was not expressed as
capabilities. It performed `Load` for bytes and then did a provider's work
itself: found a cache through a lookup that could come back empty,
checked, decoded and kept nodes, while other code kept them again its own
way. A store that keeps nodes in a form of its own (the single-file
archive of #733) could only reach the tree through a side channel. With
nodes as the tree's requests, each concern sits with whoever owns it, and
a store's byte representation is not the tree's concern.

### A provider answers at once or later, and a caller cannot tell

[#755](https://github.com/dialog-db/dialog-db/pull/755), argued in the
journal of [design/tree-node-reads.md](./design/tree-node-reads.md).

`Provider::execute` is no longer an `async fn` under `async_trait`. It
returns a `Resume<'_, Output>`: either the output at once
(`Resume::now`), or a future that produces it (`Resume::later`). A caller
awaits either one the same way, so the change is invisible where a
command is performed; what it changes is that a provider holding the
answer (a cache that has the value, a check that fails before any I/O)
hands it back without allocating a future, and a provider standing in
front of another hands back what it was handed.

What this changes for you:

- A `Provider<C>` impl is written one of two ways. Under
  `#[dialog_capability::provider]`, `async fn execute(&self, input) ->
  Output` as before: the attribute replaces the `async_trait` pair, and
  the body becomes the future. Without it, `fn execute(&self, input) ->
  Resume<'_, Output>`, returning `Resume::now` on the paths that have the
  answer and `Resume::later(async move { .. })` on the rest. A forwarding
  impl returns what the inner provider returned, boxing nothing.
- `#[derive(Provider)]` forwards without boxing, and no longer asks
  `ConditionalSend` of the command's input or `ConditionalSync` of the
  struct.
- `Resume` implements `Future` and `Unpin`, and offers `is_now`,
  `into_now` (the answer, or the future that waits for it) and `map`.
  `dialog_capability::Pending<'a, T>` names the boxed future.
- A generic provider whose future awaits another `execute` and holds its
  output needs `C::Output: ConditionalSend`, since a `Resume` is `Send`
  only when its output is; `async_trait` hid that bound inside the box.
- `nodes::Ready` and `nodes::held_node`'s role as its answer are gone from
  `dialog_search_tree`, and so is the `Nodes<Key, Value>` alias trait: a tree
  operation bounds on `Provider<Read<PersistentNode<Key, Value>>> +
  Provider<Preload<PersistentNode<Key, Value>>> + ConditionalSync` itself. `Read::through_blocks`,
  `through_loader` and `through_held_blocks`, `Preload::through_blocks`
  and `through_held_blocks`, `Write::through_blocks` and
  `through_held_blocks`, every request's `perform`, and
  `dialog_repository::read_node_or_hydrate`, `preload_node_or_hydrate`
  and `write_nodes` return a `Resume` instead of being `async fn`;
  `through_loader` takes an `FnOnce(Load) -> impl Future` instead of an
  `AsyncFnOnce`.
- `dialog_common::provider` (the test provisioning attribute) is
  unchanged; in `dialog-macros` it is now `provision`.

Why: the cost of crossing a provider boundary was a boxed future per
call, paid even when the provider had the answer in hand. The first fix
for the tree's node reads was a side door beside the capability model,
which was a symptom of the model: a provider could only ever answer
later. Answering now or later in the one trait keeps every answer inside
the model, with sync and async providers written against the same
signature, and takes the box off every hit in the workspace, not just the
tree's.

### Two claims that stand equal are told apart by the hash of their values

[#752](https://github.com/dialog-db/dialog-db/pull/752)

A cell read under `last` elects its newest claim: the greater standing
(the version, then the cause, then the succession). Two claims stand
equal when one revision wrote both under one cause, which is what two
writes to one cell in one transaction are. Between those, the election
fell to the values and the greater won. It now falls to the hash of each
value's identity key (`Value::tiebreak`), the greater hash winning. Every
place that elects does the same: the tree, a commit's settlement, a
concept read, and `top` among values of one rank.

What this changes for you:

- Where one transaction writes two values to a cell under `last`, or
  under `top` at one rank, the value a read returns may change: it is the
  one whose hash is greater, not the greater value. Claims at different
  standings, which is every concurrent edit between revisions, elect as
  before.
- `max` and `min` elect as before: by value, then between equal values
  by standing. Equal values hash alike, so the hash never decides there.
- Replicas on releases either side of this one can show different values
  for such a cell. Nothing a replica stores is rewritten, and the claims
  are the same on both; only which one a read returns differs.
- `Value::tiebreak` is new.

Why: a tie broken by value favours some values over others, so a writer
can choose the value that wins (the greater number, `true` over `false`).
The hash favours none, as the version's hash already does between
concurrent revisions. It came up while designing record values, whose
values all share one version, so every tie among them reaches this rule
([#748](https://github.com/dialog-db/dialog-db/pull/748)).

### A file store's write lock is never taken by two writers at once

[#741](https://github.com/dialog-db/dialog-db/pull/741), on [#723](https://github.com/dialog-db/dialog-db/pull/723)

A native file store guards each cell's compare-and-swap with a lock file.
It was a PID lock taken through `pidlock`, whose acquire creates the file
and writes the PID after; a second writer arriving in between read no PID,
removed the file as corrupt, and took the lock too. Both then compared
against the same version and both wrote, so the later write replaced the
earlier one and each reported success: a commit racing a pull of the same
branch could vanish without an error. Two writers that both found a dead
holder's lock could also both remove it and both take it.

The lock is still a file, `{cell}.lock`, now holding `{pid} {token}`: the
holder's process and a random token naming that one hold. A lock file is
held unless proven otherwise, so one that is empty or does not read as
`pid token` is held and never deleted; the one exception is a bare PID,
the previous release's lock file, read as that PID with token `0`. A writer writes its content to a
temporary file first and hard-links it into place, which fails if a lock
file is there, so a lock file never exists without its content; on a
filesystem without hard links it creates the file and writes it after,
and a writer arriving in between is refused. A lock naming a process
that is not running is recovered by exactly one writer, which first
links it to `{cell}.lock.stale.{token}` (a name only one writer can
create) and checks the file still names the gone holder before removing
it.

What this changes for you:

- Concurrent writers of one store, in one process or several, no longer
  lose a write to each other. A writer that finds the lock held is
  refused with `FileSystemError::Lock` (`Concurrent write in progress`),
  as before.
- A crashed writer's lock is recovered by the next writer, once the
  crashed process is gone. Whether it is gone is asked of the OS in the
  writer's own PID namespace, so writers sharing one store must share a
  PID namespace. If the crashed writer's PID has been given to another
  process, its lock looks held until the lock file is deleted by hand;
  so does a crashed writer's lock on a filesystem without hard links.
- A lock file that is empty or does not read as `pid token` (or as a
  bare PID) is held, whoever wrote it, and has to be deleted by hand if
  no writer is running.
- Lock files are removed on release, by the writer that holds them
  only: a writer whose lock file now names another holder leaves it.
- Cells whose names differ only in extension (`a.x`, `a.y`) get a lock
  each, instead of sharing `a.lock`.
- `pidlock` is no longer a dependency.

Why: a write lock that two writers can hold at once does not guard the
compare-and-swap, and the writes it loses are reported as successes, so
nothing downstream can notice or repair them. The lock stays a file
rather than an OS lock (`flock`), which errors or is not enforced on
network and FUSE filesystems that stores live on.
### An environment bounds every cache it holds, and spilled values share one budget

[#742](https://github.com/dialog-db/dialog-db/pull/742)

`HeldCaches::builder()` returns a `CacheBudget` that sets the size of
every cache an environment holds for its repositories: tree nodes and
spilled values in bytes, the rest in entries. Spilled values are now kept
in one cache for the whole environment, under one byte budget, instead of
one 8 MiB cache per repository. A snapshot taken through the environment
and the caches a transaction batch keeps to itself take their sizes from
the same budget. Nothing configured, every cache keeps the size it had.

What this changes for you:

- `HeldCaches::builder().nodes(bytes).spills(bytes).entries(n).rules(n)
  .plans(n).formulas(n).build()` builds caches that keep to each cap.
  `entries` bounds each repository's causal verdicts, contexts, revision
  records and rewritten trees; `rules` bounds the shared rule content
  (heads, bodies, selecting rules) and `plans` the query plans. `HeldCaches::new()` and
  `with_budget(bytes)` are shorthands for the defaults
  (`CacheBudget::default()`), and `HeldCaches::limits()` reads the budget
  back.
- Memory for spilled values no longer grows with the number of open
  repositories: the default 8 MiB (`SPILL_CACHE_BUDGET`) is now the total.
  `HeldCaches::release(repository)` no longer drops spilled values, since
  one cache holds them for every repository by content; they age out under
  the budget, and `clear()` drops them.
- `HeldCaches::bytes()` now counts spilled values as well as tree nodes.
  `spill_bytes()` reads the spilled share, and `entries()` counts what the
  entry-bounded caches hold (a count, not bytes).
- `Repository::snapshot_in(env, revision)` takes a cold snapshot sized by
  the caches `env` holds, reading spilled values, rule content and plans
  through them. `Repository::snapshot(revision)` keeps the default sizes.
- A cache bounded by entries now holds at most the capacity it was given
  on native targets, however small. Below 2048 entries a sharded cache
  used to keep at least one entry per shard, up to 16, so a capacity under
  16 held more than asked. The default sizes keep all 16 shards.
- New accessors: `CausalityCache::with_capacity`, `ContextCache::with_capacity`,
  `PlanCache::with_capacity` (with `PLAN_CACHE_CAPACITY`), `len` on these and
  on `dialog_search_tree::Cache` (with `capacity` and the now public
  `CACHE_CAPACITY`), and `budget`, `bytes`, `len` and `clear` on `SpillCache`.

Why: an embedder running in a fixed memory limit, such as a Cloudflare
Durable Object with 128 MB for everything, could bound only the tree
nodes. The spill budget multiplied with each repository opened, and every
other cache was fixed when it was made (#706).

### A repository an earlier release left opens, whatever layout its registry is in

Opening or loading a repository that predates layout versioning carries
its remote and upstream cells over into facts in its registry branch
(`meta`). A registry an earlier release wrote is laid out from before the
number band, and a commit into such a tree is refused with
`CommitError::NeedsMigration`. Nothing moved the registry first, since
nothing outside dialog-repository writes it, so the open failed with
`OpenRepositoryError::Upgrade` / `LoadRepositoryError::Upgrade` on every
attempt, and the repository could not be opened at all. Every write to a
registry or a host's contacts now moves the branch to this release's
layout first, when it needs it, and applies the changes on the rewrite.

What this changes for you:

- A repository last written by a release before 0.3.0 opens instead of
  failing with "The branch must be migrated before it is written".
- Upstream changes (`set_upstream`) and contact writes on such a branch
  succeed the same way, where they failed with `NeedsMigration`.
- An embedder no longer needs to migrate a registry itself; it could not,
  since the open that fails is the one that would hand it the branch.

Why: the open-time upgrade is the only thing that writes a registry an
earlier release left, and it ran before any embedder could migrate it.

### dialog-storage reads a browser file without `wasm-streams`

[#744](https://github.com/dialog-db/dialog-db/pull/744)

The web file system provider read a `File` through the `wasm-streams`
crate's `ReadableStream` adapter. That crate exports its adapters through
`#[wasm_bindgen]` under names that carry no version, so two versions of it
in one wasm module are duplicate symbols at link time. The workspace had
pinned it to 0.5 to match the copy `reqwest` brings in through `iroh`
(#521), while an embedder linking the Cloudflare `worker` crate needs 0.6,
and no version satisfied both. The provider now reads the stream through
web-sys's default reader and the crate is gone from dialog-storage.

What this changes for you:

- dialog-storage no longer depends on `wasm-streams`, so an embedder may
  link whichever version its other dependencies need.
- Nothing stored or synced changes, and the reader's behaviour is the
  same: chunks arrive as they are read, and dropping the reader before the
  end cancels the stream.

Why: a dependency that can only ever be one version across a whole wasm
module should not be pinned by a library that uses one call of it.

### A pull or a push that meets another revision under its own head's version is refused

Fixing [#705](https://github.com/dialog-db/dialog-db/issues/705)

Two replicas writing with one key on one branch share an origin, since
an origin derives from the branch and the issuer. When both build on the
same head, each mints the next edition of it over its own tree: two
revisions under one version. Every context that holds one claims the
other, so the second writer's pull took the first's head as already seen
and merged nothing, and its push then replaced that head. Both writers
were told their write synced, one write was gone, and the two replicas
kept different trees under one version for good. A pull or a push now
compares the upstream's head with the branch's own head first, and when
they are the same version over different trees, refuses with an error
that says so.

What this changes for you:

- `PullError::WriterRacedItself { branch, ours, theirs }` and
  `PushError::WriterRacedItself { branch, ours, theirs }` are new. A pull
  or a push that meets the rival head returns one and writes nothing: not
  the branch's head, not its sync base, not the upstream. A sync loop
  that pulls on `PushError::NonFastForward` and pushes again now stops
  with this error instead of reporting the write synced.
- The refused write is not lost: written again on a replica that has
  not written (one that pulls the upstream fresh), it lands on top of the
  rival. Writers that need to write at once each need a key of their
  own, which is what dialog's model has always assumed.
- A head that wrote first and a sync with no rival behave as before. The
  check compares two heads already in hand, so it reads nothing.
- Still not detected: a replica that minted two or more revisions on the
  shared head before it syncs holds a head above the rival's edition,
  and nothing on either head tells the rival from its own earlier
  revision. One write per sync per key, as the stateless workers of #705
  do, is caught.

Why: concurrent writers sharing one identity are outside the model, but
breaking that rule cost a write without an error. A refusal the writer
can see is the least the replica owes it; keeping both writes by a merge
cannot be done soundly, since each head's context already claims the
other's write and the merge would screen it out as removed.

### `ConditionalSync` means `Sync`, not `Send + Sync`

[#734](https://github.com/dialog-db/dialog-db/pull/734)

`dialog_common::ConditionalSync` was `Send + Sync` on native targets, so
a `ConditionalSync` bound quietly asked for `Send` as well, and a bound
that needed only `Sync` could not say so. It is now `Sync`, matching its
name and `dialog_ucan_core::ConditionalSync`; `ConditionalSend` is the
bound for `Send`. On wasm both remain no bound at all.

What this changes for you:

- A generic bounded only by `ConditionalSync` is no longer known to be
  `Send` on native. Where your code moves it into a `Send` future, sends
  an `Arc` of it to a task, or stores it in a type that must be `Send`,
  write `ConditionalSend + ConditionalSync`.
- These traits now name both bounds as supertraits, so what implements
  them and what a bound on them implies is unchanged:
  `dialog_artifacts::{ValueType, History}`, `dialog_did_web::Fetch`,
  `dialog_remote_ucan::Presented`, `dialog_peer_iroh::{Channel, Connect}`
  and the `dialog_peer` helpers' `Meter`.
- A store a tree reads through by value asks for `ConditionalSend` as
  well, since it is moved into the read's future: the `S` of the
  `dialog_artifacts::ArtifactTreeExt` and `BlobIndexExt` methods,
  `TreeHistory`, `blob_changes` and `spilled_refs`, and of the query
  builder's `execute`, `execute_owned` and `estimate` in
  `dialog_repository`. An env passed by reference (`perform(&env)`)
  still asks only for `ConditionalSync`.
- `dialog_query::merge_join` and `multi_merge_join` ask for
  `ConditionalSend + ConditionalSync` on their key function and key, and
  the `Resolver` and `Revocations` of `dialog_peer_iroh::accept` ask for
  both, as they always needed.

Why: a bound named `Sync` that also demands `Send` hides what code
actually needs, and makes a type that is `Sync` but not `Send` impossible
to accept through it.

### A pull that loses its publish forgets the head it minted

[#704](https://github.com/dialog-db/dialog-db/pull/704)

A pull remembered its merged head's causal context at prepare time,
before publishing it. A merge prepared on a stale view of the head mints
the version of the commit already landed on it (a version is the origin
and its edition, so two revisions minted on one parent by one origin
share it), so when its publish lost, its context stayed in the memo under
that commit's version: the commit read as having seen the upstream, and
the next merge screened the upstream's claims out as removed. The context
now enters the memo only once the head is published, as a commit's does.

What this changes for you:

- A branch whose pull loses the race to a commit through another handle
  no longer drops the upstream's claims on its next merge. Nothing stored
  changes: the memo is in memory, and a pull that lands is remembered as
  before.
- `PreparedPull::commit` still fails such a race with
  `PullError::Publish(PublishError::VersionMismatch)`; the caller refreshes
  and pulls again, as before.

Why: the memo speaks for heads that landed, and a context remembered
before the publish speaks for one that may never land.

### Distinct members inserted between the same neighbours get distinct positions

A position's fractional tail comes from a bias derived from the member's
identity. The bias was the first six base62 digits of the identity bytes,
and base62 is a big-endian re-encoding, so those digits depend only on the
identity's leading bytes and its length. Every `did:key:z6Mk…` entity
shares both, so every one of them got the same bias (`13lS7E`, or
`FvVVmY` one character shorter). The bias is now taken from the identity's
BLAKE3 digest.

What this changes for you:

- `Bias::derive` returns a different bias for the same identity, so
  `dialog_artifacts::position::insert` and the `dialog/position` formula
  derive different positions than before for the same member and
  neighbours. Derivation is still deterministic: every replica on this
  release derives the same position for the same insert.
- Two different members inserted into the same slot used to derive the
  same position, so a relation that keeps one member per position kept
  only one of them. They now land apart.

Why: the bias exists so distinct members disperse while the same member
converges. Read off the identity's head it converged for everything, so
concurrent inserts into one slot collided instead of interleaving.

### A tree reads its catalog's blocks through `Load` on the env, and the env hydrates

[#735](https://github.com/dialog-db/dialog-db/pull/735), after
[#730](https://github.com/dialog-db/dialog-db/pull/730); argued in
[notes/tree-reads-as-effects.md](./notes/tree-reads-as-effects.md).

A search tree is now a root under a catalog, and it reads a node by
performing the new `dialog_effects::archive::Load` command on the
environment it is given: the block its `Get` names, from the local
archive, or else hydrated from where the environment knows the subject's
blocks live. A spilled value is read the same way through
`dialog_effects::blob::Load`. The environment holds the checked-node
cache (`NodeCache::of(env, subject)`) and the hydration routes
(`dialog_network::Routes`, which a branch registers when it opens), so a
peer answers every tree read the same way, sharing in-flight hydrations
through its `Hydrate` scheduler. The store wrappers that bridged the old
subject-less `LoadBlock` to the env are gone. `Get` is unchanged and
never hydrates.

What this changes for you:

- `LoadBlock`, `LoadBlob`, `ArchiveReader`, `Nodes`, `Cached`,
  `ArtifactNodes` and `MemoryBlocks` are removed. A tree reads through any
  `Env: Provider<archive::Load> + Holds + ConditionalSync` (plus
  `Provider<blob::Load>` for artifact trees, and `Provider<archive::Get>`
  where a difference is computed). A test store is
  `dialog_search_tree::helpers::Blocks`, a `Volatile` under a test
  catalog.
- `dialog_repository::LocalIndex` and `NetworkedIndex` are removed: read
  through the env itself (`tree.get(key, &env)`, `select.execute(&env)`).
  `NetworkedIndex::with_priority(p)` is `tree.ranked(p)`; a tree that
  must read only what the replica holds is `tree.locally()` (or
  `Load::locally()` for a single read). `load_or_hydrate`,
  `load_blob_or_hydrate`, `hydrate` and `RemoteFallback` stay where they
  were exported.
- `PersistentTree::empty()` and `PersistentTree::from_hash(root)` are
  `PersistentTree::empty_in(catalog)` and `PersistentTree::at(catalog,
  root)`; `empty_with_manifest`, `TransientTree::new`,
  `TransientTree::with_manifest`, `TransientTree::empty_with_manifest`,
  `TransientTree::stitch`, `HitchhikerTree::empty`, `TreeHistory::from_root`,
  `TreeHistory::empty` and `distribution::capture` take the catalog, and
  `Accessor::new(env, catalog)` does too. `PersistentTree::catalog()`,
  `with_root`, `reading()`, `ranked(priority)` and `locally()` are new;
  `NodeCache::of`, `NodeCache::hold` and `NodeCache::held_by` find and
  place the cache an env holds.
- `fetch_spilled`, `fetch_spilled_cached`, `fetch_spilled_reference` and
  `merge::observe_revisions` take the archive the value lives in; a
  spilled value is read by `load_spill`.
- `Branch::history`, `Snapshot::history`, `Branch::log`, `Snapshot::log`,
  `Select::perform` and `Select::estimate_perform` ask less of the env
  (the two loads and `Holds`), and the commands that read trees ask
  `Holds` of it. Every peer and session already holds.
- `HeldCaches` holds its node cache where trees find it, so the budget,
  `release` and `clear` govern every tree read through the env, and an
  env given its own `HeldCaches` before anything opens keeps nodes under
  that budget.
- A borrowed environment is an environment: `&T` provides every command
  `T` provides (`dialog_capability::Provider`) and holds what `T` holds
  (`dialog_common::Holds`).
- A resolver premise (`tree/node`, `tree/span`, `tree/key`, `tree/value`)
  loads the block it names against the wildcard subject's archive
  (`dialog_query::resolver::anywhere`), and `Scope` asks for the two
  loads instead of `LoadBlock` and `LoadBlob`. A query over several lines
  answers from each line's catalog in turn.
- Any read of a repository one of whose open branches tracks a remote
  can hydrate a block it lacks, a snapshot's and a transaction's staged
  view included, and a branch can hydrate a block only a sibling's remote
  holds. A miss tries every remote the subject's open branches track, in
  the order they were opened and each in its tracking order, where before
  only the reading branch's first remote was asked; a miss no reachable
  remote answers fails naming the first remote that could not be reached,
  rather than reading as absent. With no remote tracked a read is local,
  as before. An export that does not download reads only what the replica
  holds.
- A push's novelty walk, its subtree forwarding and its spilled-value
  shipping read only the local archive, by `Get`, and never hydrate.

Why: the tree's read was a command with no subject, so every reader
needed a wrapper that knew the catalog, another that knew where to
hydrate from (worked out per read by `Branch::fallback`, which was about
9% of a worker's CPU), and the node cache was plumbed through them. Read
as a capability on the env, there is one way to get a node, which goes
through the cache and the env's hydration, and nothing to wrap.

### A difference reads a leaf both sides share only at the keys its buffered ops touch

[#736](https://github.com/dialog-db/dialog-db/pull/736)

When two trees hold the same leaf and differ only in the ops a buffer
above routes to it, the comparison now resolves just the keys those
ops cover: each side's winning op, or the stored entry where that side
has none. Before, the entry walk streamed every stored entry of the
leaf on both sides and dropped the ones that matched. A commit buffered
at the root routes each op to a whole leaf, so every subscription poll
over the commit decoded every entry of each leaf the commit's ops
landed in, whatever its scope. A scoped difference now resolves only
the covered keys inside its scope, so a poll whose scope the commit
missed reads nothing of those leaves. The buffered fast path also
checks the leftmost separator before decoding any buffer, since a
root's is always empty and the decode could never be used.

What this changes for you:

- A subscription poll after a commit costs in proportion to the ops
  the commit buffered, not to the size of the leaves they land in.
  `subscribe-fanout` and `subscribe-pulled` (201 polls after a one-note
  commit) go from about 203M to about 39M instructions, and
  `subscribe-repoll` from 14.2M to 12.6M.
- A poll's cost no longer depends on where a written value sorts. A
  note body sorting into a leaf that every note subscription's
  rule-discovery scope crosses used to cost each of the 201 polls about
  370k more instructions than one sorting elsewhere.
- `differentiate`, `differentiate_within`, `changes` and
  `changes_within` report the same changes. `TreeDifference::changes`
  on a difference built by `compute_within` no longer yields
  out-of-scope spillover from such a leaf; `changes_within` already
  filtered it.

Why: the stored entries of a shared leaf are the same bytes on both
sides, so only a key an op covers can differ, and decoding the rest
was work every subscriber repeated for every commit.

### A tree reads every node through its store, which keeps what it checked

[#730](https://github.com/dialog-db/dialog-db/pull/730)

A search tree no longer carries a node cache. The store a tree reads
through implements the new `dialog_search_tree::Nodes` trait: `kept()`
names the node cache it holds, if any, and `load_node()` answers a read
from that cache, else from the store's blocks, checking the node once and
keeping it. Every tree read goes there: point lookups, scans, traversals,
differences, edits loading the nodes they rewrite, and persists, which
keep the nodes they seal in the same cache. A new read path therefore
cannot bypass the cache, as the difference, `traverse_available`, push's
subtree forwarding, diagnose and `canonical_divergences` did. In the
repository the cache is the line's: `LocalIndex` holds the handle and
`NetworkedIndex` forwards it.

What this changes for you:

- Every tree method that took `Env: Provider<LoadBlock>` takes
  `Env: Nodes<Key, Value>`. A block store that keeps nothing implements
  it in one line (`impl<K, V> Nodes<K, V> for MyStore {}`), as
  `MemoryBlocks`, `ObservingBlocks` and `JournaledBlocks` do; wrap one in
  `Cached::new(cache, &store)` to keep the nodes read through it.
- `PersistentTree::node_cache`, `PersistentTree::empty_with_cache` and
  `PersistentTree::from_hash_with_cache` are removed: use `empty()` and
  `from_hash(root)`, and give the store the cache.
  `PersistentTree::empty_with_manifest` takes only the manifest.
- `TransientTree::new(root)`, `TransientTree::with_manifest(root,
  manifest)` and `TransientTree::empty_with_manifest(manifest)` drop
  their cache argument.
- `TransientTree::persist(delta, store)`, `HitchhikerTree::persist(delta,
  store)` and `HitchhikerTree::persist_mut(delta, store)` take the store
  whose cache keeps the sealed nodes. `TransientNode::persist`,
  `TransientNode::persist_mut` and `Node::into_link` take
  `Option<&NodeCache>`.
- `Accessor::new(env)` takes only the store.
- A difference's `MissingBlocks::Deny` side reads through the store's
  cache. A `MissingBlocks::Boundary` side reads the store's blocks
  directly, so absence is the store's own word.
- A store that keeps nothing skips the range scan's sibling read-ahead
  and an integration's open-ahead pass, which would only read each node
  twice.
- `dialog_artifacts::ArchiveReader` requires `ArtifactNodes`
  (`Nodes<Key, State<Datum>>`). `DeltaOverlay` keeps what its
  environment keeps.
- `TreeHistory::from_root_with_cache` and `TreeHistory::empty_with_cache`
  are removed: use `TreeHistory::from_root(root, store)` and the new
  `TreeHistory::empty(store)`. `rekey_tree` drops its cache argument.
  `distribution::capture` reads through the store.
- `LocalIndex::new(env, catalog, nodes)` and `NetworkedIndex::new(env,
  index, remote, nodes)` take the node cache the index keeps.
- `Select::execute(store)` probes the root and scans through the store,
  so a store passed in should keep a node cache for repeated selects to
  be answered from memory.
- Reads that went around the cache now hit it: a subscription poll's
  difference, a snapshot export's walk and a preload walk read the
  line's cache, and a select's scan no longer reads its root again
  after the probe. The `subscribe-repoll` perf scenario reads no block
  from the archive where it read 6, and runs 10.6% fewer instructions.
- A poll after a commit that adds one note, with a subscription per
  note of 200 (the new `subscribe-fanout` perf scenario), reads 0
  blocks instead of 806 (32 MB). Every subscription diffs the same two
  trees within its own demand, and the first to read a changed block
  keeps it for the rest. In a browser over IndexedDB, which pays a
  transaction per block read, a write from another device showed on a
  200-note page in about 0.4 s instead of 2 to 3 s.

Why: the cache was carried by trees and threaded through `*_with_cache`
constructors, so every read path had to remember to use it, several did
not, and two places copied the cache-then-store read by hand. Putting it
on the store makes the cached read exist in one place and every read go
through it.

### A read finds its remote without deriving every upstream

[#730](https://github.com/dialog-db/dialog-db/pull/730)

Every select, subscription poll and migration check asked the branch
where a read held by reference falls back to, and the branch answered
by building its full upstream list: deriving each upstream's sync
target, a hash, and comparing every pair. It now takes the first route
at a peer among the pulls and then the pushes, through
`Tracking::first_remote_route`, so the answer is the same without that
work. It was about 9% of a Tonk service worker's CPU while switching
spaces.

What this changes for you:

- `Upstreams::fallback` is removed; `Branch::fallback` no longer builds
  `upstreams()`.
- A read's fallback is the one it was: the first upstream at a peer,
  reachable or not, pulls before pushes, else the first synced route at
  a peer.

Why: the fallback is asked on every read, and the answer needs one
route, not a hashed and deduplicated set of them.

### A request the access service never answers is given up on, and a read is sent again

[#727](https://github.com/dialog-db/dialog-db/pull/727)

An invocation `dialog-remote-ucan` sends as an HTTP request (every one
when the service names no socket, a block read or write, or the fallback
when the socket does not connect) waited for its answer with no bound.
One sent on a pooled connection the network had silently dropped, as a
NAT or a mobile network does a quiet one, was never answered and never
failed. It now waits as long as an invocation over the socket waits
(`Liveness::answer`, 30 s by default), plus the time its payload takes
to upload at 16 KiB/s, and is then aborted and answered as unavailable.

What this changes for you:

- A memory operation sent this way that the service does not begin
  answering in time fails with `Rejection::Unavailable`, as one over a
  silent socket already did; a blob read or write fails with a transport
  error. Either may be tried again with an invocation of its own.
- `UcanSite::with_liveness` now bounds these requests too.
- A block or blob read the service has not begun answering after 3 s
  is sent again, up to seven copies (one more than the connections a
  browser keeps to one host), and the first answer is taken. A
  read that met connections the network forgot is answered in seconds
  rather than failing after 30 s each. The copies carry one invocation,
  so a service may see it presented more than once; the refusal of a
  copy as replayed is not taken for the read's answer.

Why: in tonk, a sync whose request was lost this way held the sync of
every space behind it, so after a few minutes idle on a NATed network,
changes from another device stopped arriving until the page reloaded.

### A rule's conclusion under `last` succeeds the claim its body read

Fixes [#724](https://github.com/dialog-db/dialog-db/issues/724)

A deductive rule that reads a cell and derives another value for it,
`x/status(e) := "todo" :- x/status(e) = "triage"`, rewrites the cell,
the way a write does. Its conclusion stood as the newest fact its body
consumed, which for such a rule is the very claim it rewrites, so the
two tied on standing and a `last` read fell to value order: `"triage"`
won over `"todo"`, and the reverse rewrite only appeared to work because
`"triage"` sorts after `"todo"`. Inside its component the rule still
reads the cell as a set, and its conclusion now remembers the values of
the cell it displaced. When the component is derived, each cell's
candidates are ranked by those successions, a rewrite after what it
read and a rewrite of a rewrite one further, and the pick elects over
the ranks: by version, then cause, then rank, then value. A conclusion
of the value it read displaces nothing, so a rule deriving a relation
from itself still settles; one whose body does not read the cell
stands as before, as the newest fact it consumed.

What this changes for you:

- A `last` read of a cell some rule rewrites returns the rule's value,
  whatever the values' order. Two rules rewriting one claim elect by
  which one read newer facts, as concurrent writes do, and a later
  write to the cell still wins. `all` is unchanged and keeps both.
- A chain of rewrites reads its last link.
- Rewrites that read each other back, `"triage"` to `"todo"` beside
  `"todo"` to `"triage"`, form a cycle no rank orders. Its members
  share one rank and are elected among as concurrent candidates, by
  version then value, so a merge that closes such a cycle leaves every
  query answerable. Before, the same pair also fell to value order,
  but so did every rewrite.
- `dialog_artifacts::Standing` carries a `succession`, zero for every
  claim, and `Standing::new` builds a claim's standing. Code building a
  `Standing` by its fields names the new one. Nothing stored or synced
  changes: a rank is computed when a component is read.

Why: a succession orders a cell's candidates by what each saw, and a
conclusion that read a claim and replaced its value saw it. Letting
value order decide made the result of a rewrite depend on the spelling
of its values and on unrelated facts its body happened to read. Ranking
at the exit rather than inside the fixpoint keeps the component
positive and finite, so a cycle of rewrites converges instead of
failing, which an open rule set has to guarantee under every merge.

### A subscription learns of the readable change to a single-valued cell, not only the moved row

Following [#710](https://github.com/dialog-db/dialog-db/pull/710) and
[#714](https://github.com/dialog-db/dialog-db/pull/714)

A poll told incremental maintenance which rows moved. What a rule reads
of a single-valued attribute is the one claim its pick elects from the
cell, and a cell can hold several: a session value over a committed one,
or two committed claims after concurrent writers merged. A row moving
then changes which claim is read without that claim moving: a session
value outvoting a committed one reported only the session value arriving,
and retracting a cell's winner reported only the winner going, not the
claim beneath it coming back. A rule reading the cell from the entity it
derives was unaffected, since that entity is re-derived whole; a
recursive rule, or one reading another entity's cell by its value, kept
a stale row. For every touched cell of an attribute some reachable rule
reads as single-valued, when any reachable rule joins across entities or
recurs, the poll now counts every claim the cell holds as asserted and
every claim it held before as retracted, so the delta-join and the
fixpoint learn of the readable change.

What this changes for you:

- A subscription over a recursive rule, a rule two concept layers deep,
  or a rule reading another entity's single-valued attribute no longer
  keeps a stale row when a session write outvotes a committed value or a
  retraction uncovers an older one. Its results match a fresh evaluation.
- Such a poll reads each touched single-valued cell once more. A
  subscription whose rules are all entity-local pays nothing new.
- `dialog_query::concept::query::affected::elected_attributes` and
  `Elected` are new: the attributes a concept's reachable rules read as
  single-valued.

Why: the invariant that a maintained result equals a fresh evaluation
holds only if maintenance learns of every change a reader can see, and
election makes a cell's readable value a function of all its claims.

### A subscription's view of the state before a change is exact over a cleared or committed-over overlay

Following [#710](https://github.com/dialog-db/dialog-db/pull/710)

A subscription maintained incrementally reads the state before a change
from its pinned revision under the session overlay rewound to the pinned
sequence. The rewind undid each instant's readable moves, and a session
line holds a fact in two independent ways, stored in it and hidden
beneath it by a tombstone, so one fact can be both at once: asserted over
its own tombstone. The readable moves do not say which of the two an
instant changed. A commit retracting such a fact takes the stored copy
out and leaves the tombstone, which the rewind undid by lifting the
tombstone instead; clearing the overlay lists the fact as removed and
un-hidden at once, which the rewind took for no move. Either left the
view before the change short of a fact, and a subscription could keep a
row a fresh evaluation no longer returned. An instant now records how
the line held each fact it moved, and the rewind restores exactly that.

What this changes for you:

- A subscription over a non-local rule no longer keeps a stale row after
  an overlay clear, or a commit retracting a fact the overlay had
  re-asserted over its own tombstone. Its results match a fresh
  evaluation again.
- `Instant` carries `found`: how the overlay held each fact the instant
  moved, before it did, as a `Hold` (stored, hidden).

Why: the rewind inverted a log that did not determine what it changed.
Recording what each instant found makes the inverse exact for any move,
instead of assuming one representation per fact.

### A subscription retracts a row whose supporting facts go together

[#710](https://github.com/dialog-db/dialog-db/pull/710), fixing
[#708](https://github.com/dialog-db/dialog-db/issues/708)

A concept subscription maintained incrementally finds the rows a change
can affect by binding each changed fact into the premise it matches and
joining the rule's remaining premises sideways. It joined them against
the state after the change, so a row two retracted facts had supported,
each under a different premise of a non-local rule, was found through
neither: bound into its premise, each was joined against a state the
other had already left. The row stayed in the subscription's results,
with an empty delta, while a fresh evaluation had none. The join now
runs against the state the derivation it looks for holds in: the state
before the change for a fact that stopped being readable, the state
after it for one that became readable, and the other way round under a
negated premise, where the mirror image (two facts asserted together
under two `unless` premises) was missed the same way. The state before
a change is the subscription's pinned revision under the session
overlay rewound to the pinned sequence, so a change through the overlay
is covered too. An optional premise is read again on each side for the
changed fact's subject, since the row through its old binding and the
row through its new one both change; before, a row that came to exist
because an optional value went absent, or stopped existing because one
arrived, was missed the same way.

What this changes for you:

- `dialog_query::concept::query::affected::affected_entities` takes the
  asserted and retracted facts separately, the environment of the state
  after the change, and a function yielding the state before it, in
  place of one list of facts and one environment.

- A poll after a commit or an overlay instant that retracts more than
  one fact supporting the same row, or asserts more than one under
  negated premises, now moves that row, through the maintenance path,
  where it used to keep it with an empty delta. `Subscription::results`
  matches a fresh evaluation again.
- A subscription whose rule has one premise that does not bind the
  head, such as a department's salaries derived from each member's,
  is now maintained per head instead of recomputed on every poll: the
  join keeps the premise the change was bound into, so it reaches the
  head through it. An election the subscription reads under (`max`,
  `last`) is per entity, so re-deriving the entity takes it again.
- A concept subscription first polled before the branch's first commit
  has no revision to read the state before a change from, so a poll of
  it that needs that state (a retraction, or an assertion under a
  negation, through the session overlay) recomputes instead of
  maintaining, until the branch has a revision. Its results are the
  same.

Why: a derivation a change touches holds on exactly one side of the
change, and its other premises hold only there. Joining every change
against the new state is exact for one changed fact, since every other
premise reads the same facts on both sides, and silently incomplete for
two.

### A difference fetches the blocks it reads a batch at a time

[#690](https://github.com/dialog-db/dialog-db/pull/690), part of [#646](https://github.com/dialog-db/dialog-db/issues/646)

`Prefetch::Lazy`, the default for every difference (`compute`,
`compute_with`, `compute_within`, `differentiate_within`), read each block
at the moment its walk reached it, so a difference over a remote store
paid one round trip per block, one after another. Each pass of the walk
now first plans its reads: a dry run of the walk finds the blocks it is
going to read without reading any, they are fetched concurrently, up to 16
at a time, and the dry run goes on from what they turned out to be until
the pass needs nothing more. The walk then makes the decisions it made
before, over blocks already fetched. It reads the same blocks as before,
each as often; a test holds it to that, read for read, over every case of
the differential's oracle. A pass also resumes where the last one stopped
instead of starting over, below any change since.

What this changes for you:

- A difference over a store with latency costs a round trip per batch
  rather than per block: on a large tree with 1 000 changes spread across
  it, 184 round trips instead of 1 053, for the same 1 054 reads. A
  one-change difference reads 8 blocks in 4 round trips instead of 7.
- Up to 16 reads of one difference are in flight at once against its
  store, and fetched blocks wait for the walk: at most 63 in that
  measurement.
- In memory, where there is no latency to save, a difference costs about
  the same: the spread one about a quarter less CPU, since a pass no
  longer rescans what the last one passed, and a small repeated one, as a
  subscription's poll takes, about 1% more instructions.
- Nothing else is observable: the same blocks, the same changes, the same
  errors and the same `MissingBlocks` outcomes. `Prefetch::Eager` is
  unchanged.

Why: the round trips of a difference were most of what a subscribed
replica waited on to catch up (#646), and they were serial only because
the walk read as it went, not because its reads depended on each other.

### A reset moves a branch forward only

Fixing [#682](https://github.com/dialog-db/dialog-db/issues/682)

`Branch::reset` now refuses a revision that does not hold the branch's
head, and the head does not move. A reset back to an older head, or onto
another writer's line that lacks this writer's commits, followed by a
commit, broke what every causal context assumes: that one origin's
revisions form one chain. Below the writer's last revision the commit
re-minted a version already used; above it, on a line without that
revision, the commit forked the origin. Either way a context holding one
revision claimed the other, and merges dropped writes as already seen. The
push guard from #681 could not catch the fork, since a context keeps one
watermark per origin: a push from such a head rewound its upstream.

What this changes for you:

- A reset to a revision that does not hold the branch's head fails with
  `PublishError::Rejected(Rejection::Unsupported { reason })`, whose
  reason says a reset only moves a branch forward. Nothing is written. To
  take another line's work, pull it; to go back to an older revision,
  start a new branch from it.
- A reset to a revision that holds the head, to the head's own tree, or
  on a branch with no head, lands as before.
- `Reset::perform` reads the archive to decide: one entry of the
  revision's context, from the memo, from its edition when the head is of
  the same origin, or one point read of its region, through the remote
  that serves the branch's blocks when they are not local. Its bounds grow
  from `Provider<Publish>` to the read bounds `Branch::history` takes plus
  `Provider<Publish>`. An environment that is a bare storage provider
  without those, such as `Volatile` on its own, can no longer reset a
  branch; a session or a peer can.
- A replica of an earlier release can still reset any way and sync with
  this one. The sync-base and push checks of #681 meet a head it moved
  back. A fork it makes (a takeover followed by a commit) is not detected
  by anything on this release: the forked revisions share an origin and
  no context can tell them apart. Upgrading every writer of a branch
  closes it.

Why: reset was documented as fast-forward bookkeeping, not rewind, and
nothing held it to that. A rewind costs other replicas their writes, and
no later check can tell the forked revisions apart.

### A sync uses its base only while both heads hold it

[#681](https://github.com/dialog-db/dialog-db/pull/681), fixing [#673](https://github.com/dialog-db/dialog-db/issues/673)

A pull merges three ways from the sync base it recorded with an upstream:
it takes everything the base holds as already in our head, and anything
the base holds that the upstream lacks as removed upstream. That holds
only while both heads hold the revision whose tree the base is, and three
paths broke it:

- Settling rival merges. When two writers each merge the same two heads
  at once, the greater keeps its head and recorded the lesser's tree as
  its base without holding it. Once the lesser had built on its merge,
  the greater side's next pull from it never imported the lesser merge's
  revision record. Every later head carried the hole: its ancestry walk
  failed, and a replica that never held the lesser merge could not
  vouch for the revisions a pull from it brought in.
- A reset that moves our head behind the base. The next pull returned at
  once when the upstream had not moved, or merged from the base and
  dropped what only the base held, facts included.
- A reset that moves the upstream's head behind the base. The merge read
  what the upstream never held as removed there, and removed it from
  ours.

A push made the same assumption. It took the upstream for a fast-forward
target whenever the upstream's tree was still the sync base, so a push
after a reset behind the base published the older head and moved the
upstream back, dropping what only the base held from every replica that
then pulled it.

A sync now records the revision whose tree the base is, in a
replica-local cell of its own beside the tracking cell, written after it.
A merge runs from the base only when both heads' contexts observe that
revision; otherwise it runs from no base, screened as a first contact is,
or adopts the upstream by root when the upstream has seen our head. When
our tree is the base, the revision is our head. Otherwise a revision that
is not known (no record, as after an upgrade, or a record of another tree,
as an older release moving the tracking cell or a crash between the two
writes leaves it) is not trusted: the pull merges from no base once, and
the sync it records writes the revision. The shortcut for an upstream
that has not moved checks the same of our head. A push lands only when
our head's context observes the upstream's head.

What this changes for you:

- A pull after a reset keeps every fact either side held, and so does a
  pull from an upstream someone else reset.
- A push from a head that does not hold the upstream's head, as after a
  reset behind the sync base, now fails with
  `PushError::NonFastForward { branch, expected, actual }`, the error a
  push to an upstream that moved already gets. `expected` and `actual`
  can name the same tree here, since the upstream has not moved; the
  local head is what is behind. Pull, then push again. A refused push
  writes nothing: no block is uploaded, the upstream's head is not
  published, and neither the head nor the sync records of the pushing
  branch change. Before, the push published the older head and moved the
  upstream back.
- What the released `0.3.0` does, measured with this release's tests run
  against it:
  - A reset followed by a pull loses the upstream's facts that only the
    base held: 40 of 40 runs.
  - A reset followed by a push rewinds the upstream to the older head:
    10 of 10 runs to a local upstream. The remote push has the same
    tree-only check, read from the code.
  - A replica that never resets is exposed too. When another writer
    resets the upstream behind the replica's sync base, the replica's next
    pull removes facts the base held: 10 of 10 runs. When another replica
    resets, pushes, commits and pushes again, a replica that never reset
    loses its own fact on its next pull: 10 of 10 runs. A deployment is
    exposed if any writer of a branch it syncs with has reset that branch,
    or pushed from a reset one.
  - This change restores nothing already lost. Heads minted by a losing
    merge lack those facts, and pulls carry the heads on. The facts may
    still be in the trees of older revisions wherever their blocks are
    kept, but nothing in dialog brings them back from there.
- Pulls that settle rival merges leave complete histories, and
  `PreparedPull::novelty` vouches for every revision such a head brings
  in. This was introduced by #623, which no release contains.
- A pull whose base either head no longer holds merges from no base. It
  reads both sides' differences in full, once, where a merge from the
  base read only what changed since it; the next pull runs from the new
  base again. The first merging pull after an upgrade does the same once
  per upstream, since a base recorded by an older release is not trusted,
  and so does a pull after a crash between a sync's two writes.
- A push that does not land as is checks that our head holds the
  upstream's: from the memo, from our edition when the upstream's head is
  of our origin, or with one point read of our tree's region. One case
  needs the network: our head reached the branch without its region being
  read (adopted by root, or reset to a head another replica made), the
  upstream's head is of another origin, and the memo does not have the
  answer. Then the read goes to the remote that serves our head's blocks,
  even for a push to a local upstream. When that remote cannot be read,
  as offline, the push fails with `PushError::Unreachable`, naming the
  remote, with a reason saying it could not confirm the branch's head
  holds the upstream's head, and writes nothing. Push again once the
  remote answers: the read is kept locally, and the push lands.
- Opening a branch reads one more replica-local cell, and a sync writes
  one more after the tracking cell. No API changes.

Why: the merge's assumption about its base was true on every path that
existed when the base was introduced, and nothing checked it once resets
and rival merges could break it. Checking it against the revision the
base stands for makes it hold however the heads moved.

### A subscription catching up re-derives its touched entities together

[#679](https://github.com/dialog-db/dialog-db/pull/679), for
[#646](https://github.com/dialog-db/dialog-db/issues/646)

A poll that finds facts changed inside a subscription's cover re-derives
each touched entity with a restricted evaluation of the query. Those
evaluations ran one after another, and on a cold replica each one is a
point read of its entity's leaf, so a catch-up cost a round trip per
touched entity. They now run sixteen at a time, and their rows are folded
into the result in entity order once all of them have finished.

What this changes for you:

- A subscription catching up on many changed entities waits on far fewer
  round trips. In the catch-up soak, a poll after a hundred one-fact
  commits goes from a chain of 80 dependent requests to 31, and from
  18.2 s to 7.5 s on the mobile profile (6.9 s to 2.8 s on broadband,
  18.2 s to 8.0 s in a browser). A poll after ten goes from a chain of 11
  to 3. The remaining chain is the cover diff's own walk.
- It reads the same blocks: request and byte counts are unchanged.
- Deltas, their order and the retained results are as before.
- Nothing stored or synced changes.

Why: the restricted evaluations are independent of one another. Each
reads its own entity's facts, and the cover they record is a union, so
nothing ordered them but the loop.

### A subscription poll that fails part-way no longer loses deltas

[#679](https://github.com/dialog-db/dialog-db/pull/679), for
[#646](https://github.com/dialog-db/dialog-db/issues/646)

When a poll's re-derivation of one touched entity failed, the poll
returned the error, but the entities re-derived before it had already
been applied to the subscription's retained results. The next poll found
those entities unchanged and never emitted their rows, so a subscriber
missed them for good. A failed poll now leaves the retained results as
they were, and the next poll re-derives every touched entity and emits
the whole delta.

What this changes for you:

- After a poll returns an error, the next successful poll emits every
  change since the last successful one, including those of the entities
  the failed poll had already re-derived.
- The error a failed poll returns is unchanged.

Why: a delta a subscriber never sees leaves its view wrong with nothing
to correct it.

### dialog-query no longer turns on dialog-common's test helpers

[#675](https://github.com/dialog-db/dialog-db/pull/675)

dialog-query asked for dialog-common's `helpers` feature in its normal
dependencies, though only its tests use anything behind it. Every build
of dialog-query, and of dialog-repository and dialog-peer above it, on
every target, therefore compiled dialog-common's test helpers: the
`helpers` module (service provisioning, `BenchData`), and the `test` and
`provider` macros. dialog-query now takes the feature as a
dev-dependency only, so a build of these crates no longer turns it on.

What this changes for you:

- A dependent that uses `dialog_common::helpers`, `dialog_common::test`
  or `dialog_common::provider` without enabling `dialog-common/helpers`
  itself, and so got them only because dialog-query turned the feature
  on, no longer compiles. Enable the feature where you use them,
  usually in `[dev-dependencies]`:
  `dialog-common = { version = "...", features = ["helpers"] }`.
- Nothing else moves. dialog-query's, dialog-repository's and
  dialog-peer's own APIs are unchanged, and nothing stored or synced
  changes.

Why: test scaffolding has no place in a production build, and a
feature turned on through a normal dependency reaches every build above
it. #670 puts a seeded source of identifiers for the soak harness behind
that same feature, which makes keeping it out of production builds a
requirement rather than a cleanup.

### A query pinned to one value of a single-valued attribute reads a cold replica in parallel

[#658](https://github.com/dialog-db/dialog-db/pull/658), for
[#650](https://github.com/dialog-db/dialog-db/issues/650)

A premise that binds a single-valued attribute to a value without binding
the entity, such as `status = "open"`, scans the value index for
candidates and checks each candidate against its entity's stored values,
so that an entity whose winning value is no longer `"open"` is not
yielded. Each check now goes out as a preload hint while an earlier one
is awaited. The other premises' entity probes were already pipelined this
way. The checks were not, and they were the chain. On a cold replica each
check fetched one entity leaf, and the probes of the next premise landed
in that same leaf, so the query cost one round trip per entity leaf it
touched.

What this changes for you:

- A cold query driven by such a premise takes about a fifth of the round
  trips it did. The soak's `rule` phase, a rule whose body pins
  `status = "open"`, goes from a chain of 98 to 18 dependent requests and
  from 8.4 s to 1.7 s on the broadband profile (22.6 s to 4.7 s on mobile,
  22.4 s to 6.4 s in a browser). The `filtered` phase, a concept query
  pinning a status, goes from 8.8 s to 1.7 s on broadband.
- It fetches no more blocks: the hinted leaves are the ones the checks
  read anyway. Request and byte counts are unchanged.
- Results are unchanged, and so is the plan. A replica whose speculation
  budget is zero (`FetchBudget::ZERO`) behaves as before.
- Nothing stored or synced changes.

Why: #536 made a rule body plan with its pinned values bound, which is
correct and reads fewer blocks. It also made the value-pinned premise the
driver, and that premise's checks were the one per-row read in the
nested-loop join that nothing read ahead.

### A difference prunes a subtree both sides route the same buffered ops to

[#669](https://github.com/dialog-db/dialog-db/pull/669), fixing [#662](https://github.com/dialog-db/dialog-db/issues/662) and [#648](https://github.com/dialog-db/dialog-db/issues/648)

Comparing two trees prunes a subtree both sides hold by the same hash
only when nothing a buffer above it routes down could differ. Before
this, any subtree with buffered ops routed down to it was expanded, even
when both sides routed it the same ops. A root's buffer reaches every
child, and a root cannot settle against the other side's root on its
buffers alone, so a commit that added one fact made the walk read
nearly every leaf of a deep tree. Now a pair with the same hash and the
same winning routed ops prunes unread. A pair with the same hash and
different routed ops is expanded one level, so the ops reach the
children they belong to and the shared children prune there.

What this changes for you:

- A push after a commit on another writer's head no longer uploads the
  whole tree. On a deep tree a replica that took turns with another
  writer could download every leaf during induction's catch-up on its
  second commit, and then upload every one of them on every later push
  (about 350 nodes and 16 MB per push in the soak's alternating-writers
  scenario, in about one run in five). Each push is the new root again.
- A joiner's first push no longer re-uploads index nodes it held of the
  adopted head (#648).
- `TreeDifference`, `differentiate` and `differentiate_within` read and
  report fewer blocks over trees whose buffers hold the same ops on both
  sides. The changes they report are the same.

Why: a wrong prune would silently drop a change in sync, so pruning
stayed conservative wherever buffered ops were pending. Ops routed from
an ancestor are not in a subtree's hash, but a subtree's whole
contribution to the difference is its hash and the winning ops routed
to it, so when both match nothing beneath can differ. The argument is
in `SparseTree::prune`'s documentation, and a property test checks it
against a materialized difference over random trees with divergent
flush histories.

### A transaction's writes to a cell under `last` commit as they would one by one

[#647](https://github.com/dialog-db/dialog-db/pull/647)

A transaction that wrote one cell under `all` and under a choosing pick
(`last`, `max`, `min`, `top`) could commit something other than what the
same writes committed one per transaction leave. The commit squashes a
cell's writes value by value: a repeated assertion of a value is
dropped, and an assertion a later retraction of its value cancels is
dropped. Neither holds once a choosing write stands between them, since
it succeeds whichever claim its pick elects at its place in the order,
and that may be the value an earlier write of the transaction asserted.
Two shapes lost a fact:

- Assert a value under `all`, write the cell under `last`, assert the
  value again: the commit dropped the repeat, and the `last` write
  retired the value. The value was missing.
- Assert a value under `all`, write the cell under `last`, retract the
  value: the commit dropped the assertion against the retraction, so
  the `last` write succeeded the claim the line held instead. That
  claim was missing.

A cell holding a choosing write is now committed as written, every
write in order, and the tree settles it as it settles one commit per
write. A cell with no choosing write is squashed as before.

What this changes for you:

- A transaction of either shape commits what the same writes committed
  one by one leave: a read after the commit returns the value asserted
  again, or the line's claim, beside the `last` value.
- A transaction that asserts and then retracts a value in a cell it
  also writes under a choosing pick now leaves a retraction record for
  that value in the history, where it left none. The facts are the
  same.
- No API moves.

Why: the squash reasons about one value at a time, which is exact only
while writes of different values commute. A choosing write is the one
write that does not commute with the others, so a cell holding one has
to reach the tree in the order it was written.

### A chain canonicalized at a link that changes nothing is canonical

[#649](https://github.com/dialog-db/dialog-db/pull/649)

A staged chain canonicalizes at its last link. When that link was an
amend that changed nothing (a retract of a fact the chain never held, a
re-assert of a value that stands), the commit returned the tip as it
was, before the seal that flushes the buffers, so the canonicalize was
dropped and the chain kept whatever buffered form its earlier links
left. An amend asked to canonicalize now canonicalizes its tip whether
or not the link changed anything, and reports the tip unchanged only
when it was already canonical.

What this changes for you:

- Two replicas holding the same facts no longer disagree on the root
  because one chain ended in a no-op amend. Before, each named a root
  that depended on where its earlier links flushed, so a sync saw
  divergence where there was none and a merge did work that found
  nothing.
- `TransactionCommit::amend` with `canonicalize` on a link that changes
  nothing returns a batch whose tree is canonical; it returned the
  buffered tip before. A commit that is not an amend keeps its rule: a
  batch that changes nothing mints nothing and canonicalizes nothing,
  unless `allow_empty` asks for a revision.
- A commit that changes nothing hands its live spine back to the line,
  so the next commit appends to it instead of opening the root frame
  from bytes again. The bytes written are the same either way.
  `BufferedBatch::release` and `SpineSlot::root` are new.

Why: the canonicalize request was checked after the no-op return. An
amend is the one commit that rewrites a revision's tree in place, so it
has a tree to canonicalize even when the link itself wrote nothing.

#### Compatibility and migration

- A `FileArchive` file is format 12; one written by an earlier build of
  this branch's `dialog-archive-file` (format 10 or 11) does not open, and is
  moved again from the replica it came from with `dialog-migrate`. Nothing
  else stored or synced changes: blocks, their hashes and what a replica
  syncs are the same.
- Nothing stored or synced changes for the corpus rename: keys, node
  encodings, spilled-value references, revisions and the wire format are
  the same bytes, nothing is rewritten on upgrade, and an older release
  reads everything this one writes. Only Rust names and the crate graph
  move.
- Bounding the caches changes nothing stored or synced: caches hold only
  what a replica can read again, nothing is rewritten on upgrade, and
  releases on either side read what the other writes. Nothing for an
  embedder to run.
- Nothing stored or synced changes for trees reading through `Load`:
  blocks, nodes, spilled values and their hashes are the same bytes,
  `Get`'s wire shape is unchanged, nothing is rewritten on upgrade, and an
  older release reads everything this one writes. `Load` is a local
  command and never crosses the wire; what changes is which reads may
  hydrate a block the replica lacks (any read of a repository an open
  branch of which tracks a remote), and that a hydrated block is written
  back locally as before. Nothing for an embedder to run.
- A reset that moves a branch forward only changes nothing stored or
  synced, and rewrites nothing. Heads an earlier release forked by a
  takeover and a commit keep their fork; nothing finds or repairs it.
- A branch has a new replica-local cell, `bases`, never synced, that
  records each sync base's revision. A replica upgraded from an older
  release has none, and its bases are not merged from: the first pull
  that has to merge runs from no base and writes the record. That covers
  a bad base an older release left behind, by a reset, a kept rival or
  `upgrade`. An older release ignores the cell, and the tracking cell's
  encoding is unchanged; when an older release moves the tracking cell
  the record no longer matches it and is not used. Nothing is rewritten
  on upgrade, and nothing an embedder runs is needed.
- Nothing repairs what earlier pulls did. Heads already minted with a
  hole by a build of `main` since #623 keep it, and facts a replica lost
  to a pull after a reset are not restored by this change. Nothing synced
  changes shape: an older release pulls everything this one mints.
- Nothing a replica stores or syncs changes shape, and nothing is
  rewritten. A head an older release published from such a chain names
  a buffered tree; it stays valid, reads, pulls and merges as before,
  and converges on the canonical root at its next canonicalizing commit
  that mints a revision (one that changes a fact, or one with
  `allow_empty`). Nothing an embedder has to run.

### A branch's live spine is held only while a handle of it is open

[#637](https://github.com/dialog-db/dialog-db/pull/637)

The environment's caches kept, per repository and branch name, the live
buffered spine a commit leaves for the next commit on that branch, for the
life of the environment. A repository visited by many writers once each
(open, commit, drop the handle) held a spine per writer: about 146 kB each
at 10 000 writers in the crowd scenario of
[#633](https://github.com/dialog-db/dialog-db/issues/633), 1.5 GB in all.
The caches now hold the spine weakly: every open handle of one branch, and
every snapshot or transaction taken from one, shares it, and the last of
them dropped takes it with it.

What this changes for you:

- A branch opened again after its last handle went decodes its root once,
  on its first commit, from the node cache (no block is read). In the
  crowd scenario that first commit costs about 0.3 ms more than one with
  the spine held at 1 000 writers (1.1 ms against 0.8 ms) and is within
  the run-to-run noise at 10 000 (2.7 to 3.0 ms either way); every later
  commit through the handle is as before, and a handle kept open commits
  as before.
- What an environment retains no longer grows with the number of branches
  written through it once. The crowd scenario's peak at 10 000 writers
  goes from 1.75 GB to 0.29 GB live (1.95 GB to 0.42 GB resident).
- `HeldCaches::release` and `clear` behave as before. `SpineSlot` gains
  `downgrade`, `shares` and `is_held`, and `WeakSpineSlot` is new.
- Nothing stored is rewritten and no format changes: the persisted bytes
  are identical with or without the spine.

Why: the spine is an accelerator for the next commit on the branch, and
only a handle can make that commit; keeping it after the handle went
bought nothing and cost a root per writer.

### A persisted node's bytes can be attributed to what they carry

[#634](https://github.com/dialog-db/dialog-db/pull/634), for
[#632](https://github.com/dialog-db/dialog-db/issues/632)

`PersistentNode::attribution` reports where a node frame's bytes go: the
prelude, the link hashes, separators and scales of an index, and its
entries' keys, values and polarity, next to the weight the pacing metered
for the same entries and a share per key layout. The perf matrix gains
`first-query`: a replica that adopted a head by root, two hundred
one-commit origins deep, runs one point query, and the gate holds the
blocks and bytes it reads and the root frame's bytes.

What this changes for you:

- A new method on `PersistentNode` and a new `FrameAttribution` type in
  `dialog-search-tree`. Nothing existing moves.
- Nothing stored or synced changes.

Why: the root a replica's first query reads carries the write buffer the
history's commits left in it, and choosing a policy for what a published
root may cost needs the bytes attributed and the shape in the gate first.

### A head carries no causal context, and its tree carries all of it

[#625](https://github.com/dialog-db/dialog-db/pull/625)

A head no longer carries one watermark per origin its history ever
included. It carries its own, as its edition and a signed count of its
own origin's revisions, and a signed marker saying its tree holds the
rest, in a new context region with one entry per origin. The region is
exact for every origin except the head's own: that entry is where the
writer's chain of commits began, and a reader brings it up to the head
from the head's edition, one edition and one revision per commit, and
refuses a region that does not bring it to the count the head signed. A
commit on a head of its own origin therefore writes nothing to the
region and reads nothing of it. A commit on another origin's head writes
two entries: that origin's exact watermark, which the head carries, and
its own, one past what the head holds of its origin, which one point read
of the region answers: a root-to-leaf path, a subset of the whole region.
A commit never reads the region whole. A merge writes its own entry and every entry its two sides hold
differently. A pull answers whether our head has seen the upstream's
from our head's record when the record names it as a parent or skip
link, so a pull right after a commit on the upstream's head reads
nothing. A pull decides "nothing new" from its own context and the
upstream head's version, and a fast-forward from the two heads' editions
when they share an origin, at zero reads; a fast-forward across origins
reads the upstream's region, its root and a leaf.

What this changes for you:

- `Revision` has fields `region: bool` and `count: Option<u64>`. A
  struct literal must name them. `Revision::publish_region` takes the
  head's own count, and `Revision::own_watermark` gives the head's own
  watermark when it carries its count.
- `Revision::context` is `None` on every head this release mints. A
  head's whole context is in its branch's memo
  (`Branch::contexts().cached(&version)`), in its tree
  (`history::context_in`, then `Revision::context_from_region`), or
  derived by the ancestry walk (`history::context_of`).
  `Revision::inline_context` gives the context of a head minted before
  the region, and `Revision::publish_region` marks a head as carrying
  its context in its tree.
- `Context::get`, `Context::set` and `FromIterator<(Origin, Watermark)>`
  are new, and so is `Watermark::check`, the per-entry bounds
  `Revision::verify` applies. `EDITION_CEILING` moves to
  `dialog_capability::history` and stays exported from `identity`.
- `ContextCache` holds `Arc<Context>`. `cached` and `context_of` return
  one, and `insert` takes anything that converts into one.
  `merge::screen_data` takes anything that converts into an
  `Arc<Context>`.
- The context region is `CONTEXT_KEY_TAG` (6). It comes with
  `context_key`, `context_entry`, `read_context_entry`,
  `merge::context_scope`, `merge::without_context`,
  `history::context_in`, `history::region_writes` and `history::Held`.
- `Revision::verify` refuses a head that claims both an inline context
  and a context region, and a count on a head without a region, a zero
  count, or a count above what the head's edition leaves room for. A
  pull refuses a head whose region holds no entry for the head's own
  origin, holds it above the head, holds an entry outside the watermark
  bounds, or derives a count other than the one the head signed, as
  `DialogArtifactsError::InvalidKey`, and keeps its own head.
- A commit no longer remembers the new head's whole context in the
  branch's memo unless it had the parent's there already. The first
  reader that needs the whole map reads the region then.
- `Revision::precedence` compares two heads by their contexts only when
  both carry them inline, and orders two heads of one origin exactly by
  edition. Any other pair is answered from editions without reading
  either tree: a higher edition is `After`, and one at or below the
  other's is `Concurrent`, never `Before`. A cell watcher therefore
  fetches a lower head of another origin (a branch reset to it, or one
  delivered out of order) instead of dropping it as stale.
- A fast-forward between heads of different origins now reads the
  upstream's root and the path to its region's leaf (3 blocks in the
  tests); before, it read nothing.
- `diagnose --head <revision cell>` prints the origin count and recent
  writers of a head that carries its context inline, and
  `ContextProfile::from_region` profiles one from its region's entries.

Why: every head carried every origin that ever wrote, so every commit
signed and every pull and cell watcher moved a head whose size grew with
the number of writers, dead ones included. Atop 10 000 origins a head
was 390 kB, its signed payload 480 kB, and a one-fact commit took
5.4 ms, against 0.25 ms atop 10. Now the head is under 300 bytes and its
signed payload 183 bytes at any origin count, and a one-fact commit atop
10 000 origins costs what one atop a history as deep from one origin
does (2.4 ms against 2.3 ms). A dead origin costs its 48
region bytes once per replica instead of once per operation.

### A differential over a buffer holding several ops for one key compares the newest op on each side

[#630](https://github.com/dialog-db/dialog-db/pull/630)

A hitchhiker link buffer can hold several ops for one key, newest last, and
every reader resolves the key to its newest op. The settled-node path of the
differential (two nodes with identical children that differ only in their
buffers) subtracted one side's ops from the other's op by op, so an op a side
had already superseded could cancel the other side's live value, or surface
as a change neither side could observe. Both sides are now reduced to their
newest op per key before the comparison.

What this changes for you:

- A differential between two trees sharing a settled node, where one side
  buffers several ops for a key below the node's lowest separator, now
  reports what each side resolves the key to. Before, with ours holding
  `k: v1` then `k: v2` and theirs holding `k: v1`, the diff reported ours'
  `v2` and nothing for theirs; it now reports ours' `v2` against theirs'
  `v1`. With theirs holding `k: v2`, the diff reported a spurious change on
  ours' `v1`; it now reports no change at `k`.
- Nothing stored is rewritten and no format changes: frames holding several
  ops for one key read as before, and an older release reads what this one
  writes.

Why: a pull of one head against another where the newer head re-asserted a
key it still buffered could miss the older head's value or carry a change
that was not one. A buffer's older ops are history, not state, and the
differential has to compare state against state, as every other reader of a
buffer does.

### A dialog peer can be reached over iroh

[#521](https://github.com/dialog-db/dialog-db/pull/521)

`dialog-peer-iroh` reaches another dialog process, on the same machine or
another one, over iroh QUIC. A peer is named by the `did:key` of its iroh
endpoint, so its address is `did:key:...?route=...` and the routes are hints:
a peer that moves is still the same peer. An iroh address is one more address
a contact can be reached at, carried in `dialog-network`'s table beside the
other sites, and the same code runs in a browser over a WebRTC data channel.
Each request is a signed invocation; the payload is bound to it, and carried
blocks are checked by blake3 rather than trusted by address. Blobs stream over
a channel of their own. A watch is refused as unsupported until a peer streams
one; the cell is read by resolving it again.

What this changes for you:

- `dialog-peer-iroh` is new: the site (`Iroh`), the responder that performs a
  verified invocation against a local store, and the transport.
- `peer::Hello` (`/use/get/dialog/peer/hello`) asks a peer which peer it is
  and which key signs for it, and `peer::Spaces`
  (`/use/get/dialog/peer/spaces`) which spaces it keeps, under the names it
  recorded them by. `Peer` answers both.
- `MemoryError`, `ArchiveError`, `BlobError` and `memory::Current` are
  `Serialize` and `Deserialize`, so a peer that performs an effect can send
  its refusal back.

Why: every replica holder is a peer, but none of the existing transports
could reach one that is another dialog process, since they all assume a
server already reachable at a URL.

### A snapshot can be exported without the bytes of its assets

[#623](https://github.com/dialog-db/dialog-db/pull/623)

`SnapshotExport::without_assets` exports a revision's tree and its spilled
values, and leaves the bytes of each asset the revision records where they
are. A replica that adopts the head this way holds every fact of it, such as
`dialog.asset/size`, and none of the bytes those facts name; they come on the
first read of the asset, as they do after a pull.

What this changes for you:

- `SnapshotExport::without_assets()` is new. An export that does not call it
  carries the asset bytes, as before.

Why: a member who joins a space should not download every file in it before
they can read its facts. With the bytes left behind, a file they never open
costs them its facts alone.

### A pull settles rival merges of simultaneous mutual pulls on one head

[#623](https://github.com/dialog-db/dialog-db/pull/623)

Two replicas that pull each other at the same moment each mint a merge of the
same history. The two merges hold the same data and differ only by their own
revision records, so a merge of them matched neither tree, and every round of
simultaneous pulls minted two new merges. A pull now recognises the pair, from
the two heads' contexts and revision records, and both sides settle on the
merge with the greater version: that side keeps its head and advances only its
sync base, and the other adopts that head by its root, as a fast-forward does.

What this changes for you:

- Two replicas that keep pulling each other reach the same head and stop
  minting merges, where they used to mint two more every round.
- The side that adopts the greater merge drops its own, which recorded no
  claim of its own.

Why: two replicas that sync with each other continuously, as two devices of
one person do, pulled at the same moment often enough to grow their history
without bound.

### A batch lands when another writer moved the induction watermark

[#623](https://github.com/dialog-db/dialog-db/pull/623)

Publishing a batch advances the branch's induction watermark to the head it
landed. A second writer over the same replica, another process over the same
store, can land a commit on that head and advance the watermark in the window
before this write. The publish then failed with `VersionMismatch`, though its
head had landed. It now reads the watermark again and moves it to its head
only while the watermark still stands at the head the batch built on, or at
none; anywhere else, another writer moved it on.

What this changes for you:

- `TransactionPublish::perform` no longer fails with
  `PublishError::VersionMismatch` from the induction watermark after its head
  has landed. It still fails that way when the head itself moved.

Why: a commit whose head has landed is in the branch. Failing it told the
caller to retry a write that had already happened.

### A peer or a session names the replica it writes as

[#623](https://github.com/dialog-db/dialog-db/pull/623)

`PeerBuilder::profile` names the profile a peer writes as, where it is not the
peer's home. A session of a persona that writes into a space of that persona
names the persona, and so does a device of a persona whose state branch is in
a repository of the device. The profile names the replica, not only the
authority: a branch the peer opens keeps its metadata, its tracking and its
records in the registry under the profile's replica of the repository.

What this changes for you:

- `PeerBuilder::profile(did)` is new. A peer that does not call it writes as
  its home, as before.
- With a profile, each revision names the profile as its authority, and every
  entity the peer writes derives from the profile's replica, not the home's.

Why: a Wasm component that holds a space is given that space as the one
storage it can reach, yet it writes on behalf of a persona. Its revisions have
to say so, and what it records about its replica has to be the persona's.

### A prepared pull names what it merges and what is new in it

[#623](https://github.com/dialog-db/dialog-db/pull/623)

A caller can now decide, before it commits a pull, whether each revision the
pull brings in may enter the branch: checking the authority each one names,
say. `PreparedPull::upstream` is the head the prepare fetched and verified,
the one revision the commit adopts. `PreparedPull::novelty` lists the
revisions the commit brings into the branch, ancestors first, each as a
`NewRevision` with its record and its claims. `history::novelty` in
`dialog-artifacts` is the walk underneath it.

What this changes for you:

- `PreparedPull::upstream`, `PreparedPull::novelty`, `NewRevision` and
  `dialog_artifacts::history::novelty` are new.
- A revision whose record neither the upstream nor the branch holds fails
  the walk with `DialogArtifactsError::IncompleteHistory`, where
  `history::log` skips it.
- A no-op pull names nothing new, and neither does one whose commit keeps the
  branch's head and advances only its sync base.

Why: a pull adopted whatever its peer's head held. A replica that checks what
it accepts needs to see every revision a pull would bring in before it lands.

### A replica can be reached at addresses its embedder keeps

[#623](https://github.com/dialog-db/dialog-db/pull/623)

`ConnectedReplica::reached_at` reaches a replica held at a peer at addresses
the embedder gives it, without recording them as contacts of the host.

What this changes for you:

- `ConnectedReplica::reached_at(host, peer, addresses, subject)` is new. Its
  `addresses` must not be empty.

Why: an embedder that keeps the addresses of its peers itself, as a daemon
that dials its peers does, had to record each one as a contact before it
could reach the replica.

### Dialog builds for WASI, and a key held outside the process can sign

[#594](https://github.com/dialog-db/dialog-db/pull/594)

`dialog-repository`, `dialog-query` and `dialog-peer` build for
`wasm32-wasip2` and `wasm32-wasip3`. The browser paths of `dialog-common`,
`dialog-ucan-core` and `dialog-credentials` are for `wasm32-unknown-unknown`
alone, and a WASI build takes the native ones. `ed25519::external` adds a
signing key that holds only a public key and an `ExternalSign`, which gets
the bytes to sign and gives back the signature.

What this changes for you:

- `Ed25519SigningKey` has a third variant, `External`, and
  `Ed25519KeyError` has a variant of the same name. A match that names every
  variant of either no longer compiles.
- An external key cannot be exported (`Ed25519KeyError::External`), and has
  no agreement key, so `agreement_key` gives `AgreementKeyUnavailable` and
  `Secret::derive` and `Secret::reveal` give
  `SecretError::AgreementKeyUnavailable`.
- `dialog_common::random::fill` is where the workspace gets random bytes.
  On WASI it uses `getrandom` 0.4, and `getrandom` 0.2 everywhere else.
- `Invocation::issued_at` reads the time an invocation was issued.
- On WASI the file system provider compiles and refuses every call.

Why: a Wasm component reaches storage, keys and the clock through the host
that runs it. It cannot use the browser's APIs, and its memory is not where
its host wants a signing key kept.

### A write is safe to send again, and a refused one says what to do next

[#564](https://github.com/dialog-db/dialog-db/pull/564)

A refused request now tells its caller what to do next: try again (and when), rebase onto what the cell holds and write again, or get authority and try again. The access service's writes are made safe to send again to get there. A write that would leave the cell as it already is succeeds, so a publish whose answer was lost, sent again, finds its own write instead of a conflict. A write that does conflict says what the cell holds, so the writer rebases without a read of its own. The cell's state is told only after the write's authority verified; a write without authority is refused for that, and learns nothing of the cell.

What this changes for you:

- `MemoryError::VersionMismatch`, `PublishError::VersionMismatch`, `RetractError::VersionMismatch` and `ResolveError::VersionMismatch` say what the cell holds instead as `current: Current`: `Current::Holds(edition)`, `Current::Empty`, or `Current::Unknown` when whoever refused the write did not say, as S3 does not. Their `actual: Option<Version>` is gone; `Current::version()` reads the version where the cell holds one. Code that builds the variant names `current`; a match with `..` is unaffected. The volatile, filesystem, IndexedDB and loopback stores answer `Holds` or `Empty`, which they used to report as a bare `actual` version.
- A remote branch's `publish` that the remote took, but whose local record was written first through another handle, is refused as `PublishRemoteBranchError::Publish(VersionMismatch)` with `Current::Unknown`: what the local record holds is not the remote's head, and `push` would otherwise rebase onto it.
- The access service answers a publish of the content the cell already holds as done, with the version the cell is at, and a retract of an empty cell as done, whatever version either named.
- It answers any other conflict with `412`, the cell's state as a DAG-CBOR `CellState` in the body (`CELL_STATE_MEDIA_TYPE`) and its version as the `ETag`. Over the socket the `Answer` carries the same. The client decodes it into `current`; an older service's bare `412` decodes as `Current::Unknown`.
- `push` onto a remote that answers with its head reports `PushError::NonFastForward` naming that head and records it, verified, so `pull().assuming_upstream()` rebases onto it without asking the remote again.
- `Rejection::Unavailable` has a `retry_after: Option<u64>` (seconds), left out of the encoding when unsaid, so an older peer reads and writes it as before; `Rejection::unavailable(reason)` builds one without it and `Rejection::retry_after()` reads it. The service sends it as `Retry-After` too (`Response::retry_after`).
- An invocation sent and not answered is `Rejection::Unavailable`, a write included, where a write was `Rejection::Unclassified` before.

Why: an error is useful when the caller can act on it. A bare conflict made every writer read the cell before it could rebase, a lost answer left it unable to tell a write that landed from one that did not, and a retryable refusal gave no hint of when.

### A socket that fails is noticed, and every way it fails has an answer a caller can act on

[#564](https://github.com/dialog-db/dialog-db/pull/564)

A space's resolves, publishes and watches share one long-lived socket, and a connection that went quiet (a laptop that slept, a changed network, a NAT that forgot the mapping) was never closed: the platform reports nothing until something it sends fails, and a watch sends nothing. Every invocation for the space then waited for ever, and replication went on counting the upstream as watched while it no longer followed it. The client now keeps each connection honest, says what became of a frame it sent, and recovers from each way a connection fails: silent, cut, lost in flight, refused, behind, or judged by a clock that differs. The socket module's documentation lists each case and what the client does.

What this changes for you:

- The client pings each connection with a text `ping` every `Liveness::heartbeat` (15 seconds by default) and closes one it heard nothing from for `Liveness::deadline` (45 seconds). A service answers `ping` with `pong`, as text, without reading it as a frame; a hibernating Durable Object answers it through `setWebSocketAutoResponse` with the pair `PING`, `PONG`. A service that does not answer has its connections closed every deadline.
- An invocation waits for its answer, and a watch for its first state, no longer than `Liveness::answer` (30 seconds). A connection nothing waits on is closed after `Liveness::idle` (a minute). Invocations that race to open a socket share one connection. `UcanSite::with_liveness` and `Sockets::with_liveness` set these.
- A socket that cannot be opened holds off every space's socket at the same service, not only its own, for 30 seconds, doubling with each failure in a row up to 15 minutes and starting over once one opens. A service without sockets, such as a deployment whose upgrade answers 404, is tried a few times an hour, not once per space every 30 seconds.
- A frame that never left goes as a request instead, whatever it does. An invocation sent and not answered, over the socket or as a request, is `Rejection::Unavailable`, so a peer's next address is tried with a fresh invocation. That is safe for a write too (see the entry on writes that are safe to send again).
- A push whose publish landed but whose answer was lost no longer leaves every later push refused as `PushError::NonFastForward`: a remote already at the head being pushed is taken as the push having landed, and the sync point is recorded.
- A watch's states are coalesced to the newest while it is not read, instead of queued per change; the last state before it ends is never dropped.
- A watch the service refuses is an error of the `Watch` effect, not the first item of its `EditionSource`.
- `branch.replicate()` begins an ended watch, or one it could not begin, again on its own, after a wait that doubles each time in a row from a second up to a minute and starts over once a watch delivers. Host ticks do not jump the wait.
- An effect at a peer refused as `AuthorizeError::Stale` is sent once more, stamped by the clock the refusal said the service read. `dialog_ucan::clock` keeps the difference per subject, for the process.
- `Access::with_clock` sets the clock a service judges by (the system's by default), and `Access::now` reads it; hand it to `Access::recheck` and `Session::deliver`. `Presented::record` is told the time by it, as a third argument.
- `dialog_common::time::sleep` waits on native and in a browser or worker alike, and `dialog_common::time::Instant` is a clock that only moves forward.
- Test helpers: `UcanSettings::fault` has the loopback service's socket fail on cue (`Fault::Cut`, `Fault::Freeze`, `Fault::LosePublish`), and `UcanSettings::clock_offset` sets its clock ahead or behind.

Why: a socket is worth having only if a caller can rely on it as it relies on a request: an answer or an error it can act on, in bounded time. Each of these was a way it gave neither.

### A peer is reached where its DID document says

[#564](https://github.com/dialog-db/dialog-db/pull/564)

A peer is named by its DID, and where it is reached is what its DID document says. `contact(did).discover()` reads the services the document names and records the access service among them as the peer's address. `Discover` asks for a DID's services beside `Resolve`: a `did:key` names none, and a `did:web` or `did:plc` document is fetched as resolving it is. A document naming a DID other than the one asked for is refused, so a host serving a DID's URL cannot point it at services the DID never named.

What this changes for you:

- `PeerBuilder::discovery` sets how a peer discovers; by default it reads `did:key` locally and `did:web` and `did:plc` over the network, and a peer's sessions discover as it does.
- `UcanAddress::from_services` reads the endpoint from a `UcanAccessService` entry (`ACCESS_SERVICE`) and the socket from a `UcanAccessSocket` entry (`ACCESS_SOCKET`). A service that wants to be discovered adds both to its DID document.
- Discovering a contact again follows its document: an address recorded at the same endpoint that the document no longer names, such as a socket the service stopped announcing, is taken back. Addresses at other services are kept. `DiscoverContact::perform` asks its environment for `Provider<RemoveAddress>` as well, which a `Peer` provides.
- A loopback `did:web` (`localhost`, `*.localhost`, `127.x.x.x`) resolves over plain `http`, so a service on the same machine is discovered as a deployed one is. Every other host stays on `https`.
- `dialog-peer` and `dialog-repository` now depend on `dialog-did-web`.

Why: a peer's address was configured by hand beside its DID, so the two could disagree, and a service could not move or add a socket without every client being told. The DID document already answers who a peer is; it now answers where too.

### Invocations say when they were issued, and a service refuses one that is stale or presented again

[#564](https://github.com/dialog-db/dialog-db/pull/564)

The client stamps every invocation with `iat`. `Access::admit` refuses one issued longer ago than its freshness window (`FRESHNESS`, 30 seconds by default) and one presented before, remembering the invocations it accepted while they are fresh through a `Presented` record. Over a socket an invocation must say when it was issued; over HTTP one that does not is still served, so a client that predates the stamp keeps working.

What this changes for you:

- New refusals: `AuthorizeError::Stale { issued_at, at }`, which carries the service's time, and `AuthorizeError::Replayed { invocation }`. Both answer 401. An exhaustive match on `AuthorizeError` needs the arms.
- The record of accepted invocations (`RecentInvocations` by default) is per `Access`, so per process. An embedder that builds a service per request, or runs several processes, shares one record across them with `Access::with_presented`, or a replay can reach another instance; `Access::with_freshness` sets the window.
- A test that presents one invocation twice to one `Access` now gets `Replayed`.
- A client whose clock is more than the window off from the service's has its first invocation for a subject refused as `Stale`; the refusal says what the service's clock read, and the effect is sent once more stamped by it (see the entry on the socket's failures).

Why: an invocation carried no time of its own and the service never asked how old one was, so a captured invocation held for as long as its delegations did.

### A cell can be watched, and a cell's invocations go over the access service's socket

[#564](https://github.com/dialog-db/dialog-db/pull/564)

`memory::Watch` follows a cell: what it holds when the watch begins, then each state it takes, through an `EditionSource` that may skip intermediate states but never the last one. Its command is `/use/get/memory/cell/watch`, so any grant to read the cell covers it. A UCAN address can name a `socket`: one connection per space, shared by every watch of a cell in that space, which also carries the cell's resolves, publishes and retracts; blocks and blobs stay requests. Every frame on it is an invocation verified on its own, and the service checks a running watch's authority again, as of the time it delivers to it, once it was last found to hold more than an interval before (`Access::recheck`). `Session` is the service's side of one connection and holds no connection or clock, so a loopback server and a service that hibernates between messages serve it alike.

What this changes for you:

- `UcanAddress` has a public `socket` field (`with_socket`, `socket`), so a struct literal of it needs the field; `UcanServiceAddress` (test helper) has one too.
- A site that cannot follow a cell answers a watch with the new `Rejection::Unsupported`, as S3, the filesystem remote and a UCAN address with no socket do; the caller resolves the cell again instead. An exhaustive match on `Rejection` needs the arm.
- `Access::subscribe` accepts a watch and answers a `Subscription`, plain data a service can store, which names the cell it follows (`subject`, `space`, `cell`).
- A frame the client sends names itself (`Request::Invoke { id, .. }`) and the service's answer echoes it, so the answer to an invocation the service could not read reaches whoever sent it. An answer that names neither a frame nor an invocation fails every invocation awaiting an answer on that connection.
- Dropping a site closes its sockets, so the service ends their sessions.
- `dialog-remote-ucan` now depends on tokio and tokio-tungstenite natively and on web-sys's `WebSocket` on wasm.

Why: a host learned that a remote branch moved only by resolving its head again and again, each time a verified invocation the service pays for. A watch tells it when the head moves.

### A branch keeps itself in step with its upstreams, and a head delivered late is not recorded over a newer one

[#564](https://github.com/dialog-db/dialog-db/pull/564)

`Branch::replicate()` keeps a branch in step with its upstreams for as long as its stream is read: an upstream that can be watched is followed and each head it delivers is pulled with no round trip of its own, and one that cannot be watched, a local one, or one whose watch ended is pulled from on the ticks the host passes to `every`, which also begin an ended watch again and push the branch's own commits. A bare replicate pulls and pushes; `pull()` or `push()` does one. `ConnectedBranch::watch` follows a remote branch's head, and `ConnectedBranch::observe` records a delivered head only when it is ahead of the one recorded, read from the local record at that moment, since more than one handle records heads of one remote branch.

What this changes for you:

- `Revision::precedence` orders two heads of one branch (`Precedence::{Same, Before, After, Concurrent}`) by their signed contexts, falling back to editions for heads without one.
- `ConnectedBranch::observe` answers `Observation::{Advanced, Stale, Diverged}`; a diverged head is not recorded, and `replicate` asks the remote where it stands instead.
- `Pull::assuming_upstream` merges the head recorded for a remote upstream without asking the remote.
- `replicate` answers `Replicated::Pulled`, `Replicated::Pushed`, and `Replicated::Unwatched(target)` when a watch ends. An ended watch is begun again on its own after a wait that doubles each time in a row, up to a minute; without `every`, an upstream that cannot be watched at all is pulled from only as the replication begins.
- A watched head that cannot be recorded is answered as an error, and the remote is then asked where it stands, so the head is still pulled.
- `Cell::watch` and `Cell::decode` give any cell a watch.

Why: a delivered head can arrive after a newer one was fetched or delivered. Recorded as it came, it would move the sync point back and make the next push fail or upload what the remote holds.

### A rule's head refuses a number its field may not hold, a difference may be negative, and floats round to integers

[#617](https://github.com/dialog-db/dialog-db/pull/617), after [#615](https://github.com/dialog-db/dialog-db/issues/615) and [#616](https://github.com/dialog-db/dialog-db/pull/616); the design is in `notes/formula-schemes.md`, "Addendum: the number band"

A compiled rule is meant not to fail or go quiet at run time over the type a number happened to take. Three things made that false for numbers since they became one band. `math/difference` shared one type across its operands and result, so a natural minus a larger natural was a non-match and a decrement over a natural counter could not be written with a `-1` literal at all. A head field's declared type was never checked against what the body binds to it, so a rule could write a negative into a `natural:` field, where no natural reader sees it. And a comparison proved nothing to the type system, so no guard could make a difference a natural again. This change fixes all three and adds the rounding formulas a float needs to reach an integer field.

What this changes for you:

- `math/difference` yields an integer for integer operands, naturals included (`3 - 5` is `-2`), and a float for float operands. A rule whose natural head took the difference directly no longer compiles (see the next point); the difference used to be a non-match on underflow.
- Compiling a rule whose head field cannot hold every number the body binds to it fails with `TypeError::HeadField`: an integer that may be negative into a `natural:` field, a float that may have a fraction into an `integer:` field. The error names the field, the variable, both types, why the field does not read it, what to do (`?n >= 0`, `math/floor`, or widening the field) and the premise that gave the variable that shape. A natural into an `integer:` or `float:` field, and an integer into a `float:` field, compile as before.
- A comparison that proves a value non-negative (`>= 0`, `> -1`) makes an integer variable a natural, so a decrement guarded by `>= 0` on its result compiles against a natural head and leaves no row for the negative case instead of writing it.
- A numeric head field narrows a body variable the body left open: a count read through a field with no declared type and stepped by `1` is a natural when the head is, and the scan feeding it reads naturals.
- `math/floor`, `math/round` (halves away from zero) and `math/truncate` take any number to an integer; a float with no finite value yields no row.
- Rust: `Difference<N>::is` is `N::Signed`; the `Number` trait has `Signed`, `subtract_signed`, `floor`, `round` and `truncate`; `Cell::signed_of` and `Cell::is_signed`, `Primitive::SIGNED_NUMERIC`, `Primitive::without`, `Type::proves_non_negative` and `Type::normalized` are new; `TypeEnv::infer_for` infers a body against its head's field types.
- Two values of different integer storage variants (a stored natural and a negative integer) now compute together in `math/sum` and the other arithmetic formulas instead of being a non-match.
- Type inference keeps two variables one variable after a static resolves one of them. Before, a polymorphic literal such as `1` resolving a formula's scheme to the whole numeric set split the formula's cells into separate variables, and a field's type reached only the cell it was written on.

Why: numbers are one band and a rule compiles against types; what the body can bind has to be checked against what the head holds at compile time, with the error saying what to do, rather than refused or dropped at run time.

#### Compatibility and migration

- Nothing stored or synced changes shape, and nothing is rewritten on
  upgrade. A fact can be missing at rest from any replica that wrote it
  under 0.2.0 or 0.3.0 in one transaction of either shape above: the
  write never reached the tree, and the history holds no record of it,
  so it cannot be found or repaired from the data. Asserting the value
  again in a new transaction restores it.
- An embedder that logs what each transaction wrote, in order, can find
  the transactions of 0.2.0 or 0.3.0 the squash bug may have affected.
  Group each transaction's writes by cell, entity and attribute, a
  concept statement's fields each their own cell. A cell is at risk when
  it holds an `all` assertion of a value, then a write under `last`,
  `max`, `min` or `top`, then another assertion of that value or its
  retraction. A cell whose relation a rule derives, or that the session
  overlay held, was settled correctly; counting it only over-reports. To
  see what a flagged cell should hold, replay its writes one per commit
  on a scratch branch and compare. One difference is not the bug: when a
  choosing write lands on a cell holding two claims of its own
  transaction, the two stand equal and the value with the greater hash
  is elected, so one transaction and one commit per write can end
  differently, before this release and after it.
- Nothing a replica stores or syncs is rewritten. A stored rule that no longer compiles under the head check (one that wrote a possibly negative integer into a `natural:` field, or a float into an `integer:` field) is held inert by `Branch::commit`, as any rule that fails to hydrate is: it stops firing and does not fail commits. Re-evaluate such a rule with a guard or a rounding formula, or widen the field, to have it fire again.

### A number binds a typed rule variable as the variable's type reads it

[#616](https://github.com/dialog-db/dialog-db/pull/616), fixes [#615](https://github.com/dialog-db/dialog-db/issues/615)

Since 0.3.0 a non-negative number reads back from storage as a natural, whatever type wrote it, and a field declared `integer` reads it as the integer it is. A scan already did. A value bound into a typed variable by any other route (a commit's induction seeding a rule from a new fact, a formula result, a merge of two rows) was instead held to the variable's exact type, and a natural meeting an `integer` variable failed the whole commit with `Commit-time induction failed: seeded rule body: ... Variable ?n of kind SignedInt cannot bind UnsignedInt(40)`. Binding now reads the value through the variable's type, as a scan does: the natural 40 binds an `integer` variable as the integer 40, and only a value the type does not read at all (a string into a number, a negative into a natural) is a mismatch, which the callers that filter rows treat as a non-match.

The route that showed it needed two replicas. A commit's induction replays every fact that reached the branch since it last induced, so the first commit after a pull seeds its rules with the pulled facts, among them a count the other replica's rule wrote, which reads back as a natural. A replica's own writes advance that watermark, so alone it only ever read the count by scan and never hit the check.

What this changes for you:

- An inductive rule over a field declared `integer` (or `float`) no longer fails the commit when the fact seeding it holds a natural, which is what every non-negative number is once stored. The first increment after a pull commits and syncs instead of being lost.
- A constant premise field agrees with a seeded fact its type reads as the constant (`count: 40` matches the stored natural 40), as a scan for that constant does.
- `Match::bind` on a typed variable stores the value as the variable's type reads it (`SignedInt(40)` for an `i128` variable given `UnsignedInt(40)`), not the value as given.

Why: numbers are one band, and the band is read through a type at every boundary, not only at the scan. A rule that compiles must not fail at run time over the type a number happened to be stored as.

### Repositories opened through one environment decode and plan the same rules once

[#611](https://github.com/dialog-db/dialog-db/pull/611)

The rule cache and the plan cache were held per branch and per repository. What rule resolution decodes and builds from content (rule bodies, keyed by entity and the bytes the reader read; inductive bodies; a rule's head re-spelled onto an attribute; selecting rules) and query plans are now held once per environment and shared by every repository opened through it. Each of those maps is bounded. Assembled rule sets, the rules set aside, rule discovery and the trigger footprint stay per branch.

What this changes for you:

- A peer with many spaces holding the same rules decodes each rule body and plans each rule once, not once per space.
- `HeldCaches::release` keeps the shared rule content and plans, since nothing in them belongs to the released repository; `HeldCaches::clear` drops them.

Why: those entries are pure functions of content and answer nothing about which rules a repository holds, so sharing them is sound. An assembled rule set is keyed by its lines' rules roots and kept one per spelling of a concept: shared across repositories, the sets of every repository querying the same concept would evict each other on every query. Sharing assembled sets between repositories whose rules are equal would need the key to name a line by its rule content rather than its root, and is left until a workload shows it is needed.

### An assembled rule set stands until a rule fact of its layers changes

[#610](https://github.com/dialog-db/dialog-db/pull/610)

A rule set, and the analysis of which rules are set aside, was kept per exact layer state: each line's tree root, each overlay's revision, each staged layer's generation. Any write moved one of those, so a session-overlay fact, a staged fact or a commit of plain facts rebuilt every rule set on the next query, and a transaction's query never shared the set its branch had assembled. Rule resolution reads only `dialog.rule/*` and `dialog.concept/transient`, so an entry is now keyed by those facts alone. A line is named by its rules root: a commit whose batch writes no rule fact carries the rules root of the head it built on to the head it mints, and any head moved another way (a pull, a merge, a raw commit) is its own rules root. An overlay or a staged layer is named by a hash of the rule facts it holds and hides, and is left out of the key when it holds none. Lookup is an exact match; nothing is replayed.

What this changes for you:

- A query or subscription after a session-overlay write, a transaction write or a commit that writes no rule fact reuses the assembled rule set instead of rebuilding it. Results are unchanged. On the `rule_cache` bench (a chain of 10 committed rules over 50 people, native, arm64), such a query takes 0.40 ms after an overlay write (1.78 ms on 0.4.0-rc.2), 0.46 ms after a commit (1.99 ms) and 0.39 ms in a transaction (1.91 ms); with nothing written in between it takes 0.39 ms on either.
- The first query after a commit that installs or retracts a rule, or after a pull or merge, assembles its rule sets once, as before. That includes rules a transaction staged and then committed: the staged layer and the committed line name the same rule facts differently, so the `rule-dispatch` perf scenario assembles 6 sets, as on 0.4.0-rc.2, not the 3 an earlier draft of this change reached by replaying reads.
- Each spelling of a concept (the same relation under different field names) keeps its own assembled rule set, at most four per concept; before, queries alternating between two spellings evicted each other's. The `subscribe-repoll` perf scenario assembles 9 rule sets instead of 18.
- A decoded rule body is cached by its entity and the hash of the bytes it was decoded from, and is used only after the reader's own read returned those bytes. A rule whose stored bytes do not hash to its entity stays inert even when the genuine body under that entity was decoded earlier; before, a cached body stood in for whatever bytes a layer held.
- A subscription now records, as rule demand, the rule reads the branch's head cache answered. Before, a rule set assembled from cached discovery did not demand that range, and a rule installed into it did not re-run the subscription.

Why: tonk writes session facts through the overlay and commits as commands run, and every such write took it off the rule cache. A rule set is a function of the rule facts of its layers, so that is what the cache is keyed by.

### A query's own overlay is held as a line, so every rule set it assembles is cached

[#609](https://github.com/dialog-db/dialog-db/pull/609)

The facts a query folds in beside its lines, the injected `db:session` metadata and whatever `.with(..)` adds, were a `Changes` batch read as a store: filtered and sorted per selector, scanned for retracts and for rules on every read. A rule in that batch turned the assembled rule-set cache off for the whole query, and its bodies were decoded every time. The batch is now held as an `Ephemeral`, the store a branch's session overlay already uses. It is read by range, and its revision joins the key of every rule-set and read-settlement cache, so a query folding in the same facts as an earlier one is served what the earlier one assembled. Every overlay and staged rule is read through one function and hydrated once per content address. `Changes` stays the write batch and the wire form; nothing reads one as a source any more.

What this changes for you:

- `QueryLayer::overlay` returns an `Ephemeral` instead of an `Arc<Changes>`.
- `Changes::select` and `impl Provider<Select> for Changes` are removed from `dialog-artifacts`. To read facts held in memory beside a tree, hold them in an `Ephemeral` (`Ephemeral::apply` takes the batch) and read it with `Ephemeral::select`.
- A query or subscription whose `.with(..)` carries a rule no longer rebuilds the rule set and re-decodes each rule body on every evaluation. Results are unchanged.
- Asset changes in a `.with(..)` batch are dropped from the query's overlay, as they were before: a query reads facts only.
- `EntityKey::from_artifact`, `AttributeKey::from_artifact` and `ValueKey::from_artifact` build the key from its parts in one pass instead of parsing and re-encoding it once per field. The bytes are unchanged. `dialog_artifacts::artifact_index_keys`, which builds all three at once, is now public.

Why: a query fell off the cache whenever its overlay held a rule. A performance report on Sapling, which hands rules to each query in its overlay, measured rule decoding and assembly at about 60% of instructions on that path. Reading the per-query batch as a separate kind of store is what let that path skip the cache, so the batch now goes through the store and the cache key every other overlay goes through.

### The storage rewrite carries blob-index keys over as they are

[#605](https://github.com/dialog-db/dialog-db/pull/605), forward-ported from `release/0.3.x`

The rewrite that moves every number into one band (`rekey`, run by `Branch::migrate`) parses each stored key as entity, attribute and value. A tree written before the blob index was retired can still hold blob-index keys, a tag and a hash with no value slot, and those did not parse, so the whole rewrite aborted. `rekey` now copies a blob-index key unchanged.

What this changes for you:

- A branch an 0.2.x release wrote with blob-index entries migrates instead of failing with `Could not convert bytes into key: a stored key does not parse`. For tonk, that was every pull of such an account's `main`.

Why: 0.3.1 ships this fix from `release/0.3.x`; without it on `main`, the next release would bring the failure back.

### `dialog-network`, `dialog-repository` and `dialog-peer` are built as `rlib` only

[#601](https://github.com/dialog-db/dialog-db/pull/601)

The three crates declared `crate-type = ["cdylib", "rlib"]` since they were created. Nothing in the workspace, its CI or tonk consumes the dynamic library the `cdylib` target produced: the wasm test jobs build `wasm-bindgen-test` binaries, and tonk's worker crates are cdylibs of their own that link these as `rlib`s. They are now `rlib` only.

What this changes for you:

- `cargo build` no longer emits a `libdialog_network`, `libdialog_repository` or `libdialog_peer` dynamic library. A build that needs one declares its own wrapper crate with that crate type.
- A clean release build of `dialog-peer` overlaps the compilation of `dialog-query`, `dialog-repository` and `dialog-peer` instead of running them one after another.

Why: with a `cdylib` target, cargo waits for every upstream crate's full object code before starting a crate, rather than starting on its metadata. A crate type nobody consumed serialized the slowest part of every build that reaches the peer.

### A certificate a remote refuses as revoked is retracted only on a signed revocation that verifies

[#602](https://github.com/dialog-db/dialog-db/pull/602)

A refusal of a revoked certificate now carries the signed revocation,
and a peer retracts the certificate only once that revocation verifies.
Before, a peer retracted whatever certificate a remote named as revoked,
so a remote could make a holder discard grants that still stand just by
naming them. A revocation checker now returns the revocation document
with its match (`RevocationMatch::proof`, a UCAN container's DAG-CBOR
bytes). The UCAN access service hands it to the holder in
`AuthorizeError::Revoked`'s new `revocation` field, in the container's
base64url tagged form. The peer decodes it and checks four things
before it retracts. The document is a `/ucan/revoke` of the very
certificate named. Its issuer stands above that certificate in a chain
the peer retains. Its signature and proofs verify. Its evidence shows
the issuer could withdraw what it names. A refusal with no revocation,
or with one that fails any check, retracts nothing and is reported as it
came.

What this changes for you:

- `RevocationMatch` has a new field, `proof: Option<Vec<u8>>`. A
  checker that builds one needs `proof: None`, or the document when it
  keeps one.
- `ContainerError::Revoked` is now `{ cid, found: Box<RevocationMatch> }`.
  The revoker that was its `revoker` field is `found.principal`.
- `AuthorizeError::Revoked` has a new field, `revocation:
  Option<String>`. A pattern that spells out its fields needs `..` or
  the field; a constructor needs `revocation: None`.
- A fork refused as revoked by a responder that sends no revocation
  (any responder before this release, and a revocation index that does
  not keep documents) retracts nothing and fails with the refusal, where
  it used to retract the named certificate and retry.
- A revocation the peer accepts must be signed by a `did:key`
  principal, the way the proof walk checks the certificates it admits.
- The UCAN test service (`dialog-remote-ucan-s3`'s `helpers`) takes
  `POST /revocations` with a signed revocation container in the body and
  returns it with each refusal. `POST /revocations/<cid>` still refuses
  the certificate, with no document, as an older service would.
  `REVOCATIONS_PATH` moves from the native-only server module to
  `helpers` itself, so a client test can name it on wasm.

Why: a refusal naming a certificate is a claim, and a holder that acts
on it discards authority it may still have. The signed revocation is the
evidence the claim rests on, and only the holder can tell whether its
issuer was entitled to withdraw that link, since only the holder knows
the chains it retains. A lenient fallback that retracted when no
revocation came would let a remote omit it and be believed anyway.

### A certificate a remote refuses as revoked is retracted, and the request is proved again through another grant

[#598](https://github.com/dialog-db/dialog-db/pull/598)

A peer that retains two grants for the same claim, one revoked and one
fresh, presented whichever the proof walk admitted first, and the walk
admits candidates in content hash order: half the time it presented the
revoked one, and the remote refused a request the peer had every right
to make. A device linked to an account again, after the account revoked
its earlier grant, holds exactly that pair until the revocation reaches
it, which is how this surfaced as a flaky sync.

The refusal now names the certificate. `AuthorizeError::Revoked` carries
`certificate`, the CID text of the revoked link, when the responder
knows which it was (the UCAN access service does). A `Peer` performing
a fork that comes back `Revoked` with a certificate retracts that
certificate where it retains grants, as one commit on the branch it
proves from, and authorizes the fork again; the walk no longer sees the
revoked link and admits the other grant. The loop ends when the request
goes through, when the named certificate is not retained here (the
original refusal is reported, since it names the authority that was
withdrawn), or when the remote names one already retracted.
`Delegations::retained(cid)` finds a retained certificate by the CID a
refusal names, and the new `AuthorizationFailure` trait, implemented by
every effect error that wraps an `AuthorizeError`, is how a caller reads
the refusal back out of an effect's error without matching each enum.

What this changes for you:

- `AuthorizeError::Revoked` has a new field, `certificate:
  Option<String>`. A pattern that spells out its fields needs `..` or
  the field; a constructor needs `certificate: None`.
- A fork refused as revoked retracts the named certificate from the
  peer's access branch and is retried once per named certificate,
  through the peer's remaining grants. A caller that retried on
  `Revoked` itself can stop; a caller that counted requests to the
  access service sees one more per revoked link in a presented chain;
  a caller that lists the grants a principal holds no longer sees a
  grant a remote refused as revoked.
- The retraction takes the remote at its word. Only a certificate the
  peer retains can be named to any effect, and a certificate retracted
  this way comes back if a later sync re-retains it, to be retracted
  again on the next refusal.
- `Delegations::retained(cid)` on a branch reads back the retained
  certificate with that CID, or `None`.
- `AuthorizationFailure::authorization_failure(&self) ->
  Option<&AuthorizeError>` on `ArchiveError`, `BlobError`, `MemoryError`,
  `BranchError`, `PeerError`, `ForkError`, `DialogStorageError`,
  `DialogArtifactsError`, `ResolveError`, `PublishError`,
  `RetractError`, `S3Error` and `AuthorizeError` itself.
- The UCAN test service (`dialog-remote-ucan-s3`'s `helpers`) accepts
  `POST /revocations/<cid>` and refuses that certificate from then on.

Why: the retaining replica is the one that learns of a revocation, and
until it does the revoked grant is a valid candidate on every other
replica. The remote is the first to know; it says which link it refused,
and the peer that asked is in the best position to drop that grant and
try the others rather than report a refusal that a different grant would
have avoided. Retracting rather than remembering the refusal in memory
makes the answer outlive the process and reach the replicas the branch
syncs to, through the one mechanism a grant already leaves a replica by.

### An entry a read yields is its key and value, and the hash of its key is kept by the segment it is edited in

[#600](https://github.com/dialog-db/dialog-db/pull/600)

[#569](https://github.com/dialog-db/dialog-db/pull/569) gave every `Entry` the cell the blake3 of its key is kept in. Only the boundary rules read that hash, on the edit path, but every entry a scan yields carried the cell too: about 48 bytes more per entry, enough that moving an entry through the scan's stream adaptors became a `memcpy` call. The cell now lives in a `SegmentEntry`, the entry as a `TransientSegment` holds it, and `Entry` is a key and a value again. A stored leaf still keeps its keys' hashes for every opening of it, a node sealed from ranked entries still opens with their hashes, and an edit's key hashed before it lands still brings its hash into the leaf. Trees are shaped byte for byte as before.

What this changes for you:

- `TransientSegment::entries`, `entries_mut`, `into_entries`, `into_parts` and `take_entries` hold `SegmentEntry`s. A `SegmentEntry` derefs to its `Entry`, `into_entry` takes the entry out, and `set_value` replaces the value and keeps the key's hash. `TransientSegment::new` takes `SegmentEntry`s, built `From<Entry>`; `upsert` takes either.
- `Entry::hashed` is gone: `SegmentEntry::hashed` and `SegmentEntry::key_hash` read the kept hash. `Entry::key_hash` computes the hash each time it is called, as in 0.3.0.
- `PersistentSegment::from_entries` and `PersistentNodeBody::segment_from_entries` take `Entry`s or `SegmentEntry`s.
- An `Entry` can be built with a struct literal again.

Why: a scan moves every entry in its range and never asks for a hash; the edit path asks for few. Measured on a cold reopen against 0.3.0 the cell cost `number-range-mixed` 6.0% more instructions and `number-range-wide` 3.4%.

### A revoked delegation's match is boxed in `Invalid::Revoked`

[#599](https://github.com/dialog-db/dialog-db/pull/599)

`Invalid::Revoked` carried the revoked delegation's CID and the matching
`RevocationMatch` (a second CID and its issuer) inline, which made every
result of `Invocation::check` and `Chain::check` at least 208 bytes. The
match is now behind a `Box`.

What this changes for you:

- `Invalid::Revoked { found, .. }` is a `Box<RevocationMatch>`. Reading
  its fields (`found.principal`, `found.revocation`) is unchanged; code
  that builds the variant wraps the match in `Box::new`, and code that
  needs the `RevocationMatch` itself takes `*found`.

Why: Rust 1.99's clippy flags the size as `result_large_err`, and every
verification paid to move it, on success as well as on refusal.

### A flush that fails leaves nothing in the held caches, and environments sharing them share branch writers

[#579](https://github.com/dialog-db/dialog-db/pull/579)

A seal keeps the nodes it produces in the node cache before they reach
the archive, and that cache outlives the handle. A commit, merge or pull
whose flush failed left those nodes held for the repository, so a later
read of them was answered from the cache and never fetched. They are now
forgotten when the flush fails. Separately, a session works through its
peer's archive and caches, so its handles of a branch and its peer's now
share one writer: the writers live in `HeldCaches` beside the caches.

What this changes for you:

- A failed flush no longer leaves the repository's scope holding blocks
  its archive never received.
- A peer and its sessions take turns moving a branch's head. `release`
  and `clear` leave writers alone.
- One `HeldCaches` is for one archive: environments over different
  storages must not share a set.
- `NodeCache::forget` and `Delta::keys` are new.

Why: a scope must hold only what its archive has, and every handle that
writes to one archive must take turns.

### A formula derived `#[formula(cached)]` is computed once per input

[#576](https://github.com/dialog-db/dialog-db/pull/576)

A formula says it wants caching where it is derived, and the engine
keeps its outputs by the formula and its input values in the
`FormulaCache` the environment holds, bounded at 4,096 applications and
evicting by SIEVE. A computation that fails is not remembered. The
revision formulas are marked cached, and the process-wide memo of
verified revision records they used is gone.

What this changes for you:

- `Scope` requires `Holds`: an environment a query runs through has to
  hold things, which a wrapper does by forwarding to its operator.
- `FormulaQuery::evaluate` takes the environment. `expand_with` expands
  through a given cache; `expand` still works without one.
- `Formula` gains `resolve_with` and `formula::emit`.
- `HeldCaches::formulas()` is the cache, and `clear()` clears it.
- An environment that holds nothing remembers nothing across
  evaluations.

Why: verification dominates projecting a record and the fixpoint
evaluator re-applies these formulas every round; any costly formula
needs the same, and each hand-written memo was another static nobody
owned.

### Branch writers and in-flight block reads belong to the environment

[#575](https://github.com/dialog-db/dialog-db/pull/575)

The writers of open branches and the in-flight block GETs of the S3 and
filesystem transports were process-wide statics: they outlived every
environment, and two environments in one process shared them whether
they meant to or not. A branch's writer is now held by the environment,
and each `S3` and `Fs` site owns its in-flight GETs, so readers join one
another's requests only through the same site.

What this changes for you:

- `S3` and `Fs` are no longer `Copy`, and are built with `default()`.
- `Holds` gains `held_or`, which `Holdings` answers under its lock, and
  `held_key::<T>(name)` names a key by its type. An environment that
  forwards `Holds` should forward `held_or` too, or two first opens at
  once may each make their own writer.
- Two environments in one process that hold nothing in common no longer
  share a writer or join each other's block reads. A race between them
  as one issuer is refused by a merging commit rather than minting an
  edition twice.

Why: none of the three registries had an owner, so none could be
scoped, bounded or released with the work it served.

### A branch's caches are held by the environment it is opened through

[#574](https://github.com/dialog-db/dialog-db/pull/574)

Tree nodes, spilled values, verified revision records, causal verdicts,
contexts, plans, rule sets, the live spine and the memo of trees
rewritten under the number band were made with each branch handle and
died with it, so a service that opened the branch per request reused
nothing. The environment now holds them as one `HeldCaches`, each at the
width it is sound to share: tree nodes under one byte budget for every
repository, each reading through its own scope; what is addressed by
content or version per repository; what is tagged by a head per branch.
Opening a branch again through the same environment finds them warm.

What this changes for you:

- `OpenBranch::perform` and `LoadBranch::perform` require the
  environment to implement `Holds`. Peers and sessions do. A bare
  `Volatile` holds nothing, so a branch opened on it directly starts
  cold every time, as before.
- Build a peer `.with(HeldCaches::with_budget(bytes))` to bound its node
  cache. `peer.caches()` gives `release(&repository)`, `clear()` and
  `bytes()`. A session shares its peer's caches unless given its own.
- Code that reopened a branch to get cold caches has to release them
  first: `HeldCaches::of(env).release(&repository)`.
- The process-wide root-manifest memo is gone.
- The per-repository caches accumulate until the repository is released
  or the environment dropped.

Why: whoever owns the environment decides how long its caches live, and
can bound or drop them; a cache nobody owns can be neither.

### The node cache is bounded by bytes and shared between scopes

[#573](https://github.com/dialog-db/dialog-db/pull/573)

`NodeCache` is its own type. It holds up to a budget of stored node
bytes (`NODE_CACHE_BUDGET`, 128 MiB, unless built with another), evicts
by SIEVE, allocates nothing until a node is kept, and serves a node
larger than it can keep without keeping it. A handle has a `Scope` and
is answered only with nodes its scope holds: `scoped` hands out a handle
on the same cache for another scope, and `release` forgets what a scope
held. A node two scopes hold is kept once.

What this changes for you:

- `NodeCache` is no longer an alias of `Cache`. Build it with
  `NodeCache::new()` or `NodeCache::with_budget(bytes)`.
- A cache that held 2,048 nodes of any size now holds 128 MiB of node
  bytes, whatever their count. What a node derives from its bytes
  (decoded keys, key hashes, its summary) is not charged, so resident
  memory can run to about twice the budget.
- `Cache<K, V>` is unchanged and still serves the count-bounded caches of
  small values.

Why: nodes differ in size by orders of magnitude, so a count bounded
nothing; and a cache that repositories share must never answer one with
a block only another holds.

### Measurement counters are compiled in by an `audit` feature

[#570](https://github.com/dialog-db/dialog-db/pull/570)

The search tree counted every hash its shaping paths asked for, and the
volatile archive looked each block up before storing it to count
duplicates, in every build. Both now exist only with the `audit` feature
of `dialog-search-tree`, which turns on `dialog-storage`'s.

What this changes for you:

- `dialog_search_tree::audit::report` and
  `dialog_storage::dup_audit::drain_sets` exist only with the `audit`
  feature. The public counter statics are gone.
- `Volatile` no longer reads a block before storing it unless the
  feature is on.

Why: measurement plumbing should cost nothing in a build that does not
measure.

### The hashes a tree is shaped by are kept by what owns the bytes, not in per-thread memos

[#569](https://github.com/dialog-db/dialog-db/pull/569), fixes
[#566](https://github.com/dialog-db/dialog-db/issues/566)

The boundary rules decide from the blake3 of a key or a separator, and
ask again on every regroup. Two thread-local memos answered them: every
thread that committed filled its own and kept it for the thread's life,
with no way to see, bound or release it. A full one held about 46 MiB at
257-byte keys, per thread. Now an entry in a segment keeps the hash of its key, a
separator its own, and a stored node the hashes of its keys and its
piece summary, each computed on first ask and dropped with its owner. A
node a persist seals is kept in the tree's node cache. Trees are shaped
byte for byte as before; nothing stored changes.

What this changes for you:

- The entries a `TransientSegment` holds keep their key's hash, so
  replace one whose key changes rather than assigning the key in place.
- `Link::separator` is a `Separator`, which derefs to `[u8]` and is
  built `From<Vec<u8>>`. `Link` no longer derives its rkyv form.
- `Distribution::rank`, `seam_rank` and `leaf_cut` take a `Hashed<'_>`:
  the bytes with wherever their hash is kept. `Hashed::from(&bytes)`
  wraps bytes with none.
- `TransientNode::persist` and `persist_mut` take the node cache the
  sealed nodes are kept in.
- A process that commits on many threads no longer grows a memo per
  thread.

Why: a memo with no owner cannot be bounded or released, and one per
thread multiplies whatever it holds by the size of the thread pool.

### Compatibility and migration

- Nothing stored or synced changes for the file store's lock; only the
  lock files beside its cells do. A lock file the previous release left
  behind holds a bare PID, which this release reads as a holder: one
  left by a writer that crashed is recovered like any other, and one
  whose writer is still running is held. Only a writer on the previous
  release that is still running excludes nothing: it and a writer on
  this release do not exclude each other while both write one store
  during an upgrade, since the older one deletes a lock file it cannot
  read as a PID as stale, and this release's is `pid token` (and for a
  cell whose name has an extension, the two releases lock different
  files). Stop writers on the previous release before starting ones on
  this release.
- A registry branch (`meta`) or host state branch an earlier release laid
  out is rewritten under the number band the first time this release
  writes it, as `Branch::migrate_storage` rewrites any branch: one pass
  over its entries, published as a revision that descends from the head.
  0.2.0 and earlier refuse the rewritten tree by name, as they refuse any
  migrated tree. An embedder runs nothing.
- Nothing stored or synced changes for the refusal of a rival head of
  one version (#705): nothing is rewritten on upgrade, and heads, trees
  and the wire format are as before, so an older release reads all this
  one writes. An older release still takes a rival head as seen, so a
  write it races with one of this release under the same key can still
  be dropped there; upgrade every replica that shares a key.
- Nothing an existing space holds changes or is read differently for the
  single-file archive: it is a new provider a space opts into. A space
  opened with `FileArchive` does not read an `fs` archive's directory of
  blocks, and the reverse; moving a space between the two means copying
  its blocks with `Import`. The blocks are the same bytes in both, so an
  older release reads any block this one stores.
- Positions already stored keep their bytes and their order: nothing is
  rewritten, and both releases read every position the other writes. Only
  newly derived positions differ. An older replica and this one derive
  different positions for the same insert, so the same insert made on both
  no longer converges to one entry; each still lands between the
  neighbours it named.
- Nothing stored or synced changes for the node cache moving to the
  store: trees, nodes and their hashes are the same bytes, nothing is
  rewritten on upgrade, and an older release reads everything this one
  writes. Only how often a node is read from the archive changes.
- Nothing stored changes for the difference that prunes shared
  subtrees: no stored data is rewritten, trees are laid out as before,
  and an older release reads everything this one writes. Only how many
  blocks a comparison reads and reports changes.
- An older replica holds heads that carry their whole context inline,
  or none, and trees with no context region. Nothing is rewritten on
  upgrade: those heads verify under their own payload shapes and pull as
  before. The first head this release mints on top of one writes the
  whole map into its tree's region, once, in the same batch as its
  revision record.
- An older release cannot verify a head this release mints: its signed
  payload ends in a `0x03` marker and the head's own count, which the
  older release does not know, so its pull refuses the head as an
  invalid signature. Upgrade every replica that pulls from one running
  this release. Nothing an embedder runs is needed beyond that.
- A head minted by a build of this release from before heads carried
  their count ends in a lone `0x02` marker. No release or pre-release
  tag carries such a build (none contains #625's commit), so these heads
  exist only where an untagged build of `main` ran, and the `0x02` shape
  can be retired before the release. Until then it still verifies and
  pulls, nothing is rewritten, and a reader derives its count from its
  region.
  A commit on such a head of another origin reads the head's region for
  the count, as those builds did, and the head it mints carries one.
- `UcanAddress` encodes an optional `socket`, left out when absent, so an address without one keeps its bytes. An older replica reads an address that names a socket and drops the socket, reaching the peer over requests.
- Invocations now carry `iat`. A service from this release refuses a stale or replayed invocation (`AuthorizeError::Stale`, `AuthorizeError::Replayed`), and over HTTP still serves one without `iat`, so an older client keeps working against it; over a socket `iat` is required, and an older client never opens one.
- The socket carries text `ping` and `pong` messages beside its frames. A service's socket must answer `ping` with `pong`, or a client closes the connection every `Liveness::deadline`; no released service has a socket, so none predates this.
- A conflict's `412` now has a body, and `Rejection::Unavailable` may carry `retry_after`. An older client ignores both; this client reads an older service's bare `412` as a conflict that does not say what the cell holds.
- Nothing stored is rewritten.
- Nothing new is stored by the rewrite fix: a blob-index entry keeps the
  key and value it had, so the tree reads as the older release wrote it
  apart from the numbers the rewrite moves. A branch whose migration
  failed on such an entry wrote nothing, and migrates on upgrade.
- Nothing new is stored. A certificate a remote refuses as revoked is
  retracted with the ordinary delegation retract, the same facts and
  blob tombstone a `Delegations::retract` by hand writes, so an older
  release reads the branch as before. The `certificate` field is
  additive on the wire: an older access service answers `Revoked`
  without it, and a peer on this release reports that refusal as
  before, without retracting or retrying. An older peer reading this
  release's answer ignores the field.
- Nothing stored changes for a refusal's revocation either. The
  `revocation` field is additive on the wire: an older access service
  answers `Revoked` without it, and a peer on this release reads that
  refusal and retracts nothing. An older peer reading this release's
  answer ignores the field and retracts the named certificate on the
  service's word, as it did before.

## 0.3.0 (2026-10-13)

### Every number is keyed in one band, and a branch an earlier release wrote is migrated in one pass

[#593](https://github.com/dialog-db/dialog-db/pull/593),
[identity, storage and notation](./notes/identity-storage-notation.md#numbers-one-band-typed-views)

A tree's manifest now sets `value_encoding` to the number band: every
number is keyed in one band of the value index, in dialog-number's
decimal encoding, the encoding identity already used. `3`, `3.0` and
the natural `3` are one value in the index as they are in identity, and
a range over a numeric attribute is one scan rather than three. The
manifest field is critical, so a release before the band refuses a
migrated tree by name rather than misreading it.

A branch an earlier release wrote is read, never extended:
`Branch::migrate` rewrites its head's tree under the band
(`migrate_storage`) and then re-installs its rules (`upgrade_rules`),
one call for everything an earlier release stored. The rewrite is
deterministic, so two replicas that migrate the same head produce the
same tree, and a pull that meets a head an old client wrote rewrites
it in memory before merging, so the old client's writes are not lost.

What this changes for you:

- A commit on a branch whose head an earlier release laid out fails with
  `CommitError::NeedsMigration` and writes nothing. Run
  `Branch::migrate` (or `migrate_storage`) first; `Branch::needs_migration`
  says whether a branch is in that state, and `storage_encoding` which
  layout its head has.
- A number read back raw (an `ArtifactSelector` select, a `Value` match)
  comes back as the narrowest type that holds it: a natural when it is
  whole and not negative, an integer when whole and negative, a float
  otherwise. Code matching `Value::UnsignedInt` on counts and timestamps
  still sees them; code matching `Value::Float(5.0)` on a stored `5.0`
  now sees `UnsignedInt(5)`. A typed read (a concept field, a derived
  struct) views the number as the type it declares, unchanged.
- An `ArtifactSelector` range bound reads each number through the lens
  its type names before comparing, as a field declared with that type
  reads it: an integer bound sees `34.5` as `34`, a natural bound does not
  see `-3`, a float bound sees every number as it is, and
  `across_numbers()` compares every number as it is for any bound.
  Before, a bound admitted only numbers written with its own type; a
  natural bound `>= 20` now finds a stored `30.0`.
- An exact number, in a selector's `is(..)` or a query's constant, is the
  range of that one number read through the same lens: `is(4)` as an
  integer finds the stored `4`, `4.0` and `4.3`; `is(4.0)` as a float
  finds the number `4` alone. This holds on the cardinality-one election
  too, which compared the constant with the stored value by type and so
  missed `+41` against a stored natural `41`.
- A cardinality-one assertion of a value its cell already reads as,
  through the lens of the asserted value's type, is a no-op
  (`Value::reads_as`): asserting the integer `4` over a stored `4.3`
  writes nothing, since an integer reader observes `4` already; asserting
  `5` supersedes it.
- A float is the decimal it prints as, in memory as in the band:
  `Value::viewed_as` reads `1e30` through an integer lens as the natural
  `10^30`, not the float's binary value, and `Value::same_number` says
  two values are one number exactly when the band keys them as one, so
  `2^64` as a float is the natural `18446744073709552000` and not
  `2^64`. Before, the in-memory views and the stored identity disagreed
  past `2^53`.
- Rows a query derives are one row per number: two rules deriving the
  natural `3` and the float `3.0` for one relation yield one row, through
  a union and through a fixpoint alike, as two such facts are one fact in
  storage; an `all` pick tells values apart the same way
  (`Value::identity_key`, the band's encoding of a number). Before, the
  rows were keyed by type as well, and a `number:` reader saw both.
- A number constant finds its matches on every path: the cardinality-one
  lookup with the entity unbound compared the constant with the elected
  claim by type, and a variable bound to a stored `4.3` refused the
  constant `4` that arrived after it; both now read through the lens
  (`Value::reads_as`), in either order.
- A concept write (`ConceptDescriptor::edit`, the derive's builders)
  writes each field under the field's pick, as an attribute statement
  spelling that pick does: a `last` field's write succeeds the claim its
  cell holds, and is the no-op above when the cell already reads as the
  written value. `Attribution` carries the field's `pick` in place of its
  `cardinality`. Before, every concept write was an `all` write, so a
  cardinality-one field accumulated a claim per write; reads elected the
  newest, so they read right while the cell grew.
- The band and the identity carry a number of any size (dialog-number
  keys any decimal), but `Value` does not yet: it holds `u128`, `i128`
  and `f64`, so no wider number enters, and a stored number wider than
  `u128` reads back as the nearest float. A `Value` variant holding a
  `dialog_number::Number`, with the fixed-width types as views over it,
  is planned for the next cycle; it changes no key and no identity, so
  nothing stored now will need rewriting for it.
- `ValueDataType::holds` reads the same for every number type: a type
  holds a number its view reads unchanged, so `float:` holds the natural
  `3` and `natural:` the float `3.0`, as `float:64` and `natural:8`
  already did; `natural:` still refuses `2.5` and `-1`. Before, an
  unsized type held values of its own type only.
- `tree::rekey::rekey_tree` and `rekey` rewrite a tree or a key between
  encodings; `Branch::rekey_storage(0)` lays a branch out the earlier
  way, for tests of the migration.
- A pull that merges a head an earlier release wrote rewrites it in
  memory first, so both sides share one encoding and the old client's
  writes merge in. A pull that fast-forwards adopts such a head as it is,
  reading nothing, and the branch then needs migrating like any other.

Why: identity and storage should agree on what a number is, a numeric
range should be one scan, and the move should happen once, with the
rule and identity upgrades, rather than as a second migration later.

### A selection reads a bare constant as the type its field declares

[#593](https://github.com/dialog-db/dialog-db/pull/593)

`select(ConceptQuery)` now conforms each constant to the type its field
declares, as compiling a rule already did for a premise's constants: a
bare text `"home:"` under a `text:` field is the text, not an entity,
and a bare `5` under `integer:` is a signed integer. A constant `this`
is an entity.

What this changes for you:

- A query whose constant was written bare in JSON matches the claims
  the field holds. Before, `"url": "home:"` under a `text:` field read
  as the entity `home:` and matched nothing.
- Every evaluation of a `ConceptQuery` conforms its constants, through
  `select`, a plan's concept premise, or the query's own `evaluate`.
  Before, only `select` did, and a plan built by an embedder read `+41`
  under a `natural:` field as a signed integer that matched nothing.
- A constant in a field with no declared type still reads as the type
  its spelling suggests.
- A bare text constant shaped like a URL (`https://example.com`) is
  still read as an entity first, and comes back as the URL's normalized
  text (`https://example.com/`). Write such a constant tagged,
  `{"text:": "https://example.com"}`, until the reader takes every bare
  string as text.

Why: a constant written bare has no type of its own; the slot it fills
does, and reading it any other way makes a query miss what it names.

### Rule bodies store the canonical spelling

[#593](https://github.com/dialog-db/dialog-db/pull/593),
[identity, storage and notation](./notes/identity-storage-notation.md#body-format-3)

A rule's `dialog.rule/source` body is now written in format 3: the
rule's canonical spelling, the one its identity hashes, in the dag-cbor
envelope earlier formats used. The body holds exactly what the identity
holds, so every spelling of one rule stores the same bytes, and a
rule's body is found by encoding the rule.

What this changes for you:

- `DeductiveRule::decode` and `InductiveRule::decode` of a format 3
  body give the rule in its canonical spelling: locals and head fields
  named `~0`, `~1`, …, a premise's fields `#0`, `#1`, …, premises
  sorted, no descriptions. A query still binds the head by its own
  concept's field names. The names and descriptions an author wrote
  are not stored.
- Installing another spelling of an installed rule stores no second
  body, and retracting either spelling removes the rule's body.
  Before, two spellings stored two bodies under one entity, and
  retracting one left the other.
- A rule with a constant no body can carry is refused when it compiles
  with `TypeError::UnencodableConstant`, naming the constant: a NaN or
  infinite float, a natural above `u64::MAX` or an integer outside
  `i64`. Before, such a rule compiled and panicked when stored.
- `Branch::upgrade_rules()` also rewrites every body of an earlier
  format into format 3, and reports those already under their identity
  in `RulesUpgraded::rewritten`.

Why: a stored rule should be one record that is a function of the
rule, so equal rules store equal bytes and a retraction finds what an
install wrote. The authored spelling is display data, and storing it
beside every rule measured at 35% to 75% more instructions per
install; it waits on a cheaper form.

### NaN is not a value

[#593](https://github.com/dialog-db/dialog-db/pull/593),
[identity, storage and notation](./notes/identity-storage-notation.md#decided)

A NaN float is refused wherever a value enters dialog, rather than
stored as a number that is not equal to itself. Infinities stay
values.

What this changes for you:

- Deserializing a `Value` from a NaN fails with "NaN is not a value",
  bare or tagged (`{"float:": NaN}` in dag-cbor), as does parsing the
  string `float:NaN` into a `Value`. No type `holds` a NaN, so a typed
  write refuses it, and a commit that would write one by a raw assertion
  fails with `CommitError::Artifact(InvalidValue("NaN is not a value"))`
  before anything lands.
- `float/parse` of `NaN` is no row.
- A float formula whose result would be NaN (`0.0 / 0.0`,
  `inf - inf`) yields no row, as integer division by zero already did.
- A rule constant of NaN is refused when the rule compiles (see above).

Why: a database value needs an equality it can join and retract by.
The absence of a claim already means "no number"; a value that exists
only to say so, and that IEEE says is unequal to itself, had to be
special-cased in equality, in the `float:32` view and in the number
encoding to behave at all.

### Attributes can declare `number:` and fixed-width number types

[#593](https://github.com/dialog-db/dialog-db/pull/593),
[identity, storage and notation](./notes/identity-storage-notation.md#number-types)

An attribute may declare `as: number:`, which holds every number, or a
fixed-width number type: `natural:8` to `natural:128`, `integer:8` to
`integer:128`, `float:32` and `float:64`, one for each Rust number.
`natural:`, `integer:` and `float:` stay as they were, without a width.
A fixed-width type is a view over the stored number. On read, it coerces
where it only loses precision and leaves out what it cannot hold:
through `natural:8`, a stored `9.75` reads as `9`, and `300` and `-1`
are not rows. A write refuses a value the type cannot hold. Values are
still stored as naturals, integers and floats, as before.

What this changes for you:

- `#[derive(Attribute)]` on a fixed-width Rust number declares its
  sized type: `struct Level(u8)` declares `natural:8`, and `f32`
  declares `float:32`. The identities of these attributes, and of the
  concepts and rules that use them, differ from those declaring
  `natural:`, `integer:` or `float:`. `usize` still declares `natural:`,
  because it has no width common to every target.
- `Typed::DECLARED` gives the type an attribute over a Rust type
  declares. `TypeDescriptor::TYPE`, and the kind of a `Term<u8>`, are
  unchanged.
- `ValueDataType` gains `Number`, `U8` to `U128`, `I8` to `I128`, `F32`
  and `F64`. No stored value has these types: they type attributes and
  query slots. `ValueDataType::holds(&value)` says whether a type admits
  a value, and `Value::viewed_as(kind)` reads a value through one.
- The query type system gains `Refinement::views`, sent as `"views"`
  on the wire.
- A view reads the same on every target: rounding to `f32` is pinned
  bit for bit on native and WebAssembly.

Why: a Rust field of `u8` cannot hold `300`, and an app should be able
to say so in the schema rather than fail on read. `number:` gives an
attribute that holds numbers of every type, which JSON already writes
as naturals, integers and floats.

### Rules are identified by merkle references

[#593](https://github.com/dialog-db/dialog-db/pull/593),
[identity, storage and notation](./notes/identity-storage-notation.md#plan)

A rule's identity is now the merkle reference of its canonical
spelling under `rule:`, not the BLAKE3 hash of its dag-cbor. The head
is its concept by reference with each field bound to its canonical
variable; `when` and `unless` are sets of premises; a concept premise
is its concept by reference with each binding keyed by the field it
binds (attribute and options), not by a field name; other premises
and reduce clauses are their wire form read as a merkle structure,
with constants identified by value.

What this changes for you:

- Every rule identity differs from 0.2.0's and from 0.3.0-rc.1's.
  `Branch::upgrade_rules()` re-installs stored rules under the new
  identities.
- An attribute's description no longer changes the identity of a rule
  that uses it, as it did when rules embedded their concepts' wire
  form.
- A rule's numeric constants are identified by value: a rule with the
  constant `5` written as a natural, an integer or a float is one
  rule.

Why: a rule's identity should depend on what it means, and the
concepts it uses should enter by reference, so a rule is the same
whether they are embedded or linked.

### Attributes and concepts are identified by merkle references

[#593](https://github.com/dialog-db/dialog-db/pull/593),
[identity, storage and notation](./notes/identity-storage-notation.md#identity-merkle-reference-v2),
[merkle-reference v2](./rust/merkle-reference/spec.md)

An attribute's and a concept's identity is now a merkle reference
(`merkle-reference`, a new crate in this workspace), not the BLAKE3
hash of their dag-cbor. Types are named by URI; the crate defines only
the shapes of composites (`list:`, `map:`, `set:`), and dialog names
its own scalar types (`text:`, `boolean:`, `number:`, `entity:`,
`symbol:`, `record:`). A definition is hashed as a map from field
names under its own type (`the:`, `concept:`), and a concept hashes its
attributes' and its conformance targets' references, so it is the same
whether they are embedded or linked. The entities keep their shape,
`the:<base58>` and `concept:<base58>`, with other digits.

What this changes for you:

- Every attribute and concept identity differs from 0.2.0's and from
  0.3.0-rc.1's. Rule identities change too (see above).
- `AttributeDescriptor::hash()` and `ConceptDescriptor::hash()` are
  `reference()`, returning a `merkle_reference::Reference`.
  `to_cbor_bytes()` is gone from both. `AttributeDescriptor::parse_uri`
  returns a `Reference`.
- `Value::reference()` gives a value's identity. Every number has one
  `number:` type and is identified by its value alone, in the encoding
  of the new `dialog-number` crate (SQLite4's decimal key encoding): a
  natural `5`, an integer `5` and a float `5.0` are one value, a float
  is the decimal it prints as (`1e23` is 10²³), and `-0.0` is `0`. An
  attribute's `as:` list hashes values this way.
  The value index still keeps naturals, integers and floats apart, so
  until it moves to one band, two attributes that differ only by
  listing `5` or `5.0` share an identity but read different claims.
- `dialog_artifacts::identity` and `dialog_query::identity` hold the
  types dialog hashes under.

Why: identity should be a function of what a definition means, cheap
to compute, and independent of where its parts are stored. This is the
first step of moving every identity to merkle references in one
migration.

### Constants name their type by an entity

[notation](./notes/notation.md#constants)

A value in a binary encoding is written as `{"<type entity>": payload}`:
`{"text:": "foo:"}`, `{"entity:": "foo:"}`, `{"integer:": 5}`.

What this changes for you:

- Rule bodies and identities: text, an entity and a symbol of the same
  spelling, or a natural and a signed integer of the same number, are
  different constants. A committed rule matching text shaped like a URI
  derives again.
- Rule bodies are format 2. Formats 0 and 1 are still read, and a
  constant in a field with a declared type takes that type.
- `Changes` batches keep each value's type through dag-cbor. Integers
  used to come back as floats.
- JSON writes a value bare where the bare form reads back as the same
  value, and tagged otherwise. It reads both forms.
- `Value::untagged()` gives the bare form, for encodings that must keep
  an earlier spelling. `Value::conform(kind)` converts a value to a
  type that reads it.

Why: an encoding that leaves out a constant's type makes the meaning
depend on how its spelling looks, and lets two different rules share
one identity.

### Picks are named by entities

[glossary](./notes/glossary.md#pick)

A pick is named by an entity, as a type is: `last:`, `all:`, `top:`,
`max:`, `min:`.

What this changes for you:

- Descriptors write `"pick": "all:"`, and the 0.2.0 spelling `"all"`
  is still read. Batches serialize `Pick` the same way.
- Attribute identities hash the entity, so every attribute, concept
  and rule identity differs from 0.2.0's.
- `Pick::uri()` returns the entity. `name()` and `Display` are
  unchanged.
- The derive takes `#[pick(last|all|max|min)]`. `#[cardinality(..)]` no
  longer compiles, and the error names its replacement.

Why: a notation that resolves every bare word needs `pick: all` to name
something. Doing this before anything ships on 0.2.0 means identities
change only once.

### Compatibility and migration

- From pre-0.2: unchanged, `Branch::upgrade_rules()` lands on this
  release's identities.
- From 0.2.0 (nothing shipped on it): `upgrade_rules()` re-installs its
  rules but does not move its transient markers.
- 0.2.0 cannot read descriptors this release writes. Stored claims are
  not rewritten.
- Rule bodies from 0.2.0 or earlier wrote constants bare. On upgrade,
  each constant takes its field's declared type. A constant in an
  untyped field keeps the type its spelling suggests. Text shaped like
  a URL may come back with its URL normalization (`https://a` becomes
  `https://a/`).
- Every rule with a constant, and every attribute with an `as:` list,
  has a new identity. `upgrade_rules()` re-installs those rules.
- Every attribute and concept has a new identity, from 0.2.0 and from
  0.3.0-rc.1 alike. Nothing stored is rewritten: definitions recorded
  under their old entities (tonk's `db.attribute/*` and concept facts)
  need re-recording under the new ones, as tonk's `upgrade_definitions`
  does for pre-0.2 replicas.
- An attribute derived over a fixed-width Rust number (`u8` to `u128`,
  `i8` to `i128`, `f32`, `f64`) has a new identity, as do the concepts
  and rules that use it. Its stored claims are not rewritten, but they
  read differently: a stored number of another type now reads through
  the view (a float `9.75` under a `u8` field reads as `9`, where it was
  no row), and a value the Rust type cannot hold is left out, where it
  failed the read. 0.2.0 cannot read the sized types.
- Values in tonk's dag-cbor and IPLD conversions come out tagged. Use
  `Value::untagged()` where an identity minted from values has to stay
  as it was.
- Rule bodies are written in format 3, the canonical spelling. Bodies
  of formats 0 to 2 are still read, and `Branch::upgrade_rules()`
  rewrites them in the same pass that re-keys rules; until it runs, a
  body an earlier release wrote is read as before and a retraction of
  the rule it holds leaves it behind. 0.2.0 refuses format 3 bodies by
  name.
- A stored NaN from an earlier release is not rewritten. It reads as it
  did; nothing new can be written beside it.
- Every tree an earlier release wrote keys each number type in its own
  band. It is read as before, and `Branch::migrate` (or
  `migrate_storage`) rewrites it under the one band, facts and history
  alike, as a new revision; until then a commit on it fails with
  `NeedsMigration`. A pull that merges rewrites a legacy head in memory;
  one that fast-forwards adopts it as it is. 0.2.0 and earlier refuse a migrated
  tree: its manifest carries the critical field `value_encoding`
  (`0x05`), which they do not know. An embedder runs `Branch::migrate`
  once per branch when it opens it; it rewrites the whole tree, so the
  cost is one pass over the branch's entries.

## 0.2.0 (2026-10-09)

### Relations, types and picks have one name each

[#592](https://github.com/dialog-db/dialog-db/pull/592),
[glossary](./notes/glossary.md)

The words the code, the notation and the notes use are now the same. A
**relation** is the named set of claims `the` denotes. An **attribute**
is a relation qualified by a value type and a pick. A **pick** is which
of an entity's claims a read returns. A **type** is named by an entity,
`text:` rather than `Text`. Stored rule bodies name the format they are
written in.

What this changes for you:

- `dialog_artifacts::Attribute` is `dialog_artifacts::Relation`.
  `DialogArtifactsError::InvalidAttribute` and `ReservedAttribute` are
  `InvalidRelation` and `ReservedRelation`, and `ATTRIBUTE_LENGTH` is
  `RELATION_LENGTH`.
- In `dialog_query`, the validated relation name the `the!` macro
  builds is `Relation` (it was `The`), and what an attribute's `the`
  holds, one relation or a keyed collection, is the enum `The`
  (`The::Relation(..)`, `The::Collection { .. }`; it was `Relation`,
  with an `Attribute` variant). `dialog_query::ArtifactsAttribute` is
  `ArtifactsRelation`, and `Reach::Attribute` is `Reach::Relation`.
- One type, `dialog_artifacts::Pick` (`Last`, `All`, `Top(..)`, `Max`,
  `Min`), replaces both `dialog_artifacts::Policy` and
  `dialog_query::Select`. `AttributeDescriptor::select()` and
  `with_select(..)` are `pick()` and `with_pick(..)`;
  `without_select()` and `select_error()` are `without_pick()` and
  `pick_error()`. `TypeError::SelectPolicy` is `TypeError::UnfitPick`
  and `CommitError::Policy` is `CommitError::Succession`.
- A descriptor writes `pick:` where it wrote `select:`. `select:` is
  still read.
- A value type is written as its entity: `text:`, `integer:`,
  `natural:`, `float:`, `boolean:`, `bytes:`, `entity:`, `symbol:`,
  `record:`. `Text`, `SignedInteger`, `UnsignedInteger` and the other
  earlier names are still read and never written. `ValueDataType::uri`,
  `legacy_name` and `named` convert between them. Its `Display` is
  unchanged.
- A stored rule body is `{ format: 1, rule: <descriptor> }`.
  `dialog_query::rule::body::FORMAT` is the format this release writes.
  A bare body reads as format 0, and a body in a later format is
  refused with an error that names its format.

Why: three names (attribute, `The`, `Relation`) were used for the set
of claims `the` names, and "attribute" also meant the typed, picked
read of it, so the code could not say which one it meant. Types spelled
as strings were the one identifier in a descriptor that was not an
entity. A stored rule body had no way to say what shape it was in, so a
later release could only guess.

### Writes succeed the claim the pick elects

[#583](https://github.com/dialog-db/dialog-db/pull/583)

A write through an attribute read under `last`, `max`, `min` or `top`
no longer replaces its cell. It succeeds one claim: the one a read under
the same pick returns. The tree elects that claim among the cell's
stored claims in the same descent that writes the value, retracts it,
and records the new claim with the elected claim's versions as its
cause, as a replacement did. Every other claim in the cell stays. Where
a rule derives the relation, the transactor settles the write first
against the derived candidates the tree cannot see, through the
commit's own view. A write under `all` appends, as before.

The guarantee this gives is transactional. A transaction is a commit
that has not been flushed: querying it returns what querying the
committed result would, every write on top is squashed into that one
commit, and an assertion succeeds whatever a reader at that point
would have observed. `all` is the one pick that asserts without
succeeding anything.

What this changes for you:

- A cardinality-one write used to retract every prior claim of its
  cell (the former `Instruction::Replace`). It now retracts the one a `last` read
  would have returned. An older concurrent claim that lost the
  election stays live. No `last` read can tell the difference; an
  `all` read over the same relation can, and will now see it.
- If the read elects a candidate a rule derives, nothing is retracted:
  a derived candidate is not a claim. The write lands beside it and
  competes under the pick.
- Writing a value a read under the pick already returns writes
  nothing: the revision's tree does not move. Writing a value the cell
  holds as a claim the read does not return succeeds the claim it does
  return, and folds the commit's version into the held claim, so the
  written value stands past the claim it succeeded and a read returns
  it. Under `last` the write is the newest fact of the cell, as a
  last-writer-wins write is.
- Every write carries its pick. `Change::Assert(Value, Pick)` and
  `Instruction::Assert(Artifact, Pick)` are the one write form, beside
  `Retract`; `Change::Replace`, `Instruction::Replace`, `Change::Succeed`
  and `Instruction::Succeed` are gone. `dialog_artifacts::Pick` is the
  pick an attribute is read under (`Last`, `All`, `Top(..)`, `Max`,
  `Min`); `Update::associate` takes it as its fourth argument, and
  `associate_unique` and `succeed` are gone with the variants they
  wrote. A write under `All` appends; any other pick succeeds the
  claim a read under it returns. A `Changes` batch serializes each
  assertion with its pick, a shape older readers do not know. Batches
  are not stored or sent between replicas today, so this bites only
  code that encodes a batch itself.
- The reset `Replace` performed, retracting every claim of the cell
  whatever a reader observed, has no spelling any more. A caller that
  wants it retracts what it observed under `all` and asserts.
- `Branch::commit` and `Snapshot::commit`, which took a stream of
  instructions, are no longer public: they ran no induction and saw no
  derived candidate, so a write through them could land a value
  without succeeding what a reader observed. Every write goes through
  a transaction. A caller holding instructions collects them into a
  batch (`Changes` implements `FromIterator<Instruction>`) and
  integrates it: `branch.transaction().integrate(changes).commit()
  .publish()`; an empty commit is `branch.transaction().commit()
  .allow_empty().publish()`.
- What a write observes is the line and the writes before it in its
  own transaction, in order. A cell's writes are replayed in that order
  at commit: a write under `all` after a write under `last` stands
  beside it, and a write under `max` after an `all` write succeeds the
  staged claim it elects. An inductive rule's head observes the round
  view it fired on.
- A transaction reads what its commit will leave. Reads and the commit
  share one settlement: the log is settled in order against the line
  and the writes before each one, the first read settles what is
  logged so far, a later read or the commit settles only the writes
  logged since, and the result is kept while the lines read stay the
  same. So the claim a write succeeds is already gone from
  `transaction.query()`, and what a read showed is what lands.
  Two `last` writes of one cell in one transaction leave the later one
  alone, never both and never a hash-decided one, and a staged claim a
  later write retires or retracts leaves no tombstone: it never
  reached the line.
  A staged write stands at the edition the commit will mint, equal to
  every other write of the transaction, as the claims of one commit
  are; what order the transaction wrote them in is settlement's
  business alone, never a reader's.
- One departure remains: commit-time induction runs at commit, so what
  an inductive rule would derive from the transaction's writes is not
  in `transaction.query()` yet. That was true before this change.
- The session overlay is the newest facts. An overlay row stands
  past the edition the next commit mints, above every committed claim
  and every staged write, so a `last` read returns it over a committed
  claim of the same cell (it used to lose to any committed claim, and
  could only shadow one by retracting it first), and `max`, `min` and
  `top` rank it with the rest. A commit never takes an overlay row
  back: a succession the read resolves to an overlay row retires
  nothing and stands beside it, as beside a candidate a rule derives,
  and a transaction reads the overlay above its own writes. A
  transaction that retracts an overlay row takes it off the overlay
  when the commit publishes, so the row stays gone after the commit as
  it was in the transaction's reads. A cell the overlay holds is
  settled by the transactor, not the tree.
- Concurrent `last` writes settle by commit. Candidates of equal
  edition order by version hash before value, in the tree, the
  transactor and every read alike, so when two replicas' commits meet,
  one commit's writes win every cell they share, never some cells each.

Why: `Replace` encoded one pick, last-writer-wins, in the write path,
while reads had grown four. A write is a claim that succeeds what the
attribute currently stands for, and what it stands for is the pick's
to say. Making the write follow the read is what lets a relation be
read one way and written another: declare two attributes over it.

### A derived value stands by the fact that bound it

[#583](https://github.com/dialog-db/dialog-db/pull/583)

Under `last`, a value a rule derives competes with stored claims by
standing. Its standing is now that of the fact that bound its value,
carried across every concept boundary with the value. It used to be
the newest fact the rule's body touched, so an unrelated input landing
could make a derived value look fresher than the stored claim it was
competing with. A value a formula computes cites no fact of its own
and keeps the old behaviour.

A recursive relation's values stand the same way. Each row of the
fixpoint stands as the newest of its derivations, carried from row to
row as the fixpoint runs; a row derived again from a newer fact
re-enters the next round, so what it derives stands as new. A
subscription that maintains the fixpoint across polls keeps the
standings too: a deletion re-derives a suspect row at the newest
derivation that survives it.

### `unless`, optional premises and elections are stratified; a rule closing a cycle through one is quarantined

[#583](https://github.com/dialog-db/dialog-db/pull/583), detail in
[`notes/attribute-heads.md`](./notes/attribute-heads.md).

A deductive rule admits `unless` and optional premises. A read under a
ranked pick (`top`, a relation chain, `max`, `min`) is treated the
same way: it returns a candidate *and nothing better*, which negates
the better candidates, so a ranked fallback is an absence test however
it is spelled. A read under `last` is too outside a cycle; inside one
it reads every candidate and elects at the cycle's exit (see below). Outside a recursive component all of these are stratified:
the premise reads a relation derived in full before the rule runs, and
rules can derive, decide and derive again from the decision.

Inside a component, such a premise would read a relation the cycle is
still deriving, which has no stratified meaning. Merging rule sets that
are each fine can close such a cycle, so the program is never refused.
Instead the program analysis *quarantines* one rule of the cycle, and
evaluation leaves it out:

- It sets aside the rule of the cycle installed last. Every read was
  well defined before that rule arrived, so setting it aside keeps
  them as they were. A committed rule is ordered by the commit that
  indexed it (edition, then version hash, as a `last` election orders
  claims); a rule not yet committed (a session's, a transaction's, a
  query's own) is newer than every commit. A `RuleRegistry` orders
  rules by registration.
- Among rules installed together, it takes the cycle's plainest
  absence test (an `unless` or an optional read first, then a ranked
  election) and sets aside the rule inside the cycle that
  derives what that test reads; when the test's own rule derives what
  it tests, that rule.
- Then the greatest rule identity.

The order comes from the commits, which every replica sees the same,
so replicas holding the same history quarantine the same rule, however
the rules reached them. A quarantine lifts by itself once a rule of
the cycle is retracted. A plain recursion under `all`, such as an
ancestor closure, is positive and unaffected.

What this changes for you:

- A query reads the rules a branch sets aside as
  `dialog.rule/quarantined`: one row per rule, of the rule's identity,
  valued with the concept it concludes. It is never stored; the branch
  answers it from the rules its layers hold (committed, session,
  staged and the query's own), so it changes as rules are installed
  and retracted. Facts written under the attribute are not read.
- `ProgramAnalysis::quarantined()` lists the rules set aside, each with
  the concept it concludes and the cycle it closed.
  `ConceptRules::without` is how a bundle leaves them out, and
  `ConceptRules::install_at` installs a rule with when it was
  installed (`Installed`).
- A recursive rule may read its own relation under `last`: inside the
  cycle the read sees every candidate the fixpoint derives, and its
  readers elect the newest once the cycle is derived in full. A
  notebook positioning a run of inserted blocks from each block's
  successor relies on this. A read under a ranked pick (`top`, a
  relation chain, `max`, `min`) inside its own cycle is an absence
  test and is quarantined: a default chain is "this, else the
  default", which has no stratified meaning while "this" is still
  being derived. Inheritance down a hierarchy through a ranked choice
  (a node's own value, else its parent's) is the case this rules out;
  lattice-valued recursion could admit it later.
- `EvaluationError::NegationThroughRecursion` is gone, and the cycle
  pick that replaced it on this branch is gone too.
  `ProgramAnalysis::absences()` now lists only what a cycle with no
  rule to set aside still holds.
- `TypeError::NegationInOpenRule` is gone. It existed on this branch
  only.
- `reduce` in a deductive rule is still refused
  (`TypeError::ReduceInOpenRule`). A fold has no reading over a set
  that is still growing. Put the fold in a query, a subscription or an
  inductive rule.

Why: rules are installed as facts and merged from several replicas, so
whatever set arrives has to be evaluable and mean one thing everywhere.
Refusing negation outright threw away every stratified negation to
prevent the unstratified ones; reading a negation inside a cycle as
holding (the cycle policy) answered, but answered wrongly, ignoring
even stored facts the negation was written against. Quarantine keeps
every query answering and every answer a stratified one, and reports
what it set aside.

What is guaranteed: every program evaluates; evaluation is
deterministic, and replicas with the same history agree; what a query returns
is the stratified answer of the program minus the quarantined rules.
What is not: that a rule's derived set only grows as rules land. A
negation can lose derivations when a rule starts deriving what it
tests; that is what negation means.

### Picks choose members; aggregators are gone from `pick`

[#583](https://github.com/dialog-db/dialog-db/pull/583)

`pick` is one of `last`, `all`, `top`, `max`, `min`. Each returns
members of the candidate set, which is what lets a rule inside a cycle
read the set and a reader outside read the choice without disagreeing
about what the relation holds. `sum`, `count`, `count-distinct` and
`avg` are not picks and have been removed, with the carrier
distinction (`PolicyInOpenRule`) that existed to fence them. They were
introduced on this branch and never released. Use `reduce` in a query,
subscription or inductive rule for folds.

Other changes to picks on the same branch, for a reader who did not
follow it:

- An attribute is a relation (what `the` names) read under a type and
  a pick. Two picks over one relation are two attributes, with
  distinct identities. A field is a concept slot holding an attribute,
  and can be optional. These are the words the code and the notes use.
- Cardinality is derived from the pick: `all` is many, every other
  pick is one. `cardinality: one` and `cardinality: many` are read as
  the older spellings of `last` and `all`, and never written. Tonk's
  notation emits `pick: all` where it said `many` and nothing where
  it said `one`.
- A list is a ranked choice. `as: [a, b]` ranks values, `the: [x, y]`
  ranks relations, best first; either implies `top`. With the entity
  bound, a `top` over listed relations stops at the first relation
  that offers a candidate. Tonk's `among:` is gone; write the list
  under `as:`.
- A rule body naming a derived relation through a raw attribute
  premise (Rust API only; the notation always writes concept premises)
  now reads the derived candidates as well as the stored facts,
  negated and optional premises included.

### Rules derive attributes; concepts select them

[#580](https://github.com/dialog-db/dialog-db/pull/580), detail in
[`notes/attribute-heads.md`](./notes/attribute-heads.md).

A deductive rule's head is a set of attribute triples, and a concept
head is sugar for one triple per field. A rule contributes to the
relation each attribute names, and any concept reading that relation
sees the derived values beside the stored ones. Before this, a rule
deriving `Employee { name, role }` was visible only to a query for
`Employee`; a query for `Named { name }` did not see the name.

What this changes for you:

- A subset concept now sees derivations. If a concept's result set
  grew after upgrading, this is why.
- Installing a rule writes `dialog.rule/derives` facts, one per head
  attribute, beside `source`, `conclusion` and `reads`. Discovery
  reads `derives`; `conclusion` is kept for tooling and for rules
  installed before the index existed, which keep resolving.
- A rule's identity is the hash of its canonical spelling, not of the
  bytes you wrote: variables are renamed by structure and premises are
  ordered. The field names a premise binds a concept's fields under
  are renamed by structure too, so two premises reading the same
  attributes under different field names are one premise. Two authors
  writing one rule under different names install one rule. A body
  stored under any other entity, the older byte-hash identity
  included, is inert on every read and at commit until
  `Branch::upgrade_rules` re-installs it (see below).
- Every rule has an identity, including one a program registers with a
  `RuleRegistry`: a rule whose premises cannot be spelled canonically
  (a raw `AttributeQuery` scan) is refused
  (`EvaluationError::RuleWithoutIdentity`). Write the premise as a
  concept or attribute read, as an installed rule would.
- The dependency graph, the fixpoint and the registry key an attribute
  concept by its relation, so every read of a relation, under any type
  or pick, meets the rules deriving it.

Why: whatever is true of an asserted fact must be true of a derived
one. Asserting `Employee` writes two attribute facts and nothing else;
deriving it must land in the same place.

### Compatibility and migration

What a replica holds from the release before this branch, and what
happens to it.

- Types are hashed as entities, so the attribute, concept and rule
  identities this release computes differ from the earlier release's
  for every typed attribute, beyond the change picks made.
  `Branch::upgrade_rules()` re-installs every rule under its identity
  and moves the transient markers, as below; `dialog_query::migration`
  still computes the earlier identities from the earlier type names.
  Nothing is rewritten until the upgrade runs.
- The earlier release cannot read a descriptor this one writes: it
  knows `Text`, not `text:`, and it does not know the rule body
  envelope. A body stored by the earlier release, bare and with the
  earlier type names, is read by this one.
- Stored claims are unchanged: a claim is its attribute, entity,
  value and cause, with the versions that carry it. A cell the older
  release wrote through a cardinality-one replacement holds one claim,
  and a `last` read returns it, as before. Nothing is rewritten.
- Stored rules decode. A rule is stored as the dag-cbor of its
  descriptor, and the older descriptor spells `cardinality: one` or
  `many` and a single relation under `the`; both read as `last` and
  `all` over that relation. Re-encoded, a rule never writes the older
  spelling back.
- Rules installed by the older release are inert until upgraded. Such
  a rule sits under the hash of its stored bytes, with its conclusion
  and its body but no `derives` fact. No read looks for it there, so
  no read pays for rules that may not exist. `Branch::upgrade_rules()
  .perform(env)` reads the branch's rule bodies, retracts the facts of
  every rule stored under an entity that is not its identity, and
  installs it again under its identity, in one commit. It is
  idempotent, and two replicas upgrading concurrently converge, since
  a rule's identity is a function of the rule. It needs the branch's
  `dialog.rule/` range, not a full replica. A replica still on the
  older release can write such rules again and sync brings them in;
  they stay inert until the upgrade runs again. When to run it, once
  or after every pull, is the embedder's decision.
- Every attribute and concept identity changes: an attribute's
  identity hashes the relations it ranks, its pick and its ranked
  values beside its domain, name and type, so two reads of one
  relation under different picks are two attributes. The upgrade
  moves the `dialog.concept/transient` markers keyed by the older
  identities, for the concepts the branch's rules conclude.
  `dialog_query::migration` reproduces the older identities, for an
  application that keyed facts of its own by them.
- Library YAML in the older spelling parses: `cardinality:` is read as
  `pick: all` for `many` and the default `last` for `one`, and a
  `pick:` beside it wins. tonk's standard libraries carry a seed
  version that is the hash of their text, so a worker on this release
  sees the mismatch on first start, retracts the installed library's
  facts by the provenance it recorded, and installs the new one.
- The older release cannot read what this one writes. A rule written
  here may list relations or values under `the` or `as`, which the
  older descriptor refuses, and carries `pick`, which the older
  descriptor ignores, reading a `max` as `last`. A batch of changes
  serializes each assertion with its pick. Upgrade every replica of
  a repository together, or upgrade readers before writers: a replica
  on the older release pulls such a branch, but a read that meets a
  rule it cannot decode fails with the decoding error.
- A descriptor's JSON no longer spells `cardinality`. It writes
  `pick` where the pick says more than the lists imply (a ranked
  `the` or `as` list reads as `top`, a plain attribute as `last`), so
  a plain `last` field carries neither key. A program that told a
  one-valued field apart by `cardinality: one` reads `pick` instead:
  a field is one-valued unless it says `pick: all`. tonk's template
  planner did this; a reader rebuilding a descriptor from stored facts
  has to keep the pick and the ranked list beside the cardinality,
  or a `top` field comes back as `last`.
- Programs the older release refused run. `unless` and optional
  premises inside a recursive component were an error
  (`NegationThroughRecursion`); the analysis now quarantines a rule of
  such a cycle and every query answers, without the rule.
