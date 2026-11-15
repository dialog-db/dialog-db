# dialog-chronicle

A space's blocks and memory cells kept in one file, `archive.dialog`,
with the tree's nodes read from it in place.

It is opt-in, and native only. A replica keeps its blocks the way the
`fs` provider does, a file per block, unless a `Space` names `Chronicle`
as its archive, and optionally as its memory. The tree, queries and sync
read and write it as they would any archive, and every block reads back
as exactly the bytes it was.

The guide's chapter [One File per Space](../../guide/src/file.md) walks
through the file byte by byte. The decision it builds is
[research/single-file-format/design.md](../../research/single-file-format/design.md).

## Why

A tree never changes a node: a commit writes a new version of each node
it touches, as a whole block. A replica's history is therefore mostly old
versions of the same few nodes, each version repeating almost everything
the one before held. With a file per block that costs a file per version,
and a file system's minimum block size for each.

`Chronicle` keeps each distinct entity, attribute, value, datum and
version once, as a token, and each node as a frame: rows of token ids,
written against the node it supersedes. The tree reads a node through
`IndexView` and `SegmentView` over the file's mapping, with no block
built, hashed or validated. A block is rebuilt only when sync or push asks
for one with `Get`.

On disk, 4,096 Stack Exchange commits take 1,169 MiB as a file per block
and 13.4 MiB in the file. tonk's team space takes 568.5 MiB and 25.4 MiB.

## Using it

A space's storage is a `Space` of five providers, the archive first and
the memory second. Name `Chronicle` in either place:

```rs
use dialog_chronicle::Chronicle;
use dialog_storage::provider::{FileSystem, Space, Storage};

// Blocks and cells, branch heads among them, in archive.dialog: for a
// replica made this way from the start.
type FileSpace = Space<Chronicle, Chronicle, FileSystem, FileSystem, FileSystem>;

// Blocks only; cells stay a file each. What a migrated replica uses.
type BlockSpace = Space<Chronicle, FileSystem, FileSystem, FileSystem, FileSystem>;

let storage = Storage::<FileSpace>::new();
```

Opened through a `Space`, each space's file is `archive.dialog` in the
directory the `fs` provider would keep that space in (`file_at` names it).
It is created on first open.

A file can also be opened on its own, which is what migrations and tools
do:

```rs
use dialog_chronicle::{Chronicle, Options};

let archive = Chronicle::open("replica/archive.dialog")?;
archive.append("index", &blocks)?;
let bytes = archive.get("index", &hash)?;
archive.sync()?; // a sync point now, instead of within two seconds

// Every block whole, no frames: for comparing against the fs provider.
let plain = Chronicle::open_with(path, Options { frames: false, ..Options::default() })?;
```

Clones of a `Chronicle` share one file, and so do two opens of one path
in a process.

`dialog-chronicle` is not on crates.io. Depend on it from git, pinned to a
release tag (see [RELEASING.md](../../RELEASING.md)).

## Moving an existing replica

`dialog-migration`, behind its `chronicle` feature, moves the blocks of
every space of a replica the `fs` provider wrote into its
`archive.dialog`. It moves blocks only: a space's cells, branch heads
among them, stay in its `memory` directory. Open a migrated replica with
`Chronicle` as the archive and the file system as the memory (`BlockSpace`
above), not as `FileSpace`, which would find no branch heads. An
application runs the migration before it opens the replica:

```rs
use dialog_migration::Migrations;
use dialog_migration::chronicle::IntoChronicle;

let migrations = Migrations::new().with(IntoChronicle::default());
migrations.run(profile_directory)?;
```

The same step is the `dialog-migrate` binary:

```sh
cargo install --path rust/dialog-migration --features chronicle
dialog-migrate --dry-run <profile directory>
dialog-migrate <profile directory>
```

- It writes into an existing `archive.dialog` through `Chronicle`, never
  over one.
- A space is marked migrated with `archive.dialog.migrated`, so running it
  again does nothing, and an interrupted run picks up where it stopped.
- `--remove-source` (or `IntoChronicle { remove_source: true }`) deletes
  each space's block directory once its file is in place. It deletes
  nothing when a block was left out.

There is no migration back to a file per block yet. The blocks are the
same bytes in both, so copying them out with `Import` is one; cells kept
in the file would need moving too.

## The file

- **Head:** a 16-byte header (`DIALOG¶`, the format number 1, and
  reserved bytes), then four slots of 64 KiB.
  - Two rolling slots, written in turn, and two copies of the durable
    slot.
  - A slot says where the records end, where the block index's root and
    the cells' index root are, which index pages are free, and which
    extents are in use.
  - Opening reads the slots and nothing else.
- **Records** are appended after the head: tokens, frames, blocks kept
  whole, cells' updates, and extents. They are never written over. The
  exception is index pages no root reaches, which are reused once a sync
  point makes that safe.
- **The block index** is a search tree in the file, in 4 KiB pages, so a
  reader holds a few tens of KB however large the file grows.
- **Frames are checked.** A node is framed only when its rows rebuild its
  block exactly; anything else is kept whole. A cell names the frames it
  points into by a CRC-32.

A file written by an earlier, unreleased build of this crate does not
open. Move it again from its source.

## Writers, readers and power loss

- **Writers in any number of processes** take turns on a lock on one byte
  of the file. A writer that finds the turn taken writes its blocks whole
  into an extent of its own, and they read back at once. The next writer
  with the turn turns them into frames.
- **Readers take no lock.** What a slot names never changes, and a read
  that misses looks again only if the file grew.
- **Durability:** a sync point syncs the file, then writes the older
  durable copy and syncs again. One is made after 64 appends, 2,048 freed
  index pages or 2 seconds of appends, by `Chronicle::sync`, and at the
  last close. A sync thread makes one once appends have waited 2 seconds.
- **A power loss** loses at most what came after the last sync point.
  Opening goes back to the newest whole state. A file cut shorter than its
  durable slot names is refused rather than read as an older state.

## Costs to know

- **Threads:** one sync thread per process, for every file it has open,
  waking once a second.
- **Writes:** every write (`Put`, `Import`, `nodes::Write`, a cell's
  `Publish` and `Retract`) runs on tokio's blocking threads when a tokio
  runtime runs the caller. Reads answer at once.
- **Large appends:** an append of more than 32 Ki blocks is committed in
  parts, so a crash can leave some of an import's blocks in the file.
- **Opens:** two opens of one file in a process wait for each other.
  Opens of other files do not.

## Not done

- **The web.** The crate compiles to nothing on wasm32. An OPFS spike is
  in [research/single-file-format/browser](../../research/single-file-format/browser).
- **Skipping unchanged entries:** a commit still encodes every entry of
  every node it writes.
- **Values handed out in place:** the tree's scans still yield owned
  values.
- **The perf and replication gates** run the `fs` provider; `Chronicle`
  is not in their scenarios yet.

## Tests

```sh
cargo test -p dialog-chronicle
```

- **Power loss:** every point of an append is replayed with nothing lost,
  each operation lost alone and each sector lost alone, then with random
  losses.
- **Concurrency:** writers in separate processes appending to one file at
  once, extents, folds, cells, and branches committed from several
  processes.
- **Nodes:** every node read both ways, every accessor compared, and the
  rebuilt block checked byte for byte.
- **Repository:** commits, a reopen and selects through a repository over
  the file (`tests/repository.rs`).
- **Platforms:** the `chronicle-platforms` workflow runs the crate on
  Windows and macOS.

`examples/guide_bytes.rs` prints the figures in the guide chapter. With
the `trace` feature (Unix only), `examples/trace_open.rs` records what the
archive does to its file, to replay the pattern elsewhere.
