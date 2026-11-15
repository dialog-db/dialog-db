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

## Opening a repository over the file

A space's storage is a `Space` of five providers: the archive, the memory,
then credentials, certificates and blobs. Name `Chronicle` as the archive,
and as the memory too if the space's cells should be in the file:

```rs
use dialog_chronicle::Chronicle;
use dialog_storage::provider::{FileSystem, Space};

/// Blocks and cells, branch heads among them, in archive.dialog: for a
/// replica that starts in the file.
type FileSpace = Space<Chronicle, Chronicle, FileSystem, FileSystem, FileSystem>;

/// Blocks in archive.dialog, cells a file each: for a replica moved in
/// from the `fs` provider (see below).
type BlockSpace = Space<Chronicle, FileSystem, FileSystem, FileSystem, FileSystem>;
```

A peer and its repositories then open over that storage as they do over
any other (see `dialog-peer`'s README); only the storage's type changes:

```rs
use dialog_capability::Subject;
use dialog_effects::storage::{Directory, Location};
use dialog_identity::OpenCredential;
use dialog_peer::{Allowance, Peer, SpaceVaultExt as _};
use dialog_repository::{Repository, RepositoryExt as _};
use dialog_storage::provider::Storage;
use dialog_storage::provider::storage::{CredentialStore, NativeSpace};
use dialog_varsig::Principal as _;

// Every space the peer opens is a directory under `base`.
let base = Directory::At("/path/to/profiles".into());

// Keys are kept in a credential store under `base`, a file each. The
// storage and the store belong to a system: opening spaces takes its
// grant, so every start must act as the same system. `open` loads the
// system's key, and makes it the first time.
let credentials = CredentialStore::<NativeSpace>::new();
let system = OpenCredential::open("system")
    .at(base.clone())
    .perform(&credentials)
    .await?;
let credentials = credentials.owned_by(system.did());
let storage = Storage::<FileSpace>::new().owned_by(system.did());

// The peer's key, opened from the store the same way.
let credential = OpenCredential::open("alice")
    .at(base.clone())
    .perform(&credentials)
    .await?;

let alice = Peer::new(credential.clone())
    .at(Location::new(base.clone(), "alice"))
    .with(storage)
    .space(Repository::from(credential.did()).branch("main"))
    .grant(Allowance::storage(&system))
    .base(base.clone())
    .build()
    .await?;

// Onboarding, the first time: the peer acts for an account, whose key a
// custodian guards and which delegates to the peer. It opens no space
// until it is onboarded.
if alice.authority().await.is_err() {
    let custodian = OpenCredential::open("alice-custodian")
        .at(base.clone())
        .perform(&credentials)
        .await?;
    let account = alice.state().vault("account").create().perform(&alice).await?;
    account.add(custodian.did()).perform(&alice).await?;
    account.delegate(alice.did()).perform(&alice).await?;
}

let session = alice
    .session(b"my-app")
    .space(alice.state())
    .allow(Subject::any())
    .await?;

// Commits, queries and sync go through the file from here on.
let notes = alice.space("notes").open().perform(&session).await?;
let main = notes.branch("main").open().perform(&session).await?;
```

Each space's file is `archive.dialog` in the directory the `fs` provider
would keep that space in (`file_at` names it), created on first open.
From there, commit and query as `dialog-repository`'s README shows.

A file can also be opened on its own, which is what migrations and tools
do:

```rs
use dialog_chronicle::{Chronicle, Options};

let archive = Chronicle::open("profiles/alice/archive.dialog")?;
archive.append("index", &blocks)?;
let bytes = archive.get("index", &hash)?;
archive.sync()?; // a sync point now, not within the next two seconds

// Every block whole, no frames: for comparing against the fs provider.
let plain = Chronicle::open_with(path, Options { frames: false, ..Options::default() })?;
```

Clones of a `Chronicle` share one file, and so do two opens of one path
in a process.

`dialog-chronicle` is not on crates.io. Depend on it from git, pinned to a
release tag (see [RELEASING.md](../../RELEASING.md)).

## Migrating a replica from the `fs` provider

A replica the `fs` provider wrote keeps a file per block under each
space's `archive/` directory. The `chronicle` migration of
`dialog-migration` moves those blocks into each space's `archive.dialog`.

It moves **blocks only**. A space's cells, branch heads among them, stay
in its `memory/` directory, so a migrated replica opens as `BlockSpace`
(`Chronicle` archive, file system memory). Opened as `FileSpace` it opens
without an error and every branch reads as empty, and the setup above
would onboard the peer again, into the file.

### From an application

Add the migration to the application's dependencies, behind its feature:

```toml
[dependencies]
dialog-migration = { git = "https://github.com/dialog-db/dialog-db.git", tag = "v<version>", version = "<version>", features = ["chronicle"] }
```

Run it on start, before the peer opens, on the directory the peer's
spaces are under (`base` above). It finds every space under that
directory by itself, and does nothing for a space it has moved already, so
running it on every start is safe:

```rs
use dialog_migration::Migrations;
use dialog_migration::chronicle::IntoChronicle;

let profiles = std::path::Path::new("/path/to/profiles");
let migrations = Migrations::new().with(IntoChronicle::default());

// What it would do: a line per space not moved yet.
for pending in migrations.plan(profiles)? {
    println!("{}: {}", pending.step, pending.detail);
}

// Do it. An interrupted run picks up where it stopped.
migrations.run(profiles)?;

// Then open the peer over Storage::<BlockSpace>, as above.
```

`plan` and `run` block on the file system; from an async application,
run them before its runtime starts or on a blocking thread.

### From the command line

The same step is the `dialog-migrate` binary:

```sh
cargo install --git https://github.com/dialog-db/dialog-db.git --tag v<version> dialog-migration --features chronicle
dialog-migrate --dry-run /path/to/profiles   # what it would do
dialog-migrate /path/to/profiles
dialog-migrate --remove-source /path/to/profiles   # and delete the moved blocks
```

### What it does

- A space with no `archive.dialog` gets one written under another name
  and renamed into place once whole.
- A space that has one already (a process opened it as `BlockSpace` before
  migrating) gets the blocks written into it through `Chronicle`, never
  over it.
- A space is marked migrated with `archive.dialog.migrated`, so running it
  again does nothing, and an interrupted run picks up where it stopped.
- `--remove-source` (or `IntoChronicle { remove_source: true }`) deletes
  each space's `archive/` directory once its file is in place. If a
  block's bytes did not match its name, or a file there was not a block,
  it fails instead and keeps the directory.

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
