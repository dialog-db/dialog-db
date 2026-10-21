# Single-file representation

## Idea

A replica on native is a directory of files, one per block. That is simple
but it is the wrong shape for what the dialog search tree actually does: every
commit supersedes a spine of nodes and nothing ever reclaims the old ones,
so the directory is the sum of every node version ever written. On the
Stack Exchange replay at 2048 commits the live tree is ~49 MB and the
store on disk is ~450 MB, about 9×, and it keeps climbing with commits.
That 9× is retained tree shape, not accidental duplicate stores, so the
two directions below attack it from different sides.

I want a single file instead. We spiked one over the summer, DCAA on
`claude/perf-6-dcaa-spike`: append-only, blake3 addressed, BAO outboard for
range verification, sorted index with per-commit deltas. As built, with an
fdatasync per commit, it was slower than file-per-block on every write
row (2.74 s vs 1.69 s on the 500 commit SE replay), and I decided against
it. With the fsync taken out it came in at 933 ms on the same replay, so
the gap is the fsync, not the format, but nobody ever ran a read
benchmark against it, so speed is open, not settled. The part that is
settled: it was append-only and had exactly the same growth as the
directory it replaced. Append-only on its own just keeps growing.

There are two ways I think this could go, and I would like both explored:

1. **The file moves things around.** It compacts, evicts intermediary nodes
   that nothing references any more, and leans on the history index to
   rebuild what it dropped if we ever need it. The file is a cache of the
   tree, the history is the truth.

2. **The file never removes anything**, but it encodes columnar, so that
   successive versions of a node mostly share bytes and the superseded
   ones compress away to almost nothing. If that works, append-only is no
   longer a big deal and we keep the crash-safety that comes with it.

Worth unpacking what direction 2 actually means. Segment nodes already
tokenize and column-encode: within one node an entity, an attribute, a
domain shows up once, and the node's slices point into that buffer. What
that does not do is help across nodes, and that is where the duplication
is. The same entity sits in the EAV, AEV and VAE nodes, and every
superseded version of a node carries the same tokens again. In one file
with the same kind of encoding there would be no copies of a token
anywhere: nodes, indexes and old versions all point into one dictionary.

The node as the rest of the system sees it does not change. When a node
is read or sent, the file realizes it copy-on-write into the current wire
format, an independent block, and it is still referenced by the hash of
that wire form. The file stores something else, the hash is of what goes
on the wire.

What that opens up, and nobody has sized yet:

- The file needs an index from wire hash to where the node's pieces live,
  since the hash is no longer the hash of any bytes stored.
- Realizing and hashing a node on every read is CPU the file-per-block
  store does not spend. Caching the realized block is the obvious answer,
  so this is a measurement, not a blocker.
- The dictionary grows too. How it gets compacted when tokens die with
  their last node is its own question.

Either way it is still an archive: you look up a block by hash and get it
back verified. Whole-file BAO is the obvious extension, so the file itself
is content-addressed and a peer can verify a byte range of it.

> 💭 The root-frame re-encode is where the bytes come from today (~120 to
> 160 KB written per commit of a couple of facts). A format that shares
> bytes across node versions attacks the same thing from the other side.

## Constraints

Whatever the file does inside, the replication contract stays: one node is
one block under its existing hash, every commit keeps a stable content
address, and a push after a commit ships one small block. Three designs
from the summer were rejected for bending this. Compaction can drop
blocks, it cannot change what a block is.

The tree is deterministic and stays that way: the same facts give the same
nodes and the same hashes on every replica. The file is not. Two replicas
holding the same tree can lay their bytes out differently, compact at
different times, and that is fine. Determinism is a property of the tree,
not of the file.

> 💭 The columnar node format is on main, but nobody has measured how
> well successive versions of a node actually compress against each
> other. Direction 2 rests on that number, so measure it first.

## Goal

Find out whether a single-file format can hold a replica at a fraction of
the size of file-per-block without giving up performance, and which of the
two directions gets there.

## What success looks like

Measured against the file-per-block `fs` provider on native, which is what
we ship today, at equal durability (neither fsyncs per commit). SQLite rows
from `dialog-baseline` are the reference point, not the target.

Performance means the `dialog-perf` gate as it stands: same engine steps,
instructions within its tolerance, archive blocks and bytes within theirs,
with the new format swapped in as the archive. Wall-clock medians from
`dialog-baseline`'s `se_replay` are a sanity check on top, not the gate.

Size means bytes on disk per live fact on the Stack Exchange replay, at
2048 and 4096 commits, next to the file-per-block directory and the live
tree.

- The perf gate holds with the new archive underneath it.
- On-disk size at least an order of magnitude smaller than the
  file-per-block directory for the same replay, and growing with the live
  facts rather than with the number of commits.
- Get by hash still works and still verifies.
- A crash leaves the last commit intact.

A spike that only hits the size target by giving up the archive property,
or only hits performance by growing like today, has answered the question
with a no, which is also fine.
