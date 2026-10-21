# Single-file representation

## Idea

A replica on native is a directory of files, one per block. That is simple
but it is the wrong shape for what the prolly tree actually does: every
commit supersedes a spine of nodes and nothing ever reclaims the old ones,
so the directory is the sum of every node version ever written. On the
Stack Exchange replay at 2048 commits the live tree is ~49 MB and the
store on disk is ~450 MB, about 9×, and it keeps climbing with commits.
That 9× is retained tree shape, not accidental duplicate stores, so the
two directions below attack it from different sides.

I want a single file instead. We spiked one over the summer, DCAA on
`claude/perf-6-dcaa-spike`: append-only, blake3 addressed, BAO outboard for
range verification, sorted index with per-commit deltas. It was 1.8× faster
than file-per-block at equal durability, so the machinery is not the
problem. It stalled because it was append-only and had exactly the same
growth as the directory it replaced. Append-only on its own just keeps
growing.

There are two ways I think this could go, and I would like both explored:

1. **The file moves things around.** It compacts, evicts intermediary nodes
   that nothing references any more, and leans on the history index to
   rebuild what it dropped if we ever need it. The file is a cache of the
   tree, the history is the truth.

2. **The file never removes anything**, but it encodes columnar, so that
   successive versions of a node mostly share bytes and the superseded
   ones compress away to almost nothing. If that works, append-only is no
   longer a big deal and we keep the crash-safety that comes with it.

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
