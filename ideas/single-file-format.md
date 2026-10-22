# Single-file representation

## Idea

A replica on native is a directory of files, one per block. That is simple
but it is the wrong shape for what the dialog search tree actually does:
every commit supersedes a spine of nodes and nothing ever reclaims the
old ones, so the directory is the sum of every node version ever written.
On the Stack Exchange replay at 2048 commits the live tree is ~49 MB and
the store on disk is ~450 MB, about 9×, and it keeps climbing with
commits. That 9× is retained tree shape, not accidental duplicate stores.

The replay is the mild case. One real tonk replica reached about 2 GB
after roughly a thousand commits, which is 2 MB per commit, ten times
what the replay pays. That number is the reason to ship this: if a
replica like that comes out way smaller, the format earns its place
whatever the perf gate says within reason.

The thing to keep in view is what those superseded blocks are for. Once
history moves on they are redundant for every read of the current
revision, and useful for exactly one thing: checking out an old revision.
They give you that for free. The only other way to get it with the
current architecture is to replay history and rebuild the blocks, which
is complicated and far too slow to be an option. So today the choice is
keep everything and pay 9× or more, or write a garbage collector and lose
old revisions. If everything in the file boils down to offsets you jump
through, retaining every block costs very little, and the garbage
collector we would otherwise need now can be deferred for a long time,
longer still if the frames compress well. That is the real motivation,
more than the size number on its own.

Segment nodes already tokenize and column-encode: within one node an
entity, an attribute, a domain shows up once, and the node's slices point
into that buffer. What that does not do is help across nodes, and that is
where the duplication is. The same entity sits in the EAV, AEV and VAE
nodes, and every superseded version of a node carries the same tokens
again.

What I want is one append-only log with the same kind of encoding applied
to the whole file. A token is written once, at the first commit that
mentions it, and from then on every node that needs it points at its
offset. A node becomes a columnar frame of offsets, not of bytes. Three
indexes and every superseded version of a node then share one copy of
every token, and the log only grows by what is genuinely new: new tokens
and new frames.

The dictionary itself can be a tree in the log. Each index node over the
tokens is written as a frame that points at earlier index frames by
offset, so a lookup hops across the log instead of reading a dictionary
whole. HyperDB arrived at the same shape for its trie, see prior art.

The node as the rest of the system sees it does not change. When a node
is read or sent, the file realizes it copy-on-write into the current wire
format, an independent block, and it is still referenced by the hash of
that wire form. The file stores something else, the hash is of what goes
on the wire.

> 💭 The root-frame re-encode is where the bytes come from today (~120 to
> 160 KB written per commit of a couple of facts). A frame of offsets is
> a fraction of that, so this attacks write amplification from the same
> side.

Compaction, rearranging the file, trimming old nodes: that is a second
tier, a follow-up that could be built on top of this once it exists, and
it is roughly what a garbage collector would be. We may still need it.
Whether we do, and how soon, is a measurement: a superseded node still
leaves its frame behind, an offset array per column, and that is a
per-commit cost that never goes away. The deferral is only as long as
that frame is small next to the tokens it points at, on a real workload.

## Assets

Nothing here says assets get inlined. They are separate blocks today and
they can stay that way, external to the file, with the file holding the
tree and the facts. Inlining them is a choice the spike can make if the
measurements favour it, not something the format implies, and I do not
want to impose either way.

One consideration that bears on that choice, not a requirement for the
spike: an asset someone uploaded may have to be removed on request, and
in an append-only file removal means redaction, zeroing the bytes in
place so the record still exists but cannot be read. The summer DCAA spec
(`notes/dcaa.md` on `claude/perf-6-dcaa-spike`) had a redaction sentinel
for exactly this. Redacting an external block is trivial. Redacting bytes
inside the file means verification has to tell redacted from corrupt,
and anything shared in the dictionary stays shared. Worth keeping in mind
when deciding where assets live.

## What has to be true for reads

The goal is to remove the duplication without giving up the lookup and
read cost of the block-based store. That is the hard part and it is where
the spike should spend its time:

- **A key comparison must not cost a hop.** Binary search over a frame
  compares keys, and if every comparison dereferences a token offset the
  descent becomes a random walk over the file. Either the frame carries
  enough of the key inline to decide (a prefix, a fingerprint), or the
  token region is small enough to sit in memory. The live token set is
  roughly the live fact bytes, tens of MB on the replay, so the second
  may just be true.
- **Pointer overhead versus token bytes.** An offset is 4 to 8 bytes, a
  front-coded token in today's node is often not much more. The saving
  is entirely cross-node: three indexes times every version. HyperDB
  pays O(M log M) metadata for the same shape of design. Measure the
  frame size against today's block on the replay before building
  anything else.
- **Realizing a node on read.** The wire block has to be rebuilt and
  hashed for every read and every send, which is CPU the directory store
  never spends. A cache of realized blocks is the obvious answer, and the
  wire-hash to frame-offset index is one more tree in the log.
- **Verified range reads.** The whole file as a BAO stream, so a peer
  can verify a frame or a token range against the file's root. Hypercore
  verifies its log the same way, and their experience with it is the
  most direct lesson Dat has for us.

## Constraints

Whatever the file does inside, the replication contract stays: one node is
one block under its existing hash, every commit keeps a stable content
address, and a push after a commit ships one small block. Three designs
from the summer were rejected for bending this. The file can store a node
however it likes, it cannot change what a block is.

The tree is deterministic and stays that way: the same facts give the same
nodes and the same hashes on every replica. The file is not. Two replicas
holding the same tree can lay their bytes out differently, and that is
fine. Determinism is a property of the tree, not of the file.

## Prior art

None of this is new ground, and the point of this section is to learn
from people who walked it, not to lift their designs. The Dat project is
the closest in spirit.
[DEP-0004 HyperDB](https://www.datprotocol.com/deps/0004-hyperdb/) keeps a
key/value trie inside one append-only log. Each entry carries only the
trie pointers it changed and refers back to earlier entries by log index
for the rest, so a node is never copied forward. That is the same
instinct as this idea, applied to structure rather than tokens, and it is
evidence that realize-on-read holds up at database scale. Where our
situation differs: their entries are addressed by position in a writer's
feed, not by content hash, so names mean nothing across replicas; nothing
is ever removed, deletions are tombstones that "persist in the database
forever"; and the merkle tree over the log is per writer.
[DEP-0002](https://www.datprotocol.com/deps/0002-hypercore/) describes the
flat-tree hashing. No DEP specifies an on-disk body layout, so what there
is to take from Dat is the thinking and the lessons, not a format.

Other places to learn from, each for one thing:

- **FSST** (Boncz, Neumann, Leis 2020): a static symbol table for string
  compression that decodes any single string without touching the rest.
  The token region could use the same trick without losing random
  access.
- **Lucene's term dictionary**: a finite state transducer over sorted
  terms, prefix-shared and randomly addressable. The closest existing
  thing to "the dictionary is a tree you hop across".
- **WiscKey and other key/value-separated LSMs**: keys in the index,
  values in an append-only log by offset. Same split as frames and
  tokens, and their garbage collection is what direction 1 would look
  like if we ever need it.
- **Parquet and DuckDB dictionary pages**: dictionary encoding per column
  chunk, which is what segment nodes already do. Useful mostly as the
  thing this idea generalizes past.
- **Datomic and XTDB segments**: immutable, content-addressed segments in
  a blob store with retraction and no deletion. The same growth problem,
  solved there by not caring.

## Deliverable

A single-file store that provides the capabilities dialog needs today, as
a provider the rest of the system can be pointed at without knowing the
difference. It serves `archive` (get, put, import of blocks by hash) and
ideally `memory` too (cells, resolve, publish, watch), so a replica is one
file. Archive only is acceptable for the spike, memory can come after.
Along with it, the measurements below against the current system, on the
same workloads, so the decision to adopt it is a comparison and not a
guess.

It does not have to replace the directory store. It can ship as another
provider an embedder picks, the way a browser build picks IndexedDB or
OPFS today. One embedder wants the smallest replica, another wants the
fastest, and the measurements are what let them choose.

## Goal

Find out whether one append-only, columnar log can hold a replica with
every token stored once, at a fraction of the size of file-per-block,
with lookups and reads as cheap or nearly as cheap as the block-based
store.

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
tree. It also means the 2 GB replica, or one shaped like it, replayed
through both stores. The spike should get hold of that replica or a
recording of its commits before it measures anything else, because the
replay understates the problem by an order of magnitude.

Success is no major slowdown and a clear win on space:

- The perf gate holds with the new archive underneath it, or comes within
  10% of today on instructions and bytes where it does not.
- On-disk size at least an order of magnitude smaller than the
  file-per-block directory for the same replay, and growing with the live
  facts rather than with the number of commits.
- Get by hash still works and still verifies.
- A crash leaves the last commit intact.

Failure is very little gain on space with a loss on performance: under 2×
smaller, or slower than today by more than the gate allows. A spike that
lands there has answered the question with a no, which is also fine.

In between it is about the ratio. A modest slowdown bought with a large
space win is still a win, especially as an alternative provider rather
than the default. A small space win at equal speed is not worth a second
provider. The spike reports both numbers and the ratio, and where it
lands between the two bars is a judgment I will make on the numbers, not
one the spike makes for me.

## What we tried before, and why now

DCAA on `claude/perf-6-dcaa-spike` (July 2026): append-only, blake3
addressed, BAO outboard for range verification, sorted index with
per-commit deltas. As built, with an fdatasync per commit, it was slower
than file-per-block on every write row (2.74 s vs 1.69 s on the 500 commit
SE replay), and I decided against it. With the fsync taken out it came in
at 933 ms on the same replay, so the gap is the fsync, not the format, but
nobody ever ran a read benchmark against it. It stored whole blocks, so it
grew exactly like the directory it replaced. The index, footer and
recovery design is still worth reusing.

Space was not that spike's goal. It asked what if a replica were one
file, the way SQLite is one file, and once it fell behind the block
store on speed there was not much value left in answering. It also
predates the columnar segment encoding and the hitchhiker-style novelty
buffers the tree has today. It stored whole opaque blocks, so the
compression this idea rests on, tokens shared across nodes and frames
that are mostly offsets, was not even testable then.

This time the value is space, and the columnar encoding is what makes
the gain plausible rather than hoped for. Speed is a constraint, not the
promise. I do not expect it to be faster out of the gate, though with
enough effort on it there is no reason it could not end up faster than
what we have today.
