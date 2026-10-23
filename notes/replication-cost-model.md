# The replication cost model

Status: the rule for changes to storage layout, write policy and sync
paths, from issue #642. Measured by `rust/dialog-soak` over the `Fs`
transport's simulated network (`dialog_remote_fs::simulation`).

## Three costs, never one number

A replica pays for replication in three currencies. A report states each
per phase and never sums them, because a change that trades one for
another is only reviewable when all three are visible.

**Round trips** are the longest chain of dependent requests: how many
requests the client issued one after another, each only once the one
before it had answered. Over a link with latency this is the cost that
dominates. A tree descent is a chain as deep as the tree; a query that
awaits one select per outer row is a chain as long as its outer rows; a
push that uploads children before parents is a chain as deep as the
novelty. Reading more blocks in fewer rounds beats reading fewer blocks
in more rounds whenever latency dominates (`notes/parallel-fetch-scheduler.md`).
Behind an access service every cold object costs two round trips, the
permit redeem and the fetch, so the chain is paid twice there.

The soak reports this twice, and the two columns are not the same
quantity. `chain` (`round_trips` in the JSON) is exact: the simulation
assigns every request a depth one greater than the deepest request that
had completed when it began, and the phase's chain is the greatest depth
assigned. It is a property of the schedule the client ran, so it reads
the same on every profile, including the unshaped `none` profile, and
the gate holds it tight there. `rounds` is older and is a bound: modeled
time divided by the per-request serial cost. Bandwidth and compute also
advance the clock, so `rounds` reads above `chain` by the share of the
phase that was bytes on the link rather than latency; the gap between
the two columns is itself informative. `chain` counts two requests that
happened to be ordered as dependent even when they were not. That is
deliberate: the measurement is of what the client did, and the only way
to lower it is to issue the requests together, which is the behaviour
the number exists to reward. The `Counting` meter in
`dialog_repository::helpers` reports a different quantity again, the
longest run of fetches that were each alone in flight; a chain of
parallel waves has no solo fetch and still costs one round trip per
wave, which is why the soak counts the chain and not the run.

**Objects** are requests: archive block reads and writes, head-cell
reads and writes, and blob opens, counted together. This is the cost of
per-request overhead. Behind an access service it is one redeem per
object; on an S3-like store it is request pricing; on any HTTP transport
it is a header exchange per object. A layout that spills values into
blobs moves objects from the block column to the blob column without
changing the total, so the report carries blob opens as their own column
beside the total.

**Bytes** are the cost of bandwidth: every payload that crossed the
link, archive blocks, head cells and the bytes a blob streamed after its
open. A narrow query that reads whole leaves pays this in read
amplification; a commit that rewrites a path pays it in write
amplification on push.

Modeled time is the fourth number and the one a user feels. It is not a
cost of its own; it is how a given link combines the three. The presets
are stated as what they charge per request and per byte:

| preset | round-trip latency | per-request redeem | bandwidth | connections |
|---|---|---|---|---|
| `none` | 0 | 0 | unlimited | unlimited |
| `localhost` | 0.4 ms | 0.4 ms | 1 Gbit/s | unlimited |
| `broadband` | 30 ms | 50 ms | 100 Mbit/s | unlimited |
| `mobile` | 80 ms | 120 ms | 20 Mbit/s | unlimited |
| `browser` | 80 ms | 120 ms | 20 Mbit/s | 6 |
| `intercontinental` | 250 ms | 250 ms | 50 Mbit/s | unlimited |

Each request pays the redeem and then the latency; those overlap freely
across requests in flight together, so a wave of sixteen costs one
latency. Bytes queue on one shared link, so a wave of sixteen payloads
costs sixteen payloads' worth of transfer. A connection cap makes a
request wait for a connection before it pays anything, so under
`browser` a wave of sixteen costs three latencies: a browser allows six
connections per host over HTTP/1.1, tonk's replicas are browsers, and
the fetch scheduler's window is 64, so the uncapped presets are
optimistic for exactly that client. The cap changes modeled time and
not the chain: the chain measures what the client's schedule depends on,
and a request queued for a connection was still issued when it was.
Modeled time for a phase is therefore close to
`chain × (redeem + latency) + bytes / bandwidth`, with compute on top and
the cap stretching the first term, and the whole run executes under
tokio's paused clock, so the number is deterministic for a deterministic
schedule. The `none`
profile pins the protocol's shape exactly: its counts are what the code
does, with no timing in them. The decision on a change is made on
modeled time under the profiles tonk's users are on, mobile and
broadband, with the three costs on `none` saying why the time moved.

## What the harness cannot see

The harness is a model, and the things it leaves out are as much part
of reading a report as the things it counts.

- **Network variance.** Every request pays the same latency; there is
  no jitter, no loss, no retransmission, no slow start, no head-of-line
  blocking within a connection. A real mobile link has a tail the model
  has no shape for.
- **The transport's own shape.** The cap is a count of connections, not
  a model of HTTP/2 multiplexing, connection setup, keep-alive or
  request pipelining. A browser over HTTP/2 holds one connection and
  many streams, which the uncapped presets approximate; one over
  HTTP/1.1 holds six, which `browser` models; the truth of either has
  per-connection costs the harness does not charge.
- **Server-side cost.** The remote is a directory. A real store spends
  CPU and I/O per request and per byte, and an access service spends a
  signature check per redeem; none of that advances the clock here.
- **Concurrent clients.** One client talks to one remote. Contention
  for the link, the store, or the head cell under other writers is not
  modeled; a push that would be refused as non-fast-forward in the
  field succeeds here.
- **Cache eviction.** The client keeps what it fetched for the whole
  run. A device whose cache is evicted between sessions pays the cold
  path again; the harness's warm phases (`requery`) measure the best
  case.
- **Real clients.** The phases mirror tonk's join and a UI's standing
  query, in order. A real application interleaves them, repeats some,
  and reads through a staging pool the harness does not have.
- **Blob reads outside the sync path.** Blob bytes are metered when a
  blob is opened through the remote. An application that reads a blob
  it already holds pays nothing here, correctly; one that streams a
  blob through a different transport is not measured.
- **CPU.** Compute advances the paused clock by its real duration on
  the machine running the harness, so a phase that is compute-bound
  reports machine-dependent time. The perf gate (`rust/dialog-perf`)
  owns compute; the soak owns the wire.

## The rule

A pull request that changes the storage layout, the write buffer, the
flush policy, the record format or a sync path carries the per-phase
table in its body: `none` and `mobile`, before and after, from
`soak sweep` over the scenarios in `rust/dialog-soak`. Without the
table the change is not reviewable, because the argument "more objects
per commit means more objects exchanged on replication" is plausible and
has been wrong, and only the table says which. A change that moves one
cost in exchange for another states all three; the decision is made on
modeled time under mobile and broadband, with `none` holding the
protocol shape exact.
