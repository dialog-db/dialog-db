# dialog-soak

A sync/join soak harness: measures what replication actually costs a client
over a **simulated network** — in round trips, transferred bytes, and modeled
wall-clock time — so regressions in join/sync performance show up as numbers
instead of anecdotes, and so protocol changes (batching, permit scoping,
branching-factor tuning) can be evaluated before they ship.

## Why this exists

Running the stack locally hides the problem this harness exists to expose:
on localhost every round trip is free, and a join that costs 40 sequential
round trips *feels* instant. On a real link every one of those round trips
costs latency — and behind a UCAN remote each cold block fetch costs **two**
(access-service redeem + storage GET). The harness makes that cost visible
and repeatable.

## How it works

- A **server** replica seeds a space of configurable shape and pushes it to a
  local-directory remote (the `dialog-remote-fs` transport).
- A fresh **client** joins through that remote, phase by phase, mirroring a
  real application join (tonk's space join): `pull` (adopt the head),
  `probe` (the validation point-reads), `roster` (membership selects),
  `claim` (commit + push the joiner's facts), `render` (first content
  query), `entity` (open one item), `requery` (warm re-read), `concept`
  and `filtered` (the landing page as the query engine runs it — a
  five-attribute concept join on a fresh cold client, balanced and
  status-pinned), and `download` (full materialization, on another fresh
  client).
- The `Fs` transport meters every remote effect (`dialog_remote_fs::simulation`)
  and, when a `NetworkShape` is configured, delays each one by its modeled
  cost: a **connection**, when the shape caps how many a client holds open
  at once; per-request **auth latency** (the per-object access-service
  redeem); **round-trip latency** (overlapping across concurrent requests);
  and serialization on a **shared bandwidth-limited link** (concurrent
  payloads queue, as on a real last-mile link).
- The whole run executes under tokio's **paused test clock**: modeled delays
  complete instantly while virtual time advances by exactly the modeled
  amount. A run over a 300 ms link finishes in real seconds and reports
  deterministic modeled times.
- Every identity a run mints derives from one **seed**, `DIALOG_SOAK_SEED`
  (unset is a fixed default): the profile key, the space keys and names,
  the vault peer, and, through `dialog_common::helpers::entropy`, the line
  identifier a branch's first commit allocates. Revisions carry no
  timestamp, so the same seed writes the same tree, byte for byte; every
  report records its `seed` and the `seeded_root` it wrote. The unshaped
  `none` profile also runs the simulated link in **lockstep**
  (`dialog_remote_fs::simulation::lockstep`): each request waits one tick
  of the paused clock, so the chain counts the rounds the client issued
  requests in, not the order the filesystem's thread pool returned them
  in. Together they make a `none` run repeat: the tree, and every count of
  what each phase fetched, byte for byte; the chain too, but for one
  phase (see below).

## Scenarios

- **join** (`src/join.rs`): a fresh client joins a seeded space, phase by
  phase, as above.
- **catchup** (`src/catchup.rs`): a joined client with a live subscription
  catches up on 1, 10 and 100 commits from the upstream writer (`pull-k`,
  `poll-k`), then on 10 commits from 3 writers and 100 from 10, each
  writer merging the others' pushes before its own (`pull-kxm`,
  `poll-kxm`): what one head move costs a subscribed replica, and what
  crossing origins adds.
- **writer** (`src/writer.rs`): a joined client commits one fact, ten
  facts in ten commits and a 2 000-fact batch, pushing after each
  (`commit-n`, `push-n`): what a writer uploads, in objects, bytes and
  rounds.

Each module's docs state what its phases read and write against the
code paths that serve them.

## Running

```sh
# One run, one profile
cargo run -p dialog-soak --release -- run --network mobile

# Another scenario
cargo run -p dialog-soak --release -- run --scenario catchup --network none

# Custom link
cargo run -p dialog-soak --release -- run --network custom \
    --latency-ms 120 --auth-ms 200 --bandwidth-mbps 8

# Sweep the tree's branching factor (fanout 2^5 = 32 vs 2^8 = 256)
DIALOG_TREE_FANOUT_N=5 cargo run -p dialog-soak --release -- run --network mobile
```

Every scenario seeds the same space (`--entities`, `--commits`,
`--members`) before its measured phases.

Network presets: `none` (counts only), `localhost`, `broadband`, `mobile`
(a 4G-ish link: 80 ms RTT, 120 ms redeem, 20 Mbit/s), `browser` (the
same link with six connections per host, what a browser allows over
HTTP/1.1 and what tonk's replicas get), `intercontinental`. A custom link
takes `--connections` too; `DIALOG_FS_NETWORK` takes `connections=6`.

The JSON report goes to stdout, a human-readable table to stderr.

## The soak sweep and regression gate

```sh
# Full sweep (networks × optionally fanouts), one JSON per configuration
cargo run -p dialog-soak --release -- sweep --out-dir target/soak-new --fanouts 5,8

# Compare against a stored baseline; non-zero exit on regression
cargo run -p dialog-soak --release -- compare soak/baseline target/soak-new
```

The sweep runs every scenario (`--scenarios join,catchup,writer`) over
every profile, each configuration three times, and keeps the median run.
Under the default fixed seed the repeats must agree (the same seeded root,
and on `none` the same requests, bytes, duplicates and blob opens in every
phase) or the sweep fails: something the seed does not reach moved the
tree. The chain is exact in every phase but one: the join's `filtered`
reads 106 instead of 107 in about one run in ten (one read-ahead joins the
round of the read beside it), so it is left to the compare's slack. With `DIALOG_SOAK_SEED=random`
each run draws its own seed, so the repeats exercise the spread identities
cause (a leaf seam moves, and a block or two with it between phases).

The `none` profile run under the baseline's seed is exact, so the gate
holds it tight in every phase; shaped profiles, and a `none` run under
another seed, gate their lazy-join and download totals loosely, and every
profile gates duplicate fetches per phase (see the `dialog_soak::compare`
module docs). The nightly `soak:sync` arm runs this against the
checked-in baseline under `soak/baseline` with random seeds, so variance
is still exercised there. The pull-request workflow's `Replication gate`
job (`soak:gate`) runs the `none` profile alone, under the fixed seed,
against the same baseline, so a change to the protocol's shape is caught
before it lands; the shaped profiles stay nightly.

To replay a nightly run, take the `seed` from its report:

```sh
DIALOG_SOAK_SEED=<seed> cargo run -p dialog-soak --release -- run --scenario join --network mobile
```

The baselines are recorded from CI's own run, not a developer machine:
download the `soak-current` artifact the gate uploads and copy its
reports over `soak/baseline`, the way `perf/baseline` is kept.

## Reading a report

Per phase: `virtual_ms` (modeled time), `rounds` (modeled time over the
per-request serial cost: an upper-bound estimate of the longest
sequential fetch chain, 0 when unshaped), `chain` (`round_trips` in the
JSON: the exact longest chain of requests each issued only after the one
before it completed, counted by the simulation on every profile; on
`none`, in lockstep, the rounds requests were issued in),
`requests`, `unique` (distinct blocks received), `dup` (requests that
re-downloaded an already-received block), `empty` (requests the remote
had no block for), `blobs` (blob opens, in either direction), `bytes`,
`blob bytes` (the share of `bytes` that streamed through blob readers
and writers), and a per-effect-kind breakdown (`archive.get`,
`memory.resolve`, ...). The three costs the harness reports are
`chain`, `requests` and `bytes`; `notes/replication-cost-model.md` says
what each stands for. Things to watch:

- **`pull` should stay O(1)** — one head resolve, no block reads (the
  fast-forward adoption by root). If block gets appear here, a merge path
  regressed.
- **`probe`/`roster`/`render` request counts** are the lazy-join cost a
  user actually waits on. Sequential descents cost
  `depth × (auth + latency)` each; growth here means deeper trees, lost
  cache locality, or a new sequential round trip.
- **`requery` should be 0 requests** — a warm replica must not re-fetch.
- **`dup` should stay 0 everywhere** — a duplicate fetch means a block
  was paid for and thrown away (the walker's range-bounded read-ahead
  and `NetworkedIndex` hydration exist to prevent exactly this).
- **`concept`'s `chain`** is the sequential-chain depth of the query
  engine's cold join — the number the parallel-replication work (#492)
  drives down. `rounds` bounds the same quantity from modeled time and
  reads higher by the share of the phase that was bytes on the link.
- **`download`** tracks total space size and the transfer's shape
  (`ceil(blocks/16)` waves of latency + bytes/bandwidth).

## Caveats

- The auth delay models the *uncached* redeem cost per object; the real
  permit cache never helps a first replication (it is keyed per object
  path), which is exactly the modeled case. Set `--auth-ms 0` to model an
  amortized-authorization future (batched permits / prefix scoping).
- Bandwidth is modeled as one shared FIFO link; parallel streams do not
  get extra capacity. Real links are somewhere between this and
  per-stream capacity.
- Without a connection cap the client opens as many requests as it
  issues, which a fetch window of 64 does and a browser cannot: the
  `browser` preset is the one to read for a browser replica, and the gap
  between it and `mobile` is what the window cannot buy there.
- Blob bytes are metered as they stream through the returned reader or
  writer, and charged to the shared link like a block's payload. The
  join scenario carries no blobs (its values fit inline), so its blob
  columns read zero; a layout change that spills values shows up there.
