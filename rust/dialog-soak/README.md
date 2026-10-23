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

## Running

```sh
# One run, one profile
cargo run -p dialog-soak --release -- run --network mobile

# Custom link
cargo run -p dialog-soak --release -- run --network custom \
    --latency-ms 120 --auth-ms 200 --bandwidth-mbps 8

# Sweep the tree's branching factor (fanout 2^5 = 32 vs 2^8 = 256)
DIALOG_TREE_FANOUT_N=5 cargo run -p dialog-soak --release -- run --network mobile
```

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

The sweep runs each configuration three times and keeps the median run
(identities and commit timestamps shift leaf boundaries slightly between
runs). The `none` profile's request counts are deterministic, so the gate
holds them tight; shaped profiles gate their lazy-join and download
totals loosely, and every profile gates duplicate fetches per phase (see
the `dialog_soak::compare` module docs). The nightly `soak:sync` arm runs
exactly this against the checked-in baseline under `soak/baseline`. The
pull-request workflow's `Replication gate` job (`soak:gate`) runs the
`none` profile alone against the same baseline, so a change to the
protocol's shape is caught before it lands; the shaped profiles stay
nightly.

The baselines are recorded from CI's own run, not a developer machine:
download the `soak-current` artifact the gate uploads and copy its
reports over `soak/baseline`, the way `perf/baseline` is kept.

## Reading a report

Per phase: `virtual_ms` (modeled time), `rounds` (modeled time over the
per-request serial cost: an upper-bound estimate of the longest
sequential fetch chain, 0 when unshaped), `chain` (`round_trips` in the
JSON: the exact longest chain of requests each issued only after the one
before it completed, counted by the simulation on every profile),
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
