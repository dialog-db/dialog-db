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
  query), `entity` (open one item), `requery` (warm re-read), and
  `download` (full materialization, on a second fresh client).
- The `Fs` transport meters every remote effect (`dialog_remote_fs::simulation`)
  and, when a `NetworkShape` is configured, delays each one by its modeled
  cost: per-request **auth latency** (the per-object access-service redeem),
  **round-trip latency** (overlapping across concurrent requests), and
  serialization on a **shared bandwidth-limited link** (concurrent payloads
  queue, as on a real last-mile link).
- The whole run executes under tokio's **paused test clock**: modeled delays
  complete instantly while virtual time advances by exactly the modeled
  amount. A run over a 300 ms link finishes in real seconds and reports
  deterministic modeled times.

## Running

```sh
# One run, one profile
cargo run -p dialog-soak --release -- --network mobile

# Custom link
cargo run -p dialog-soak --release -- --network custom \
    --latency-ms 120 --auth-ms 200 --bandwidth-mbps 8

# Sweep the tree's branching factor (fanout 2^5 = 32 vs 2^8 = 256)
DIALOG_TREE_FANOUT_N=5 cargo run -p dialog-soak --release -- --network mobile
```

Network presets: `none` (counts only), `localhost`, `broadband`, `mobile`
(a 4G-ish link: 80 ms RTT, 120 ms redeem, 20 Mbit/s), `intercontinental`.

The JSON report goes to stdout, a human-readable table to stderr.

## The soak sweep and regression gate

```sh
# Full sweep (networks × optionally fanouts), one JSON per configuration
SWEEP_FANOUT=1 OUT_DIR=target/soak-new scripts/soak.sh

# Compare against a stored baseline; non-zero exit on regression
scripts/soak-compare.py soak/baseline target/soak-new
```

The sweep runs each configuration three times and keeps the median run
(identities and commit timestamps shift leaf boundaries slightly between
runs). The `none` profile's request counts are deterministic — no delays
means no duplicate in-flight fetches — so the gate holds them tight;
shaped profiles gate their lazy-join and download totals loosely (see
`scripts/soak-compare.py`'s docstring). The nightly `soak:sync` arm runs
exactly this against the checked-in baseline under `soak/baseline`.

## Reading a report

Per phase: `virtual_ms` (modeled time), `requests`, `bytes`, and a
per-effect-kind breakdown (`archive.get`, `memory.resolve`, ...). Things to
watch:

- **`pull` should stay O(1)** — one head resolve, no block reads (the
  fast-forward adoption by root). If block gets appear here, a merge path
  regressed.
- **`probe`/`roster`/`render` request counts** are the lazy-join cost a
  user actually waits on. Sequential descents cost
  `depth × (auth + latency)` each; growth here means deeper trees, lost
  cache locality, or a new sequential round trip.
- **`requery` should be 0 requests** — a warm replica must not re-fetch.
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
- Blob bytes are not metered (they move through readers/writers outside
  the invocation); the join scenario carries no blobs.
