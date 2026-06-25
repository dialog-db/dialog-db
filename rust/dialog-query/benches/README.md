# Query-engine benchmarks

Measures the query engine on three signals, isolating the planner's
round-trip objective from raw CPU:

| Bench          | Backend                       | Signal                                   |
|----------------|-------------------------------|------------------------------------------|
| `query_reads`  | `MemoryStorageBackend`        | block-read count for a single attribute scan |
| `query_join`   | `MemoryStorageBackend`        | block-read count for a 2-attribute concept join |
| `query_memory` | `MemoryStorageBackend`        | in-memory wall-clock (CPU isolation)     |
| `query_disk`   | `NativeTempSpace` (tempdir)   | on-disk wall-clock (real-world latency)  |

The `*_reads` benches `println!` the **read counts** (`reads` = total block
fetches, `unique_reads` = distinct blocks) before timing. Read count is the
planner's true objective: it is deterministic and machine-independent, so a
planner change that touches more blocks shows up as a count change even when
in-memory wall-clock is unmoved. Wall-clock benches catch CPU regressions
that read-count cannot see.

The benches drive queries only through the **public** surface
(`#[derive(Concept)]` + `Query::<T>::perform`) and seed via concept-instance
asserts, so the same bench source compiles against different engine versions
unchanged — see "old-vs-new comparison" below.

## Running

Benches need the `helpers` feature (it gates `BenchEnv`, included into each
bench via `#[path = "../src/helpers.rs"]`):

```sh
# all query benches
cargo bench -p dialog-query --features helpers

# one bench, quick (short warm-up/measurement)
cargo bench -p dialog-query --bench query_join --features helpers \
  -- --warm-up-time 2 --measurement-time 4
```

In the nix dev shell, wrap in `bash -lc` so the menu banner does not
intercept the cargo invocation:

```sh
nix develop --command bash -lc \
  'cargo bench -p dialog-query --bench query_join --features helpers'
```

Benchmarks should be run **plugged in**: on battery, macOS throttles the CPU
and wall-clock numbers become unreliable (the read counts stay correct).

## Tracking over time with criterion

Criterion saves every run under `target/criterion/<bench>/<id>/` and, on the
next run, prints the delta against the previous run plus a regression verdict:

```
query_join/1000  time: [25.196 ms 25.355 ms 25.522 ms]
                 change: [-0.95% +0.00% +0.85%]  No change in performance detected.
```

It also writes an HTML report with violin plots and regression lines:

```
target/criterion/report/index.html
```

### Named baselines (used for the old-vs-new engine comparison)

Save a labelled baseline, switch code, save another, then diff:

```sh
# on the current engine
cargo bench -p dialog-query --bench query_join --features helpers -- --save-baseline new

# check out the comparison branch / older engine, then
cargo bench -p dialog-query --bench query_join --features helpers -- --save-baseline old

# compare two saved baselines (statistical, not eyeballing)
cargo bench -p dialog-query --bench query_join --features helpers -- --baseline old
```

For CI trend dashboards across commits, feed criterion's JSON output to
`cargo-criterion` or Bencher.dev. Not wired yet.
