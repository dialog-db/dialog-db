# Rule heads: journal

## Where things stand

**Exploring.** The harness is being written. No run yet.

## Datasets

None on disk. Each scenario seeds its own to-dos, lists, owners and teams from an index.

## How to reproduce

```
cargo build -p dialog-perf --release
for run in 1 2 3; do
  target/release/perf sweep --only "$(target/release/perf list | grep -o 'heads-[a-z0-9-]*' | paste -sd,)" --out-dir target/heads-$run
done
```
