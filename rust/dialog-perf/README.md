# dialog-perf

The engine's cost matrix: one scenario per path an application drives
through dialog, each measured in numbers that do not drift between
machines or runs, gated in CI against the baselines under
[`perf/baseline`](../../perf/baseline).

## Why

A change to the engine can make one path cheaper and another ten times
dearer, and the test suite says nothing about either. The path that
pays is usually one no benchmark covered: a transaction read between
its writes, a field four rules state a case for, a commit through a
relation a rule derives. This crate covers those paths one by one,
measures each the same way, and fails the build when one grows.

## What a scenario measures

Each scenario runs its setup (seeding, rule installation, a warm-up
poll) off the record, then its **measured phase**, and reports:

| field | what it is | gate |
|---|---|---|
| `outcome` | what the phase produced: rows read, facts committed | exact |
| `counters` | archive blocks read and written (and their bytes), the engine's own steps: rule resolutions, rule hydrations, cell settlements, commits, tree writes, and any measure the scenario adds (`first-query` adds the root frame's bytes) | steps exact; blocks within two, bytes within 15% |
| `instructions` | instructions the phase retired, under callgrind | within 3% |

The counters come from the tracing spans the engine emits
(`resolve_bundle`, `resolve_rules`, `hydrate_rule`, `settle_cell`,
`commit`, `write_instructions`, ...) and from a metered operator that
sees every archive block beneath the branch's caches. The steps are
the same on every machine and every run: a count that moves is a
change in what the engine does, which is what a review needs to know.
The blocks are nearly so: the peer runs as a fixed-seed credential
and names everything by index, so two runs write the same records,
and a later commit's leaf seams still land a block or two apart.

The instruction count is callgrind's, over the measured phase alone:
collection starts off and toggles on the `dialog_perf_measured` symbol
the harness calls the phase through. It stands in for the wall clock
without the noise; on one binary it repeats to well under a percent.

## Running

```sh
cargo run -p dialog-perf --release -- list
cargo run -p dialog-perf --release -- run rule-ranked          # one scenario, JSON to stdout
cargo run -p dialog-perf --release -- sweep --out-dir target/perf-current
cargo run -p dialog-perf --release -- compare perf/baseline target/perf-current
```

`sweep` runs each scenario in its own child process under valgrind
(`--no-callgrind` skips it and reports counters alone) and leaves
`<scenario>.callgrind` beside each report for `callgrind_annotate`.
`compare` prints one line per scenario and exits non-zero on a
regression. In the nix dev shell, `perf:gate` runs both; CI runs it on
every pull request and every push to main (the `Perf gate` job) and
uploads the sweep.

On a pull request the gate also compares against the sweep main's own
CI run recorded for the commit the pull request lands on
(`compare --base <that sweep> target/perf-current`, fetched by
`scripts/ci/base-sweep.sh`), and fails on a regression against either.
The baseline files leave room: main often measures under them, after
improvements nobody re-baselined, and three changes that each stay
inside the allowance of a file sum past it. Against its own base a
change has no such room. `--markdown` writes every row, moved or not,
and the `Delta report` job posts both gates' tables as one comment on
the pull request, edited on each push, so the before and after need not
be carried by hand. To reproduce locally, download the base commit's
`perf-current` artifact into `target/perf-base` and run `perf:check`.

An instruction count also depends on the CPU: valgrind passes the
host's feature flags through, and code that picks a SIMD path at
runtime retires a different count on another CPU model. A sweep records
its CPU in `runner.txt`. When the base ran on the same model and flags,
instructions are held to the tolerance; when not (GitHub's runner pool
mixes several), each scenario is allowed its measured cross-CPU spread
on top (`cross_cpu_pct` in the catalog, from eight identical sweeps in
#653: rough, and meant to go once the measured build stops dispatching
on CPU features, #652).

Use `--release`: the baselines are recorded under the release profile,
and `compare` refuses to compare across profiles.

A scenario whose instruction count is known to vary between runs of
one binary past the allowance is marked volatile in the catalog, with
the reason, and the gate holds its counters alone until the variance
is understood. `commit-succeed` is one: its commit's tree write varies
by up to 12% between runs of one binary, every counter equal, in the
delete path's forced merges.

## When the gate fails

- **A counter grew.** The engine took more steps or moved more blocks
  for the same workload. Find out why before anything else; a
  resolution or settlement that runs once per row where it ran once
  per query is this crate's reason to exist.
- **Instructions grew past 3%.** Run the scenario under
  `callgrind_annotate --inclusive=yes target/perf-current/<scenario>.callgrind`
  on both heads and diff the inclusive costs.
- **The outcome changed.** The workload answers differently: either a
  wrong answer, or a scenario whose shape changed. Both are a bug or a
  deliberate change to pin, never a re-baseline.
- **A cost is the price of a change you mean to make** (a new record
  per commit, say): re-baseline. A scenario whose baseline file the
  pull request re-records has accepted its cost, so its growth against
  the base shows in the delta report marked as re-baselined and does
  not fail the gate. The report is the before and after the review
  reads.

## Re-baselining

An instruction count belongs to a binary, and a different toolchain
builds a different binary: a sweep from this container's own `rustc`
ran 2 to 6% under the same code built by the nix-pinned one CI uses.
The baselines are therefore the nix toolchain's numbers. Record them
in the nix dev shell:

```sh
nix develop --command bash -lc \
  'cargo run -p dialog-perf --release -- sweep --out-dir perf/baseline'
git add perf/baseline
```

or take them from the `Perf gate` job's uploaded `perf-current`
artifact, which is the same sweep on the same toolchain. Commit the
reports with the pull request that changes the cost, with the numbers
in its body. `compare` also lists improvements past the allowance;
re-baseline after one so the gate holds the new level.

## Adding a scenario

A scenario is a `Prepared` implementation in `src/scenario/`: an
`async fn prepare(size) -> Box<dyn Prepared>` that builds the
environment and seeds it, and a `measure` that runs the phase and
returns its outcome. Register it in `catalog()` with its baseline size
and a line on what it measures, sweep into `perf/baseline` to record
it, and commit both. Everything a scenario writes must be a function
of its size (entities named by index, values by index) so two runs
move the same blocks; `Env` and the builders in `src/env.rs` keep it
so.

## Limits

The matrix runs natively. The counters are target-independent, so a
step count that holds here holds in the browser; the instruction count
is native code's, and a path whose cost ratio differs in wasm (deep
generic async machinery in a debug build) can regress there by more
than it does here. Replication cost over a network is dialog-soak's
matrix, not this one's.
