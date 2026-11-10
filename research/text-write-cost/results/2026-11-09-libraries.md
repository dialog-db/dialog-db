# E2, E4 and the E1 library bar: four text CRDT libraries

Date 2026-11-09. Worktree `/home/claude/wt-libs`, branch `research/text-libs`. The first harness is in commit `efaab95`. The Diamond Types git main changes are in the working tree but not committed: the commit and push were denied by the permission system. This report is not in the repo.

## Paths and how to run

- Spike crate: `/home/claude/wt-libs/research/text-crdt-survey/spike` (own empty `[workspace]`, `target/` is git-ignored).
  - `src/main.rs`: trace loader, faithful concurrent replay driver, measurements, table output.
  - `src/libs/{automerge,loro,yrs,dt,dt2}.rs`: one adapter per library behind a small `Lib` trait. `dt` is Diamond Types 1.0.0 (crates.io), `dt2` is Diamond Types at git main.
  - `examples/am_scaling.rs`: side check of Automerge's per-call import cost.
- Versions: automerge 0.12.0, loro 1.17.1, yrs 0.28.1 and diamond-types 1.0.0 from crates.io. Diamond Types git main is `josephg/diamond-types` at rev `89ae3a0ab8d941a2885e6ec89d094bc9ce9d2922` (root package `diamond-types` 2.0.0), as dependency key `diamond-types-git`.
- Run:

```
cd /home/claude/wt-libs/research/text-crdt-survey/spike
export RUSTUP_TOOLCHAIN=stable-x86_64-unknown-linux-gnu
cargo build --release
D=/home/claude/data/editing-traces/concurrent_traces
./target/release/text-crdt-spike $D/friendsforever.json.gz $D/clownschool.json.gz --runs 7   # --libs automerge,loro,yrs,dt,dt2
cargo run --release --example am_scaling /home/claude/data/editing-traces/sequential_traces/friendsforever_flat.json.gz
```

- Datasets (not copied): friendsforever.json.gz sha256 `c80f9ee7...2769` (26 078 txns, 2 agents), clownschool.json.gz sha256 `b53866c8...0b94` (23 136 txns, 3 agents). Both are pure ASCII, so position units (code points, UTF-16, bytes) do not matter. The README's txn counts (~4000, 5380) do not match the files.
- The full table run took about 25 min on 2 vCPU (Xeon 2.1 GHz). The DT 1.0 vs git run (`--libs dt,dt2`) took 83 s and ran alone on the machine.
- Raw output: `/tmp/claude-0/-home-claude/4507dc3e-fd42-59bc-b8b8-da41034e480a/scratchpad/results.md` and `.../scratchpad/results-dt.md`.

## Method

- **Replay.** Every library got the faithful concurrent replay: one replica per agent, each with its own peer id. Before a txn, its agent's replica is brought to exactly the parents' state by importing the missing txns' exported change bytes in trace order. The driver asserts that the replica is never ahead of the parents. Then the txn's patches are applied locally as one library transaction and exported as one change. **All remote merging goes through single-change bytes**, so the replay itself exercises H2. At the end every replica imports everything and is compared with `endContent`.
- **Per-change export:**
  - Automerge: `commit` + `get_last_local_change().bytes()`.
  - Loro: `export(Updates { from: vv_before })`.
  - Yrs: `TransactionMut::encode_update_v1`.
  - DT: `oplog.encode_from(ENCODE_PATCH, version_before)`.
- **Snapshot:**
  - Automerge: `save()`.
  - Loro: `ExportMode::Snapshot`.
  - Yrs: `encode_state_as_update_v1(empty)`.
  - DT: `encode(ENCODE_FULL)`.
- **Batch** (all history in one export):
  - Automerge: `bundle(all hashes)` (experimental API).
  - Loro: `all_updates()`.
  - Yrs: `merge_updates_v1(all per-change updates)`.
  - DT: `encode(ENCODE_PATCH)`.
- **Out of order.** All change bytes are shuffled with a fixed-seed Fisher-Yates and imported one call each into a fresh document; then I count errors and compare the text.
- **Load timings.** Median of up to 7 runs, each verified against `endContent`. Runs stop early after 30 s, and `n` is shown.
  - "Each" means one import call per change, in causal order.
  - "One call" means all individual changes handed over at once (Automerge `apply_changes(vec)`, Loro `import_batch`, Yrs one transaction). DT 1.0 has no multi-chunk import, so its "one call" column is a live branch merging each change.
  - DT's "each" fills the op log and checks out the text once at the end.

## Results (measured)

B = bytes. Per-change average = sum / changes.

| library | trace | converged | snapshot B | sum per-change B (avg) | batch B | sum / batch | out of order | load snapshot ms | load changes, each ms | load changes, one call ms | change id |
|---|---|---|---|---|---|---|---|---|---|---|---|
| Automerge | friendsforever | yes | 45 483 | 2 457 428 (94) | 51 840 | 47x | buffers, 0 errors, converged | 51 | 71 327 (n=1) | 175 | hash (32 B), also (actor, seq), deps by hash |
| Automerge | clownschool | yes | 49 766 | 2 222 963 (96) | 56 755 | 39x | buffers, 0 errors, converged | 48 | 56 936 (n=1) | 159 | same |
| Loro | friendsforever | yes | 70 015 | 2 284 777 (88) | 49 155 | 46x | buffers (ImportStatus.pending on 26 072 of 26 078 imports), converged | 0.15 | 1 577 | 213 | (peer, counter span), counter per op |
| Loro | clownschool | yes | 70 241 | 2 036 618 (88) | 54 023 | 38x | buffers, converged | 0.33 | 1 311 | 213 | same |
| Yrs | friendsforever | yes | 38 742 | 362 140 (14) | 205 064 | 1.8x (9.3x vs snapshot) | buffers (pending structs), nothing pending at end, converged | 0.81 | 22.8 | 21.7 | (client, clock span) + delete set |
| Yrs | clownschool | yes | 32 910 | 331 368 (14) | 192 969 | 1.7x | buffers, converged | 0.56 | 19.4 | 15.9 | same |
| Diamond Types 1.0 | friendsforever | yes | 41 971 | 1 630 721 (63) | 41 971 | 39x | **refuses**: `BaseVersionUnknown` on 26 072 of 26 078, rolls back cleanly; converged only with a caller-side retry buffer | 427 | 219 | 258 | (agent name, seq span) |
| Diamond Types 1.0 | clownschool | yes | 50 482 | 1 479 004 (64) | 50 482 | 29x | same, 23 117 refusals | 687 | 355 | 229 | same |
| Diamond Types git | friendsforever | yes | 42 272 | 1 630 721 (63) | 42 272 | 39x | same as 1.0 | 4.6 | 23.2 | 188 | same |
| Diamond Types git | clownschool | yes | 50 778 | 1 479 003 (64) | 50 778 | 29x | same as 1.0 | 7.5 | 23.4 | 180 | same |

The DT 1.0 rows above are from the first, contended run. The uncontended rerun (`results-dt.md`) gives 436 / 787 ms (snapshot), 229 / 381 ms (each) and 284 / 261 ms (one call).

Other measurements:

- **Shuffled import of the whole history:**
  - Automerge: 116 to 203 s.
  - Loro: 14 to 20 s.
  - Yrs: 9 to 10 s.
  - DT with retry buffer: 14 to 17 s (1.0), 12 to 15 s (git).
- **Full concurrent replay wall time** (friendsforever / clownschool):
  - Automerge: 156 / 112 s.
  - Loro: 1.9 / 2.9 s.
  - Yrs: 0.17 / 0.19 s.
  - DT: 0.8 / 1.1 s (1.0), 1.0 / 1.3 s (git).
- **Automerge per-call import cost grows linearly with document size** (`am_scaling`, flat friendsforever, one `apply_changes` call per change): 0.56 ms per call for the first 2000 changes, rising to 11 ms per call by change 24 000. Importing one change per call is therefore quadratic. Handing all changes over in one call takes 160 to 175 ms.
- **Yrs deletes do not advance a clock.** Delete-only txns carry no new `(client, clock)`: 2358 of 26 078 in friendsforever and 809 of 23 136 in clownschool. Their change bytes are just a delete set.
- **Snapshot contents differ by library:**
  - Yrs snapshots garbage-collect deleted content and keep no history.
  - Loro snapshots hold history plus state (hence 70 KB).
  - DT's saved form is the op log only: no materialized text, so loading means replaying. That replay takes 427 to 787 ms on 1.0 and 4.6 to 7.5 ms on git main.
- **Caveats on timing:**
  - The friendsforever Automerge row overlapped with the `am_scaling` run on a 2-core machine. Its replay was 92 s and its per-call load 131 s in an earlier uncontended n=1 run.
  - The n=1 Automerge "each" numbers are single runs and noisy.

## Diamond Types at git main

Rev `89ae3a0a` (2026-09-02). The library is the root package `diamond-types` 2.0.0; the workspace's `crates/*` are tools and bindings (cli, wasm, swift, bench). Its public API is close to 1.0:

- `list::{ListCRDT, ListOpLog, ListBranch}`;
- `ListOpLog::encode_from(&EncodeOptions, frontier)` for changes since a version;
- `decode_and_add` and `ListCRDT::merge_data_and_ff` for import;
- `checkout_tip` for the text.

The merge underneath is the newer Eg-walker style one. Per the README, the crates.io release is out of date in both API and performance.

### What failed or is missing

- **Build.** The crate does not build as-is. Its `smallvec = "2.0.0-alpha.12"` requirement resolves to 2.0.0-beta.4, which fails with three E0119 conflicting `From` impls (`frontier.rs`, `buffered_iter.rs`, `causalgraph/graph/mod.rs`). There is no upstream lockfile at the root. The spike pins it with `cargo update -p smallvec@2.0.0-beta.4 --precise 2.0.0-alpha.12`, recorded in the spike's `Cargo.lock`.
- **End content in the snapshot.** `EncodeOptions::experimentally_store_end_branch_content(true)` writes an `ExperimentalEndBranch` chunk: 53 468 / 62 191 B, against 42 272 / 50 778 B without it. Nothing in the decoder reads that chunk, so a snapshot cannot skip the replay at this rev. With git main's replay speed it does not need to.
- **No buffering of out-of-order changes.** `decode_and_add` still refuses a change with missing parents (`BaseVersionUnknown`) and rolls back. There is still no multi-chunk import.

### Measured (uncontended, median of 7)

| | 1.0.0 ff / cs | git main ff / cs |
|---|---|---|
| converged | yes / yes | yes / yes |
| snapshot B | 41 971 / 50 482 | 42 272 / 50 778 |
| sum per-change B | 1 630 721 / 1 479 004 | 1 630 721 / 1 479 003 |
| batch B | 41 971 / 50 482 | 42 272 / 50 778 |
| out of order | refuses, rolls back; converges with caller retry | same |
| load snapshot ms | 436 / 787 | **4.6 / 7.5** |
| load changes, decode_and_add per change + one checkout, ms | 229 / 381 | **23.2 / 23.4** |
| load changes, live branch merge per change, ms | 284 / 261 | 188 / 180 |
| change id | (agent name, seq span) | same |

### What this shows

- **Measured.** On git main, opening from the saved op log is 60 to 100x faster than on 1.0. Rebuilding from individual changes is 10 to 16x faster.
- **Measured.** Merging each change into a live branch is the slow path on both versions (180 to 290 ms over the whole history, about 7 to 11 us per change). An editor pays this one keystroke at a time; a fresh open does not.
- **Inference.** Wire and storage bytes are unchanged, so E1 sees no difference between the versions. Only H6 moves.

## Changes since a version (item 5)

All four need their own state to answer "changes since X":

| library | how it answers | what it needs |
|---|---|---|
| Automerge | `get_changes(heads)` | its change graph |
| Loro | `export(Updates { from: vv })` | its op log |
| Yrs | `encode_state_as_update(state_vector)` | its block store |
| DT | `encode_from(frontier)` | its op log |

When Dialog stores each change under `(origin, sequence)` and the tree differential answers "what is missing", none of these is used for sync. The libraries only have to cut the change just made:

- Automerge and Yrs do that directly (`get_last_local_change`, `encode_update_v1`).
- Loro and DT do it as "since the version just before this local txn", which is local and cheap.

Inference: needing a version vector or heads to compute a diff stops mattering once Dialog does the sync. That removes the objection recorded in H2 against version-vector libraries.

One caveat for keying: Dialog should use its own `(origin, sequence)` and not reuse the library's id.

- Yrs delete-only transactions have no clock.
- Loro and Yrs counters count ops or characters, not transactions.
- Automerge's natural key is a hash, with `(actor, seq)` alongside.
- DT agents are strings.

## Verdicts

### H2: a library fits if Dialog does the sync

The test was: export one change on its own, and import it out of order or in causal order.

| library | verdict | evidence | cost to note |
|---|---|---|---|
| Automerge | **Confirmed** | Single-change export with hash id. Import buffers missing deps (internal queue). Converged on both traces, both in order and fully shuffled. | Per-change import is O(doc size) per call. Live sync of one remote keystroke into a 21 KB doc costs ~10 ms. Rebuild must hand changes over in bulk. Per-change bytes are 39 to 47x the bundle. |
| Loro | **Confirmed** | `Updates { from: vv_before }` yields one change. Import holds back pending ops (`ImportStatus.pending`). Converged shuffled. | Per-change bytes 38 to 46x the batch (88 B each). |
| Yrs | **Confirmed** | `encode_update_v1` per transaction. `apply_update` stashes pending structs and delete sets. Converged shuffled with nothing left pending. | Per-change bytes only 1.7 to 1.8x the merged batch (14 B each). Ids cannot key delete-only changes. |
| Diamond Types (1.0 and git main) | **Confirmed with a condition** | Single-change export works and converges, on both versions. | Import of a change whose parents are missing is refused (`BaseVersionUnknown`) and rolled back, so **Dialog must deliver in causal order** or keep its own retry buffer (shown to converge). The parents are inside the encoded bytes, so ordering them needs either the retry loop or Dialog storing parents beside the change (inference). Per-change bytes 29 to 39x the batch (63 B each). |

The refutation clause ("can only export ranges since a version vector, and splitting loses compression") does not hold for any library: all can export one change. The loss of compression is real, though, at 30 to 47x for all but Yrs. That is E1's question, not E2's.

Inference on delivery order: Dialog's differential returns entries in key order, so within one origin they arrive in sequence order (causal), but across origins they do not. Three libraries absorb this; DT does not.

### H6: rebuild on open under ~100 ms (21 KB docs)

| library | verdict | evidence |
|---|---|---|
| Yrs | **Confirmed** | 16 to 23 ms from individual changes, either one call per change or one call total. Under 1 ms from a snapshot. |
| Automerge | **Refuted from changes, confirmed from snapshot** | 159 to 175 ms in one call; 57 to 71 s one call per change. 48 to 51 ms from `save()`. |
| Loro | **Refuted from changes, confirmed from snapshot** | 213 ms in one call; 1.3 to 1.6 s one call per change. Under 1 ms from a snapshot. |
| Diamond Types git main | **Confirmed** | 23 ms from individual changes (one `decode_and_add` per change, then one checkout). 4.6 to 7.5 ms from its saved form, which stores no text and is a replay of the op log. Nothing positional is persisted. |
| Diamond Types 1.0 | **Refuted** | 229 to 381 ms from changes. 436 to 787 ms from its saved form (also a replay). |

H6's refute clause says "the index has to be persisted, at which point it needs a home". For Automerge and Loro this means a local snapshot cache, which never has to be replicated. Whether a local, unreplicated snapshot counts as "a home" is the maintainer's call. For Yrs and DT git main, no cache is needed at this size.

## Best fit for "Dialog stores and syncs individual changes" (inference)

**Yrs looks like the best fit.**

- It is the only library whose per-change encoding is close to its batch encoding: 14 B per change, sum 1.7x the merged batch. Splitting history into one entry per change costs it little on the wire. The others pay 30 to 47x before any Dialog overhead.
- It rebuilds from individual changes well under 100 ms (16 to 23 ms), so no persisted positional index is needed. DT git main now matches it here.
- It buffers out-of-order delivery. DT does not.

Its costs:

- Dialog must assign its own `(origin, sequence)`, because delete-only updates have no clock.
- Yjs ordering can interleave in some multi-replica backward-insertion cases (survey [6]).
- Its snapshot drops history. That is acceptable when Dialog holds the changes, but there is no time travel from the snapshot.

**Diamond Types git main is now a close second, and first on H6's model.**

- It rebuilds from changes in 23 ms and from its op log in 5 to 8 ms. It stores only the op log, never a positional index, which is exactly the H6 and DeltaDB split.
- It is the Eg-walker implementation the hypotheses name.

What keeps it behind Yrs:

- 4.5x the per-change bytes (63 B vs 14 B).
- No out-of-order buffering. Dialog must deliver in causal order across origins or keep a retry buffer.
- The unpinned `smallvec` pre-release breaks the build.
- No crates.io release of this code.

If E1 shows Dialog's per-entry overhead dwarfs the 49 B difference, DT git main and Yrs are close to even (inference). The deciders then become ordering quality, where Yjs interleaves in some cases (survey [6]) and DT's behavior is marked unknown in the survey, and out-of-order handling.

**Loro is third.** It has Fugue ordering, buffering and sub-millisecond snapshot load. Per-change export is heavy (88 B), and it needs a local snapshot cache to open fast.

**Automerge works functionally**, but its per-call import cost grows with document size. Live single-change sync gets slower as the document grows (inference from the linear trend, measured to 26k changes).

**DT 1.0** requires causal delivery and is slow to open; it is superseded by git main.

These rankings hold for 2 to 3 agents, about 21 KB of ASCII text and one change per keystroke. Larger or non-ASCII documents were not measured.

## Not done

- Yrs update v2 encoding.
- Compressed (gzip) sizes.
- Sequential flat traces, except for the Automerge scaling check.
- Any Dialog-layout cost (E1 proper).
