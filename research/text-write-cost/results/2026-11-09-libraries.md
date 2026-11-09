# E2, E4 and the E1 library bar: four text CRDT libraries

Date 2026-11-09. Worktree `/home/claude/wt-libs`, branch `research/text-libs`, nothing committed.

## Paths and how to run

- Spike crate: `/home/claude/wt-libs/research/text-crdt-survey/spike` (own empty `[workspace]`, `target/` is git-ignored).
  - `src/main.rs`: trace loader, faithful concurrent replay driver, measurements, table output.
  - `src/libs/{automerge,loro,yrs,dt}.rs`: one adapter per library behind a small `Lib` trait.
  - `examples/am_scaling.rs`: side check of Automerge's per-call import cost.
- Versions (crates.io, all built): automerge 0.12.0, loro 1.17.1, yrs 0.28.1, diamond-types 1.0.0. The git version of Diamond Types was not tried, since crates.io built.
- Run:

```
cd /home/claude/wt-libs/research/text-crdt-survey/spike
export RUSTUP_TOOLCHAIN=stable-x86_64-unknown-linux-gnu
cargo build --release
D=/home/claude/data/editing-traces/concurrent_traces
./target/release/text-crdt-spike $D/friendsforever.json.gz $D/clownschool.json.gz --runs 7   # --libs automerge,loro,yrs,dt
cargo run --release --example am_scaling /home/claude/data/editing-traces/sequential_traces/friendsforever_flat.json.gz
```

- Datasets (not copied): friendsforever.json.gz sha256 `c80f9ee7...2769` (26 078 txns, 2 agents), clownschool.json.gz sha256 `b53866c8...0b94` (23 136 txns, 3 agents). Both are pure ASCII, so position units (code points, UTF-16, bytes) do not matter. The README's txn counts (~4000, 5380) do not match the files.
- Full table run took about 25 min on 2 vCPU (Xeon 2.1 GHz). Raw output: `/tmp/claude-0/-home-claude/4507dc3e-fd42-59bc-b8b8-da41034e480a/scratchpad/results.md`.

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

Other measurements:

- **Shuffled import of the whole history:**
  - Automerge: 116 to 203 s.
  - Loro: 14 to 20 s.
  - Yrs: 9 to 10 s.
  - DT with retry buffer: 14 to 17 s.
- **Full concurrent replay wall time** (friendsforever / clownschool):
  - Automerge: 156 / 112 s.
  - Loro: 1.9 / 2.9 s.
  - Yrs: 0.17 / 0.19 s.
  - DT: 0.8 / 1.1 s.
- **Automerge per-call import cost grows linearly with document size** (`am_scaling`, flat friendsforever, one `apply_changes` call per change): 0.56 ms per call for the first 2000 changes, rising to 11 ms per call by change 24 000. Importing one change per call is therefore quadratic. Handing all changes over in one call takes 160 to 175 ms.
- **Yrs deletes do not advance a clock.** Delete-only txns carry no new `(client, clock)`: 2358 of 26 078 in friendsforever and 809 of 23 136 in clownschool. Their change bytes are just a delete set.
- **Snapshot contents differ by library:**
  - Yrs snapshots garbage-collect deleted content and keep no history.
  - Loro snapshots hold history plus state (hence 70 KB).
  - DT's saved form is the op log only: no materialized text, so loading means replaying (427 to 687 ms).
- **Caveats on timing:**
  - The friendsforever Automerge row overlapped with the `am_scaling` run on a 2-core machine. Its replay was 92 s and its per-call load 131 s in an earlier uncontended n=1 run.
  - The n=1 Automerge "each" numbers are single runs and noisy.

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
| Diamond Types 1.0 | **Confirmed with a condition** | Single-change export works and converges. | Import of a change whose parents are missing is refused (`BaseVersionUnknown`) and rolled back, so **Dialog must deliver in causal order** or keep its own retry buffer (shown to converge). The parents are inside the encoded bytes, so ordering them needs either the retry loop or Dialog storing parents beside the change (inference). Old crates.io release. |

The refutation clause ("can only export ranges since a version vector, and splitting loses compression") does not hold for any library: all can export one change. The loss of compression is real, though, at 30 to 47x for all but Yrs. That is E1's question, not E2's.

Inference on delivery order: Dialog's differential returns entries in key order, so within one origin they arrive in sequence order (causal), but across origins they do not. Three libraries absorb this; DT does not.

### H6: rebuild on open under ~100 ms (21 KB docs)

| library | verdict | evidence |
|---|---|---|
| Yrs | **Confirmed** | 16 to 23 ms from individual changes, either one call per change or one call total. Under 1 ms from a snapshot. |
| Automerge | **Refuted from changes, confirmed from snapshot** | 159 to 175 ms in one call; 57 to 71 s one call per change. 48 to 51 ms from `save()`. |
| Loro | **Refuted from changes, confirmed from snapshot** | 213 ms in one call; 1.3 to 1.6 s one call per change. Under 1 ms from a snapshot. |
| Diamond Types 1.0 | **Refuted** | 219 to 355 ms from changes. 427 to 687 ms from its saved form, which is also a replay. Newer DT on git may differ; not measured. |

H6's refute clause says "the index has to be persisted, at which point it needs a home". For Automerge and Loro this means a local snapshot cache, which never has to be replicated. Whether a local, unreplicated snapshot counts as "a home" is the maintainer's call. For Yrs, no cache is needed at this size.

## Best fit for "Dialog stores and syncs individual changes" (inference)

**Yrs looks like the best fit.**

- It is the only library whose per-change encoding is close to its batch encoding: 14 B per change, sum 1.7x the merged batch. Splitting history into one entry per change costs it little on the wire. The others pay 30 to 47x before any Dialog overhead.
- It is the only one that rebuilds from individual changes well under 100 ms, so no persisted positional index is needed.
- It buffers out-of-order delivery.

Its costs:

- Dialog must assign its own `(origin, sequence)`, because delete-only updates have no clock.
- Yjs ordering can interleave in some multi-replica backward-insertion cases (survey [6]).
- Its snapshot drops history. That is acceptable when Dialog holds the changes, but there is no time travel from the snapshot.

**Loro is second.** It has Fugue ordering, buffering and sub-millisecond snapshot load. Per-change export is heavy (88 B), and it needs a local snapshot cache to open fast.

**Automerge works functionally**, but its per-call import cost grows with document size. Live single-change sync gets slower as the document grows (inference from the linear trend, measured to 26k changes).

**DT 1.0** requires causal delivery and is slow to open in this old release.

These rankings hold for 2 to 3 agents, about 21 KB of ASCII text and one change per keystroke. Larger or non-ASCII documents were not measured.

## Not done

- Diamond Types from git.
- Yrs update v2 encoding.
- Compressed (gzip) sizes.
- Sequential flat traces, except for the Automerge scaling check.
- Any Dialog-layout cost (E1 proper).
