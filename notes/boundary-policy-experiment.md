# Leaf boundary policy experiment

Three-arm experiment for taming the unbounded leaves the Phase A capture exposed. Phase A findings (SE dataset, replay of the retro transaction log): 29% of rows are `se.post/body` values in the 512-4096 byte inline band. Their keys exceed `max_separator = 512`, so the length-guarded coin demotes every one of them to rank 0. Demoted keys never end a segment, they sort contiguously in AEV and VAE, and the runs they form grow without bound: a 7.4 MB leaf at replay depth 8192, with 87-90% of all tree bytes in nodes over 100 KB, and every commit that touches such a leaf rewrites the whole block (disk-filling write amplification). Tonk, which demotes almost nothing, shows a plain geometric spread instead: leaf p10 12 KB, mean 54 KB, max 182 KB. Target node size is ~50 KB.

## Baseline mechanics (today)

- Rank `r(k) = geometric(blake3(k))` per key, pure; the length guard demotes any key with `len(k) > max_separator` to rank 0 (`rust/dialog-search-tree/src/distribution.rs`, `Geometric::rank`).
- A leaf segment ends at an entry with rank `> BOTTOM_RANK` (= 1); a level-n index node starts at a seam whose separator ranks `> BOTTOM_RANK + n` (one seam coin punches through all levels).
- A seam's separator is the shortest prefix of the right key that sorts strictly above the left key, front-coded in the parent (M3). Separators are bounded at `max_separator + 1` bytes by construction, and the seam coin's length guard keeps that one-byte edge quiet.

## The arms

All three run against the same captures, switched by manifest fields (with an env override in `Manifest::default()` as experiment plumbing, so the whole artifact stack picks the arm up for fresh trees):

- `DIALOG_TREE_MAX_SEGMENT` sets `Manifest::max_segment` (arm 1; 0 = off = baseline),
- `DIALOG_TREE_INLINE_N` sets `Manifest::inline_n` (arm 3).

`FORMAT_VERSION` stays 1: nothing has shipped, so format evolution needs no bump.

### Arm 1 "demotion + cap" (implemented)

Keep the demotion coin exactly as-is; add `max_segment` (new `Manifest` field, experiment value 128 KB, i.e. 2-3x the 50 KB target). When a run between accepted seams exceeds `max_segment` weighted bytes, force break(s) at deterministic positions.

Mechanics as implemented:

- **Weight proxy.** The cap compares against a pure per-entry weight: `key length + 32` (`ENTRY_WEIGHT_OVERHEAD`, covering the value slot and per-entry encoding overhead). True encoded size is not available at cut time (front coding and per-node dictionaries are computed at persist), and any deterministic proxy preserves convergence; for artifact trees the key carries the value (value-in-key epoch), so key length dominates real cost. Front coding means real blocks come in somewhat under the proxy.
- **Break positions.** Candidate seams inside a run are those whose forced separator would exceed `max_separator` (see below); this makes forced seams self-identifying in stored form and structurally quiet at index levels. The run is split recursively: while a piece exceeds the cap, cut at the best candidate, preferring seams where the adjacent keys share the fewest leading key components (the biggest semantic change), tie-broken by lowest `blake3(right key)`. This is a pure function of the run's entry list, so different edit histories converge.
- **Forced separators.** A forced seam's separator is `right[.. max(lcp+1, min(max_separator+1, len(right)))]`: the shortest prefix of the right key that both sorts above the left key and exceeds `max_separator` when the right key allows it. Demoted keys are all longer than `max_separator`, so every forced seam inside a demoted run carries a separator longer than `max_separator`, which the existing seam-coin length guard ranks 0: forced seams are leaf-level only and can never punch index cuts, with no new mechanism. Front coding amortizes the length: the run shares the prefix.
- **Runs of short keys.** A run made only of natural-length keys can exceed the cap (probability ~1.4% at 256-entry expected runs and ~118 B entries) but has no eligible candidate seam (a short forced separator could not be told apart from a natural one, which would break both index-quietness and incremental run detection). Such runs stay uncapped. The pathology being fixed lives entirely in the demoted band, so this is a deliberate non-goal.
- **Incremental convergence (the load-bearing part).** Cap decisions are a function of the whole run, so any edit whose leaf touches a capped run must re-shape the whole run. Forced seams never punch index cuts, hence a run's segments are always contiguous children of one parent index node, and membership in a run is detectable from stored separators alone (separator longer than `max_separator` joins a segment to its left sibling). The edit path widens its window before shape decisions: it merges the touched leaf with its contiguous run siblings (lifting them), and the boundary-delete / orphan-append path does the same for the right neighbor's run head, since deleting a run's terminating boundary joins two runs whose union re-caps. The rare natural separator of exactly `max_separator + 1` bytes matches the join predicate too; that only widens the window, and a wider regroup reproduces the same canonical shape.
- **Fast-path bypass.** The in-place fast path is skipped whenever the window merged more than one segment or the post-edit weight exceeds the cap, so a cap-relevant edit always reaches the regroup.

Known gaps (documented, not blocking the experiment): `stitch`/`carve` and the differential merge path do not widen to whole runs, so stitching trees built under a non-zero `max_segment` may produce non-canonical (still correct, just not byte-converged) shapes at piece seams. Artifacts does not use stitch; the convergence gate exercises the insert/delete path.

### Arm 2 "veto + cap" (veto IMPLEMENTED as step 1, see below; cap retained, off by default)

Retires demotion: delete the length guard and rank every key. A proposed seam between adjacent keys `p < k` is vetoed (rank clamped to 0 at all levels; one decision per seam, uniform across levels because the seam between `p` and `k` is the same seam everywhere) iff `len(shortest_separator(p, k)) > max_separator`; that string is exactly what the shaper already computes for the separator, so the veto is a length check on an existing value. The same `max_segment` cap as arm 1 applies on top, for vetoed runs that still grow without bound (VAE near-duplicate values).

Expectations to verify: AEV/EAV body runs split naturally (entities diverge around byte 35, prefixes 40-70 bytes, no veto), vetoes concentrate in VAE near-duplicates, and edit sensitivity is bounded: an insert or delete flips at most the two seams adjacent to the edited key. The bounded-flip claim must be measured, because a veto variant was once rejected over edit-sensitivity fears.

Implementation sketch: the leaf-cut rule becomes pair-aware (`cut after k` iff `rank(k) > BOTTOM_RANK` and the seam to the successor is not vetoed), which threads a successor into `regroup_entries` and every fast-path boundary check; the run-scoped machinery from arm 1 (window widening, self-identifying seams) is reused with the veto as the join predicate. A `boundary_policy` manifest field switches it. Deferred to the next pass; arm 1's gates come first.

### Arm 3 "spill-512" (diagnostic control, implemented via config alone)

`inline_n` 4096 -> 512 in the manifest, nothing else. Every value longer than 512 bytes spills to the block store and its key carries only the 64-byte order-preserving prefix plus the value hash, so no key exceeds `max_separator` by much and the demoted band shrinks to near nothing. This isolates how much of the pathology is the demoted band alone, at the cost of an extra block fetch for every large value read. Not a candidate to ship on its own (it trades read amplification for the write pathology); it calibrates the other two arms.

## Gates

1. **Convergence property test**: two different insertion orders, persisted per edit, produce byte-identical roots, and a delete-vs-rebuild oracle agrees, under an arm-1 manifest with keys straddling the demotion threshold. This is the load-bearing gate; it runs in the dialog-search-tree suite permanently.
2. **Full capture re-run per arm**: tonk harness (`DIALOG_IMPORT_CSV`) and SE replay (`DIALOG_TXN_MEM=1 DIALOG_TXN_LOG=retro-full.csv DIALOG_TXN_LIMIT=8192`). Success: SE leaf max at or under the cap and the 100K+ byte share collapses; tonk essentially unchanged (the cap should almost never fire there; if it fires often the mechanism is too eager).
3. **Write amplification**: SE replay at LIMIT 2048 on disk, twice per arm, report `store_bytes`. Baseline ~461-477 MB; arms 1-2 should drop materially.
4. **Perf guard**: in-memory warm replay total at LIMIT 2048 stays within ~10% of the ~2.0-2.1 s baseline.
5. Full nix suite green, `clippy -D warnings`, `fmt`; the existing review pins stay green.

## Results (2026-07-21)

### SE replay, in-memory, LIMIT 8192, leaf segments

| metric | baseline | arm 1 (cap 128 KB) |
|---|---|---|
| segments | 236 | 786 |
| total bytes | 49.18 MB | 49.25 MB |
| mean | 208.4 KB | 62.7 KB |
| p50 | 69.7 KB | 58.5 KB |
| p90 | 383.8 KB | 117.8 KB |
| p99 | 1.76 MB | 137.9 KB |
| max | 7.44 MB | 268.2 KB |
| bytes in 100K+ nodes | 90.1% | 40.2% (mostly 100-140 KB, i.e. at the cap; the bucket boundary sits below the cap) |
| root index | 28.2 KB, 236 links | 349.6 KB, 786 links (long forced separators bloat the parent) |

The 268 KB max is consistent with a short-key over-cap run that offers no candidate seam (expected ~10% of natural runs at this depth); the demoted-band pathology itself is fully collapsed. Success criterion met.

### Tonk import, leaf segments

| metric | baseline | arm 1 | arm 3 (inline 512) |
|---|---|---|---|
| segments | 25 | 27 | 27 |
| mean | 54.3 KB | 50.3 KB | 39.8 KB |
| p50 | 34.7 KB | 34.7 KB | 27.8 KB |
| max | 182.5 KB | 142.8 KB | 122.4 KB |

Arm 1 split exactly the two largest leaves (tonk's own small demoted band: >512 B description strings); everything else identical. The cap is not too eager. Arm 3 shrinks the tree by pushing those values out as blocks.

### Perf guard (SE replay, in-memory, LIMIT 2048): FAILS for arm 1

Baseline 3.37-3.39 s; arm 1 11.47-11.49 s (3.4x). At 8192: 21 s vs 145 s (7x), worsening with depth because every edit into a capped run merges and regroups the WHOLE run (the run, not the segment, is the shape unit) and re-hashes candidate seams. A membership-change shortcut (value-only updates and no-op deletes skip the widened window; landed, since cap decisions are key-derived) measured no gain on SE — value-in-key means nearly every SE edit inserts a new key. Mitigations, in order of expected payoff: batch-apply a flush's ops against the merged run once instead of per-op; memoize candidate blake3 hashes; or move to a per-key weight-proportional boundary coin (P(cut after k) = weight(k)/max_segment, forced separator rule unchanged), which needs no window machinery and no run merging at all — O(1) per edit, trivially convergent — at the cost of a soft cap (expected segment weight = max_segment, geometric tail) instead of a hard one.

### Write amplification (SE replay, on disk, `TXNLOG store_bytes`)

| limit | baseline | arm 1 |
|---|---|---|
| 2048 (x2) | 469.6 / 450.6 MB | 448.3 / 445.9 MB (-1..-4%) |
| 4096 | 1516.9 MB | 1285.7 MB (-15.2%) |

Smaller than hoped at these depths because per-link novelty buffering already amortizes leaf flushes and the disk store is content-addressed (unchanged re-serialized pieces overwrite their own file, contributing no growth). The win grows with depth as the giant-leaf flush cost dominates; at 8192+ (where baseline leaves are 7.4 MB) the gap should widen substantially, but a full disk run at that depth was not affordable here.

### Convergence gates: PASS

`it_converges_on_capped_runs_across_insertion_orders` (three insertion orders, persist per edit, byte-identical roots, plus a proof the cap actually fired) and `it_matches_rebuild_after_deletes_in_capped_runs` (incremental deletes, including a whole demoted cluster, land on the fresh-build root) both pass, and are permanent tests in dialog-search-tree. The full search-tree (244) and artifacts (158) suites pass both with the cap off (baseline unchanged) and with `DIALOG_TREE_MAX_SEGMENT=131072` exported.

### Arm 3 SE replay: BLOCKED by a spill-read bug in the repository commit path

`DIALOG_TREE_INLINE_N=512` SE replay fails in the first commits with `Artifact decode failed during commit: spilled value block missing from store`. The tonk harness (which commits through `Artifacts::commit_instructions`) works fine under the same setting, so the repository branch-commit path reads a value spilled by the same batch through a store view that does not yet contain the block. Lowering the threshold merely surfaces it: any value in the 512-4096 band asserted and re-read within one batch should reproduce it at defaults with a >4096 value. Needs its own investigation.

## Step 1: the veto replaces demotion (2026-07-21)

The length-guard demotion is deleted: `Geometric::rank` now ranks every key by its hash alone, whatever its length. In its place, one decision per SEAM: the seam between adjacent keys `p < k` (`p` the immediate predecessor in the full key order) is vetoed — treated rank 0 at every level — iff `len(shortest_separator(p, k)) > max_separator` (`Distribution::vetoes`, checked without allocating: the shortest separator is `min(lcp + 1, len(k))` bytes). The leaf cut rule in `regroup_entries` is pair-aware (`rank(p) > BOTTOM_RANK` AND seam not vetoed); the seam coin (`seam_rank`) carries the length guard explicitly now that `rank` no longer supplies it, which keeps forced separators (the cap's, `> max_separator` by construction) leaf-level-only exactly as before. `max_segment` and the whole arm-1 cap machinery remain, off by default; every measurement below runs with the cap OFF (pure veto).

What makes the veto edit-stable (the reason the old edit-sensitivity fear did not materialize): under the lower-bound convention a seam's separator — and with it both the veto and the index ranks — is a pure function of its two partner keys, and it is invariant under every edit that keeps both partners (a key routed left of a separator shares exactly the separator's divergence with the right key; a min-move can only shorten the separator, never lengthen it past the old one). The only edits that change a stored seam's status remove a partner key, and those (boundary delete, orphan append, single-entry vanish) already widen their re-shape window across the seam. One genuinely new transition existed: deleting the last key of a vetoed stretch can un-veto its high-coin predecessor's seam, which the in-place fast path used to swallow; `fast_path_keeps_canonical` now detects it (cheap old-seam veto test first, rank hash only at a real un-veto joint) and a red/green-pinned test (`it_recreates_a_cut_when_a_delete_unvetoes_a_seam`) covers it. The symmetric insert case is provably impossible (a key inserted between a vetoed pair shares at least the pair's common prefix, so both new seams stay vetoed — downward closure, now documented as a `vetoes` override requirement).

### Convergence gates: PASS

The two arm-1 capped tests pass unchanged under the veto coin. New permanent tests: `it_converges_on_vetoed_clusters_across_insertion_orders` (three orders, byte-identical roots, in-cluster seams provably absent, and a wide-bound control proving the veto is what shaped the tree), `it_matches_rebuild_after_deletes_in_vetoed_clusters` (delete oracle incl. a whole cluster), `it_handles_a_band_of_fully_vetoed_seams` (rewrite of the demotion band test: near-duplicate 600-byte keys, one open segment, delete convergence), and the fast-path pin above. Suites: dialog-search-tree 251, dialog-artifacts 158, dialog-query 737+43, all green; clippy `-D warnings` and wasm32 check clean.

### Seam-flip sensitivity: PASS (≤ 2, asserted permanently)

`it_flips_at_most_adjacent_seams_per_edit` builds the mixed cluster/short tree, applies 31 edits (near-duplicate inserts, cluster deletes, short-key inserts and deletes), and after each edit diffs the persisted leaf-boundary set against the previous state, excluding the edited key itself. No edit flips more than 2 pre-existing seams; the bound is a permanent assertion. This was the measured answer the rejected veto variant never got.

### SE replay, in-memory, LIMIT 8192, leaf segments (same-day interleaved A/B, cap OFF)

| metric | baseline (today) | veto |
|---|---|---|
| segments | 239 | 339 |
| total bytes | 49.18 MB | 49.11 MB |
| mean | 205.8 KB | 144.9 KB |
| p50 | 53.7 KB | 80.8 KB |
| p90 | 413.2 KB | 383.1 KB |
| p99 | 1.76 MB | 778.7 KB |
| max | 7.44 MB | 1.20 MB |
| bytes in 100K+ nodes | 89.3% | 84.9% |
| root index | 28.5 KB / 239 links = 119 B/link | 36.3 KB / 339 links = 107 B/link |

The 7.4 MB demotion-band leaf collapses WITHOUT any cap: the body runs split naturally where keys diverge early. Index links stay thin — 107 B/link mean (hash + offsets included) vs arm 1's 445 B/link forced-separator bloat, and thinner than the baseline's own 119. The residual 1.2 MB max (and the p99 tail) is the genuinely-indistinguishable band: near-duplicate runs (revision copies of one long value, VAE-shaped) whose every seam the veto correctly rejects. That residue is exactly what step 3's last-resort forced-break backstop is scoped for; no natural coin can split it.

### Tonk import: byte-neutral

Baseline and veto produce the identical 7-segment shape on today's export (5756-row CSV, 704 committed facts; today's repo export is smaller than the Phase A one, so absolute numbers are not comparable to the earlier table — the A/B on identical input is the signal). The veto changes nothing where nothing is near-duplicate: as hoped.

### Perf guard: PASS — veto is FASTER than baseline

In-memory replay, same-day interleaved (the report's 3.37-3.39 s / 21 s baseline states were not reproducible on today's machine state, so the baseline was re-measured alongside):

| limit | baseline (x2) | veto (x2) |
|---|---|---|
| 2048 | 4.22 / 4.31 s | 3.66 / 3.53 s (-16%) |
| 8192 | 47.0 / 47.0 s | 30.1 / 30.6 s (-35%) |

No widened windows, no run merging, no per-edit candidate hashing: the veto adds one lcp comparison per proposed boundary, and the smaller leaves make every touched-leaf rewrite cheaper — the deeper the replay, the bigger the win (baseline edits into the growing 7.4 MB leaf dominate at depth). Contrast arm 1's 3.4x/7x regression.

### Write amplification (SE replay, on disk, `TXNLOG store_bytes`, cap OFF)

| limit | baseline | veto |
|---|---|---|
| 2048 | 452.7 MB (6.5 s) | 446.1 MB (5.9 s) (-1.4%) |
| 4096 | 1528.3 MB (19.5 s) | 1428.3 MB (15.7 s) (-6.5%) |

Smaller than arm 1's -15.2% at 4096: the residual near-duplicate leaves (up to 1.2 MB) still amplify every edit that lands in them. The remaining gap is step 3's target; step 1 takes the free part of the win at negative CPU cost.

### Data provenance note

The Phase A input files were no longer on disk; both were regenerated for this pass: `retro-full.csv` from the archive.org retrocomputing dump (same Apr 2024 vintage, same `scripts/se-transform.py`; 117236 facts across 50553 transactions) and the tonk CSV via `tonk export` on `~/Projects/tonk`. The SE baseline capture reproduces the Phase A shape (7.44 MB max leaf, ~90% bytes in 100K+ nodes), confirming like-for-like input.

## Step 2: the weight-proportional coin (2026-07-21)

With the veto in place, the leaf coin becomes byte-pacing: under a non-zero `Manifest::max_segment`, `Geometric::rank` cuts after a key with probability `entry_weight(key) / max_segment` (`entry_weight` = key length + 32, arm 1's proxy), decided from `blake3(key)` alone — no accumulation, no window, O(1) per key, trivially convergent. `max_segment == 0` keeps the entry-counted geometric coin byte for byte (the whole suite, which runs at the default 0, is the identity proof). Expected run weight between cuts is `max_segment` with an `e^(-W/S)` tail (renewal argument), pinned by a permanent statistical test (`it_paces_runs_at_the_weight_target`: 60k mixed-length keys, mean within 10%, tail near `e^-2`).

Two derivation choices, documented here as the note of record. The coin reads the seam's LEFT key (the entry that closes its segment — the existing cut-after convention, not "begins a leaf"): a separator is a prefix of the seam's RIGHT key and sorts strictly above the left key, so the leaf coin and the seam coin can never read the same byte string, keeping the two coins independent by construction (under a "begins" convention a separator can equal the whole coin key and the coins would correlate). Levels above keep the geometric ladder unchanged: the seam coin ranks each accepted seam's separator geometrically, which IS uniform `1/m`-per-level subsampling of leaf boundaries; index entries are separator-sized, so entry-counted index fanout is exactly what bounds index nodes. `Geometric::seam_rank` is now an explicit override (guard + geometric) so the weight coin cannot leak into it.

Deleted (the 7x regression source): the arm-1 edit-path run machinery — `merge_capped_run`, the membership/over-cap fast-path gates, the neighbor run-widening, and the forced-split calls in `regroup_entries` (`seal_run`/`seal_part` collapsed back to the plain seal). The `cap` helpers stay as pure functions for step 3's rescoped backstop; `entry_weight` feeds the coin. The two arm-1 convergence tests now pin the weight coin (renamed `it_converges_on_weight_paced_trees_across_insertion_orders`, `it_matches_rebuild_after_deletes_under_the_weight_coin`) and pass with no window machinery at all, as the per-key purity argument predicts. Seam-flip sensitivity is unchanged by the coin swap (still per-key), and the whole battery of step-1 veto tests runs at `max_segment: 0` unaffected.

### Captures (veto + weight coin, same-day; SE mem LIMIT 8192, leaf segments)

| metric | veto only | veto + weight S=128K | veto + weight S=64K |
|---|---|---|---|
| segments | 339 | 323 | 643 |
| mean | 144.9 KB | 152.0 KB | 76.5 KB |
| p10 | 7.1 KB | 15.4 KB | 9.8 KB |
| p50 | 80.8 KB | 103.9 KB | 50.0 KB |
| p90 | 383.1 KB | 326.4 KB | 170.8 KB |
| p99 | 778.7 KB | 831.2 KB | 333.7 KB |
| max | 1.20 MB | 942.5 KB | 565.9 KB |
| replay time | 30.1-30.6 s | 30.7 s | 30.1 s |

Tonk under the same manifests: S=128K is too coarse for a 600 KB tree (2 segments, one 471 KB — a legitimate `e^-3.6` draw with only ~4 expected cuts, but a bad shape); S=64K gives 7 segments, mean 84.9 KB, p50 68.6 KB, min 39.2 KB, max 176.8 KB. At S=64K tonk (84.9 KB) and SE (76.5 KB) means converge to the same byte scale — the point of byte pacing — and the tiny-leaf floor lifts (tonk min 16.6 KB -> 39.2 KB; SE p10 holds while p50 centers on the scale).

The tail is exponential where the coin can pace: at S=64K, p90 = 2.6S against `e^-2.6` = 7.4% predicted (observed 10%), p99 = 5.1S against 0.6% predicted (observed 1%). The excess beyond ~5S (up to the 565.9 KB max) is the vetoed near-duplicate band the coin is not allowed to cut — step 3's target, now cleanly isolated as the only unpaced residue.

### Small-leaf cost accounting (steps 1-2 arms, SE mem 8192 per-node dumps; independent replay runs, so leaf counts differ by a few from the tables above)

Count-weighted vs bytes-weighted leaf-size percentiles, plus the small-leaf mass against the 64K byte scale:

| arm | leaves | count-wtd p10/p50/p90 | bytes-wtd p10/p50/p90 | <0.1x64K count/bytes | <0.25x64K count/bytes | disk blocks @2048 |
|---|---|---|---|---|---|---|
| baseline | 235 | 6.2K / 65.2K / 379.1K | 90.2K / 581.6K / 7262.6K | 10.2% / 0.19% | 18.7% / 0.71% | 3570 files (472.2 MB) |
| veto | 349 | 9.7K / 73.8K / 374.1K | 69.5K / 299.8K / 840.2K | 8.6% / 0.23% | 17.5% / 1.02% | 4234 files (444.9 MB) |
| veto + weight S=64K | 649 | 8.2K / 51.4K / 166.8K | 38.6K / 119.7K / 311.6K | 7.1% / 0.36% | 19.7% / 2.19% | 4567 files (412.1 MB) |
| veto + weight S=128K | 349 | 12.6K / 95.1K / 317.8K | 75.2K / 234.2K / 569.5K | 10.3% / 0.55% (vs 0.1xS) | 22.6% / 2.47% (vs 0.25xS) | — |

Tonk (all arms): zero leaves below 0.25x64K — the small-leaf question is SE-only.

Reading: the sub-0.1x-target leaf population is 7-10% by COUNT under every arm, including the baseline (whose geometric coin makes them freely), and carries 0.2-0.6% of BYTES; the weight coin does not create the small-leaf tail, it inherits a slightly smaller one (7.1% vs baseline's 10.2% by count) because a small key's cut probability shrinks with its weight. Count-scaled costs move accordingly: the S=64K arm stores +28% more block files than baseline at 2048 (4567 vs 3570) against -13% store bytes, and its root index carries ~2.7x the links (each ~107 B). Bytes-scaled costs (fetch payloads, write amplification) all improve. Nothing here motivates a dedicated cut-suppression mechanism for short runs: the count-side overhead is the price of the smaller mean the pacing is FOR (fetch counts on range scans track the mean, and the mean is the knob), and the sub-0.1x band is byte-trivial in every arm. If block-count overhead ever matters, raising S is the lever, not a new mechanism.

Mem replay: 2048 in 3.39-3.46 s (veto 3.53-3.66, baseline 4.22-4.31); 8192 in 30.1-30.7 s (veto 30.1-30.6, baseline 47.0). The weight coin costs nothing on the edit path.

Disk `store_bytes` (S=64K): 2048: 413.2 MB (-8.7% vs baseline 452.7, veto-only 446.1); 4096: 1188.4 MB (-22.2% vs baseline 1528.3; veto-only 1428.3; arm 1's hard cap managed 1285.7 at 7x CPU). The soft cap beats the hard cap on write amplification at negative CPU cost, because pacing the COMMON leaves near the target matters more than clamping the rare monster exactly.

## Step 3: forced breaks as last-resort backstop (2026-07-21)

The arm-1 force-split machinery returns, rescoped to the one shape no coin may cut: a maximal stretch of vetoed seams whose summed entry weight exceeds `max_segment`. `regroup_entries` keeps the per-pair veto verdicts, scans the maximal vetoed stretches, and force-splits any over-target stretch at the anchors `cap::forced_cut_positions` picks. Anchors are rendezvous style per the refined spec: while a piece exceeds the target, cut at the candidate key with the LOWEST blake3 hash inside it, recursively — boundaries are sticky by placement, not memory (an insert relocates an anchor only by beating the local hash minimum; a delete only by removing the anchor key itself). The threshold is symmetric (same `max_segment` creates and dissolves cuts, no hysteresis — trajectory-dependence would break independent-import byte-identity), anchors are DERIVED at cut time (no materialized synthetic entries; the read path filters nothing), and the whole decision stays a pure function of the key set. Forced seams keep arm 1's self-identifying long separators (over `max_separator` by construction), so the seam coin's length guard keeps them out of every index level and an edit can find a stretch's pieces from stored structure alone.

Edit path: the arm-1 window machinery returns as `merge_vetoed_stretch`, gated to membership-changing edits and joining only siblings across long (forced) separators — for every ordinary edit the scan is two separator length reads and joins nothing, which is what keeps the backstop off the common path (the arm-1 hard cap ran the same widening for every over-target natural run — the 7x source). The fast path additionally re-shapes any insert or delete whose adjacent seam is vetoed (stretch membership changes move anchors); the checks fail on a cheap length test for ordinary keys.

### Gates: PASS

Full suites green (search-tree 251, artifacts 158, query 737+43), clippy `-D warnings`, fmt, wasm32 check. New permanent tests: `it_backstops_fully_vetoed_stretches_at_the_target` (a 48-key fully vetoed cluster force-splits into pieces that each weigh in under the target, interior piece heads carry the long forced separators, everything reads back) and the weight-paced convergence pair now exercises the backstop live (the 60-key clusters exceed the 512 target, so forced anchors must converge across insertion orders and the delete-vs-rebuild oracle — they do, via the widening).

### Anchor flap rate (the new step-3 metric)

`it_keeps_forced_anchors_stable_under_churn`: an 80-key force-split cluster takes 25 interleaved edits (13 mid-stretch inserts, 12 deletes including anchor keys). 5 anchor moves across 25 edits, 5 edits moved any anchor, and each move is attributable (a deleted anchor's successor taking over, or a weight-threshold crossing adding/dropping a cut); 20 of 25 edits moved nothing. Asserted permanently at "at most a quarter of edits may move any anchor"; the exact 5/25 is the measured characterization.

### SE and tonk captures: the backstop never fires on the real data — and that is the finding

The capture walker now reports `forced_links` (links whose separator exceeds `max_separator`, the exact stored mark of forced seams). SE replay at S=64K, LIMIT 2048 and 8192: `forced_links=0` everywhere; tonk: 0. Perf confirms the same from the other side: mem 2048 in 3.38 s, 8192 in 30.9 s (step-2 levels: 3.39-3.46 / 30.1-30.7), disk 2048 407.7 MB / 4096 1160.4 MB (step-2: 412-413 / 1188).

So the SE residual tail (max 565.9 KB at 8192, unchanged by the backstop) is NOT the fully-vetoed shape the backstop targets. What it is instead: COMPOSITE stretches — vetoed sub-stretches, each under the target, glued together by accepted seams whose coins happened to come up low. The mechanism gap is precise: a cut can only be proposed AFTER the last key of a vetoed sub-stretch (interior keys are vetoed regardless of coin), so a run built of vetoed clusters gets one coin flip per cluster, at the TERMINATING KEY'S OWN weight — the uncuttable weight behind the terminator is never charged. A run of C-byte clusters with w-byte terminators paces at ~C*S/w instead of S, and the observed heavy tail beyond ~5S (1% at p99 vs 0.27% exponential) matches. The synthetic near-duplicate fixture (one uniform cluster) cannot exhibit this; only the real data did.

Candidate follow-up, NOT implemented (design departure from the approved per-key coin, needs sign-off): charge a vetoed stretch's full weight to its terminating key's coin — cut after terminator `t` with probability `(stretch_weight + weight(t)) / max_segment`. Still a pure function of the key set, still window-local (the same widening that serves the backstop already guarantees whole stretches in the window, and the step-3 fast-path checks already re-shape every stretch-membership edit), and it would restore true byte-renewal pacing over "atoms" (a maximal vetoed stretch is one uncuttable atom). The backstop would then be genuinely vestigial for atoms under the target and remain the guard for single atoms above it.

## Step 4: the weight bank (accumulated-weight coin, 2026-07-21)

The user-approved fix for the composite-stretch gap. Walking seams left to right at grouping time, a weight bank rides along: a vetoed seam banks its left key's entry weight (no cut is possible there); an ACCEPTED seam's leaf coin is fed `bank + entry_weight(left key)` and the bank resets to zero at EVERY accepted seam, cut or no cut. The reset rule is the load-bearing part: the bank is "weight since the last accepted seam" — a structural property of the key sequence — never "weight since the last cut" (a cut-outcome bank would cascade every downstream decision off one flip, dolt-style, and break convergence); no coin anywhere reads a cut outcome. An uncuttable vetoed stretch therefore funds the first accepted seam at its end with its whole weight, restoring byte-renewal pacing over atoms (a maximal vetoed stretch is one atom). Probability saturates cleanly in 128-bit arithmetic (`weight >= max_segment` cuts with certainty). Away from vetoed stretches every accepted seam sees bank 0 — exactly the step-2 per-key coin — and `max_segment == 0` ignores the bank entirely (byte-identical baseline, unit-pinned).

Mechanically: `Distribution::leaf_cut(key, banked, manifest)` is the new authoritative leaf decision (default: geometric when the target is unset, else `weight_paced_cut` with the bank); `Geometric::rank` remains the bank-zero floor the edit path's structural checks read (a bank only raises the cut probability, so `rank > BOTTOM_RANK` stays a sound "may split" test). Two edit-path cases became bank-sensitive, both routed into the existing rightward-fusion machinery: an append past the terminal boundary re-partners the terminal seam (the banked decision says whether the boundary moves to the newcomer or the orphan fuses right — `is_orphan_append` now evaluates it), and an interior delete can DEFUND a terminal cut financed by the trailing vetoed stretch (`dissolves_terminal_cut`: re-run the terminal decision over the post-delete tail; a defunded boundary fuses right with the delete applied up front). Both detections hide behind veto length-tests, so ordinary edits pay nothing. The rendezvous backstop stays for the one shape the bank cannot reach: a single fully-vetoed run with no accepted seam inside.

Tests (red-first where behavior is new): `it_ignores_the_bank_when_the_target_is_unset` (identity), `it_converges_on_composite_stretches_across_insertion_orders` (+ delete oracle), `it_keeps_bank_effects_inside_the_enclosing_stretch` (edit locality: boundary changes confined to the edited cluster's stretch), `it_cuts_composite_stretches_at_their_glue_seams` (every glue seam behind an over-target cluster cuts with certainty; verified failing under the bank-less coin), and the delete-unveto fast-path pin still green.

### SE replay, mem LIMIT 8192, S=64K (leaf segments)

| metric | veto+weight (step 2) | +backstop (step 3) | +bank (step 4) |
|---|---|---|---|
| segments | 643 | 624 | 705 |
| mean | 76.5 KB | 78.8 KB | 69.8 KB |
| p50 | 50.0 KB | 52.8 KB | 49.1 KB |
| p90 | 170.8 KB | 179.3 KB | 153.4 KB |
| p99 | 333.7 KB | 387.3 KB | 346.3 KB |
| max | 565.9 KB | 565.9 KB | 565.9 KB |
| bytes in 100K+ nodes | — | 61.9% | 56.2% |
| replay time | 30.1 s | 30.9 s | 31.9 s |

Small-leaf accounting (per-node dump): count-wtd p10/p50/p90 = 8.8K/48.0K/149.8K, bytes-wtd = 34.4K/110.6K/303.5K; below 0.1x64K: 6.0% of leaves / 0.35% of bytes. **P(leaf > 2S) = 13.9% against the exponential's e^-2 = 13.5%** — the distribution is now genuinely byte-renewal all the way into the tail (pre-bank the composite stretches made it ~4x heavy beyond 5S).

The unchanged 565.9 KB max is byte-identical across the pre-bank and bank runs, which is itself diagnostic: a leaf the bank cannot touch and the backstop does not claim (`forced_links=0`) is an accepted-seam-only run — every byte in it was charged to some accepted seam's coin and every draw came up tails. That is the soft cap's honest extreme-value tail (max of ~700 exponentials ≈ S ln 700 ≈ 6.5S; observed 8.6S), not a mechanism gap. Bounding IT would take a hard cap on natural runs, which is arm 1's road and rejected.

Tonk: byte-identical to steps 2-3 (7 segments, mean 84.9K, `forced_links=0`) — no vetoed stretches, bank inert, exactly as designed.

### Perf and write amplification

Mem 2048: 3.54/3.71 s (step 3: 3.38; step 2: 3.39-3.46; baseline 4.22-4.31). Mem 8192: 31.9 s (steps 2-3: 30.1-30.9; baseline 47.0). The append and delete detections cost nothing measurable. Disk `store_bytes`: 2048: 407.5 MB / 5091 files (step 3: 407.7 / 4819); 4096: 1196.3 MB (step 3: 1160.4, step 2: 1188.4 — the three sit within ±1.5% of each other; all ~-22% vs baseline's 1528.3). The bank splits composite runs into more, smaller leaves, trading a few percent more block files for the tighter rewrite unit; net disk bytes are a wash at this depth against the pre-bank arms and far below baseline.

## Step 5: the frame ceiling with pluggable anchors (2026-07-21)

The natural exponential tail gets a hard bound. The rendezvous backstop's frame generalizes from "runs between accepted seams" to FRAMES — the entries between coin-decided cuts, snapshotted before any forced overlay, so forced cuts never feed back into frame definition and there is no cascade. When a frame's summed entry weight exceeds `frame_ceiling_factor * max_segment` (a new manifest knob, `Manifest::frame_ceiling()`, 0 = off, env `DIALOG_TREE_CEILING_FACTOR`; deliberately not a hardcoded constant), it is force-split at accepted seams until every piece fits, recursively, derived at cut time like every other anchor. Frame anchors need the same three properties stretch anchors have — rank 0 above, self-identification for the widening, piece contiguity under one parent — and all three come from the same stored mark: a lower-bound separator longer than `max_separator`. For a long right key that is the step-3 right-prefix form; for short keys it is the left key zero-padded one byte past the bound (`cap::frame_separator`) — a valid separator that is NOT a prefix of the right key, safe for exactly the reason step-3 separators are: the widening dissolves forced seams before any regroup or reseparate can meet them. The rare seam with no over-bound separator at all (right key inside the padding gap) is simply not a candidate.

Anchor selection is pluggable (`Manifest::anchor_selector`, env `DIALOG_TREE_ANCHOR_SELECTOR`), one rule for both backstops, and — per the refined spec — elections read the hash TAIL (`cap::anchor_order`, bytes 8..32), disjoint from the leading 8 bytes every coin draw consumes: in an over-target frame every draw came up tails, so full-hash-minimal election would systematically anchor at the key that came closest to cutting. Pinned by `it_elects_anchors_on_coin_disjoint_bits` (flipping all coin bytes never changes an election; flipping any tail byte does). The selectors:

- A, rendezvous (0): hash-tail-minimal candidate in the piece.
- B, hybrid (1): shortest-separator-length class first (the most semantically different adjacent pair — the user's original instinct, formalized), hash-tail-minimal within the class. The sandwich property makes B insert-proof: for `p < q < k`, `q` shares `p`,`k`'s common prefix, so inserts never mint a strictly shorter class; only deletes that merge seams can. Pinned by `it_keeps_semantic_anchors_still_under_inserts` (inserts inside a sub-cluster leave the 25-byte-separator anchors in place; red under selector A) and `it_reanchors_when_the_semantic_anchor_is_deleted` (deleting the anchor key re-anchors at the merged seam's right key).

Edit path: `merge_vetoed_stretch` generalizes to `merge_forced_run` (identical mechanics — the long-separator join predicate already covers both anchor kinds), and a new fast-path gate re-shapes any membership-changing edit whose segment exceeds the ceiling (the in-place path would otherwise grow a frame past the hard bound). Other tests: convergence + delete oracle over an over-ceiling all-tails run (`it_converges_on_over_ceiling_frames_across_insertion_orders`, shape-compared control), the hard bound itself at both factors and both selectors (`it_bounds_frames_at_the_ceiling`, red with the overlay disabled), edit locality across frames (`it_keeps_ceiling_effects_inside_the_touched_frames`), anchors only at accepted seams in composite frames (`it_anchors_frames_only_at_accepted_seams`), and `max_segment = 0` identity unchanged (suite). The three older "mechanism actually fired" controls were also strengthened from root-hash comparisons (vacuous: the manifest is stamped into every node, so any knob change flips the root) to boundary-set comparisons.

### SE replay, mem LIMIT 8192, S=64K (leaf segments; step-4 no-ceiling row for reference)

| metric | no ceiling | 2x A | 2x B | 3x A | 3x B |
|---|---|---|---|---|---|
| leaves | 705 | 898 | 900 | 761 | 768 |
| mean | 69.8K | 54.9K | 54.7K | 64.7K | 64.1K |
| count-wtd p90 / p99 | 149.8K / 338.2K | 110.4K / 139.1K | 108.0K / 143.7K | 138.8K / 200.6K | 137.3K / 201.1K |
| bytes-wtd p50 / p90 | 110.6K / 303.5K | 83.1K / 127.5K | 84.4K / 127.2K | 100.9K / 179.8K | 101.5K / 182.1K |
| max | 565.9K | 272.6K | 257.5K | 325.9K | 423.8K |
| leaves > 2S by count | — | 3.2% | 3.3% | 13.0% | 12.6% |
| forced links / sep bytes | 0 / 0 | 165 / 110.9K | 154 / 103.1K | 48 / 35.1K | 52 / 31.7K |
| replay time | 31.9 s | 42.2 s | 40.4 s | 36.0 s | 37.0 s |

The 566 KB accepted-only leaf splits everywhere — at 2x into pieces ending at 251-273 KB max, at 3x 318-424 KB. Byte caveat: the ceiling bounds the WEIGHT proxy (key + 32); encoded leaves carry value payloads and per-entry overhead beyond it, so the byte tail lands near 2x the weight-implied figure. Small-leaf mass is unchanged (<0.1x64K: 6.6-6.8% of leaves, ~0.45% of bytes, all configs). Tonk: at 2x exactly one anchor appears (the 176.8K leaf splits to 152.9K max under A, 158.7K under B, one 513-byte forced separator); at 3x nothing fires — tonk is under the ceiling, as it should be.

Perf and disk: mem 2048 in 5.2-6.0 s at 2x, 4.2-4.4 s at 3x (no-ceiling 3.5-3.7 s); disk 2048 402-407 MB, 4096 1119-1151 MB (no-ceiling 407.5 / 1196.3 — the ceiling actually SAVES 4-6% disk at depth by shrinking the rewrite unit of the monsters). The replay CPU cost is real and tracks the number of split frames (e^-2 = 13.5% of frames at 2x vs e^-3 = 5% at 3x): every membership edit into a split frame merges and regroups the whole frame. That is the arm-1 cost formula scoped to the tail the factor chooses, and it is the knob's price axis.

### Anchor churn and evenness (synthetic all-tails frame, shared edit protocol; boundary moves excluding the edited key)

| config | insert-driven moves / 25 | delete-driven moves / 11 | piece weights min/mean/max |
|---|---|---|---|
| no ceiling | 0 | 0 | one 3700-weight leaf |
| 2x A | 7 | 2 | 111 / 616 / 999 |
| 2x B | 2 | 3 | 222 / 616 / 888 |
| 3x A | 4 | 1 | 370 / 925 / 1480 |
| 3x B | 1 | 2 | 222 / 740 / 1332 |

The asymmetry the comparison was for: B is 3-4x more insert-stable (the sandwich property working — inserts cannot mint a shorter class, so the semantic anchors never move under insert churn), at one extra delete-driven move (a deleted anchor key forces re-anchoring under either selector). B also packs pieces more evenly at 2x (min 222 vs 111) and stores slightly fewer forced-separator bytes on SE (103.1K vs 110.9K at 2x — mostly fewer links; in natural runs the separator-length classes tie broadly and B degrades toward A, exactly as predicted; the per-separator savings shows in vetoed-run territory, where B's 25-byte sub-boundary separators beat A's 33-byte in-sub picks in the sandwich fixture).

### Recommendation

Selector: B (hybrid). It wins insert-stability decisively, matches A everywhere else, costs nothing measurable, and anchors where a human would cut (the biggest semantic break). Ceiling: factor 3 as the default-candidate — it converts the unbounded tail into a ~1/3-of-before max for a 13-16% replay-CPU cost and 48 forced links, where factor 2 pays 27-70% CPU for a further 25% off the max; the knob stays a knob (`DIALOG_TREE_CEILING_FACTOR`), and workloads that prize a tight fetch ceiling over write CPU can run 2x. If the CPU cost of 2x ever matters enough, the follow-up lever is incremental segment-weight bookkeeping on the edit path (the per-edit weight sum plus whole-frame regroups dominate), not a different mechanism.

### Decision table (running)

| arm | SE max leaf @8192 | SE 100K+ bytes | mem replay 8192 | disk bytes @4096 | verdict |
|---|---|---|---|---|---|
| baseline (demotion) | 7.44 MB | 90% | 47 s (today) / 21 s (report) | 1516.9-1528.3 MB | the pathology |
| arm 1 (demotion + hard cap) | 268 KB | 40% | 145 s (7x) | 1285.7 MB | size goal met, perf fail |
| step 1 (veto, no cap) | 1.20 MB | 85% | 30 s (0.65x) | 1428.3 MB | free win; residue = near-duplicate runs, deferred to backstop |
| step 2 (veto + weight coin S=64K) | 566 KB | 62% | 30 s (0.65x) | 1188.4 MB | byte pacing works; residue = clustered stretches |
| step 3 (+ rendezvous backstop) | 566 KB | 62% | 30.9 s (0.66x) | 1160.4 MB | mechanism proven on synthetic clusters, flap 5/25; fires ZERO times on SE — the residue is COMPOSITE stretches (see finding) |
| step 4 (+ weight bank) | 566 KB (accepted-only tail, e^-8.6) | 56% | 31.9 s (0.68x) | 1196.3 MB | approved fix landed; composite stretches split (p90 153K, P(>2S)=13.9% vs e^-2=13.5% — true byte-renewal); remaining tail is the soft cap's own extreme value, not a gap |
| step 5 (+ frame ceiling 3x, hybrid anchors) | 424 KB (weight-bounded; p99 201K) | — | 37.0 s (0.79x) | 1119.4 MB | hard bound on the natural tail; selector B insert-stable 1/25; 2x available for tighter tails at 27-70% CPU |
