# Hypotheses: collaborative text editing

Each one says what would confirm it, what would refute it, and the
experiment that decides. Experiments run in research/text-write-cost.md
unless noted. Ordered by how much one answer decides.

```
                    where text changes live
   ┌──────────────┬───────────────┬────────────────┬──────────────┐
   │ outside the  │ a text region │ the history    │ the library's │
   │ tree (H5)    │ in the tree   │ region (H3)    │ own sync      │
   │              │ (H1, H2, H4)  │                │ (template)    │
   └──────────────┴───────────────┴────────────────┴──────────────┘
          ▲ every option still needs: a positional index (H6),
            and a commit that is not per keystroke (H0)
```

## H0. A text change does not need to be a commit

Text changes accumulate locally (an append-only log, like DeltaDB's
writer thread or the novelty buffer) and enter the tree at sync points;
one signed revision covers many changes.

- **Confirm:** on the concurrent traces, bytes on disk and on the wire
  fall roughly with the flush interval, reaching within 2× of the
  library alone at an interval an editor would accept (a second, a pause).
- **Refute:** the per-commit floor (~120 to 160 KB today) stays over 2×
  at every interval that keeps sync interactive.
- **Experiment:** E1. This decides more than any other: if it fails, no
  in-tree option passes until the storage work lands.

## H1. Text changes in their own region of the tree

**Scoped (2026-11-09).** Commits already write region entries without
claim records (`with_entries`, as the context and blob regions do).
Gap: the replay, screened and lost-race merges diff only the history and
data scopes and would silently drop a new tag; an older release would
drop text and still count it seen, so it needs a format gate. Estimate
8 to 10 files, ~400 lines plus tests. Cost numbers wait on E1.

A reserved key range `tag ‖ document ‖ origin ‖ sequence → change`,
beside the history, coverage and context regions. The tree differential
already syncs only the entries a peer lacks, so Dialog's sync is the
text sync and no library sync runs.

- **Confirm:** the differential between two replicas that edited one
  document moves within 2× of the library's own update bytes.
- **Refute:** node overhead (keys, hashes, leaf rewrites) dominates
  small changes.
- **Experiment:** E1 with the region layout.

## H2. Any library fits if Dialog does the sync

**Confirmed (2026-11-09)** for Automerge, Loro and Yrs: each exports one
change alone and buffers changes whose parents are missing; all
converged on both traces with every merge going one change at a time.
Diamond Types 1.0 refuses out-of-order changes, so Dialog must deliver in
causal order. Key changes by Dialog's `(origin, sequence)`: Yrs
delete-only changes have no id, and Loro and Yrs counters count
characters. [results](../../research/text-write-cost/results/2026-11-09-libraries.md)

Earlier I wrote that version-vector libraries (Yjs, Loro) do not fit
because computing "changes since" needs the library. That holds only if
the library's sync answers it. If changes are keyed `(origin,
sequence)` in a region, the tree differential answers it, and the
library only has to export one change and import changes in causal
order. Their ids `(peer, counter)` have the shape of Dialog's
`(origin, edition)`.

- **Confirm:** a library exports a change on its own and imports out of
  order (holding back until parents arrive), for Loro, Yjs and Automerge.
- **Refute:** a library can only export ranges since a version vector,
  and splitting them per change loses its compression.
- **Experiment:** E2, a small spike per library; reading code is enough
  for a first answer.

## H3. Text changes as entries in the history region

**Refuted (2026-11-09).** Mechanically possible: a batch is many records
under one revision, and a record covering nothing skips screening. But
history is keyed `origin ‖ edition`, so one document's changes scatter
across every revision that touched it; opening it needs a
document-to-versions index, and partial replicas skip history.
[results](../../research/text-write-cost/results/2026-11-09-regions.md)

Instead of a new region, a text change is a claim record. History is
already origin-first, append-only and synced.

- **Confirm:** claim records can carry a change without a data-region
  index entry, and screening ignores them.
- **Refute:** claim records are one per instruction in a signed
  revision, so this collapses back into one revision per change (H0
  then decides), or screening has to learn about text.
- **Experiment:** read `history::region_writes` and the screen; no run
  needed to refute.

My prior: H1 over H3. History records describe facts; a text change is
not a fact.

## H4. A region can be replicated whole while the rest stays partial

**Confirmed (2026-11-09).** Opening one document of 500 changes reads 4
to 7 blocks, ~190 to 260 KB, flat from 100 to 10 000 documents (tree
9 MiB to 893 MiB). `Download::within(ranges)` already fetches chosen
ranges eagerly. Overhead: ~172 bytes per stored entry, ~5× its payload.

A partial replica fetches the range `tag ‖ document` in full when a
document is opened, and nothing else from the region.

- **Confirm:** opening one document reads that document's leaves and
  their path, independent of how many other documents the region holds.
- **Refute:** documents share leaves enough that one fetches others'
  changes.
- **Experiment:** E3: block reads to open one document among 1, 100 and
  10 000.

## H5. An accreting resource outside the tree beats a region

Change blocks by hash outside the tree, a heads pointer fact inside it.

- **Confirm:** cheaper than H1 on disk and wire, because change blocks
  skip tree node overhead.
- **Refute:** H1 is within 2× of it, in which case the region wins on
  moving parts (one sync, no new resource kind).
- **Experiment:** E1, both layouts on one trace.

## H6. The positional index is local and never replicated

**Split (2026-11-09).** Rebuild from individual changes: Yrs 16 to 23 ms
(confirmed); Automerge 159 to 175 ms and Loro 213 ms (refuted, but both
open from a snapshot in ~50 ms and under 1 ms, so a local snapshot cache
rescues them); Diamond Types 1.0 220 to 690 ms either way (refuted).

As in DeltaDB: replicate changes, rebuild position → fragment per
replica. It need not be in the search tree, so the tree's lack of
positional summaries does not matter.

- **Confirm:** rebuilding the text from a document's changes on open
  takes under ~100 ms for a paragraph-to-chapter document.
- **Refute:** rebuild on open is too slow and the index has to be
  persisted, at which point it needs a home.
- **Experiment:** E4: time to load the concurrent traces' final state
  from their changes, per library (H2) and for a native Eg-walker.

## Not hypotheses yet

- Which library: decided by H2 and E1, not up front.
- Built in versus a library: if H2 holds, a library is only the merge
  engine; transplanting DeltaDB's ideas (delta = parent + offset + text;
  local positional index) is a later swap of that engine, not a separate
  direction.
