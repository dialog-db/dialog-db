# MusicBrainz benchmark dataset

## Why

The current benchmark file (`~/dialog-bench-handoff/tonk_export.csv`) is too weak to measure the version-control and novelty work:

- **1855 facts, but only 54 bugs** once grouped by entity. Nothing to stress.
- **The `cause` column is entirely empty.** It is a flat state snapshot with zero lineage, so re-importing it yields a two-commit history no matter how much data it holds. Every per-commit history cost (skip-table construction, ancestry walks, context derivation) measures as free because there is no ancestry to walk.

MusicBrainz gives real entity shapes, real cardinality distributions (artists to releases to tracks), and real string-length distributions. It is also the recognized benchmark in this space (via Datomic's mbrainz sample), so numbers are legible to people outside the project.

## Source availability, as checked 2026-07-20

- **Datomic's `mbrainz-sample` repo is queries and rules only** (1.7 MB), not data.
- **The old S3 sample (`mbrainz-1968-1973.tar`) is gone** — returns 404.
- **The canonical MetaBrainz dumps are live**: `https://data.metabrainz.org/pub/musicbrainz/data/fullexport/`, `LATEST` = `20260718-002132`.
  - `mbdump.tar.bz2` (core tables): **7.36 GB compressed**
  - `mbdump-derived.tar.bz2`: **0.5 GB**

So the full dump is far too large to commit anywhere, and more than the benchmark needs. Datomic's sample was itself a subset (1968-1973 releases), which is the right model.

## Open decisions

These shape the repo question, so decide before building.

### 1. Subset selection
Datomic used releases from 1968-1973. Options: mirror that window (comparable to published Datomic numbers), pick a different window, or take a random sample of N artists with their full release/track closure. A closure-based subset keeps referential integrity, which matters because the join benchmarks traverse artist -> release -> track.

### 2. Target size
The knob that decides whether a separate repo is needed at all. Rough shape, to be measured rather than assumed:
- ~10k facts: fits in-repo as a test fixture, but is only ~5x the current dataset
- ~100k-1M facts: needs its own home, and is where per-commit history cost actually shows up
- ~10M+ facts: closer to the full dump, useful for scale testing but slow to iterate on

Recommend generating at a couple of scales rather than picking one, with the largest hosted and a small one committed for CI.

### 3. Synthetic history
**MusicBrainz is also a flat snapshot**, so the commit structure has to be synthesized either way. The difference from the current file is that we would be synthesizing history over *realistic* data rather than synthesizing both.

Shape to aim for, mirroring how the data actually accumulated: an artist created in one commit, releases added in later commits, track listings and metadata edits as separate transactions. That produces real ancestry depth, which is the thing nothing currently measures.

Note MusicBrainz has a genuine edit history in `mbdump-edit.tar.bz2`. Worth checking whether it can drive realistic commit boundaries instead of synthesizing them, which would make the dataset genuinely history-derived rather than history-shaped.

### 4. Hosting
If the target size exceeds what belongs in the main repo, a separate repo under the `dialog-db` org. Consider: git LFS versus release artifacts versus a plain fetch script with a checksum. A fetch script keeps the repo small and makes the provenance explicit, at the cost of needing network in CI.

## Licensing

MusicBrainz core data is public domain (CC0); some derived tables are CC BY-NC-SA. Check which tables the subset draws from before redistributing, and record the attribution the license requires.

## Related

[[design_self_describing_config_and_history_export]] for the export format the dataset would round-trip through, and `notes/history-export.md` for the revision-parameter design.
