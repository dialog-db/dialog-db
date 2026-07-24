# History-aware export/import

## Problem

The current export (`Branch::export` -> `CsvExporter`) streams the EAV range of the head revision's tree and writes one row per surviving fact. That is a flat point-in-time snapshot: the `cause` column comes out empty for every row, and there is no representation of *when* each fact was written or which commit wrote it.

That makes the exported dataset unable to exercise the version-control work at all. `~/dialog-bench-handoff/tonk_export.csv` has 5755 rows and an entirely empty `cause` column, so importing it produces a two-commit history no matter how much data it holds. Skip links, ancestry walks, context derivation, and per-commit history cost all measure as free because there is no ancestry to walk.

## Goal

An export that, re-imported, **reproduces the history index**, so the benchmark dataset has realistic commit structure: each issue creation its own commit, each status/assignee change its own transaction.

## Shape

One CSV, revision rows inline, ordered by a `txn` column:

```
the,of,as,is,cause,txn
dialog.revision/record,rev:<entity>,record,<base58 RevisionRecord>,,1
squash.bug/title,bug:abc,text,Login broken,,1
squash.bug/status,bug:abc,text,triage,,1
squash.bug/status,bug:abc,text,in-progress,,7
```

Chosen over a sidecar-txn column or a two-file split because it is the tree's own representation: `dialog.revision/*` rows carry the `RevisionRecord` exactly as the history region already stores it, so the file round-trips rather than re-deriving lineage on import.

`txn` orders the commits. The importer groups rows by `txn` and commits each group in order.

## The revision parameter

Export takes a **revision**, not a boolean flag. "From revision R" means **the history reachable from R** (R and its ancestors), so:

- **full history** = export from the head
- **snapshot** (`--snapshot`) = export from the head at depth 1
- **state as of R** = export from R at depth 1
- **history as of R** = export from R

A snapshot is framed as **a single genesis revision** whose tree is the state at that point, not as "history omitted". Both modes therefore emit the same shape (revision rows plus fact rows); a snapshot just has exactly one revision with no parents. The importer has ONE code path, and every export is a valid history index rather than a degenerate file the version-control code special-cases.

## Mechanics

- `Branch::log(env, limit)` already returns `Vec<(Version, RevisionRecord)>` newest-first, which is the walk the export needs. Reverse it for `txn` ordering (oldest = 1).
- `Version` is `{ origin: Origin, edition: Edition }` and `Display`s as `edition@origin`. It has **no `FromStr`** yet: accepting one as a CLI/API argument needs that parse added as the inverse of the existing `Display`.
- Per revision, the facts belonging to that commit come from the history region (the claims that revision recorded), not from diffing whole trees.
- `RevisionRecord` carries `format`, `lineage`, `issuer`, `authority`, `parents`, `skips`, `signature`. It serializes via `to_bytes` (dag-cbor) and is already stored as a `Value::Record`, so the CSV `record` value type carries it unchanged.

## Signatures on re-import

Open question, decide before building the importer. A `RevisionRecord`'s signature binds `lineage` + `issuer` + `parents` + `skips`. On import into a *different* repository, the lineage entity differs, so a replayed record either:

  (a) keeps the original signature and fails `verify()` (the record is valid only in its source lineage), or
  (b) is re-minted locally with the importing operator's key, reproducing the *structure* of the history (depth, branching, skip links) but not the original attribution.

(b) is what a benchmark needs and is almost certainly right: we want realistic ancestry to measure traversal cost, not to smuggle another repository's signed claims. (a) would be a different feature (verified history transfer), and it is what push/pull already do properly.

## Not doing

Re-deriving `cause` for facts from the history region on export. The `cause` column stays as it is; commit membership is carried by `txn` and the revision rows.
