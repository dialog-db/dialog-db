# The benchmark dataset

## Transformed form

`scripts/se-transform.py` turns an extracted Stack Exchange site dump into a flat, transaction-ordered fact log so the ingester does no joining, replaying, or type interpretation:

```
txn,at,the,of,as,is
1,2016-04-19T20:11:37.597,se.post/kind,post:1,text,question
1,2016-04-19T20:11:37.597,se.post/author,post:1,entity,user:24
1,2016-04-19T20:11:37.597,se.post/body,post:1,text,"We are planning to build ..."
1,2016-04-19T20:11:37.597,se.post/title,post:1,text,Where can I find software for the PDP-6?
1,2016-04-19T20:11:37.597,se.post/tag,post:1,text,pdp-6
```

Rows sharing a `txn` are ONE commit, read top to bottom. `txn` derives from the dump's own `RevisionGUID`, which Stack Exchange assigns to the rows of a single atomic edit, so **commit boundaries are the site's real ones, not inferred from timestamp proximity and not synthesized**.

Facts are asserted, never retracted: a later commit asserting the same `(the, of)` supersedes under cardinality-one, which is what an edit is.

## Measured on `retrocomputing.stackexchange.com`

39.5 MB compressed dump to 87.9 MB CSV. All figures below are measured with a real CSV parser, not shell field splitting (post bodies contain commas and newlines, so `cut -d,` silently produces nonsense here).

| quantity | value |
|---|---|
| facts | 117,236 |
| **transactions (commits)** | **50,553** |
| entities | 21,771 |

Attributes: `se.post/body` 46,668 · `se.post/author` 21,516 · `se.post/kind` 20,831 · `se.post/tag` 19,491 · `se.post/title` 7,878 · `se.post/closed` 679 · `se.post/deleted` 140 · `se.post/locked` 33.

Value types: text 94,868 · entity 21,516 · boolean 852.

## Why this dataset exercises the work

**Ancestry depth.** 50,553 commits against the previous dataset's 2. Every per-commit history cost (skip-table construction, ancestry walks, context derivation) previously measured as free because there was no ancestry to walk.

**Supersession, which is what version control is for.** 44.0% of cardinality-one `(attribute, entity)` pairs are written more than once, and one pair is rewritten **120 times**. So nearly half the workload is genuine value replacement rather than first insertion.

**Small commits.** Facts per transaction: p50 1, p90 5, max 9. Real edits are small and frequent, which is the opposite of the bulk-load shape the old dataset had and the case a novelty buffer is meant to amortize.

**Both sides of the spill boundary, without tuning for it.** Value sizes: p50 11 bytes, p90 2,168, p99 7,303, max 29,597.
- **4,223 values exceed the default `inline_n` of 4096**, so the spilled-value path is genuinely exercised.
- **49,064 values exceed the default `spill_prefix` of 64**, so the in-band prefix ordering is exercised across a wide range.

That distribution is the real one from the source data. It was measured, not chosen to hit those thresholds.

**Scattered writes.** ~11.5 distinct posts touched per active day over 2,882 days (2016-04-19 to 2024-03-31), so commits land across the keyspace rather than in one region.

**Edit-frequency skew.** Revisions per post: p50 3, p90 7, p99 12, max 122. The long tail is what stresses skip links; a uniform synthetic generator would flatten it.

## Scale knob

Same schema across every Stack Exchange site, so one ingester covers a range: `retrocomputing` 39 MB, `datascience` 85 MB, `cs` 125 MB, `academia` 189 MB, `scifi` 322 MB, `worldbuilding` 349 MB. `--limit N` truncates to the first N transactions for a small committed fixture.

## Licensing

Stack Exchange dumps are CC BY-SA 4.0. Redistribution is fine with attribution, which must be recorded wherever the transformed data is hosted.

## Open

Hosting. 88 MB of CSV is too large for the main repo. Options are a separate `dialog-db` org repo, release artifacts, or a fetch-plus-checksum script. A small `--limit` fixture can live in-repo for CI regardless.

## Related

`notes/benchmark-dataset-survey.md` for why Stack Exchange over IMDb, MusicBrainz, and Wikidata.
