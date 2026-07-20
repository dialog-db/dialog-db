# Benchmark dataset survey

All sizes checked 2026-07-20, compressed download size.

## The criterion that actually matters

We are benchmarking **version control**: skip-table construction, ancestry walks, context derivation, per-commit history cost. Those all scale with *how much history precedes a commit*.

So the question is not "which dataset is realistic" but **"which dataset comes with real transaction boundaries and timestamps."** A flat snapshot forces us to synthesize commits, and synthesized ancestry measures our own generator's assumptions rather than reality.

That splits the candidates cleanly.

## Has real per-edit history

| source | size | history quality | notes |
|---|---|---|---|
| **Stack Exchange** (per-site archives) | 39-349 MB | **Excellent.** `PostHistory.xml` carries every edit with `CreationDate`, `UserId`, `PostHistoryTypeId` (title edit / body edit / tag edit / close / reopen). | Per-site dumps mean the scale knob is "which site", from `retrocomputing` (39 MB) to `worldbuilding` (349 MB). CC BY-SA. |
| MusicBrainz `mbdump-edit` | **15.1 GB** | Real, but it is edit *forms* (a moderation queue), not clean transactions. Needs heavy interpretation. | Larger than the data it edits (7.36 GB). Poor effort-to-value. |
| Wikidata truthy | **66.1 GB** | Real revision history exists but in separate, enormous dumps. | Too big to iterate on. |

## Flat snapshots (history would be synthetic)

| source | size | notes |
|---|---|---|
| IMDb | 214 MB (`title.basics`), 737 MB (`title.principals`), 293 MB (`name.basics`), 8 MB (`title.ratings`) | Clean TSV, no auth, well-known shapes. **No timestamps at all** beyond release year. Non-commercial license. |
| MusicBrainz core (`mbdump`) | 7.36 GB | Rich relational shape, CC0 core. Datomic precedent, but their sample is **gone** (S3 404, and `Datomic/mbrainz-sample` is queries only). |

## Recommendation

**Stack Exchange**, and specifically a small site to start.

The reasoning: it is the only candidate where the *history is real*. `PostHistory.xml` gives an actual sequence of edits with timestamps and authors, which maps directly onto commits: a question posted, then edited, then answered, then re-tagged, then closed. That is exactly the shape of ancestry the version-control paths need to walk, and we would not be inventing any of it.

It also has natural properties we want and would otherwise have to fake:
- **Real per-entity edit frequency skew.** Most posts are edited never or once; a few are edited dozens of times. That skew is precisely what stresses skip links, and a uniform synthetic generator would miss it.
- **Real interleaving.** Edits to different posts interleave in wall-clock order, so commits touch scattered parts of the keyspace, which is what a novelty buffer has to cope with.
- **A built-in scale knob**: pick the site. 39 MB to 349 MB compressed, same schema throughout, so the ingester is written once.

Entity shapes are also genuinely relational (posts, users, comments, votes, tags), so the join benchmarks stay meaningful.

## VERIFIED by inspection, not assumed

Downloaded `retrocomputing.stackexchange.com.7z` (39.5 MB compressed, 220 MB extracted) and measured it.

**`PostHistory.xml` carries an explicit transaction id.** Rows share a `RevisionGUID` when they are part of ONE atomic edit (e.g. a body edit and a title edit submitted together). So commit boundaries are *given in the data*, not inferred from timestamp proximity. This is the single most important finding: it removes the last piece of synthesis.

Row shape:
```
<row Id="1" PostHistoryTypeId="2" PostId="1"
     RevisionGUID="4f1b41df-..." CreationDate="2016-04-19T20:11:37.597"
     UserId="24" Text="..." ContentLicense="CC BY-SA 3.0" />
```

Measured on this one small site:

| quantity | value |
|---|---|
| PostHistory rows | 70,022 |
| **distinct RevisionGUIDs (= transactions)** | **53,238** |
| Posts | 21,769 |
| Users | 23,137 |
| Comments | 80,538 |
| date range | 2016-04-19 to 2024-03-31 (8 years) |
| distinct days with activity | 2,882 |
| mean distinct posts touched per day | 11.5 |

**53,238 real commits** against the current benchmark's 2. That is the gap that made every per-commit history cost measure as free.

**Edit-frequency skew, measured** (revisions per post): min 1, **p50 3, p90 7, p99 12, max 122**. This is the property a uniform synthetic generator would have flattened, and the long tail is exactly what stresses skip links. Worth stating plainly: this distribution was measured, not assumed.

**Edit types** are a genuine mix, so transactions are not homogeneous: 25,559 type 5 (body edit), 21,771 type 2 (initial body), 6,428 type 1 (initial title), 5,958 type 3 (initial tags), 2,894 type 24 (suggested edit applied), 1,922 type 6 (tag edit), 1,920 type 4 (title edit), plus close/reopen/delete events.

**Interleaving is real**: ~11.5 distinct posts touched per active day over 2,882 days, so commits scatter across the keyspace rather than arriving as a bulk load. That is the shape a novelty buffer has to cope with.

Scale knob confirmed: same schema across sites, so `retrocomputing` (39 MB) through `worldbuilding` (349 MB) with one ingester.

## Licensing

Stack Exchange dumps are CC BY-SA 4.0: redistribution is fine with attribution. IMDb is non-commercial-use only, which is a real constraint for a public benchmark repo. MusicBrainz core is CC0, derived tables CC BY-NC-SA.

CC BY-SA is compatible with a public `dialog-db` org repo provided attribution is recorded.

## Related

`notes/mbrainz-dataset.md` for the MusicBrainz specifics, `notes/history-export.md` for the export format the dataset round-trips through.
