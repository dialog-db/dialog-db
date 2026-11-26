# Entity spelling: what a key-rewriting migration would rewrite

A sibling of the type-byte gap, put to this topic by the coordinator on
2026-11-26: RFC 3986 section 6.2.2.1 makes three parts of a URI
case-insensitive, and an index key is spelled with the bytes stored, so
two spellings of one URI are two keys. A read that normalizes the
spelling would answer one entity that a scan finds and a lookup by the
normalized spelling misses. The maintainer's direction, via the
coordinator: keep such a claim passed over (#884) until a migration
rewrites the keys; this note says what that migration would rewrite.

## What the stored spelling is today

From a probe of the `url` crate rendering that `Uri::from_str` stores
and `Uri::check_stored` compares against (2026-11-26, `7e9f621`; the
probe was not committed):

| case-insensitive part | what a local write stores | can a local write store a non-normal spelling | can a stored key hold one |
|---|---|---|---|
| scheme | lowercased: `Ref:qq` is stored as `ref:qq`, `File:///Tmp/A` as `file:///Tmp/A` | no | only from a writer that bypasses the parse (another implementation, or `Uri::stored_unchecked`); #884 passes such a claim over |
| host, when the URI has an authority | lowercased for the `url` crate's special schemes (`http`, `https`, `ws`, `wss`, `ftp`, `file`); kept as given for any other: `foo://Host.Example/Path` is stored as is and passes `check_stored` | yes, for an entity with an authority and a non-special scheme | yes, from local writes |
| hex digits of a percent-encoding | kept as given: `node:%e2%9c%93` and `ref:q%2Fq` are stored as is and both pass `check_stored` | yes | yes, from local writes |

## How many such keys could exist

- A non-lowercase scheme: none from this release's writes. Only a
  foreign writer, or a caller of `Uri::stored_unchecked` (a public
  function the write-boundary pull request closes at assertion), can
  have stored one.
- Host case: none minted by dialog. Every entity dialog mints is opaque
  (`did:key:`, `asset:`, `blob:`, `node:`, `revision:`); the only
  `scheme://` strings dialog builds are storage locations
  (`file://{path}/{name}`, `rust/dialog-effects/src/storage.rs`), which
  `rust/dialog-repository/src/repository/peer.rs` hashes into a seed and
  never stores as an entity. An application can store one: an entity an
  embedder parses from `foo://Host/x` is stored with the host as given.
  Whether tonk does is a question for tonk (open).
- Percent-encoding hex: none minted by dialog (its opaque identifiers are
  base58 or a hash, with no `%`). An application entity holding a
  percent-encoding is stored with its hex as given. Open for tonk.

## What the migration would rewrite

- Every key (EAV, AEV, VAE, history, coverage) whose entity column or
  entity value spells a scheme in uppercase, a non-special host with
  uppercase letters, or a percent-encoding with lowercase hex, into the
  normal spelling: scheme lowercase, host lowercase, hex uppercase.
- Two keys that normalize to one collapse into one claim: their versions
  fold as two same-value claims of one cell do (`Datum::absorb_versions`),
  and a cardinality-one cell keeps its elected claim by the usual order.
- From then on `Uri::from_str` stores the normal spelling, so a local
  write of `foo://Host/x` and a stored `foo://host/x` are one entity.
  That changes the identity of an application's existing entities whose
  host or percent hex was not normal, which is why it belongs to the
  gated migration and not to a read.

## Open

- Does tonk store any entity with an authority or a percent-encoding?
  If not, the host and hex rewrites have nothing to do on any replica
  this release can meet, and only the scheme rule (already true of
  local writes) needs the migration's read-side check.
