# A replica cannot step over a value whose type it does not know

**Phase: Directing.** The key layout is written down as facts, four
approaches are in a [matrix](unknown-value-type/direction.md), and the
choice is the maintainer's. The [journal](unknown-value-type/journal.md)
says where the topic stands, the [ledger](unknown-value-type/claims.md)
what it rests on.

## Description

The maintainer, Irakli, 2026-11-26, on a claim whose value type byte a
release does not know: "Lets close this gap", bundled with the
stored-format migration that is coming anyway, and his lean, not yet
checked against the layout: the type tag itself carries a framing class
(ranges of tag values meaning, for example, fixed 8 bytes, fixed 32
bytes, escaped-terminated), so an engine that does not know a type still
knows how to step over it, at no byte cost and without disturbing order.

What is on main (`7e9f621`), from #884's work on claims a replica cannot
read yet:

- A key's value slot is one type byte followed by the value's
  order-preserving encoding. How far that encoding runs is decided by the
  type byte alone, from a table: `varkey::value_payload_len`
  (`rust/dialog-corpus/src/key/varkey.rs`). A type byte the table does not
  hold makes the key unparseable.
- #884 passes over a claim whose value does not decode, but cannot pass
  over one whose type byte is unknown: such a key yields neither its
  entity, attribute and value nor, in the VAE ordering, anything at all.
  Reads of its cell fail; a cardinality-one write leaves it in place.
- The stored-format topic (#857, `design/stored-format.md` on
  `design/stored-format`) plans a one-time rewrite of every rule and
  concept, not of fact keys. The last rewrite of fact keys was the number
  band (`Branch::migrate_storage`, tonk #1058), behind a storage-layout
  gate.

## Problem

A release that meets a claim whose value is of a type it does not know
cannot tell where the value ends, because the end of a value is decided
by a table of the types the release knows. So it cannot parse the key,
cannot list, elect, evict or retract the claim, and fails reads of the
claim's cell and, when the claim's history record covers an earlier
one, the pull that brings it. Any new value type is therefore
unreadable to every release before it, including the parts of the key
that do not depend on the type.

## Approach

Not decided. See the [matrix](unknown-value-type/direction.md).
