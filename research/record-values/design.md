# Record values: proposed design

Status: proposed, for the maintainer. Nothing here is decided until the
[journal](journal.md) says so. The [topic](../record-values.md) holds the
question and the decisions made; the [spike's results](results/2026-11-11-spike.md)
hold the numbers cited below. Each section is one open question: what was
found, the options, the proposal and why, and what would test it.

## 0. The model: a record is a view over a buffer

The maintainer's framing, which the rest of this design follows: every record
is a view over the buffer that holds it, its names (and values) resolved
through that buffer's table. A buffer is whatever holds records: a tree
node's block, a stream's packet, the single-file archive, and a record on its
own, which is a buffer of one.

- **A record on its own** (a spilled value's block, a rule constant, a value
  a caller builds, anything hashed or signed) is a buffer of one record: a
  table of its names, sorted, and its fields naming them by index. That is
  the header layout of section 2, and it is the canonical form.
- **A record in a buffer of many** names attributes (and values) in the
  buffer's table. Modelled over buffers of 64 records, rows over one table
  take 0.83 to 0.87 of dag-cbor for revision records with names in the
  table, 0.29 to 0.45 with values in it too, and 0.53 to 0.66 for tonk's
  bindings ([results](results/2026-11-11b-layouts.md#records-as-rows-over-one-table-per-buffer)).
- **Equality** inside one buffer compares rows, provided a buffer holds
  each distinct token once; across buffers it resolves tokens, or compares
  canonical forms.
- **Identity** (`Hash`, a spilled value's reference, a rule's identity) is
  over the canonical form, so a view realizes it to be hashed; or identity is
  defined over its parts' hashes, which a buffer can keep, and nothing is
  realized. Which is a decision (section 13).

**Where it stands** (#733 at `5b1ba37`): inside the single-file archive this
is how nodes are served already: rows of token references into the file's
mapping, keys compared in place, values read in place, nothing copied out.
It stops at two places: the tree's scans hand values out owned
(`into_owned`), which #733 lists as open, and blocks for sync and for every
other store are rebuilt whole, a record one opaque value in them. The file's
table is the whole file, not one per node, and no record is split into its
fields anywhere yet.

**What it takes**, in the order the measurements say to try it:

1. a record keyed so it splits into its parts (sections 1, 5);
2. the file splitting records into its tokens, and interning a value's
   payload apart from its type byte (section 4);
3. the tree's segment encoder holding a node's records as rows over the
   node's table, so blocks carry them that way on the wire (section 2, d);
4. the tree's scans yielding views, and `Value::Record` holding a view (a
   buffer and a span) or canonical bytes;
5. streams sending only the table entries a peer lacks (section 2).

**Risks.** A view keeps its buffer alive: a small record can pin a node or a
mapping. Moving a record between buffers (a query's result asserted into a
transaction) writes it again into the target's table. Hashing a view costs a
realization unless identity is over parts.

## 1. The layout

**Proposal.** A record is its attribute and value pairs, each a key's tail
(the attribute as `encode_bytes` writes it in a key, the value's type byte,
the value's order-preserving encoding with numbers in the one band), sorted
by their bytes, repeats dropped, ended by a lone `0x00`. A field holding a
record holds its encoding inline after the record type byte. Where the
names sit, in each field or once in a header, is section 2's choice; the
rest of this section holds either way. In memory,
`Value::Record` holds these canonical bytes; equality is their equality, and
`Hash` hashes them.

**Why.** The spike's codec holds every property the topic claims, over
200,000 generated records, each check seen to fail with its property broken:
it re-encodes the same, delimits itself, ignores pair order and repeats,
keys `3` and `3.0` alike, and compares by bytes as its pairs compare by
meaning. A spilled prefix cut at a pair boundary is itself a record that
sorts before the whole, so spilling needs no new rule. Reading one field
takes 0.33 to 0.82 of the time decoding the dag-cbor record whole takes.

**Token equality** stays a store's business: a store that serves records
from tokens (the single-file archive) compares token names where both come
from it and realizes canonical bytes otherwise. `Value` never carries a
source, so `Hash` and `Eq` cannot disagree.

**To test.** A commit through the perf gate with records stored: the spike
measured reads of canonical bytes in memory, not a commit or a read served
from the file.

## 2. Attribute names on the wire

**Found.** Names are 33 to 70% of a record's bytes. On the wire, uncompressed,
pairs are 1.25 to 1.44 times dag-cbor for revision records and 1.89 for tonk's
bindings, where each nested event repeats the same names. Under gzip over 64
records, as a node's block would carry them, pairs and dag-cbor are level
(1.00 to 1.05). Blocks are not compressed today.

**Names once per record**, measured ([second sitting](results/2026-11-11b-layouts.md)):
a header listing every name once, sorted, with fields naming attributes by
index, takes tonk's bindings to 1.02 times dag-cbor; names written at first
use with back-links after, to 1.05. Flat records gain nothing (revision
records with versions as bytes: 1.25 to 1.24). Finding a field costs about
the same in every layout. Taking a nested record out as a record of its own
costs 6 to 7 times more, because its fields name attributes in its outer
record's table.

**Correction.** An earlier version of this section rejected names once per
record for losing canonical bytes, the split into the file's tokens, and
finding a field without a full read. None holds: a fixed rule for the
table's order keeps one byte string per record; importing a record into the
file interns each table entry once and maps indices to tokens; a header (or a
trailer) finds a field without a full read. What it does give up is that the
bytes order records by content, which only an index range over record values
would use, and decided reads of records are equality and destructuring.

**Options.**

| | what it does | what it costs |
|---|---|---|
| a. names inline | each field writes its name | 1.25 to 1.89 times dag-cbor on the wire |
| b. a header per record | each name once, sorted, at the front; fields by index | a nested record is not a record on its own: binding one either writes it again (2.5 to 3 µs a view here) or holds it as a view into its outer record |
| c. names at first use, back-links after; a trailer for lookup | as b, written in one pass by a writer that does not know every name first | as b; without the trailer a reader cannot skip a nested record without reading it |
| d. names once per node | the tree's segment encoder splits each record in a node into its parts, as its columns already do for entities and attributes | a new node layout version; the encoder's cost per record |
| e. compress blocks | gzip or zstd on every block, on the wire and at rest | a decompress per block read; a change to every store and transport |

Sharing across records (d, a stream's table, the single-file archive's
tokens) is the same idea as b, one level up. It belongs to whatever carries
many records, never to a record's own bytes: a record is hashed, signed and
compared alone, so its canonical bytes cannot depend on what came before it
in a stream. A stream that sends only new names in each packet (as HTTP/2's
header compression does) is a transport encoding that decodes back to
canonical records.

**Proposal.** b for a record on its own, which is a buffer of one (section
0); every record held in a buffer of many is a view over that buffer's table
(d for a node, a stream's table, the file's tokens). The maintainer settled
the condition this section first asked about: all records are views over
the buffer that holds them, so a nested record bound in a query is a view,
and taking one out costs nothing until it leaves its buffer. b over c,
because a record on its own is written whole, so a one-pass writer gains
nothing, and the header finds a field without a trailer.

**To test.** A nested record bound as a view (outer bytes and a span):
taking one out, comparing two, and storing one. And d: the spike's records
encoded into segment blocks both ways, block bytes, encode time and read time.

## 3. Versions and other fixed binary values in a record

**Found.** A version's entity is a hash, from which its edition does not come
back, and the ancestry walk needs editions: a revision record cannot write its
versions as entities. Written as their 40 key bytes they are compact and the
fastest to read (0.51 of dag-cbor); written as nested records (`origin`,
`edition`) they destructure to the edition (0.33 of dag-cbor) and cost 15%
more bytes.

**Proposal.** Bytes, for revision records and any value that is a fixed
binary name. A formula reads a version's edition and origin from its bytes,
as `dialog/revision` already projects the record. Nested records stay for
values whose parts a query reads by name.

## 4. Sharing a record's values with the rest of the file

**Found.** Every version a revision record names is already a token in the
file, the 40 raw bytes a history key holds; none of the 7,857 is shared,
because a value slot carries its type byte and escapes the edition's zero
bytes. The same holds for any entity a claim's value names and another claim
is about: the subject's token is the payload, the value's token the type byte
and the payload.

**Proposal (for the single-file archive, #733).** Intern a value slot as two
parts, its type byte and its unescaped payload, so a value shares a token with
the same bytes wherever a key holds them. This is a change to `key_tokens`,
not to any format a block or a record has, and it helps every claim whose
value is an entity, records or not.

**Then, for records.** A record value is split recursively, as history keys
already are (`split_record_components`): its parts become tokens and the
record a list of their names. A file row has a fixed shape (a key's form, at
most four parts, a datum), so the list is a composite token, a token whose
bytes are the names of others; realizing a key realizes it recursively, and
comparing keys part by part descends into it. Modelled, split revision records
take 0.40 of dag-cbor in the file, most of it names; the file's delta-varint
columns would shrink those.

**To test.** On #733's datasets (tonk 1× and 10×, MusicBrainz): file size and
split time with value payloads interned apart, then with records split. The
file's own largest cost is already splitting (about 13% of a warm command),
and a node's tokens sit on about 500 pages; both are to be measured, not
assumed.

## 5. Keying, and telling old records from new

**Found.** The tree's manifest already names how values are keyed
(`value_encoding`, `NUMBER_BAND = 1`).

**Proposal.**

- A tree whose manifest says `value_encoding = 2` keys a record as its pairs
  (section 1); a tree at 1 keeps keying record bytes escaped, as today. The
  manifest field is critical, as the number band's is.
- No type tag, no new value type. A pairs record starts with `0x00` (empty)
  or the first byte of a relation, which is ASCII or a UTF-8 lead byte
  (`0xC2` to `0xF4`). A dag-cbor map starts with `0xA0` to `0xBF`, an array
  with `0x80` to `0x9F`: UTF-8 continuation bytes, which no relation starts
  with. Every record written today is a dag-cbor map (revision records,
  tonk's bindings and embeds), so the first byte says which layout a
  record's bytes are in. Old records stay readable as opaque `record:`
  bytes, forever; a signed revision record is never re-encoded.

**Risk.** A record some other writer stored that is not a dag-cbor map would
be misread as pairs and fail to decode. A census of stored records (section
11) checks that none exists.

## 6. A table keyed by names known only at run time

**Found.** tonk's two records today (`Bindings`, `Embeds`) are maps from a
name to a structure. Fields are attributes, so the name cannot be a field.

**Proposal.** A set of nested records under one many-valued attribute, the
name a field of each: `{tonk.view/event: {tonk.event/name "on/click",
tonk.event/type "click", ...}}`. Destructuring finds an entry by its name the
way a query finds anything: `{tonk.view/event: {tonk.event/name "on/click",
tonk.event/type ?type}}`. Its cost is the repeated names, which section 2
addresses.

## 7. Destructuring in queries

**Proposal.** A pattern in the notation, `{location: {geo/lat: ?l}}`, compiles
to a destructure premise: its input is a bound record, it never scans, and it
yields a row per combination of its fields' values (a field holding several
values yields one row each, as `.[]` does in jq). A field is an attribute,
bringing its type and pick; `last` over several values falls to the hash of
the value (#752). A missing field yields no row, unless the pattern marks it
optional as a concept field would be. A formula reading a path (jq's paths,
each step an attribute) is the same premise spelled differently, and can come
later.

**Why one premise.** The planner then knows destructuring is a function of a
bound input, as it knows a formula is, and costs it as one; selection keeps
meaning only stored claims.

## 8. Documents that grow

**Proposal.** Not records. An automerge document's changes are a set under an
`all` attribute: a write is one change, a merge between replicas is the union
of the sets, which is automerge's own merge, and a snapshot that retracts the
changes it covers leaves concurrent changes standing. A record is for a value
replaced whole.

## 9. Requiring a record to satisfy a concept

**Proposal.** Later, as a refinement over `record:` like `conforms` over
`entity:`: an attribute may say its records must hold the attributes of a
concept, and a read filters those that do not, as schema on read does for
entities.

## 10. Records in JSON

**Found.** The claim API writes a record as base64 bytes, so nothing reading
JSON can see inside one (tonk's `KIND` comment says as much).

**Proposal.** `{"record:": [["geo/lat", 37.77], ["geo/lon", -122.42]]}`: the
pairs in canonical order, each value in its own JSON form (tagged where it
would not read back), a nested record nested the same way. A list, not an
object, since an attribute may repeat.

## 11. The census still owed

The spike's revision records have one issuer and seeded shapes, and its
bindings are rebuilt from the library rather than lowered. On tonk-team's
real history: how many records, by attribute; their sizes; how many spill;
how much of each repeats across records and with claims; and whether any
stored record is not a dag-cbor map (section 5).

## 12. The word

"Record" names a file's records (`kind | length | crc32 | body`), history
records (`RevisionRecord`, the claim records `split_record_components`
splits) and `Value::Record`. **Proposal:** keep `record:` for the value,
since it is the type's name in the notation and in tonk, and call the file's
units something else (entries) while #733 is still a draft. Renaming the
history's records is a larger change and not needed for this one.

## 13. Identity of a view

**Options.** The hash of the canonical form (a view realizes it to be hashed,
once, kept with the value), or a hash over the hashes of its parts (a buffer
keeps each token's hash, as the single-file archive's index already keys
tokens by one, and a record's identity combines them without realizing
anything). The second changes what a record's reference is, everywhere it is
used, so it is a decision, not an optimization.

## What would settle it, in order

1. The maintainer sets the bars, and decides how a view's identity is
   computed (section 13).
2. Interning value payloads apart in #733 (section 4): cheap, useful without
   records, and it measures the sharing the hypothesis rests on.
3. The segment encoder splitting records (section 2, option d).
4. The census on tonk-team (section 11), and a commit through the perf gate.
