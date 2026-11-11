# Record values: journal

What we tried, what it measured, and what we decided, newest last. The
[topic](../record-values.md) says why and sets the bars.

## Where things stand

**Status: Exploring.** See the [research index](../README.md) for what the
statuses mean.

- **Built:** a spike, `rust/dialog-repository/examples/record_spike.rs`:
  the codec (pairs as key tails), its canonicality checks, a dataset of
  revision records through the repository, and the measurements.
- **Headline** ([results](results/2026-11-11-spike.md)): canonical over
  200,000 generated records; one field reads in 0.33 to 0.82 of the time
  dag-cbor takes to decode; split in the file (modelled), revision records
  take 0.40 of dag-cbor; on the wire, pairs are 1.25 to 1.89 of dag-cbor,
  most of it attribute names, and 1.00 to 1.05 once compressed in a node's
  worth of records.
- **Decided:**
  - A record is an atomic value.
  - It carries no type tag. Its fields are typed on read by attributes,
    as claims are, and an attribute with no `as:` takes any value.
  - Fields are named by attributes and destructured as `{lat ?lat}`.
  - A field may hold several values, so a record is a set of pairs.
  - A field may hold an entity, which is followed by an ordinary join, or
    a nested record, which is destructured.
  - Selecting an attribute finds only claims the replica stores. A
    record's fields are never claims: they are reached by destructuring a
    bound record, never by selection, and a record cannot stand where an
    entity is expected (`of`).
  - Equality follows tokens as key comparison does, resolving them across
    sources, and the wire carries the canonical form.
  - Repetition is expected mostly across records, and shared through the
    replica's tokens. Measured otherwise for records nesting a set: in
    tonk's bindings, repeated attribute names are half the record.
- **Decided, continued:** in destructuring, a field is named by an
  attribute, which brings its pick and its type. `last` over a field
  holding several values elects by the hash of each value, the tiebreak
  every election should end on when the version cannot decide.
- **Open:**
  - the surface of destructuring: a pattern in the notation, a formula
    over a path, or both;
  - documents that grow (options in the third entry);
  - whether an attribute can require its records to satisfy a concept;
  - how a reader tells today's dag-cbor records from the new layout.
- **Proposed, from the single-file archive:** a record has the two forms a
  tree node has in the file, canonical bytes and rows over the file's
  tokens, and is keyed as its tails rather than as one escaped byte string,
  so the file splits it as it splits a key.
- **Next:** the maintainer sets the bars; the [design](design.md) proposes
  a solution for each open question. Then the census on tonk-team's real
  history, and a commit through the perf gate.
- **Blocked:** tonk-team's history is not in this session's container.

## Datasets

- **revisions-600-7, revisions-4000-11:** revision records a repository
  wrote over 600 and 4,000 steps on three branches, by `record_spike
  revisions`; one issuer. sha256 `0f22104a…31a7e63` and `0fada0fe…3e02f85`.
- **tonk-bindings:** each view's bindings in tonk's library at `bfb2d72`, by
  `bench/tonk_bindings.py`; 13 views, 38 events. sha256 `a06cf2ca…970ed8d16`.

- **tonk-team:** the team space's pushed history, the 9,070 blocks #733
  fetched (568.5 MiB). Its records are revision records and tonk's view
  bindings and embeds. Not committed; read from a copy.
- **Stack Exchange:** the `retrocomputing` replay (`DIALOG_SE_CSV`). Its
  only records are revision records, one per commit, so it measures those
  at scale and nothing else.

## 2026-11-06 — Framing: what a record is, and where sharing lives

**Question.** What should a record value be, given a placeholder, three
ad hoc dag-cbor uses, and the single-file archive's tokens?

**Method.** Read `notes/record-value.md`, `Value::Record`'s keying
(`ordvalue.rs`, `key.rs`), every writer of a record in dialog and tonk,
#636 and #733, and the fressian and schemaboi documentation.

**Results.**

- Every record today is dag-cbor, and each writer invents its own type
  tag: the attribute for revisions, a `kind` field for tonk's bindings.
- Fressian, schemaboi and FlexBuffers each let the writer choose an
  encoding, so none is canonical as it stands. Fressian's cache and
  schemaboi's schema depend on the stream or file they sit in.
- A record's bytes are hashed, sent in blocks and signed, so they must
  stand alone. Sharing across records belongs to the file, as it does for
  nodes.

**Decision.** A record names its type and has one canonical, standalone
encoding; equality is of those bytes, and token ids compare them inside a
replica. Field names live in the record, written once, with later
occurrences referring back. Where the shared strings sit (one region or
interleaved) and whether references are offsets is for the spike. The bars
in the topic are proposed and await the maintainer.

## 2026-11-06 — Equality follows tokens, and fields are attributes

**Question.** What a record's canonical form has to carry, once the
replica shares tokens across records, and how its fields are named.

**Method.** Discussion; nothing measured.

**Results.** As the maintainer stated them:

- Equality follows the pattern of key comparison: records read over the
  same source compare by their token references, records from different
  sources resolve their tokens, and the wire carries the resolved,
  canonical form.
- Repetition is expected across records, not within one.
- Fields are named by attributes: in `{lat ?lat}`, `lat` is an attribute
  reference, not a free name.
- Records are flat for now; nesting may come later.

Consequences raised, not yet decided:

- `Hash` cannot take the same-source shortcut: equal records from
  different sources must hash alike, so it runs over the canonical form,
  computed once and kept with the value.
- A sorted run of attribute and value pairs is an EAV run without its
  entity. Laid out as key tails, a record would compare, find a field and
  split into file tokens with the code and the tokens keys already use.
- Such a record is a concept instance without `this`. A concept's identity
  is the sorted set of its attributes' identities (`notes/concept.md`), so
  the attributes may already name the record's type.
- The alternative is claims on an entity identified by the hash of its
  pairs: atomic through the claim that refers to it, every field indexed,
  at (n+1) claims in three orderings per write, the cost #636 measured.

**Decision.** The four results above are settled. They replace the first
entry's string table inside the record and its "later occurrences
referring back": sharing moves from the record to the replica's tokens.
Not decided: whether attributes name the type, value or entity, and
many-valued attributes.

Commits: this entry.

## 2026-11-06 — Fields are typed on read, records are values, a field holds a set

**Question.** How a record is typed, whether it is a value or claims on an
entity, and whether a field holds one value or several.

**Method.** Discussion. Read how dialog types attributes
(`notes/concept.md`, `AttributeDescriptor` in
`dialog-query/src/attribute/descriptor.rs`, `type_system.rs`) and what
happens to concurrent values (`notes/version-control.md`).

**Results.**

- Dialog stores no type beyond each value's own type byte. An attribute
  is a relation read under an optional type and a pick
  (`AttributeDescriptor::content_type` is an `Option`); two attributes over
  one relation under different types are two attributes. Concepts are
  read, not written: one entity satisfies any number of them.
- So a record needs no type tag. A field stores its relation and its value
  with the value's type, as a claim does, and an attribute's `as:` reads
  it. A record satisfies whichever concepts its fields satisfy. Tonk's
  `kind` check becomes reading by concept; whether that refuses foreign
  data as reliably is a hypothesis.
- A record is an atomic value (the maintainer). The alternative, claims on
  an entity whose identity is the hash of the record's pairs, is set
  aside, and with it indexed fields.
- A field may hold several values: a record is a set of pairs, sorted,
  without duplicates.
- Corrections to the previous entry: flat is preferred, not decided, and
  repetition inside a record is possible, only not expected to be common.
  A field value is any dialog value and a record is one, so a record nests
  unless that is forbidden; what is undecided is whether destructuring and
  import recurse.

Raised, not decided:

- `last` has no meaning inside a record: its pairs have no causal order.
  `all`, `max`, `min` and `top` read a set of pairs as they read claims. A
  `last` attribute over a field holding two values needs a rule.
- Documents that grow, such as automerge. As one value, every edit writes
  the whole document, every version is kept, and concurrent edits both
  survive while `last` shows one (`notes/version-control.md`: concurrent
  values "both survive ... until a later write that has seen both cites
  and supersedes them"). As a set of changes under an `all` attribute, a
  write costs one change, a merge between replicas is a union of the sets
  (which is automerge's own merge), and a snapshot that retracts the
  changes it covers leaves concurrent changes standing, since a
  retraction removes only the claims its author saw. The second needs no
  record at all. #388, which put automerge in records, was closed as
  "reconsidered".

**Decision.** Records are atomic values with no type tag, typed on read
by attributes; a field holds a set. This replaces the first entry's "a
record names its type". Open: nesting, `last` over a set, documents that
grow.

Commits: this entry.

## 2026-11-06 — Destructuring is not selection

**Question.** What "fields are not indexed" means, whether records nest,
and how reading a field relates to selecting an attribute.

**Method.** Discussion. Read how the `of` slot is typed
(`dialog-query/src/attribute/query/all.rs`) and how the unifier treats a
conflict (`type_system/unifier.rs`).

**Results.** As the maintainer stated them:

- Selecting `geo/lat` finds nothing unless the replica stores `geo/lat`
  claims. Holding a record with a `geo/lat` field does not change that.
- `{ location: { geo/lat: ?l } }` destructures: it reads a field out of a
  record already bound. Binding a record to `?p` and then selecting
  `geo/lat` `of: ?p` is not allowed, because selection must work with its
  entity unbound, and a record's fields are not in the indexes it scans.
- A field may hold an entity, whose own claims are found by an ordinary
  join, or a nested record, which is destructured. Records nest.
- A formula reading a record by path (jq notation was suggested) is an
  alternative surface; it is still destructuring, not selection.

Found in the code:

- `of` is typed `entity` (`all.rs`), and the unifier refuses an empty meet
  (`unifier.rs`). A record-typed binding used as `of` is therefore refused
  when the rule is compiled. When the binding is untyped, it is refused
  only when the row is read, presumably as no row; that is a hypothesis.
- "Fields are not indexed", in the previous entry, says the same as the
  first result: only the whole record is a value in the indexes, found by
  equality.

**Decision.** Records nest. Destructuring reads a bound record and never
scans; selection sees only stored claims. This replaces the earlier
"flat preferred". Not decided: whether an attribute reference brings its
type and pick into destructuring, and the surface of destructuring.

Commits: this entry.

## 2026-11-06 — `last` inside a record elects by value

**Question.** What a `last` attribute reads from a record field holding
several values.

**Method.** Discussion. Read `ArtifactView::elect`
(`dialog-artifacts/src/artifacts/artifact.rs`) and `value_beats`
(`dialog-artifacts/src/artifacts/update.rs`).

**Results.**

- The maintainer: `last` can elect inside a record as queries already do.
- `elect` decides in three tiers: the revision's standing (edition, then
  the hash of the version), the cause, then the value, the greater
  winning (`value_beats`: the values' own order, their encoding's
  otherwise). The hash orders revisions, not values.
- Every pair of a record was written by one revision, so the first two
  tiers tie and `last` falls to the value. Inside a record, `last` reads as
  `max` does, which is what it already reads between two values one
  revision asserts. Concurrent records still compete as whole claims, by
  revision.

**Decision.** In destructuring, a field is named by an attribute, which
brings its pick and its type; `last` over several values elects the
greater. The type applying is inferred from the pick applying, as it
does for a concept's field, and is to be confirmed. This replaces the
earlier proposal to refuse `last` over several values.

Commits: this entry.

## 2026-11-06 — The topic, revised to the decisions so far

**Question.** What the topic states, now that the first entry's framing
is replaced.

**Method.** Rewrote [the topic](../record-values.md) from the entries
above. Nothing measured.

**Results.** The topic now has a question and a hypothesis (records as
sorted pairs laid out as key tails, sharing the replica's tokens), the
decisions above, and four variants against main. The string table inside
a record, the type tag and the candidate encodings B and C (CBOR with
stringref, a canonical FlexBuffers profile) are gone. One question is
new: with no type tag, a reader still has to tell today's dag-cbor
records from the new layout.

**Decision.** The bars changed with the hypothesis: a size bar on the
wire, allowed 25% over dag-cbor because attributes are full relation
names, and a size bar in the file. They were proposed, never set, and no
result has been seen, so nothing was moved after a result. They remain
the maintainer's to set.

Commits: this entry.

## 2026-11-07 — `last` is decided by version, which a record's values share

**Question.** Whether the previous entry's rule, `last` over several values
in a record elects the greater, is right.

**Method.** The maintainer objected that `last` resolves by version, not by
value. Read `Pick::newer` and `Standing`'s order
(`dialog-artifacts/src/artifacts/update.rs`).

**Results.**

- `last` is `Pick::newer`: the greater standing wins, and the value is
  compared only when two standings are equal. A standing orders by the
  version (edition, then the version's hash), then the cause, then the
  succession. The version decides `last`; the value only breaks a tie
  between values one revision wrote under one cause.
- Every value in a record belongs to one claim and shares its version, so
  inside a record the version never decides, and `last` always falls to
  the value. The previous entry described the code correctly, but its
  decision rested on electing "as queries do", and queries elect by a
  version a record's values do not have.

**Decision.** The previous entry's rule is withdrawn: what `last` reads
from a field holding several values is open again. The options: fall back
to the value (deterministic, but `last` then reads as `max` does), refuse
(a reader wanting one value of a set says `max`, `min` or `top`), or read
it as `all`. Attributes are read, not written, so whichever is chosen
applies on read. Proposed: refuse.

Commits: this entry.

## 2026-11-07 — When the version ties, the hash of the value decides

**Question.** What `last` reads from a record field holding several
values, given that they share one version.

**Method.** Discussion. Read `Pick::newer` and `value_beats`
(`dialog-artifacts/src/artifacts/update.rs`), `ArtifactView::elect`
(`artifact.rs`), and the hashes a value already has
(`artifacts/value.rs`).

**Results.**

- The maintainer: when everything else ties, an election should choose by
  the hash of the value, not by the value. Today it chooses by the value,
  the greater winning (`value_beats`). That is a defect in dialog, rarely
  reached: it decides only between values one revision wrote under one
  cause. It also lets a writer pick a value that wins such ties, which a
  hash does not, as the version's hash already does not for concurrent
  revisions.
- Inside a record the version always ties, so `last` over several values
  always reaches this tiebreak. Under the hash rule it elects one value
  deterministically on every replica, without reading as `max`.
- Which hash: `Value::to_reference` hashes the payload without its type,
  so text and bytes of the same bytes tie. `Value::identity_key` is the
  type byte and the bytes, every number in one band, which agrees with
  value equality. Hashing it is proposed. For a record it is the hash of
  the canonical form, never of token references.
- The value tiebreak is reached from `elect` and `Pick::newer`, and from
  `dialog-repository`'s transaction oracle and `dialog-query`'s concept
  query, all through `value_beats`. Changing it changes which value a read
  returns for such ties, so replicas on releases either side of the change
  can show different values: a changelog entry under what changes for you.

**Decision.** `last` over several values in a record elects by the hash
of each value. The same tiebreak replaces the value order in dialog's
elections; that change is separate from this topic. This replaces the
proposal to refuse `last` over several values.

Commits: this entry.

## 2026-11-11 — What the single-file archive already does for records

**Question.** What of the single-file archive (#733) records can share,
and what it changes in this topic.

**Method.** Read the single-file topic's design and journal and
`dialog-archive-file/src/node.rs` on `worker/pensive-edison-fu3bx5`
(`f39b10f`), and `Datum` in `dialog-corpus`. Nothing measured.

**Results.**

- A leaf entry in the file is a row: the key's form, the tokens of its
  parts (entity, attribute, value), and a datum token (`entry_row`). A
  value token is the key's value slot, its type byte and its
  order-preserving payload. Keys compare part by part over the tokens
  (`KeyParts::cmp_bytes`), and a node's wire block is realized from its
  rows. Tokens are named by where they sit, and a writer that finds a token
  another appended first takes theirs, so a canonical value has one token
  in a file: the condition equality by token needs.
- History keys had no column schema and were kept whole; splitting them
  into version, entity, attribute and value tokens
  (`split_record_components`) took tonk-team's modern nodes from 15.3 to
  5.7 MiB. Splitting composites so their parts share the replica's tokens
  is the file's largest measured win after frames themselves.
- `Datum` holds no value (its cause, versions and blob), so a value is
  stored once, in the key, inline or spilled. A record is not stored
  twice.
- A `record:` value is keyed today as one escaped, terminated byte string
  (`ordvalue::encode_value`). The file keeps it as one value token, and
  could split it only after unescaping it. A record keyed as its tails
  themselves would split as a key does.
- Rows have a fixed shape: a form and at most four parts. A record has any
  number of pairs.
- Costs the file measured: splitting nodes into rows is its own largest
  cost (12.6 to 13.7% of a warm command), and a node's tokens sit on about
  500 pages written across the whole history.
- The file is native. The web keeps records canonical only, until the
  OPFS work lands.
- "Record" names three things in the code this topic touches: a file's
  records, history records (`RevisionRecord`, the claim records), and
  `Value::Record`.

**Decision.** Proposed, for the maintainer: a record has the two forms a
tree node has in the file, canonical bytes and rows over tokens; it is
keyed as its tails, so it is splittable as a key is; and the census and
the tokenized variant measure the two costs above, a commit and a cold
destructure. Open, added: how the file holds a record's parts (a value
token of token names, or rows of its own), how the tree's manifest names
the new keying, and the word.

Commits: this entry.

## 2026-11-11 — The first spike: pairs against dag-cbor

**Question.** Is a record as key-tail pairs canonical, cheaper to read a
field of, and smaller in the file, and what does it cost on the wire?

**Method.** The spike's codec over three datasets: revision records a
repository wrote (versions written as nested records and, apart, as their
key bytes), and tonk's view bindings rebuilt from its library. Canonical
properties over 200,000 generated records, each check seen to fail with its
property broken. Sizes against dag-cbor, raw and under gzip in groups of 64; a
model of the file's tokens; one-field reads, seven interleaved rounds. Full
tables in [the results](results/2026-11-11-spike.md).

**Results.**

| | revisions, versions as records | revisions, versions as bytes | tonk bindings |
|---|--:|--:|--:|
| on the wire ÷ dag-cbor | 1.44 | 1.25 | 1.89 |
| the same, each attribute once a record | 1.30 | 1.23 | 1.05 |
| gzip over 64 records ÷ dag-cbor's | 1.05 | 1.02 | 1.00 |
| in the file, split (model) ÷ dag-cbor whole | 0.40 | 0.47 | 1.26 |
| one field, median ÷ dag-cbor decode | 0.33 to 0.82 | 0.51 | 0.51 |

- The canonical form holds every property checked.
- Attribute names are 33 to 70% of a record's bytes. In tonk's bindings,
  where every event repeats the same names, repeats are half the record.
  That contradicts the earlier entry's expectation that repetition sits
  across records, not inside one: it holds for flat records, not for a
  record that nests a set.
- A version's entity is a hash, from which its edition does not come back,
  so a revision record cannot write its versions as entities and keep what
  it holds. Written as bytes or as records, it can.
- Every version a record names is already a token in the file, as a history
  key's 40 raw bytes, and none is shared: a value slot carries the type byte
  and escapes the edition's zero bytes.
- Blocks are not compressed today, on the wire or at rest.

**Decision.** Not decided: the bars are the maintainer's, and the results
sit between them (revisions pass, bindings fail the wire and file bars
uncompressed). The [design](design.md) proposes what to do about each.

Commits: this entry.

## How to reproduce

The commands are in [the results](results/2026-11-11-spike.md#reproduce).
