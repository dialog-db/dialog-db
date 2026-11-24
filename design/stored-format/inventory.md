# What a replica stores about rules, concepts and attributes: inventory

Read on main at 2b12204, 2026-11-24. Every row is a fact with its file
and line unless it says otherwise. Paths are under `rust/`.

## The facts dialog itself stores

| fact | `of` | `is` | written by | stored in |
|---|---|---|---|---|
| `dialog.rule/source` | rule entity | the body: dag-cbor `{ "rule": .., "format": N }` | `dialog-query/src/rule/statement.rs:305` (deductive), `:382` (inductive) | the branch's tree, a stack layer's tree, a session or query overlay (`dialog-repository/src/repository/branch/session.rs:2440` to `:2452` reads all of them) |
| `dialog.rule/conclusion` | rule entity | the concluded concept's entity, `concept:..` | `statement.rs:298` | same |
| `dialog.rule/reads` | rule entity | `on:<domain>/<name>` per attribute a premise reads | `statement.rs:313` | same |
| `dialog.rule/derives` | rule entity | `on:<domain>/<name>` per attribute of the conclusion | `statement.rs:321` | same |
| `dialog.rule/induces` | rule entity | the concluded concept's entity | `statement.rs:388` | same |
| `dialog.rule/on` | rule entity | `on:<domain>/<name>`, or `on:<domain>/_position` / `_symbol` for a collection | `statement.rs:395`, `Reach::on_entity` `:170` | same |
| `dialog.concept/transient` | concept entity | `true` | `dialog-repository/src/rules.rs:123` (`Transient`) | same |
| `dialog.provider/command` | `session:` | concept entity | `dialog-repository/src/stack/effect.rs:484` | the stack's own `session:` layer, one row per concept a provider serves |
| `dialog.command/answered` | a command entity | concept entity | `stack/effect.rs:625` | `control:`, one tick; a rule may copy it into a layer that keeps it (`stack/effect.rs:29` to `:31`) |
| `dialog.rule/invalid`, `dialog.rule/quarantined` | rule entity | a reason, a concept | answered from the bodies, never stored (`statement.rs:78` to `:95`) | nowhere |

A concept and an attribute are never stored as facts of their own by
dialog. A concept is embedded whole in a rule body, its `conforms`
target too (`dialog-query/src/concept/descriptor.rs:2156`, the test
that the full target descriptor is written under `conforms`), and named
by its entity in the rows above.

## What each identity is a hash of

| identity | entity | hash of | file |
|---|---|---|---|
| attribute | `the:<base58>` | under `ATTRIBUTE`: `the` (the relation as a symbol), `cardinality` (`one`/`many`), `as` (the type's entity), `among` (the listed values) | `dialog-query/src/attribute/descriptor.rs:798` to `:822` |
| concept | `concept:<base58>` | under `CONCEPT`: each field's attribute reference mapped to its options, `optional`, `conforms` (the target's reference), `pick` (the aggregator's name). Field names and descriptions are not in it | `dialog-query/src/concept/descriptor.rs:280` to `:305` |
| rule | `rule:<base58>` | under `RULE`: the head (`deduce`, `assert!`, `retract!`) as the concept's reference and each head field bound to a canonical variable, `when` and `unless` as sets of premise references, `reduce` | `dialog-query/src/rule/reference.rs:48` to `:115` |

No identity holds a format number: the number is on the body's envelope
only (`rule/body.rs:63` to `:67`). The doc of `identity.rs:12` to `:15`
says a concept's identity maps `optional` and `conforms`; it also maps
`pick`, as `concept/descriptor.rs:293` shows. The doc is stale.

A fact is keyed by its relation, `domain/name`, and its entity and
value (`dialog-corpus/src/model/fact.rs:32` to `:44`; the cause is the
hash of the three, `model/cause.rs:39` to `:52`). No fact is keyed by an
attribute's or a concept's identity, and the trigger and head indexes
are keyed by relation too (`on:<domain>/<name>`, `statement.rs:100`).
An induced fact carries no cause naming the rule that wrote it
(`dialog-repository/src/repository/branch/transaction/induce.rs`, every
emitted claim has `cause: None`).

## Spellings read today that are not written

| what | read as | file | written? |
|---|---|---|---|
| rule body with no envelope (format 0, before 0.2) | the descriptor, constants bare | `rule/body.rs:129` | no |
| rule body format 1 (0.2) | constants bare, read as the first type their payload fits; `conformed` re-types them to the field's type | `rule/body.rs:39` to `:42`, `concept/query.rs:286`, `rule/deductive/descriptor.rs:111` | no |
| rule body format 2 | constants tagged `{ "text:": .. }`, the authored descriptor, not the canonical one | `rule/body.rs:37` | no |
| rule body format 3 (0.3 to 0.5.0) | the canonical spelling with each attribute under a `pick`. A body spelling no pick decodes; one spelling a pick is refused by name | `rule/body.rs:20` to `:27` | no |
| rule body format 4 (main only, never tagged) | the canonical spelling with cardinalities | `rule/body.rs:59` to `:61` | yes, for a rule that holds no pattern |
| rule stored under the hash of its bytes (before canonical identities) | decodes, inert; `upgrade_rules` re-installs it | `repository/branch/upgrade.rs:4` to `:9`, `:269` to `:317` | no |
| attribute `pick: "last"` / `"all"` | cardinality one / many | `attribute/descriptor.rs:425` to `:430` | no |
| attribute `pick` of any other name, `pick` as a map, `select`, `the` as a list (a chain) | refused by name | `attribute/descriptor.rs:421`, `:431`, `:438`, `:442` | no |
| attribute `"type": ..` (an old key) | ignored: the wire form has no `deny_unknown_fields` | `attribute/descriptor.rs`, test `it_ignores_type_field` (`:1195`) | no |
| a type named `Text`, `Entity`, `UnsignedInteger`, .. | that type | `dialog-corpus/src/model/value.rs:1594` (`ValueDataType::named`), reached from `as` | no; `legacy_name` (`:1561`) is used to reproduce 0.2 identities |
| concept field `pick: "all"` | cardinality many | `concept/descriptor/named_attributes.rs:104` | no |
| concept field `pick: "top"` | the value listed first in `as` | `named_attributes.rs:107` | yes; being removed on `feat/listed-type-order`, not redone here |
| concept field pick of another name or shape (`last:`, `top:`, `{ "top:": .. }`) | refused by name | `named_attributes.rs:109`, `:111` | no |
| a concept with a key it does not know (`maybe`, ..) | ignored: no `deny_unknown_fields` | `concept/descriptor.rs:58` | no |
| `Cardinality` in a batch spelled `last:`, `all:`, `max:`, `min:`, `top:` | one or many | `dialog-corpus/src/model/update.rs:58` to `:80` | no |
| a 0.2 attribute or concept identity | reproduced to find a marker | `dialog-query/src/migration.rs:52`, `:84` | no |
| a 0.3 to 0.5.0 attribute or concept identity | reproduced to find a marker | `migration.rs:123`, `:161`, `:213` | no |
| a 0.3 to 0.5.0 body spelling picks | rewritten to cardinalities and fields that pick | `migration.rs:265` (`rewrite_body`) | no |

Not in this table, because they are not rules, concepts or attributes:
the tree manifest's legacy layout (`dialog-search-tree/src/manifest.rs:29`),
the number band (`repository/branch/migrate.rs`), the repository layout
version (`repository/upgrade.rs`), upstream cells
(`repository/branch/upstream/legacy.rs`).

The `dialog-migration` crate holds one migration, a replica into the
single-file archive (`dialog-migration/Cargo.toml`, feature
`chronicle`). It reads no rule, concept or attribute spelling.

## What tonk-team holds, from the fixture in this repository

`dialog-baseline/fixtures/tonk-team-schema.jsonl` was exported from a
tonk-team replica (`dialog-baseline/examples/tonk_dataset.rs:21` to
`:30`). Run against main on 2026-11-24 with a probe outside the
repository, which decoded each body with `DeductiveRule::decode` /
`InductiveRule::decode` and ran `rewrite_body` on it:

| | count |
|---|---|
| rule bodies | 135 (20 deductive, 115 inductive), every one in format 3 |
| decode on main as stored | 126; each re-encodes to the stored bytes but for the last byte, the format number |
| refused for spelling `pick: "all:"` | 9; `rewrite_body` rewrites all 9 and each decodes |
| rules whose conclusion's identity moved since 0.5.0 | 135 of 135 (`this()` differs from `concept_identity_v1`) |
| concepts tonk names (`db.name/referent`) | 284, every identity moved since 0.5.0 |
| `dialog.concept/transient` markers | 89: 1 on a concept a rule concludes, 76 on a concept tonk names and no rule concludes, 12 on no concept the fixture names; all at 0.5.0 identities |

Tonk keeps its own records of concepts as user data:
`db.name/referent` from `id:<name>` to a concept entity,
`db.concept.with/<field>` from that entity to an attribute record,
`db.concept.optional/<field>`, and on the attribute record
`db.attribute/id`, `db.attribute/as` and `db.attribute/pick`, the last
spelling `last:` and `all:` (`tonk_dataset.rs:421` to `:497`). Whether
those entities are dialog's `concept:` and `the:` identities is not in
this repository.

## What "format 5 for everything" removes, and what each removal needs

A removal needs a **migration** when a live space can hold the thing
and a release after the removal must still mean what it meant. It needs
only a **refusal** when no live space holds it, or when what holds it is
a request (a query, a batch in flight) the sender can send again. Which
spaces hold what is answered for tonk-team by the fixture, and for other
spaces by T6 and T7.

| removed | live spaces hold it? | needs |
|---|---|---|
| body format 4, `TERMS_FORMAT`, `format_for` | no tag wrote it; only a client on an untagged revision of main (T6) | refusal; the upgrade can read it for one release if T6 says a client wrote it |
| body format 3 and the picks its attributes spell | yes: every rule of a 0.3 to 0.5.0 space, 135 of 135 in tonk-team | migration (the upgrade), then refusal |
| body formats 0, 1, 2, bare and authored constants, `conformed` re-typing a bare constant in a stored body | not in the tonk-team fixture; others unknown (T7) | refusal if T7 says none; else migration as 3. `conformed` stays for JSON, which still writes bare constants |
| rules stored under the hash of their bytes | only a space from before canonical identities (0.2) | refusal; or migration, as 0 to 2 |
| 0.2 and 0.3 to 0.5.0 identities (`migration.rs` v0, v1) | every marker and index fact of a 0.5.0 space | migration, then delete with the upgrade |
| attribute `pick: "last"` / `"all"` | not stored by dialog outside format 3 bodies; tonk's `db.attribute/pick` is tonk's data | refusal, generic once the wire form refuses unknown keys (open question 3) |
| refusals by name: `select`, a chain, `top:`, `{ "top:": .. }`, a pick beside a disagreeing cardinality | no | can go once unknown keys are refused; until then they are what keeps a spelling from being read as another |
| legacy type names (`Text`, `UnsignedInteger`, ..) through `ValueDataType::named` | only in format 0 and 1 bodies and 0.2 identities | refusal on the wire; `legacy_name` stays only while the 0.2 identity does |
| field `pick: "all"` | no; nothing writes it | refusal |
| field `pick: "top"` | yes, in a 0.5.0 field and in rules main's upgrade rewrote | being removed on `feat/listed-type-order`; not taken on here |
| batch cardinality spelled as a pick | only in a batch serialized by 0.3 to 0.5 and kept somewhere (T8) | refusal, unless T8 says tonk keeps batches |
| the upgrade's re-install of bytes-hash rules, `remarked` for v0 | 0.2 spaces only | delete with the upgrade |
