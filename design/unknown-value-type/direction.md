# Direction

The problem, from the [top story](../unknown-value-type.md): a release
that meets a value of a type it does not know cannot tell where the
value ends, so it cannot parse the key, and fails reads of the cell and,
for a covering history record, the pull.

Every approach keeps today's eleven type bytes and their encodings,
except D, which renumbers them. "Unassigned" means every byte the table
does not hold today: 10 to 21 and 23 to 255, with 23 to 25 to be
confirmed unused in keys (ledger).

| Decision: how a release finds the end of a value whose type it does not know, for: the problem above |
|---|---|---|---|---|
| criterion \ approach | A. Status quo: the type table alone frames a value; an unknown byte is corruption | B. One rule: every unassigned type byte is escaped-terminated, as the composite band (22) already is | C. Framing classes by tag range, for unassigned bytes only: one range escaped-terminated, ranges of fixed widths (for example a range per width, the width a function of the tag) | D. Renumber every type byte into class ranges at a key rewrite, so the tag alone names the class for every type, old and new |
| steps over an unknown value in every ordering (VAE included) | 🔴 does not: the key does not parse | 🟢 yes: the value runs to its terminator, then VAE's attribute and entity, or EAV's spill tail, follow as today | 🟢 yes, for each class the ranges define | 🟢 yes, for every type |
| order of the existing bands | unchanged | unchanged: no assigned byte moves | unchanged: no assigned byte moves | 🟡 changes: bands sort by tag, so renumbering reorders the bands of an attribute's values against one another (a range scan across types sees them in a new order) |
| order within a new type's band | n/a | bytewise order of its escaped encoding: what text, bytes and composites have | bytewise for the terminated range; for a fixed-width range, its big-endian bytes | as C |
| byte cost per key | none | none | none | none |
| freedom of a future type's encoding | any, with every older release unable to read it | 🟡 terminated only: a fixed-width type pays the escaping of its `0x00` bytes, at most one byte per `0x00` plus a terminator | fixed widths the ranges name, or terminated; a self-delimiting codec (as the number band's) cannot be a class an unknown reader steps over (ledger) | as C, for every type |
| existing keys rewritten | none | none | none | 🔴 every key of every index and every history and coverage record, through a `migrate_storage`-style rewrite behind a gate; the number band rewrite is the precedent and its rollout failures are in `design/stored-format/last-migration.md` |
| what a release before the rule does with a key of a new type | fails as today | fails as today: the rule helps only releases that carry it | fails as today | fails as today, and cannot read the rewritten keys at all |
| when a writer may use a new type | never safely | once every replica of a space carries the rule, which needs a fence a release checks before writing a new tag (?) | as B | after the rewrite and its fence |
| relation to the stored-format migration (#857) | none | can ship before or with it: no data moves, so it does not need the migration; the fence for new tags can be the same generation record #857 plans | as B | 🟡 needs a fact-key rewrite #857 does not plan; it would join the two rewrites into one gated step |
| complexity of the rule a reader carries | the table | the table plus one default | the table plus a range table | one range table |
