# Glossary

| term | meaning |
|---|---|
| cell | the claims of one attribute on one entity: one `(relation, entity)` in the tree. A cell is keyed without its pick. |
| claim | one stored value of a cell, with its cause and the version of the revision that wrote it. |
| live claim | a claim in the operational index. History keeps every claim, live or not. |
| operational index | the index reads use. It holds live claims, and it is the read's shortcut: a write keeps it at the claim a read would return. |
| pick | how a read elects among a cell's live claims (`last`, `max`, `min`, `top`), or that it returns them all (`all`). It travels with each write, and a write under it retires what the read would no longer return. |
| electing pick | every pick but `all`. |
| elected claim | the live claim a read under the pick returns. |
| succeed | a claim succeeds another when its cause names the other's version: it was written having seen the other, and replaces it. |
| retire | take a claim out of the operational index on a write. History still holds it. |
| concurrent | of two claims, neither was written having seen the other. Whether "seen" is read from claim lineage or from the revision DAG is open; see the claims. |
| come back | a claim that a write retired, but which a replica receives after the claim that succeeded it, through partial sync. |
