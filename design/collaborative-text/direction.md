# Direction: collaborative text editing

Decision: how Dialog supports collaborative text, for: several people
edit the same text, peer to peer and offline, converging without lost
edits; today last writer wins on a text field loses one side.

The axis that separates most columns is where changes travel: through
Dialog's facts, through Dialog's sync engine but outside its tree, or
through another sync engine.

```
                 where changes travel
   ┌────────────────┬──────────────────┬───────────────────┐
   │ Dialog facts   │ Dialog sync,     │ another sync      │
   │                │ outside the tree │ engine            │
   ├────────────────┼──────────────────┼───────────────────┤
   │ doc as value   │ resource synced  │ template          │
   │ changes as     │ by Dialog        │                   │
   │ facts          │                  │                   │
   │ built in       │                  │                   │
   └────────────────┴──────────────────┴───────────────────┘
```

Columns:

- **Status quo**: text fields, last writer wins; apps split text into
  per-paragraph fields to shrink the loss.
- **Doc as value**: a text CRDT document is an opaque value; two values
  on one entity and attribute merge into a new one. Spiked, shelved.
- **Changes as facts**: a document is a mutable resource; its changes
  travel as ordinary facts and are applied to it. Dialog can read its text.
- **Resource synced by Dialog**: the resource lives outside the tree; the
  sync engine answers "what is its current version" and "changes since
  this version", the way memory cells answer a branch's revision.
- **Template**: a text CRDT library with its own sync beside Dialog;
  Dialog holds a key; we document how and what to deploy.
- **Built in**: a native text CRDT that reuses what Dialog already has.

| criterion \ approach | Status quo | Doc as value | Changes as facts | Resource synced by Dialog | Template | Built in |
|---|---|---|---|---|---|---|
| concurrent edits converge, none lost | 🔴 both values survive in the log, a read shows one, the next write that saw both supersedes both | reads merge the siblings (the spike's fold); storage converges on the next edit | the library merges | the library merges | the library merges | ? |
| storage and sync within 2× of a text CRDT library | n/a | 🟡 a whole-document snapshot per commit, retained in the log; the spike committed per edit session, not per keystroke | 🔴 a commit per edit; a commit of a couple of facts writes ~120 to 160 KB today | ? one block per library change, plus a version fact per sync point | the library's own cost | 🔴 same commit cost as changes as facts, unless edits batch |
| knows which documents need syncing | the tree differential | the tree differential; large values spill to blocks push already ships | the tree differential | ? a version fact per document, written at sync points, as assets have a size fact | 🟡 the library's own sync | the tree differential |
| changes to Dialog's core | none | 🟡 the spike: a value type, a merge seam in winner selection, an edge crate; its spec says no storage or sync changes; shelved for core changes (maintainer) | ? | ? a resource kind, change blocks, a version fact, a "changes since" read | none | 🟡 likely the most |
| peer to peer, offline, no server | yes | yes | yes | ? holds if the store can serve "changes since" from hashes and parent links; Automerge's change format allows it | 🟡 depends on the library's sync | yes |
| sync mechanisms the developer deals with | one | one | one | one; change blocks outside the tree, as asset bytes are | 🟡 two | one |
| needs the whole document on the editing peer | no | yes | ? | yes, to apply changes | yes | ? |
| a query returns the current text (nice to have) | yes | 🟡 through decoding the snapshot | yes | ? Dialog materializes the text | no, opaque key | ? |
| no interleaving of concurrent runs (nice to have) | n/a | from the library | from the library | from the library | from the library | ? whole document per peer allows a Fugue style order |
| library agnostic (nice to have) | n/a | one format per edge crate | ? | 🟡 needs readable hashes and parents; Automerge yes, version-vector libraries no | each library a template | n/a |
| rich text (nice to have) | no | from the library | from the library | from the library | from the library | ? |
| book-chapter size (nice to have) | n/a | 🟡 snapshot grows with history | ? | from the library | from the library | ? |

Reading the matrix today:

- On napkin numbers (a hypothesis until text-write-cost runs), every
  column that commits per edit through the tree misses the cost row by
  orders of magnitude unless commits batch hard or the storage work lands
  (single-file format, a durable novelty buffer). That is a property of
  commits, not of any one column.
- The columns that keep changes outside the tree avoid it, and the
  "which documents need syncing" row brings back one small fact per
  sync point, not per edit.
- Template is the only column with no core change, and the only one with
  two sync mechanisms.
