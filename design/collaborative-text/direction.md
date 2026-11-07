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
| concurrent edits converge, none lost | 🔴 one edit per field is lost | merge on the outer side | the library merges | the library merges | the library merges | ? |
| peer to peer, offline, no server | yes | yes | yes | ? does "changes since" need a remote that understands the CRDT | 🟡 depends on the library's sync | ? |
| storage and sync within 2× of a text CRDT library | ? | ? copies the whole document per change, plus a root node per commit | ? a root node per commit | ? | the library's own cost | ? |
| changes to Dialog's core | none | 🟡 a lot; part of why it was shelved | ? | ? a resource kind and sync primitives | none | 🟡 likely the most |
| moving parts: sync engines and stores beside the replicated tree | one engine, the tree | one engine, the tree | one engine, the tree | 🟡 one engine, a store beside the tree | 🟡 two engines, two stores | one engine, the tree |
| needs the whole document on the editing peer | no | yes | ? | ? | yes | ? |
| a query returns the current text (nice to have) | yes | ? | yes | yes | no, opaque key | ? |
| no interleaving of concurrent runs (nice to have) | n/a | from the library | from the library | from the library | from the library | ? whole document per peer allows RGA or Fugue style order |
| library agnostic (nice to have) | n/a | ? | ? | 🟡 deeper integration with one library | each library a template | n/a |
| rich text (nice to have) | no | from the library | from the library | from the library | from the library | ? |
| book-chapter size (nice to have) | ? | 🟡 copy grows with size | ? | ? | from the library | ? |
