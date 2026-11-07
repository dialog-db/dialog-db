# Direction: collaborative text editing

Decision: how Dialog supports collaborative text, for: several people
edit the same text, peer to peer and offline, converging without lost
edits; today last writer wins on a text field loses one side.

Columns so far are the status quo and the earlier spikes. Not filled
beyond what was said; `?` is open.

| criterion \ approach | Status quo: text fields, last writer wins | CRDT doc as value: a text CRDT document is an opaque value; two values on one entity and attribute merge into a new one | Key plus side lane: Dialog holds an opaque key; the document lives and syncs beside Dialog | Built in: Dialog merges text itself |
|---|---|---|---|---|
| concurrent edits converge, none lost | 🔴 one edit per field is lost | merges on the outer side | the side lane merges | ? |
| works peer to peer, offline, no server | yes | yes | ? depends on the side lane | ? |
| write amplification per edit | spine cost per commit | 🟡 spine cost plus a copy of the whole document per edit; maintainer judged it not viable, unmeasured | ? spine cost only on key changes; side lane cost unknown | ? spine cost per edit at least |
| engineering, and how much is outside Dialog's domain | none | ? | 🟡 a lot, mostly outside Dialog's domain (maintainer) | ? |
| needs the whole document on a replica | no | yes, the value is the document | ? | ? |
| text queryable with other facts (nice to have) | yes, as whole field strings | no, opaque | no, opaque | ? |
| rich text (nice to have) | no | ? from the library | ? from the library | ? |
| book-chapter size (nice to have) | ? | 🟡 copy grows with size | ? | ? |
