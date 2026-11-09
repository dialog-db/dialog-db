# Survey table

Filled from primary sources, 2026-11-06. `?` is unknown, with what would
settle it. Cost numbers are deliberately absent: they come from
[text write cost](../text-write-cost.md) on one trace, not from each
project's own benchmarks.

| | changes since a version | what the other side must understand | no server | interleaving | Rust and Wasm |
|---|---|---|---|---|---|
| **Yjs** | `encodeStateAsUpdate(doc, stateVector)` writes only the missing part; `diffUpdate(update, stateVector)` and `mergeUpdates` work on update bytes without loading a document [1] | a relay that only forwards bytes needs nothing; one that diffs or merges stored updates runs Yjs's update functions [1] | yes | only in a limited multi-replica backward-insertion case [6] | via Yrs |
| **Yrs** | Yjs update formats v1 and v2, state vectors, diffs, merges [2] | same as Yjs | yes | same algorithm as Yjs | Rust; `ywasm` for Wasm [2]; still reaching feature parity with Yjs [2] |
| **Automerge** | changes after a set of heads; each change serializes to bytes on its own [3]; automerge-repo stores each change keyed by its hash and compacts into snapshots keyed by heads [15] | the sync protocol runs Automerge on both ends [3]; a change's dependencies sit before its operations and its hash is computed from its bytes, so a store can serve changes since heads without decoding them [14]; Sedimentree does this for content-blind servers [16] | yes | RGA family, which the Fugue paper finds interleaving [6] | Rust core with Wasm (? cite) |
| **Loro** | `export({mode: "update", from: versionVector})`; import holds back changes whose prerequisites are missing [4] | ? not stated; the Loro Protocol server hooks receive the CRDT, and relays end-to-end encrypted records without decrypting them (experimental) [17] | implied: updates broadcast directly to peers [4] | Fugue based, avoids interleaving [5] | Rust, JS via Wasm, Swift [5] |
| **Diamond Types** | ? not described in the README; BINARY.md and INTERNALS.md would say [13] | ? | yes, P2P without a central server [13]; simple peers can also send positional (OT-style) updates [13] | ? not described | Rust; Wasm via `diamond-types-node` and `diamond-types-web` [13]. Work in progress; plain text only; crates.io release out of date [13] |
| **Eg-walker** (paper) | events carry an id and parent ids; merging is a union of event sets; a peer applies an event once its parents arrive [8] | to apply: the algorithm. To store and relay: ids and parents only [8] | yes [7] | ? | Diamond Types (Rust), a TypeScript reference [7] |
| **Fugue** (paper) | n/a, an ordering algorithm | n/a | yes | Fugue: forward non-interleaving; FugueMax: maximally non-interleaving, proven [6] | TypeScript in Collabs [6]; Loro uses a Fugue-based algorithm [5] |
| **Peritext** | n/a, rich text marks over a list CRDT | n/a | yes | inherits the list CRDT's | prototype in TypeScript; not in Automerge per its README [9]; ? Automerge's own marks |
| **json-joy** | JSON CRDT patches with a compact binary codec [10]; ? since a version | ? | ? | ? | TypeScript |
| **DeltaDB** (Zed) | an edit is `(parent version, offset, text)`; a version is a frontier of delta ids, not a version vector; sync sends logarithmically spaced versions to find a common ancestor, then the missing deltas [11][18] | to apply: the CRDT, which reconstructs the parent version and maps offset to a fragment [18]; the wire format is decoupled from the CRDT's internals [18] | 🟡 a server per thread (one Durable Object each) stores and fans out deltas [18] | concurrent inserts at one spot ordered by actor id [11]; Eg-walker lineage, loosely based on Diamond Types [11] | Rust; state is copy-on-write B+ trees (a SumTree variant) in a SQLite nodes table, refcounted [18] |

## Sources

1. [Yjs: document updates](https://docs.yjs.dev/api/document-updates)
2. [yrs on docs.rs](https://docs.rs/crate/yrs/latest)
3. [Automerge sync protocol (R bindings article)](https://posit-dev.github.io/automerge-r/articles/sync-protocol.html)
4. [Loro: sync tutorial](https://www.loro.dev/docs/tutorial/sync)
5. [Loro 1.0](https://loro.dev/blog/v1.0)
6. [The Art of the Fugue](https://arxiv.org/abs/2305.00583)
7. [Kleppmann on Eg-walker](https://martin.kleppmann.com/2025/03/30/eg-walker-collaborative-text.html)
8. [Eg-walker paper](https://arxiv.org/abs/2409.14252), section 3
9. [Peritext repository](https://github.com/inkandswitch/peritext)
10. [json-joy JSON CRDT patch codec](https://streamich.github.io/json-joy/types/json-crdt-patch_codec_compact.CompactCodecNewStrOperation.html)
11. [Introducing DeltaDB](https://zed.dev/blog/introducing-deltadb)
12. [Editing traces: concurrent](https://raw.githubusercontent.com/josephg/editing-traces/master/concurrent_traces/README.md)
13. [Diamond Types repository](https://github.com/josephg/diamond-types)
14. [Automerge binary format spec](https://automerge.org/automerge-binary-format-spec/)
15. [Automerge storage](https://automerge.org/docs/reference/under-the-hood/storage/)
16. [Sedimentree](https://github.com/automerge/beelay/blob/main/docs/sedimentree.md)
17. [Loro Protocol](https://loro.dev/blog/loro-protocol)
18. [Fine-grained version control for agents](https://delta.dev/blog/fine-grained-version-control-for-agents); [The future of version control](https://delta.dev/blog/future-of-version-control)
