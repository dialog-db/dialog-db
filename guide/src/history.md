# Commits and History

A tree root names one state of the facts. To go from one state to the next, an app makes a commit. This chapter follows Bob as he checks the eggs off the list, and looks at everything his commit leaves behind.

Two questions drive the design. When Bob's change reaches Alice, how does her replica know whether it has already seen it? And when Bob asserts that the eggs are done, superseding `done false`, how does Alice know that her own copy of `done false` is the one his change removes, and not a newer one she wrote since? Both answers come from giving every change a version.

## Who is writing

Every commit is written by someone, on some branch, of some replica. Dialog names each of these with a hash of the level above:

<figure class="dg">
<svg class="dg" viewBox="0 0 848 210" width="848" height="210" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="repository and profile hash to a replica; replica and name to a branch; branch and session key to an origin">
<defs><marker id="l-arrow" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto"><path d="M0,0 L8,4 L0,8 z"/></marker></defs>
<rect class="shade" x="10" y="10" width="160" height="40"/>
<text class="label" x="90.0" y="27" text-anchor="middle">repository</text>
<text class="small muted" x="90.0" y="42" text-anchor="middle">did:key:zR…</text>
<rect class="alice" x="10" y="60" width="160" height="40"/>
<text class="label" x="90.0" y="77" text-anchor="middle">Alice's profile</text>
<text class="small muted" x="90.0" y="92" text-anchor="middle">did:key:zA…</text>
<path class="wire" d="M170,30 H190 V62" /><path class="wire" d="M170,80 H190 V78"/>
<rect class="hash solid" x="196" y="57" width="60" height="26"/>
<text class="small" x="226" y="74" text-anchor="middle" style="fill:var(--dg-paper)">hash</text>
<line x1="256" y1="70" x2="278" y2="70" marker-end="url(#l-arrow)"/>
<rect class="alice" x="282" y="50" width="110" height="40"/>
<text class="label" x="337.0" y="67" text-anchor="middle">replica</text>
<text class="small muted" x="337.0" y="82" text-anchor="middle">Alice's copy</text>
<rect class="shade" x="282" y="110" width="110" height="40"/>
<text class="label" x="337.0" y="127" text-anchor="middle">name</text>
<text class="small muted" x="337.0" y="142" text-anchor="middle">"main"</text>
<path class="wire" d="M392,70 H412 V112"/><path class="wire" d="M392,130 H412 V128"/>
<rect class="hash solid" x="418" y="107" width="60" height="26"/>
<text class="small" x="448" y="124" text-anchor="middle" style="fill:var(--dg-paper)">hash</text>
<line x1="478" y1="120" x2="500" y2="120" marker-end="url(#l-arrow)"/>
<rect class="alice" x="504" y="100" width="120" height="40"/>
<text class="label" x="564.0" y="117" text-anchor="middle">branch</text>
<text class="small muted" x="564.0" y="132" text-anchor="middle">main, on Alice's</text>
<rect class="alice" x="504" y="160" width="120" height="40"/>
<text class="label" x="564.0" y="177" text-anchor="middle">session key</text>
<text class="small muted" x="564.0" y="192" text-anchor="middle">did:key:zS…</text>
<path class="wire" d="M624,120 H640 V162"/><path class="wire" d="M624,180 H640 V178"/>
<rect class="hash solid" x="646" y="157" width="60" height="26"/>
<text class="small" x="676" y="174" text-anchor="middle" style="fill:var(--dg-paper)">hash</text>
<line x1="706" y1="170" x2="724" y2="170" marker-end="url(#l-arrow)"/>
<rect class="critical" x="728" y="150" width="110" height="40"/>
<text class="label" x="783.0" y="167" text-anchor="middle">origin</text>
<text class="small muted" x="783.0" y="182" text-anchor="middle">32 bytes</text>
</svg>
</figure>

- A **replica** is one person's copy of a repository: the hash of the repository's DID and the profile's DID.
- A **branch** is a named line of work in a replica, like `main`: the hash of the replica and the name.
- An **origin** is one writer on one branch: the hash of the branch and the key that signs the commits.

An origin writes one commit at a time, in order, never two at once. Everything below depends on that.

## Versions

A version is a pair: an origin, and an edition.

The **edition** counts depth, not writes. A commit's edition is its parent's edition plus one. A merge's edition is the larger of its parents' editions, plus one. So when Bob pulls Alice's two commits and then makes his first, his first commit is already edition 2:

<figure class="dg">
<svg class="dg" viewBox="0 0 640 160" width="640" height="160" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Editions count depth: Bob&#x27;s first commit on top of two of Alice&#x27;s is edition 2; a merge takes the max plus one">
<defs><marker id="e-arrow" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto"><path d="M0,0 L8,4 L0,8 z"/></marker></defs>
<text class="title alice" x="10" y="44">▲ Alice</text>
<text class="title bob" x="10" y="124">● Bob</text>
<line class="alice" x1="80" y1="40" x2="620" y2="40"/>
<line class="bob" x1="80" y1="120" x2="620" y2="120"/>
<circle class="alice solid" cx="110" cy="40" r="9"/>
<text class="small" x="110" y="24" text-anchor="middle">(oA, 0)</text>
<text class="small muted" x="110" y="66" text-anchor="middle">add oat milk</text>
<circle class="alice solid" cx="210" cy="40" r="9"/>
<text class="small" x="210" y="24" text-anchor="middle">(oA, 1)</text>
<text class="small muted" x="210" y="66" text-anchor="middle">add eggs</text>
<line class="dashed" x1="216" y1="48" x2="304" y2="112" marker-end="url(#e-arrow)"/>
<text class="small muted" x="244" y="100" text-anchor="end">Bob pulls</text>
<circle class="bob solid" cx="310" cy="120" r="9"/>
<text class="small" x="310" y="104" text-anchor="middle">(oB, 2)</text>
<text class="small muted" x="310" y="146" text-anchor="middle">eggs done</text>
<circle class="alice solid" cx="360" cy="40" r="9"/>
<text class="small" x="360" y="24" text-anchor="middle">(oA, 2)</text>
<text class="small muted" x="360" y="66" text-anchor="middle">rename milk</text>
<line class="dashed" x1="316" y1="112" x2="474" y2="48" marker-end="url(#e-arrow)"/>
<circle class="alice solid" cx="480" cy="40" r="9"/>
<text class="small" x="480" y="24" text-anchor="middle">(oA, 3)</text>
<text class="small muted" x="496" y="64">merge: max(2, 2) + 1</text>
</svg>
</figure>

Editions give a rough sense of order that every replica agrees on without a clock. Versions compare by edition first, then by origin, so any two versions have a definite order.

## What a commit writes

Bob's commit asserts `done true` on `item:2`. The done flag has one value, so the assertion supersedes `done false`. Every entry the commit writes carries its version, `(oB, 2)`. Here is all of it:

<figure class="dg">
<svg class="dg" viewBox="0 0 680 184" width="680" height="184" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="One commit writes fact keys, a history record, a coverage record and a revision record">
<text class="label small muted" x="10" y="20">written into the tree by one commit</text>
<rect class="shade" x="10" y="30" width="70" height="26"/>
<text x="45" y="47" text-anchor="middle">00 01 02</text>
<rect class="value" x="84" y="30" width="400" height="26"/>
<text class="small" x="94" y="47">demo.grocery/done of item:2 is true</text>
<text class="small muted" x="496" y="47">version (oB, 2)</text>
<rect class="shade" x="10" y="62" width="70" height="26"/>
<text x="45" y="79" text-anchor="middle">03</text>
<rect class="hash" x="84" y="62" width="400" height="26"/>
<text class="small" x="94" y="79">history: (oB, 2) item:2 demo.grocery/done true</text>
<text class="small muted" x="496" y="79">supersedes (oA, 1)</text>
<rect class="shade" x="10" y="94" width="70" height="26"/>
<text x="45" y="111" text-anchor="middle">05</text>
<rect class="hash" x="84" y="94" width="400" height="26"/>
<text class="small" x="94" y="111">coverage: (oB, 2) item:2 demo.grocery/done …</text>
<text class="small muted" x="496" y="111">it covered something</text>
<rect class="shade" x="10" y="126" width="70" height="26"/>
<text x="45" y="143" text-anchor="middle">00 01</text>
<rect class="hash" x="84" y="126" width="400" height="26"/>
<text class="small" x="94" y="143">dialog.db/revision of the version's entity</text>
<text class="small muted" x="496" y="143">signed record</text>
<text class="label small muted" x="10" y="172">then, outside the tree, the branch head cell moves to the new root</text>
</svg>
</figure>

**The fact keys.** The new fact goes into EAV, AEV and VAE, as in the [Keys](./keys.md) chapter. Its payload records the version that wrote it. The old fact, `done false`, is removed from all three.

**A history record.** Under tag `03`, the commit writes one record for each fact it changed. The record's key starts with the origin and then the edition, so each writer's records sit together, in edition order. Its payload lists the versions this change supersedes. Bob's record says *"at (oB, 2), item 2's done became true, superseding what (oA, 1) wrote."* A retraction writes a record too, marked as a retraction.

**A coverage record.** A record that removed something, a retraction, or an assertion that superseded at least one earlier version, is copied under tag `05`, keyed by version and by the hash of the value rather than the value itself. That keeps the question *"did this version remove anything?"* cheap to answer during a merge.

**A revision record.** Last, the commit writes one fact about itself: attribute `dialog.db/revision`, on an entity derived from the version. Its value is a signed record naming the commit's parent versions, its issuer, and links further back for fast ancestry walks. So the tree also holds the commit graph.

The fact indexes answer *what is true now*. The history region answers *what happened*. Both live in the same tree, under the same root.

## The head

The commit then seals the tree, writes the new blocks (see [Blocks and Storage](./storage.md)), and moves the branch's **head**. The head is a small signed record in a cell named `branch/main/revision`. It holds:

- the branch,
- the issuer, the key that signed it,
- the tree root,
- the edition,
- a watermark, described below,
- the signature over all of the above.

These are the bytes the signature covers:

<figure class="dg">
<svg class="dg" viewBox="0 0 652 104" width="652" height="104" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="The bytes a head&#x27;s signature covers">
<rect class="shade" x="10" y="10" width="140" height="30"/>
<text class="small" x="80.0" y="30" text-anchor="middle">"dialog/head@1\n"</text>
<path class="wire" d="M11,46 v4 H149 v-4"/>
<text class="label small muted" x="80.0" y="66" text-anchor="middle">14 bytes</text>
<rect class="entity" x="152" y="10" width="110" height="30"/>
<text class="small" x="207.0" y="30" text-anchor="middle">branch</text>
<path class="wire" d="M153,46 v4 H261 v-4"/>
<text class="label small muted" x="207.0" y="66" text-anchor="middle">length + bytes</text>
<rect class="alice" x="264" y="10" width="110" height="30"/>
<text class="small" x="319.0" y="30" text-anchor="middle">issuer</text>
<path class="wire" d="M265,46 v4 H373 v-4"/>
<text class="label small muted" x="319.0" y="66" text-anchor="middle">length + bytes</text>
<rect class="hash" x="376" y="10" width="90" height="30"/>
<text class="small" x="421.0" y="30" text-anchor="middle">tree</text>
<path class="wire" d="M377,46 v4 H465 v-4"/>
<text class="label small muted" x="421.0" y="66" text-anchor="middle">32 bytes</text>
<rect class="shade" x="468" y="10" width="70" height="30"/>
<text class="small" x="503.0" y="30" text-anchor="middle">edition</text>
<path class="wire" d="M469,46 v4 H537 v-4"/>
<text class="label small muted" x="503.0" y="66" text-anchor="middle">8 bytes</text>
<rect class="shade" x="540" y="10" width="100" height="30"/>
<text class="small" x="590.0" y="30" text-anchor="middle">context</text>
<path class="wire" d="M541,46 v4 H639 v-4"/>
<text class="label small muted" x="590.0" y="66" text-anchor="middle">optional</text>
<text class="label small muted" x="321.0" y="92" text-anchor="middle">the issuer signs these bytes; the signature travels with the head</text>
</svg>
</figure>

The head is updated with compare-and-swap. Bob's replica says *"move the head from the revision I built on to this new one."* If another commit moved it first, the swap fails with a version mismatch and nothing is lost: Bob's blocks are stored, and the app can rebuild on the new head. A commit made with `merge()` does that by itself: it merges with the winner and tries again.

## The watermark

The watermark says, for every origin, how far into that origin's commits this head has seen. It is a map from origin to two numbers: the highest edition seen, and how many of the origin's commits that covers.

| origin | highest edition | commits seen |
|---|---|---|
| `oA` | 3 | 4 |
| `oB` | 2 | 1 |

Because an origin writes one commit at a time, "everything up to edition 3" is an exact description of what the head has seen from `oA`. So the question *"has this head seen version v?"* is one lookup: v is seen when its edition is at most the watermark's edition for its origin.

That one question answers both questions from the start of the chapter. When a change arrives that the receiver has already seen, it can be skipped. And when a fact arrives from another replica that the receiver has seen *and* no longer holds, the receiver knows it was removed on purpose, not missing, and does not bring it back. Deleted facts need no tombstone in the fact indexes. The [Sync](./sync.md) chapter shows this in action.

<div class="aside">

**Implementations.** Versions, editions, origins and watermarks are in [`dialog-capability/src/history`](https://github.com/dialog-db/dialog-db/tree/main/rust/dialog-capability/src/history). The head is `Revision` in [`dialog-capability/src/identity/revision.rs`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-capability/src/identity/revision.rs). History and revision records are in [`dialog-artifacts/src/history`](https://github.com/dialog-db/dialog-db/tree/main/rust/dialog-artifacts/src/history). The commit itself is [`dialog-repository/src/repository/branch/commit.rs`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-repository/src/repository/branch/commit.rs). The design is written up in [`notes/version-control.md`](https://github.com/dialog-db/dialog-db/blob/main/notes/version-control.md).

</div>

<div class="aside">

**Transactions.** An app can also stage several commits and publish them with a single swap of the head, so other readers see all of them or none. That is `Transaction` in [`branch/transaction.rs`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-repository/src/repository/branch/transaction.rs), with the staging and the single swap in [`transaction/batch.rs`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-repository/src/repository/branch/transaction/batch.rs).

</div>
