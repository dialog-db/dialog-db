# Sync

Alice and Bob each have a replica. Now they need to agree. Dialog does this through a remote: a store of blocks and cells that both can reach, such as an S3 bucket, a Cloudflare R2 bucket behind a UCAN service, or a shared folder. The remote stores bytes and swaps cells. It does not understand facts, trees or merges. All of that happens on the replicas.

A branch that syncs with a remote remembers one extra thing per remote: its **sync base**, the tree the remote's head pointed at the last time the two agreed.

## A whole conversation

Here is everything that happens between Alice's laptop, the remote, and Bob's phone, from Alice's first push to the moment both replicas hold the same root. Each message is written as what it says:

<figure class="dg">
<svg class="dg" viewBox="0 0 800 768" width="800" height="768" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Alice pushes, Bob pulls, both edit, Bob pushes first, Alice&#x27;s push is refused, she pulls and merges, pushes, and Bob adopts her merge">
<defs><marker id="q-arrow" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto"><path d="M0,0 L8,4 L0,8 z"/></marker></defs>
<text class="title alice" x="110" y="22" text-anchor="middle">▲ Alice</text>
<line class="dashed" x1="110" y1="32" x2="110" y2="762"/>
<text class="title hash" x="400" y="22" text-anchor="middle">remote</text>
<line class="dashed" x1="400" y1="32" x2="400" y2="762"/>
<text class="title bob" x="690" y="22" text-anchor="middle">● Bob</text>
<line class="dashed" x1="690" y1="32" x2="690" y2="762"/>
<rect class="alice" x="6" y="46" width="290.7" height="22" rx="3"/>
<text class="small" x="151.35" y="61" text-anchor="middle">commits (oA, 0), (oA, 1): milk, then eggs</text>
<line class="" x1="114" y1="92" x2="392" y2="92" marker-end="url(#q-arrow)"/>
<text class="small" x="255.0" y="86" text-anchor="middle">What is the head of main?</text>
<line class="" x1="396" y1="122" x2="118" y2="122" marker-end="url(#q-arrow)"/>
<text class="small" x="255.0" y="116" text-anchor="middle">There is none yet.</text>
<line class="" x1="114" y1="152" x2="392" y2="152" marker-end="url(#q-arrow)"/>
<text class="small" x="255.0" y="146" text-anchor="middle">Store these blocks.  (children first)</text>
<line class="" x1="114" y1="182" x2="392" y2="182" marker-end="url(#q-arrow)"/>
<text class="small" x="255.0" y="176" text-anchor="middle">Set the head to mine, if there still is none.</text>
<rect x="299.0" y="201" width="202" height="18" style="stroke:none"/>
<text class="label small muted" x="400" y="214" text-anchor="middle" font-style="italic">Bob opens the list on his phone</text>
<line class="" x1="686" y1="240" x2="408" y2="240" marker-end="url(#q-arrow)"/>
<text class="small" x="545.0" y="234" text-anchor="middle">What is the head of main?</text>
<line class="" x1="404" y1="270" x2="682" y2="270" marker-end="url(#q-arrow)"/>
<text class="small" x="545.0" y="264" text-anchor="middle">Alice&#x27;s head, root 9c1e…</text>
<rect class="bob" x="496.59999999999997" y="286" width="297.40000000000003" height="22" rx="3"/>
<text class="small" x="645.3" y="301" text-anchor="middle">verify the signature; adopt the root as is</text>
<rect x="338.0" y="321" width="124" height="18" style="stroke:none"/>
<text class="label small muted" x="400" y="334" text-anchor="middle" font-style="italic">Both edit, offline</text>
<rect class="alice" x="6" y="346" width="277.3" height="22" rx="3"/>
<text class="small" x="144.65" y="361" text-anchor="middle">commit (oA, 2): milk is &quot;Oat milk, 1 L&quot;</text>
<rect class="bob" x="476.5" y="378" width="317.5" height="22" rx="3"/>
<text class="small" x="635.25" y="393" text-anchor="middle">commit (oB, 2): eggs done, milk is &quot;Soy milk&quot;</text>
<line class="" x1="686" y1="424" x2="408" y2="424" marker-end="url(#q-arrow)"/>
<text class="small" x="545.0" y="418" text-anchor="middle">Store these blocks.</text>
<line class="" x1="686" y1="454" x2="408" y2="454" marker-end="url(#q-arrow)"/>
<text class="small" x="545.0" y="448" text-anchor="middle">Set the head to mine, if it is still Alice&#x27;s.</text>
<line class="" x1="114" y1="484" x2="392" y2="484" marker-end="url(#q-arrow)"/>
<text class="small" x="255.0" y="478" text-anchor="middle">Set the head to mine, if it is still mine.</text>
<line class="critical" x1="396" y1="514" x2="118" y2="514" marker-end="url(#q-arrow)"/>
<text class="small" x="255.0" y="508" text-anchor="middle">No: it has moved.</text>
<line class="" x1="114" y1="544" x2="392" y2="544" marker-end="url(#q-arrow)"/>
<text class="small" x="255.0" y="538" text-anchor="middle">What is the head of main?</text>
<line class="" x1="396" y1="574" x2="118" y2="574" marker-end="url(#q-arrow)"/>
<text class="small" x="255.0" y="568" text-anchor="middle">Bob&#x27;s head.</text>
<rect class="alice" x="6" y="590" width="384.5" height="22" rx="3"/>
<text class="small" x="198.25" y="605" text-anchor="middle">merge: replay my change onto Bob&#x27;s tree, commit (oA, 3)</text>
<line class="" x1="114" y1="636" x2="392" y2="636" marker-end="url(#q-arrow)"/>
<text class="small" x="255.0" y="630" text-anchor="middle">Store these blocks, then set the head to mine.</text>
<line class="" x1="686" y1="666" x2="408" y2="666" marker-end="url(#q-arrow)"/>
<text class="small" x="545.0" y="660" text-anchor="middle">What is the head of main?</text>
<line class="" x1="404" y1="696" x2="682" y2="696" marker-end="url(#q-arrow)"/>
<text class="small" x="545.0" y="690" text-anchor="middle">Alice&#x27;s merge.</text>
<rect class="bob" x="476.5" y="712" width="317.5" height="22" rx="3"/>
<text class="small" x="635.25" y="727" text-anchor="middle">it includes everything I have: adopt the root</text>
</svg>
</figure>

The rest of the chapter walks through it.

## Push

A push moves the remote's head forward to the local one, and only forward.

1. **Check.** Read the remote's head. If it is not the sync base, someone else has pushed since this replica last looked, and the push stops with a refusal: *not a fast-forward*. The replica must pull first.
2. **Find what is new.** Compare the local tree with the sync base, skipping every subtree whose hash matches, as described in [The Search Tree](./tree.md). What is left are the blocks the remote does not have.
3. **Upload.** Send spilled values and blobs, then the new tree nodes, children before parents. A node never arrives before the nodes it points at, so the remote never holds a node with a dangling link.
4. **Swap the head.** Ask the remote to set its head cell to the new signed head, *if it still holds what step 1 read*. If another push slipped in between, the swap fails and nothing points at the uploaded blocks.
5. **Record.** The new tree becomes the sync base.

On S3 the swap in step 4 is a conditional `PUT` with `If-Match` on the cell's ETag, or `If-None-Match: *` when there is no head yet.

## Pull

A pull brings the remote's changes in. It reads the remote's head, checks the head's signature, and then looks at two watermarks: the one in the remote's head, and the one in the local head. Each watermark says what its side has seen (see [Commits and History](./history.md)). Comparing them tells the replica which case it is in:

| Case | What the replica says | What it does |
|---|---|---|
| The local head has seen everything the remote has | *"Nothing new here."* | keeps its head, updates the sync base |
| The remote has seen everything local, and nothing changed locally since the last sync | *"I'll take yours."* | adopts the remote root as is, reading no blocks at all |
| Both sides have changes the other has not seen | *"We need to merge."* | merges, then makes a merge commit |

When Bob pulls for the first time, he has no local changes, so he takes Alice's root as it is. He reads no nodes to do it. The nodes arrive later, on demand, as his queries walk through them ([Blocks and Storage](./storage.md)).

## Merge

When Alice's push is refused, she pulls and finds that both sides have changes. Her replica has to combine them.

Dialog picks the cheaper direction. It takes the side with fewer changes since the two last agreed, and replays those changes onto the other side's tree. Here Alice made one commit and Bob made one, so she replays hers onto Bob's tree. When both sides have more than 8 commits the other has not seen, and they share a sync base, Dialog instead grafts whole subtrees from one tree into the other, which costs less than replaying every change.

Replaying is not blind copying. Unless the two replicas have never seen a single commit from each other's origins, incoming changes are screened against what the receiving side has seen:

- **A fact the receiver has already seen, and no longer holds, stays gone.** If Bob had retracted a fact and Alice's replica still held it, her copy's version would fall inside Bob's watermark: Bob saw it, and removed it. It is not brought back. This is how deletes survive a merge without tombstones.
- **A removal only removes an entry that matches it exactly, down to the version that wrote it.** Alice's change to the milk's name removes the name `"Oat milk"` written at `(oA, 0)`. If the receiver holds a copy of a fact written by some other version, the removal leaves that copy alone.
- **A record that superseded something retires what it superseded.** Bob's history record says his `done true` superseded `(oA, 1)`'s `done false`. After the merge, `done false` is gone on both sides, even on a replica that still held it.

History records are merged before facts, so a removal always lands before an incoming fact could contest the slot it clears.

If the merged tree equals one of the two sides, that side's head is used as it is. Otherwise the replica makes a merge commit: its parents are both heads, its edition is one more than the larger of theirs, and its watermark covers both. Alice's merge is `(oA, 3)`.

## When both change the same thing

Alice renamed the milk to `"Oat milk, 1 L"`. Bob, without seeing that, renamed it to `"Soy milk"`. A name has one value, so each assertion superseded only the `"Oat milk"` its writer had seen. Neither saw the other's value, so the merge keeps both:

<figure class="dg">
<svg class="dg" viewBox="0 0 780 118" width="780" height="118" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Two concurrent names for the same item; a read elects one, the same way on every replica">
<defs><marker id="k-arrow" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto"><path d="M0,0 L8,4 L0,8 z"/></marker></defs>
<text class="label small muted" x="10" y="16">in the merged tree: two names for item:1</text>
<rect class="attribute" x="10" y="28" width="170" height="24"/><text class="attribute" x="95" y="44" text-anchor="middle">example.grocery/name</text>
<rect class="entity" x="184" y="28" width="70" height="24"/><text class="entity" x="219" y="44" text-anchor="middle">item:1</text>
<rect class="value" x="258" y="28" width="140" height="24"/><text class="value" x="328" y="44" text-anchor="middle">&quot;Oat milk, 1 L&quot;</text>
<rect class="alice" x="402" y="28" width="70" height="24"/><text class="alice" x="437" y="44" text-anchor="middle">(oA, 2)</text>
<rect class="attribute" x="10" y="58" width="170" height="24"/><text class="attribute" x="95" y="74" text-anchor="middle">example.grocery/name</text>
<rect class="entity" x="184" y="58" width="70" height="24"/><text class="entity" x="219" y="74" text-anchor="middle">item:1</text>
<rect class="value" x="258" y="58" width="140" height="24"/><text class="value" x="328" y="74" text-anchor="middle">&quot;Soy milk&quot;</text>
<rect class="bob" x="402" y="58" width="70" height="24"/><text class="bob" x="437" y="74" text-anchor="middle">(oB, 2)</text>
<line x1="476" y1="54" x2="520" y2="54" marker-end="url(#k-arrow)"/>
<rect class="hash solid" x="524" y="40" width="80" height="28"/>
<text class="small" x="564" y="58" text-anchor="middle" style="fill:var(--dg-paper)">elect</text>
<line x1="604" y1="54" x2="634" y2="54" marker-end="url(#k-arrow)"/>
<text class="label small" x="640" y="50">one name,</text>
<text class="label small" x="640" y="64">the same everywhere</text>
<text class="label small muted" x="10" y="106">Deeper edition wins; a tie goes to the version hash. Both facts stay stored.</text>
</svg>
</figure>

A name is meant to have one value, so a reader that asks for it with cardinality one sees an election between the two. The fact whose commit had seen more wins: the deeper edition. If the editions are equal, as they are here, the hash of the version decides. Every fact a commit wrote carries the same version, so a whole commit wins or loses together: a reader never sees half of Alice's edit and half of Bob's.

The election is a function of the stored facts alone, so every replica elects the same name without talking to anyone. The losing value is not deleted. It stays in the store until someone asserts a new name, and an app that wants to show "edited on two devices" can ask for every value.

## After the merge

Alice pushes her merge. This time the remote's head is Bob's, which is her new sync base, so the push goes through. Bob pulls, sees that Alice's merge has seen everything he has, and adopts its root without reading a single node. The two replicas now hold the same root, and they agree on every fact.

<div class="aside">

**Implementations.** Push is [`branch/push.rs`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-repository/src/repository/branch/push.rs) and pull is [`branch/pull.rs`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-repository/src/repository/branch/pull.rs), both in `dialog-repository`. The screens are documented and implemented in [`dialog-artifacts/src/merge.rs`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-artifacts/src/merge.rs). The election is `ArtifactView::elect` in [`artifacts/artifact.rs`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-artifacts/src/artifacts/artifact.rs). The remotes are [`dialog-remote-s3`](https://github.com/dialog-db/dialog-db/tree/main/rust/dialog-remote-s3), [`dialog-remote-ucan`](https://github.com/dialog-db/dialog-db/tree/main/rust/dialog-remote-ucan) and [`dialog-remote-fs`](https://github.com/dialog-db/dialog-db/tree/main/rust/dialog-remote-fs).

</div>
