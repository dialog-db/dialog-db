# How Dialog Works

Dialog is an embeddable database for local-first software. Each participant, usually one per device, keeps its own working copy of a repository, called a replica. An app reads and writes its replica directly, with no server in the way. When two replicas can both reach a storage service they share, they exchange what changed through it and end up agreeing.

This guide follows one piece of data through the whole system. It starts when an app writes a fact, follows that fact into a sorted tree of keys and down to the bytes on disk, and then carries it to another device, where it is merged and queried. Each chapter picks up where the previous one left off.

The guide is written for two kinds of readers. If you want to build something that talks to Dialog, or write a second implementation, you should find enough detail here to get started. If you are just curious how a local-first database can work, you can skip the byte tables and still follow the story.

## Alice and Bob's grocery list

Alice and Bob share a grocery list. Alice edits it on her laptop. Bob edits it on his phone. Sometimes they are both offline, and sometimes they edit the same item at the same time.

<figure class="dg">
<svg class="dg" viewBox="0 0 640 210" width="640" height="210" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Alice's laptop and Bob's phone each hold a replica and sync through a shared remote">
<defs><marker id="intro-arrow" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="8" markerHeight="8" orient="auto-start-reverse"><path d="M0,0 L8,4 L0,8 z"/></marker></defs>
<rect class="alice" x="20" y="40" width="170" height="130" rx="6"/>
<text class="title alice" x="105" y="64" text-anchor="middle">Alice's laptop</text>
<rect x="45" y="80" width="120" height="70"/>
<text class="label" x="105" y="104" text-anchor="middle">replica</text>
<text class="small muted" x="105" y="124" text-anchor="middle">facts, tree,</text>
<text class="small muted" x="105" y="138" text-anchor="middle">history</text>
<rect class="bob" x="450" y="40" width="170" height="130" rx="6"/>
<text class="title bob" x="535" y="64" text-anchor="middle">Bob's phone</text>
<rect x="475" y="80" width="120" height="70"/>
<text class="label" x="535" y="104" text-anchor="middle">replica</text>
<text class="small muted" x="535" y="124" text-anchor="middle">facts, tree,</text>
<text class="small muted" x="535" y="138" text-anchor="middle">history</text>
<rect class="hash" x="255" y="70" width="130" height="70" rx="6"/>
<text class="title" x="320" y="98" text-anchor="middle">remote</text>
<text class="small muted" x="320" y="118" text-anchor="middle">blocks + a head</text>
<line x1="192" y1="95" x2="252" y2="95" marker-end="url(#intro-arrow)"/>
<line x1="252" y1="115" x2="192" y2="115" marker-end="url(#intro-arrow)"/>
<line x1="448" y1="95" x2="388" y2="95" marker-end="url(#intro-arrow)"/>
<line x1="388" y1="115" x2="448" y2="115" marker-end="url(#intro-arrow)"/>
<text class="small muted" x="222" y="88" text-anchor="middle">push</text>
<text class="small muted" x="222" y="132" text-anchor="middle">pull</text>
<text class="small muted" x="418" y="88" text-anchor="middle">push</text>
<text class="small muted" x="418" y="132" text-anchor="middle">pull</text>
<text class="small muted" x="320" y="195" text-anchor="middle">The remote stores bytes. It never needs to understand them.</text>
</svg>
</figure>

The same list appears in every chapter, so the same facts, keys and hashes turn up again and again. By the end you will have seen one grocery item as a sentence, as three keys, as a leaf in a tree, as bytes in a block, as an entry in a signed history, and as a row in a query result.

## What the guide covers

1. [Facts](./facts.md): the one shape all data takes.
2. [Keys](./keys.md): how a fact becomes sortable bytes, three times over.
3. [The Search Tree](./tree.md): how sorted keys are cut into nodes so that equal data gives equal trees.
4. [Nodes](./nodes.md): the bytes of a single node, and how a node says how to read itself.
5. [Blocks and Storage](./storage.md): content addressing, and where blocks live.
6. [Commits and History](./history.md): versions, the log inside the tree, and signed heads.
7. [Identity and Capabilities](./identity.md): who may read and write, and how a remote checks.
8. [Sync](./sync.md): how Alice's and Bob's replicas converge.
9. [Queries](./queries.md): how an app asks questions of all this.

<div class="aside">

**How to read the diagrams.** Colors mean the same thing in every diagram:

- <span class="chip entity">entity</span> the thing a fact is about
- <span class="chip attribute">attribute</span> what is said about it
- <span class="chip value">value</span> what is said
- <span class="chip hash">hash</span> content addresses, signatures and other machinery
- <span class="chip critical">critical</span> something a reader must understand, or refuse
- <span class="chip alice">Alice</span> and <span class="chip bob">Bob</span> their replicas and everything they write

The palette is the muted Bauhaus palette of tonk's Dialog diagnose view, which pairs each color with a shape: blue with the circle, yellow with the triangle, red with the square. Alice and Bob borrow two of those shapes as their marks.

Bytes are drawn as boxes holding two hexadecimal digits. Where a byte is a printable character, the character is shown and its hex value sits beneath it. A bracket under a run of bytes names the field those bytes make up.

</div>

<div class="aside">

**Status.** Dialog is experimental. The formats described here, including the node layout, are drafts and may change before a stable release. Where the guide describes something that is still moving, it says so. The implementation lives in the [dialog-db repository](https://github.com/dialog-db/dialog-db), and each chapter links to the code it describes.

</div>
