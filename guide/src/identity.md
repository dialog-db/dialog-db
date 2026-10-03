# Identity and Capabilities

So far the grocery list has lived on one device. Before Alice's commits can reach Bob, they have to be stored somewhere both can reach, and that somewhere has to know who is allowed to write. Dialog has no user accounts on a server and no passwords. Everyone is a key, and permission is something one key signs over to another.

## Everyone is a key

Every key Dialog creates is an Ed25519 key pair, named by its public key written as a [`did:key`](https://w3c-ccg.github.io/did-method-key/). The name is built from bytes like this:

<figure class="dg">
<svg class="dg" viewBox="0 0 490 148" width="490" height="148" xmlns="http://www.w3.org/2000/svg" role="img">
<rect class="shade" x="8" y="6" width="26" height="26"/>
<text x="21.0" y="24.0" text-anchor="middle">ed</text>
<rect class="shade" x="36" y="6" width="26" height="26"/>
<text x="49.0" y="24.0" text-anchor="middle">01</text>
<rect class="alice" x="64" y="6" width="26" height="26"/>
<text x="77.0" y="24.0" text-anchor="middle">25</text>
<rect class="alice" x="92" y="6" width="26" height="26"/>
<text x="105.0" y="24.0" text-anchor="middle">83</text>
<rect class="alice" x="120" y="6" width="26" height="26"/>
<text x="133.0" y="24.0" text-anchor="middle">90</text>
<rect class="alice" x="148" y="6" width="26" height="26"/>
<text x="161.0" y="24.0" text-anchor="middle">8e</text>
<rect class="alice" x="176" y="6" width="26" height="26"/>
<text x="189.0" y="24.0" text-anchor="middle">98</text>
<rect class="alice" x="204" y="6" width="26" height="26"/>
<text x="217.0" y="24.0" text-anchor="middle">b6</text>
<rect class="alice" x="232" y="6" width="26" height="26"/>
<text x="245.0" y="24.0" text-anchor="middle">16</text>
<rect class="alice" x="260" y="6" width="26" height="26"/>
<text x="273.0" y="24.0" text-anchor="middle">27</text>
<rect class="alice" x="288" y="6" width="26" height="26"/>
<text x="301.0" y="24.0" text-anchor="middle">f0</text>
<rect class="alice" x="316" y="6" width="26" height="26"/>
<text x="329.0" y="24.0" text-anchor="middle">e5</text>
<rect class="alice" x="344" y="6" width="26" height="26"/>
<text x="357.0" y="24.0" text-anchor="middle">d1</text>
<rect class="alice" x="372" y="6" width="26" height="26"/>
<text x="385.0" y="24.0" text-anchor="middle">a1</text>
<rect class="alice" x="400" y="6" width="26" height="26"/>
<text x="413.0" y="24.0" text-anchor="middle">85</text>
<rect class="alice" x="428" y="6" width="26" height="26"/>
<text x="441.0" y="24.0" text-anchor="middle">49</text>
<rect class="alice" x="456" y="6" width="26" height="26"/>
<text x="469.0" y="24.0" text-anchor="middle">b4</text>
<rect class="alice" x="8" y="77" width="26" height="26"/>
<text x="21.0" y="95.0" text-anchor="middle">91</text>
<rect class="alice" x="36" y="77" width="26" height="26"/>
<text x="49.0" y="95.0" text-anchor="middle">69</text>
<rect class="alice" x="64" y="77" width="26" height="26"/>
<text x="77.0" y="95.0" text-anchor="middle">ea</text>
<rect class="alice" x="92" y="77" width="26" height="26"/>
<text x="105.0" y="95.0" text-anchor="middle">94</text>
<rect class="alice" x="120" y="77" width="26" height="26"/>
<text x="133.0" y="95.0" text-anchor="middle">fd</text>
<rect class="alice" x="148" y="77" width="26" height="26"/>
<text x="161.0" y="95.0" text-anchor="middle">40</text>
<rect class="alice" x="176" y="77" width="26" height="26"/>
<text x="189.0" y="95.0" text-anchor="middle">af</text>
<rect class="alice" x="204" y="77" width="26" height="26"/>
<text x="217.0" y="95.0" text-anchor="middle">6f</text>
<rect class="alice" x="232" y="77" width="26" height="26"/>
<text x="245.0" y="95.0" text-anchor="middle">ff</text>
<rect class="alice" x="260" y="77" width="26" height="26"/>
<text x="273.0" y="95.0" text-anchor="middle">f6</text>
<rect class="alice" x="288" y="77" width="26" height="26"/>
<text x="301.0" y="95.0" text-anchor="middle">35</text>
<rect class="alice" x="316" y="77" width="26" height="26"/>
<text x="329.0" y="95.0" text-anchor="middle">47</text>
<rect class="alice" x="344" y="77" width="26" height="26"/>
<text x="357.0" y="95.0" text-anchor="middle">2c</text>
<rect class="alice" x="372" y="77" width="26" height="26"/>
<text x="385.0" y="95.0" text-anchor="middle">42</text>
<rect class="alice" x="400" y="77" width="26" height="26"/>
<text x="413.0" y="95.0" text-anchor="middle">f9</text>
<rect class="alice" x="428" y="77" width="26" height="26"/>
<text x="441.0" y="95.0" text-anchor="middle">6f</text>
<rect class="alice" x="456" y="77" width="26" height="26"/>
<text x="469.0" y="95.0" text-anchor="middle">dd</text>
<path class="wire" d="M9,37 v4 H61 v-4"/>
<text class="label small" x="35.0" y="55" text-anchor="middle">key type</text>
<path class="wire" d="M65,37 v4 H481 v-4"/>
<text class="label small" x="273.0" y="55" text-anchor="middle">public key, 32 bytes</text>
<path class="wire" d="M9,108 v4 H481 v-4"/>
</svg>
</figure>

The two-byte prefix `ed 01` says what kind of key follows. The 34 bytes are written in base58, and prefixed with `z` to say so. The bytes above become `did:key:z6MkgyhVBiwiVdpYtCepzRZFdQcdBNEZZjNBZN6CwyYC97tg`. Every Ed25519 `did:key` starts with `z6Mk`, because that is what the prefix bytes turn into.

Several kinds of things are keys:

- **A repository** has its own key. Its DID is the repository's name everywhere: in storage paths, in permissions, and in the branch identity a signed head carries.
- **An authority** is someone responsible for changes: usually a person, like Alice, with a presence across several peers, such as her laptop and her phone. It is the key those peers act for.
- **A peer** is a place where replicas live, such as Alice's laptop or Bob's phone. One peer can hold replicas of many repositories. Its key never leaves it. In a browser the key is a non-extractable WebCrypto key, so even the page that uses it cannot read its secret half.
- **A session** is also a peer, just a more constrained one. It has its own key, usually derived from its parent peer's key for one app or one purpose, and acts only under a permission from that peer, so the app never holds the parent peer's key.

When Alice's laptop creates the grocery list repository, it makes a fresh key for it. It seals the secret half so that only Alice, as its authority, can open it. The repository's key then signs one permission, to Alice, allowing everything. Storage keeps only the public half. From then on, the repository is Alice's because the repository itself said so.

## Capabilities

A permission in Dialog is a capability: a signed statement that one key may do something on behalf of another. Dialog uses [UCAN](https://github.com/ucan-wg/spec) for these.

A **delegation** says *"you may do this, as me."* It names an issuer, an audience, a subject, and a command, plus optional conditions and a time window. The subject is the repository the permission is about, or *any*, which lets the audience act wherever the issuer may. Delegations to devices and sessions are usually of that second kind. The command is a path, and a delegation for a path covers every command under it. A delegation for `/` covers everything.

An **invocation** says *"do this, for this subject."* It is signed by the key that wants the work done, and it carries the delegations that prove it may.

The commands Dialog sends to a remote peer look like paths:

| Command | What it asks for |
|---|---|
| `/use/get/archive/block` | read a block |
| `/use/put/archive/block` | store a block |
| `/use/get/memory/cell` | read a cell, such as a branch head |
| `/use/put/memory/cell` | update a cell, with compare-and-swap |
| `/use/delete/memory/cell` | remove a cell |
| `/use/get/archive/blob`, `/use/put/archive/blob` | read or store a blob |

Here is a typical chain that lets the session on Alice's laptop store a block in the grocery list's remote peer:

<figure class="dg">
<svg class="dg" viewBox="0 0 870 196" width="870" height="196" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="A delegation chain from the repository key down to a session key, and the invocation it authorizes">
<defs><marker id="c-arrow" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto"><path d="M0,0 L8,4 L0,8 z"/></marker></defs>
<rect class="shade" x="10" y="20" width="140" height="46"/>
<text class="label" x="80" y="40" text-anchor="middle">repository</text>
<text class="small muted" x="80" y="57" text-anchor="middle">did:key:zR…</text>
<rect class="alice" x="225" y="20" width="140" height="46"/>
<text class="label" x="295" y="40" text-anchor="middle">Alice</text>
<text class="small muted" x="295" y="57" text-anchor="middle">authority</text>
<rect class="alice" x="440" y="20" width="140" height="46"/>
<text class="label" x="510" y="40" text-anchor="middle">Alice's peer</text>
<text class="small muted" x="510" y="57" text-anchor="middle">her laptop</text>
<rect class="alice" x="655" y="20" width="140" height="46"/>
<text class="label" x="725" y="40" text-anchor="middle">peer session</text>
<text class="small muted" x="725" y="57" text-anchor="middle">did:key:zS…</text>
<line x1="150" y1="43" x2="221" y2="43" marker-end="url(#c-arrow)"/>
<text class="small" x="185.5" y="36" text-anchor="middle">may do /</text>
<text class="small muted" x="185.5" y="60" text-anchor="middle">signed</text>
<line x1="365" y1="43" x2="436" y2="43" marker-end="url(#c-arrow)"/>
<text class="small" x="400.5" y="36" text-anchor="middle">may do /</text>
<text class="small muted" x="400.5" y="60" text-anchor="middle">signed</text>
<line x1="580" y1="43" x2="651" y2="43" marker-end="url(#c-arrow)"/>
<text class="small" x="615.5" y="36" text-anchor="middle">may do /</text>
<text class="small muted" x="615.5" y="60" text-anchor="middle">signed</text>
<rect class="critical" x="520" y="100" width="340" height="84"/>
<text class="title" x="530" y="120">invocation, signed by the session</text>
<text class="small" x="530" y="140">cmd  /use/put/archive/block</text>
<text class="small" x="530" y="156">sub  did:key:zR…   args  digest, checksum</text>
<text class="small" x="530" y="172">prf  the three delegations above</text>
<line class="dashed" x1="725" y1="66" x2="725" y2="96" marker-end="url(#c-arrow)"/>
<text class="label small muted" x="10" y="130">Each arrow is a delegation: the issuer on the</text>
<text class="label small muted" x="10" y="146">left lets the audience on the right act for it.</text>
<text class="label small muted" x="10" y="162">The chain starts at the repository's own key.</text>
</svg>
</figure>

Each delegation's issuer must be the previous one's audience, and the first issuer must be the repository itself. Bob gets access the same way: someone who holds a delegation for the repository signs a new one to Bob, possibly for a narrower command.

## Asking a remote peer

A remote peer that speaks UCAN takes one HTTP request per operation. The signed container goes in the `Authorization` header and the body is the raw bytes. The command and subject are also copied into the URL, but only as labels for network logs: the remote peer reads them from the signed invocation and ignores the URL:

<figure class="dg">
<svg class="dg" viewBox="0 0 640 182" width="640" height="182" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="A direct UCAN request: command and subject in the URL, the signed container in the Authorization header, the bytes in the body">
<rect class="shade" x="10" y="8" width="620" height="110" rx="3"/>
<text x="22" y="30">POST /ucan/?cmd=<tspan class="critical" style="fill:var(--dg-alarm);font-weight:600">/use/put/archive/block</tspan>&amp;sub=<tspan style="fill:var(--dg-circle);font-weight:600">did:key:zR…</tspan></text>
<text x="22" y="52">Authorization: UCAN <tspan style="fill:var(--dg-closure);font-weight:600">P</tspan><tspan style="fill:var(--dg-closure)">H4sIAAAAAAAA…</tspan></text>
<text x="22" y="74">Content-Type: application/octet-stream</text>
<text class="muted" x="22" y="104">…the block's bytes…</text>
<text class="label small" x="10" y="144">In red, the command. In blue, the subject. Both are labels for logs; the signed container is what counts.</text>
<text class="label small muted" x="10" y="170">P means the container is gzipped, then base64url. It holds the invocation and every delegation it needs.</text>
</svg>
</figure>

The remote peer checks, in order:

1. The chain links up: each issuer is the previous audience, and the first issuer is the subject.
2. The command is covered by every delegation in the chain, and the arguments meet their conditions.
3. The current time falls inside every delegation's window.
4. Every signature holds, and no link has been revoked.
5. The body is what the invocation promised. For a block, its BLAKE3 hash is the digest the invocation named and its SHA-256 is the checksum. For a cell, its SHA-256 is the checksum. For a blob, its size and digest match.

Only then does the remote peer store the block or answer the read. The remote peer never needs to know who Alice is. It only needs to know the repository's DID, and to check signatures.

A second exchange exists for storage the remote peer does not proxy. There the remote peer answers an invocation with a presigned S3 URL, a permit, and the client talks to S3 directly. Which exchange to use is part of the remote peer's address, not negotiated per request.

<div class="aside">

**Revocation.** UCAN lets an issuer revoke a delegation it signed, and the verifier in Dialog asks a revocation checker about every link of the chain. The checker Dialog ships today does not consult any store, so it reports every delegation as not revoked. Until a real store is plugged in, a delegation stays valid until its time window ends, and the repository's delegation to its authority has no window at all.

</div>

<div class="aside">

**Implementations.** Keys are in [`dialog-credentials`](https://github.com/dialog-db/dialog-db/tree/main/rust/dialog-credentials) and signatures in [`dialog-varsig`](https://github.com/dialog-db/dialog-db/tree/main/rust/dialog-varsig). Capabilities are [`dialog-capability`](https://github.com/dialog-db/dialog-db/tree/main/rust/dialog-capability), and the commands are defined in [`dialog-effects`](https://github.com/dialog-db/dialog-db/tree/main/rust/dialog-effects). UCAN tokens and chain checks are [`dialog-ucan-core`](https://github.com/dialog-db/dialog-db/tree/main/rust/dialog-ucan-core). The HTTP exchange is [`dialog-remote-ucan`](https://github.com/dialog-db/dialog-db/tree/main/rust/dialog-remote-ucan). Creating a repository and delegating it to its authority is in [`dialog-peer/src/peer/space.rs`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-peer/src/peer/space.rs).

</div>
