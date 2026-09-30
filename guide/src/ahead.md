# Looking Ahead

That is the whole trip. A grocery item started as a sentence, became three sorted keys, landed in a leaf chosen by coin flips, sat in a block named by its hash, was recorded in a signed history, crossed to another device under a chain of signed permissions, was merged with a concurrent edit, and came back out as a row in a query.

Dialog is experimental, and several of the pieces above are still moving. These are the ones most likely to change what this guide says.

**The node layout is a draft.** Every assigned code in the code table is marked `draft`. The layout could still change before a stable release. A change to the layout would get a new version byte, and a new setting gets a new code; neither reinterprets old nodes silently. Whether the table stays a CSV that the code is checked against, or becomes a schema the code is generated from, is also open.

**Encryption.** Nodes are stored in the clear today, so a remote can read what it stores even though it cannot change it. The node prelude leaves room for encrypted bodies as a future layout version.

**A level in every node.** A node does not say how deep it sits in its tree. Sync and diffing could use that to plan their work without descending, and it would be one more byte in the prelude, under a new version.

**Revocation.** Delegations can be revoked in UCAN, and Dialog's verifier asks about every link, but no revocation store is wired in yet. Until one is, a delegation lasts until its time window, if it has one, ends.

---

This is the end of the guide. The design notes behind each chapter are in the [`notes`](https://github.com/dialog-db/dialog-db/tree/main/notes) directory of the repository, and the code is in [`rust`](https://github.com/dialog-db/dialog-db/tree/main/rust). If something here disagrees with the code, the code is right, and a pull request to this guide is welcome.
