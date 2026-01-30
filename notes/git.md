Got it — here’s the same document rendered cleanly *without wrapping the whole thing in a code block*, so it reads naturally and can be copied or annotated easily.

---

# Git Internal Model — Concise Reference

## 1. Commit Objects

**Concept**
A commit is an immutable object capturing a snapshot of the repository state, its ancestry, and metadata.

**Structure**

```
Commit = {
  tree: <TreeID>,
  parents: [<CommitID>],
  author: <Person + Time>,
  committer: <Person + Time>,
  message: <String>,
  (optional) gpgsig: <Signature>
}
```

**Addressing**

* Each commit has a **content hash** (SHA-1 / SHA-256) that uniquely identifies it.
* All references to commits are via this hash.

**Storage**

* Serialized and compressed under `.git/objects/`, split by hash prefix:
  `.git/objects/aa/bbcccc...`
* Commits are immutable and never modified once written.

---

## 2. Branches

**Concept**
A branch is a **mutable named reference** to a commit.

**Addressing & Storage**

* Stored as plain text files under `.git/refs/heads/<branch-name>`.
  Each file contains one line: `<CommitID>`.
* May also be packed in `.git/packed-refs` for efficiency.

**Updates**

* Creating a new commit on a branch rewrites that branch’s file with the new commit hash.
* The underlying commits remain immutable.

---

## 3. Remotes

**Concept**
A remote is a configured external repository definition describing where to fetch/push and how to map refs.

**Storage**

* Defined in `.git/config` under sections like:

  ```ini
  [remote "origin"]
      url = git@github.com:user/repo.git
      fetch = +refs/heads/*:refs/remotes/origin/*
  ```
* The remote-tracking branches (local mirror of remote refs) live under:
  `.git/refs/remotes/<remote>/<branch>`

**Updates**

* Running `git fetch` updates these remote-tracking refs.
* Local branches (`refs/heads/*`) are not changed by fetch.

---

## 4. Branch Upstream (Tracking)

**Concept**
A branch can track a specific remote branch — defining its default fetch/push targets.

**Storage**

* Recorded in `.git/config` as:

  ```ini
  [branch "main"]
      remote = origin
      merge  = refs/heads/main
  ```
* This means: local `main` tracks remote `origin/main`.

**Usage**

* `git pull` uses this to know which remote/branch to fetch and merge from.
* `git push` uses it (along with `push.default`) to decide where to push.

---

## 5. Fetch / Pull / Push Overview

### `git fetch`

* Reads remote and refspec from `.git/config`.
* Connects to remote, lists its refs, compares commit graphs, and downloads missing objects.
* **Updates:**

  * `.git/refs/remotes/<remote>/*` → new commit IDs
  * `.git/FETCH_HEAD` log file
* Does **not** change local branches or working tree.

---

### `git pull`

* Equivalent to `fetch` + `merge` (or `rebase`) with the branch’s upstream.
* **Uses:** branch’s `remote` and `merge` entries in config.
* **Updates:**

  * Remote-tracking refs (same as fetch)
  * Current branch ref (`refs/heads/<name>`)
  * Working directory and index after merge/rebase.

---

### `git push`

* Determines target remote and destination branch via the branch’s `remote` / `merge` config or global `push.default`.
* Compares local vs. remote commits to identify what to send.
* **Updates:**

  * On remote: `.git/refs/heads/<branch>`
  * Locally: may update `.git/refs/remotes/<remote>/<branch>` after success.

---

## 6. Summary Table

| Concept                    | Representation                   | Stored In                     | Mutable | Updated When                 |
| -------------------------- | -------------------------------- | ----------------------------- | ------- | ---------------------------- |
| **Commit**                 | Object (tree, parents, metadata) | `.git/objects/`               | ❌       | Never                        |
| **Branch**                 | Ref → CommitID                   | `.git/refs/heads/`            | ✅       | New commit/reset             |
| **Remote**                 | Config entry + refspec           | `.git/config`                 | ✅       | `git remote` commands        |
| **Remote-tracking branch** | Ref → CommitID                   | `.git/refs/remotes/<remote>/` | ✅       | `git fetch`                  |
| **Upstream mapping**       | `[branch "<name>"]`              | `.git/config`                 | ✅       | `--set-upstream`, first push |
| **HEAD**                   | Symbolic ref                     | `.git/HEAD`                   | ✅       | Checkout / detach            |

---

**Mental model**
Git = immutable object graph (commits, trees, blobs)

* mutable layer of named references (branches, remotes, HEAD, config).
  Operations like fetch, pull, and push only rewrite these references —
  never the underlying immutable data.
