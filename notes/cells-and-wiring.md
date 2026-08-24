# Cells and Wiring: Network Topology as Data

## Status

Design note. Nothing here is implemented; the staged plan at the end names
the first slice. Companion reading: `version-control.md` (the merge this
note leans on), `subject-routing-options.md` (the local half of routing),
`scope-and-delegation.md` (wildcard scopes, referenced by the trust
section).

## Problem

Remotes and upstreams live in CAS memory cells beside the tree:

- `memory/remote/{name}/address` → `RemoteAddress { SiteAddress, subject }`
- `memory/branch/{name}/upstream` → `Upstreams(Vec<Upstream>)`, each carrying
  its sync-base `tree`
- `memory/remote/{name}/branch/{b}/revision` → cached remote head

Because this configuration sits outside the tree, it is not shared across
replicas (every device re-declares its remotes), not versioned (no answer to
"who repointed origin, and when"), and not subject to merge (concurrent edits
on two devices silently last-write-win at the cell). Meanwhile the
*credential* half of "how do I reach and write to that peer" has already
moved into the tree: `branch/delegation.rs` retains UCAN chains as
`dialog.ucan/*` facts plus envelope blobs, and `repository/access.rs` retains
the profile's certificates in its access branch. The address half should
follow.

The proposal: represent peers and the connections between repositories as
ordinary version-controlled facts — an address book of nodes and a set of
directed edges — and demote the cells to the three jobs only they can do
(bootstrap, local override, trust anchoring).

## The sync engine is already a propagator network

The framing that makes the design fall out is Sussman-style propagators:
cells holding join-semilattice values, autonomous propagators that fire
monotonically and idempotently when an input cell gains information, and
convergence guaranteed by the lattice algebra rather than by scheduling.
This is not an analogy to grow into — it is what the merge in
`version-control.md` already is:

| Propagator model              | Dialog today                                          |
| ----------------------------- | ----------------------------------------------------- |
| Cell holding a lattice value  | A branch: log + watermark (OR-set; merge is log union, "same log, same cache") |
| Join is commutative/assoc/idempotent | R1/R2/R3 screens; re-pulling is harmless by construction |
| Propagator fires on new info  | Pull; scenarios 1–2 (watermark inclusion) are the O(1) "nothing new, don't fire" test |
| Cycle tolerance               | Mesh sync: joins commute, cycles cost only idle ticks |
| Quiescence                    | All pairwise pulls hit scenario 1/2                   |

What the model does *not* have today is its wiring inside the system. The
upstream and remote cells are the network's wiring diagram, held off to the
side. Moving them into the tree makes the network **self-describing**: the
wiring is cell content, subject to the same join, replicated to every
replica, and a new replica pulls once and inherits its whole I/O behavior.
The sync daemon stops being configured and becomes a stateless interpreter
of the tree.

Three mechanisms in the codebase are already instances of one propagator
family, currently unrelated in code:

- `dialog.rule/on` triggers: intra-cell propagators (commit fires rule).
- Session layering (`SessionBranch`): a local junction joining several
  branch cells into one read view.
- Upstream sync: an inter-cell propagator whose trigger is "peer head
  moved" and whose body is the join.

This note only reifies the third, but the schema below is chosen so the
other two could later share its vocabulary.

## Pull-only semantics, and why push is not a primitive

Every flow in dialog is *receiver-driven*: pull is the trust boundary — the
head signature is verified before a single block is read
(`it_refuses_to_pull_a_forged_head`) — and the receiver's screens decide what
integrates. Nobody ever writes into another replica's working state.

What we call "push" is already something else: writing to a **site** — a
rendezvous (cell + archive, e.g. an S3 bucket) that the other party watches
and pulls from through their own screens. Push is "advance a rendezvous",
never "mutate a peer". This dissolves the push/pull asymmetry into two
independent questions per edge:

1. **Who writes the rendezvous?** Writing someone else's site requires a
   delegation from its authority. Writing your own requires nothing.
2. **Who watches it?** Always the receiver, always screened. This is pull,
   and it is the only integration primitive.

The classic hub topology (everyone pushes to a shared server) and the
peer-to-peer topology (everyone publishes to their own site; peers track
each other) are the same propagator with the rendezvous placed differently.
"Our remotes are PRs": publishing your head to a site you control and
letting a peer track it is a pull request; a delegation to write their site
is commit access. Symmetric peering needs no delegations at all — if they
list you as a peer with an input edge from your site, you never need write
access to anything of theirs. Delegated write access is only necessary when
the receiver will not poll a site you control (e.g. the receiver is a
browser replica whose only reachable rendezvous is the shared server).

The one genuinely missing piece in a pull-only world is **notification**:
cells are poll-based, and "let them know so they pull" is today "they poll".
Scenario 1 makes polling nearly free (zero reads on the idle tick), so
notification is an optimization layered on later (a site that supports
watch), never a semantic requirement. Nothing in this note depends on it.

## The scope lattice

Entity derivation already provides a lattice of scopes, and the design
principle is: *every fact goes on the rung matching who should share it.*

| Rung              | Entity derivation          | Shared by                    | Example                         |
| ----------------- | -------------------------- | ---------------------------- | ------------------------------- |
| repository        | `subject.this()`           | everyone with the repo       | peers, shared edges, "pages that exist" |
| repository branch | `hash(subject, name)`      | everyone, per branch         | branch-scoped tracking intent   |
| replica           | `hash(profile, subject)`   | one profile's devices        | device-local wiring, "active page" |
| replica branch    | `hash(replica, name)`      | one profile, one branch      | origins (already lives here)    |

`schema::Branch::new(replica: impl AsRef<Entity>, name)` is already generic
over the owner entity, so the repository-branch rung costs nothing new —
but the scope choice must be visible at call sites (two named constructors,
not one polymorphic one), because attaching topology to the replica-scoped
branch entity would make every collaborator write to a different entity and
share nothing. Origins stay on the replica-branch rung; that rung is what
makes an origin a sequential actor, and nothing here touches it.

The address book sits on the repository rung. A device-local mirror ("this
laptop also syncs to my NAS") is the *same fact shape* one rung down, on the
replica entity. Shared and local wiring differ only in which entity the
edge hangs off.

## Schema

### Nodes: the address book

The entity is the peer's DID — intrinsic, global, collision-free. Petnames
are attributes, cardinality-many:

```text
did:key:zPeer   dialog.peer/petname   "origin"
did:key:zPeer   dialog.peer/site      <site descriptor>
```

Keying by DID rather than by name is load-bearing for merge behavior. Two
replicas binding the same petname to different DIDs is a *visible* conflict
(the petname query returns two rows; the resolver refuses to act on an
ambiguous name) instead of a silent hijack where the merge picks a winner
and a push lands somewhere unexpected. Two people calling the same peer
different things merges to two petnames on one entity, which is correct.

Site descriptors are location *hints* — mutable, environment-specific,
possibly several per peer, and advisory: a replica may know a better route
(LAN address) than the tree does, via the override tier below.

### Edges: the wiring

One relation, read from both ends. "Upstream" is an edge whose sink is a
local cell; "push remote" is an edge whose source is local. The edge entity
derives from `(source, sink)` so concurrent additions of the same edge
converge to one entity whose attributes merge:

```text
edge = hash(source-cell, sink-cell)

edge   dialog.flow/source   <cell ref: did, or (did, branch)>
edge   dialog.flow/sink     <cell ref>
edge   dialog.flow/scope    "user/*"            # optional; cardinality many, union
edge   dialog.flow/proof    blob:<delegation>   # only for writing a foreign rendezvous
```

Constraints the propagator model imposes, stated here so they are design
commitments and not later discoveries:

- **Edges do identity-join, optionally scoped. Nothing else.** A key-range
  scope is a sublattice projection — monotone, so partial replication
  converges like everything else, and the span/graft machinery is already
  range-shaped. An edge function that *transforms* facts (a map, a derived
  view) is a propagator that must be proven monotone and idempotent or
  convergence dies; if cross-repo materialized views ever happen, derived
  facts must be keyed by provenance version so re-derivation is a no-op.
  Until someone does that work, transforms on edges are out.
- **Multiple scopes union.** Concurrent scope additions widen the edge;
  they never conflict. Narrowing is a retraction, with the caveat below.
- **Retracting an edge stops future flow and rescinds nothing.**
  Propagators never un-propagate; everything already joined stays joined.
  Same asymmetry as capability revocation, worth surfacing in any UI that
  offers "remove remote".
- **Reflective wiring converges.** Pulling can add edges, which triggers
  pulls, which add edges. The edge set is an OR-set and fires are
  idempotent, so the fixpoint exists and transitive discovery is safe. What
  it is *not* is safe to auto-fire in both directions — see trust below.

### Delegating across repositories

An edge whose sink is a foreign site becomes executable by pairing it with
a retained delegation — the machinery `branch/delegation.rs` already has:

1. B issues a UCAN: `sub: B`, `aud: A_did`, attenuated to publishing B's
   branch cell (and writing B's archive), ideally with an expiry.
2. A retains the chain via `branch.delegations().retain(chain)` — facts
   plus envelope blob, replicating with A's tree.
3. Any operator holding a delegation from A assembles
   `B → A → profile → operator` and writes B's rendezvous.

Address book + edge + retained proof = a connection that clones with the
database: anyone with access to A can push to B. That is the feature, and
also the risk: A's repository becomes a *principal*, and the set of people
who can write B's site is defined by A's access set, which changes without
B's knowledge. Attenuate hard (one branch, one command, an expiry), and
note that powerline delegations make the grant retroactive across
everything A later receives.

## What stays outside the tree

Three jobs the cells keep, because they cannot move:

1. **Bootstrap.** You cannot learn from the tree where to fetch the tree.
   At least one (subject, site) pair is irreducibly local. This is git's
   `.gitmodules` / `.git/config` split: the tree is the shared directory
   and default answer; the cells are the entry point and the override.
   Resolution order: local override → in-tree → bootstrap.
2. **Trust anchors.** A trust anchor must not live inside the thing it
   protects. The pin set that guards "where do I write" (below) is what the
   tree is checked *against*; putting it in the tree is circular however
   well it is signed.
3. **Sync state.** The sync base (`Upstream.tree`), cached remote heads,
   and the induction watermark are per-replica bookkeeping that advances on
   every fetch. As facts they would turn every idle tick into a revision —
   an auto-sync loop that currently hits scenario 1 at zero cost would
   become a commit generator, and each such commit would invalidate every
   peer's scenario-1 check in turn.

Open question that moves this boundary: **is the sync base derivable?** The
peer's published watermark (on their signed head) and the local `Context`
may together determine the merge base without storing a tree hash per
upstream. If so, `Upstream.tree` becomes a pure cache, edges become pure
config that changes only when a human changes it, and tier 3 nearly empties.
Worth a spike before implementing anything in tier 3.

Secrets are a fourth category with a sharper answer: bearer credentials
never appear as fact values, encrypted or not. Encrypted values break the
index (equality search needs deterministic encryption, which leaks equality
to every observer), and the history is immutable — ciphertext anyone has
fetched is theirs forever, so key leakage is retroactive and rotation is
forward-only. Where sealed material is genuinely needed, use the
delegation-retention shape: an opaque blob sealed to recipient DIDs,
indexed by plaintext facts naming recipient and purpose, outside the
queryable region. But for storage backends the capability path
(`dialog-remote-ucan-s3`) is strictly better — attenuated, expiring,
revocable — and every sealed bearer secret is an admission that a backend
cannot do capabilities.

## Trust: the two consents

In-tree wiring is writable by anyone who can commit, so the two directions
of an edge need different guards, and the split is clean:

- **Input edges (pull) may auto-fire.** Integrity is safe: blocks are
  content-addressed and heads signature-verified before any block is read.
  A malicious site can serve stale state or withhold — an availability
  attack — but cannot forge content. Transitive discovery of pull sources
  is therefore safe by default.
- **Output edges (writing a rendezvous) are an exfiltration vector.** A
  pulled edge with a valid proof is a self-authorizing instruction to copy
  the repository somewhere. The proof settles only *sink consent* — it is
  issued by the sink's authority, verifiable from the tree, no human
  needed. Nothing in the tree can settle *source consent*: whether this
  replica's owner wants the data to go there. That is local policy:
  **an output edge fires only where the local pin set blesses it.**

The pin set is TOFU like `known_hosts` — first use pins (peer DID → sink
site); a changed or new output target requires an explicit local act. Pins
want to be **profile-scoped and sealed to the profile's key**, not merely
replica-local: your devices then inherit your trust decisions (a new laptop
pins what your old one pinned) while collaborators see only an opaque blob.
Sealing gives confidentiality, not authorization — a collaborator can still
retract the blob's facts, which is a visible denial (your pins are gone,
nothing fires) rather than a silent redirect. That failure mode is
acceptable; the redirect is not.

A related soundness question to settle early: in-tree claim attribution
(origin fields in records) is *asserted* by whoever's tree you pulled,
while head signatures are verified. Whether "facts from my own origin" is a
cryptographic filter or a conventional one decides how much weight
replica-scoped facts can bear against a malicious collaborator. If it is
conventional, anything that must be tamper-evident uses the signed-envelope
pattern (signature in the blob, facts as index), same as delegations.

## The topology region must always be materialized

Pull adopts subtrees by reference and hydrates lazily (scenario 3 adopts a
head by root with zero reads). Unmitigated, the wiring facts could be
exactly what is not local when a replica is offline and needs to know where
to sync. The fix is the one `branch/download.rs` already motivates for the
access branch: an always-materialized region. Constraints:

- Root adoption stays by-reference; materializing the topology region is a
  separate, bounded fetch after adoption, and the zero-read property of
  scenario 3 (pinned by `it_adopts_an_upstream_head_without_reading_its_novelty`)
  is preserved for everything else.
- Always-materialized implies always-pushed, so nothing replica-private may
  live in the region — which reinforces the tiering above.
- The region must stay small by construction. Peers and edges qualify. It
  must not become a general "important stuff" region.

Whether this is a reserved key range in every branch or a dedicated branch
layered into sessions via the existing multi-branch join is an
implementation choice for slice 2; the layered branch composes better with
the access branch, which has the same materialization need.

## Sideline: branches as cells

If branches were subjects (own DIDs) sharing an archive, remotes and
upstreams collapse further: edges connect cells, uniformly, and the address
book covers branches too. The merge layer is already indifferent —
watermarks compare across repository boundaries, the router already maps
many DIDs to one provider, and origins are per-(branch, issuer) regardless.
What changes is that a branch stops being free: it costs a keypair and a
provable link to its repository (hierarchical derivation is convenient but
not publicly verifiable for Ed25519; a retained parent-issued attestation
is verifiable and uses existing machinery — do both). This note does not
depend on that unification, but the edge schema deliberately references
cells as `did` or `(did, branch)` so adopting it later narrows a type
instead of reshaping the relation.

## Staged plan

Each slice is independently shippable and none breaks existing cells.

1. **Peer directory** (`dialog.peer/*`): nodes on the repository rung,
   resolution order local-override → tree → bootstrap, cells demoted but
   fully functional. No sync behavior changes. Deliverable: a pulled
   address book — replica B resolves a petname replica A asserted.
2. **Edges** (`dialog.flow/*`) for *input* wiring: which peers a branch
   tracks, on the repository-branch rung; sync bases and head caches stay
   in cells (or fall away if the derivability spike lands). Deliverable: a
   fresh replica self-configures its pulls from the tree.
3. **Output edges + pin set**: retained proofs make edges executable;
   profile-scoped sealed pins gate firing. Deliverable: "clone and it just
   works" — safely.
4. **Always-materialized topology region** (can land with 2).
5. Later, separately argued: scoped edges, notification, branch-as-cell.

### Slice 1, concretely

Mirror the retained-delegation surface, which is the in-tree config
precedent this codebase already has:

- `dialog.peer/petname` and `dialog.peer/site` reserved attributes beside
  the `dialog.ucan/*` constants; site descriptors serialize the existing
  `SiteAddress` (`remote/address.rs`).
- A `Peers` handle on `Branch` shaped like `Delegations`
  (`branch/delegation.rs`): `branch.peers().retain(peer)` /
  `.retract(...)`, writing through the internal instruction path (the
  `dialog.` namespace stays closed to user transactions).
- A resolver: petname → `RemoteAddress`, consulting override cell, then
  tree (refusing ambiguous petnames), then bootstrap cell.
- Tests: round-trip retain/resolve; ambiguity refusal; and the headline —
  A asserts a peer, pushes; B pulls, resolves the petname, connects.

## Open questions

- Sync-base derivability from watermarks (moves the tier-3 boundary; spike
  first).
- Is in-tree claim attribution verifiable or conventional against a
  malicious collaborator holding write access?
- Reserved region vs. layered topology branch for materialization.
- Which capability authorizes `dialog.peer/*` / `dialog.flow/*` writes —
  ordinary commit authority, or an attenuated topology command?
- Petname ambiguity surface: refuse at resolve time (proposed) vs. refuse
  at merge time (impossible without breaking convergence) vs. rank by
  recency (silent, rejected).
