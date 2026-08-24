# Propagation Network: Topology as Data

## Status

Design note. Nothing here is implemented; the staged plan at the end names
the first slice. Companion reading: `version-control.md` (the merge this
note leans on), `subject-routing-options.md` (the local half of routing),
`scope-and-delegation.md` (wildcard scopes, referenced by the trust
section).

A word on terminology. The propagator literature (Sussman/Radul) calls its
unit of state a *cell*, but that word is taken here: `Cell` already names
the CAS mutable pointer in `dialog-repository`, and those memory cells play
their own distinct role in this design. So this note says **node** for the
propagator-network unit of state — concretely, a branch (or a whole
repository, viewed as its branches sharing one archive) — and reserves
"cell" for the CAS pointer, as in the codebase.

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
ordinary version-controlled facts — an address book of peers and a set of
directed edges between nodes — and demote the memory cells to the three
jobs only they can do (bootstrap, local override, trust anchoring).

## The sync engine is already a propagator network

The framing that makes the design fall out is Sussman-style propagators:
nodes holding join-semilattice values, autonomous propagators that fire
monotonically and idempotently when an input gains information, and
convergence guaranteed by the lattice algebra rather than by scheduling.
This is not an analogy to grow into — it is what the merge in
`version-control.md` already is:

| Propagator model                     | Dialog today                                          |
| ------------------------------------ | ----------------------------------------------------- |
| State unit holds a lattice value     | A branch: log + watermark (OR-set; merge is log union, "same log, same cache") |
| Join is commutative/assoc/idempotent | R1/R2/R3 screens; re-pulling is harmless by construction |
| Propagator fires on new info         | Pull; scenarios 1–2 (watermark inclusion) are the O(1) "nothing new, don't fire" test |
| Cycle tolerance                      | Mesh sync: joins commute, cycles cost only idle ticks |
| Quiescence                           | All pairwise pulls hit scenario 1/2                   |

What the model does *not* have today is its wiring inside the system. The
upstream and remote cells are the network's wiring diagram, held off to the
side. Moving them into the tree makes the network **self-describing**: the
wiring replicates with the data, merges like the data, and a new replica
pulls once and inherits its whole I/O behavior. The sync daemon stops being
configured and becomes a stateless interpreter of the tree.

Three mechanisms in the codebase are already instances of one propagator
family, currently unrelated in code:

- `dialog.rule/on` triggers: propagators *within* a branch (commit fires
  rule).
- Session layering (`SessionBranch`): a local junction joining several
  branches into one read view.
- Upstream sync: a propagator *between* branches, whose trigger is "peer
  head moved" and whose body is the join.

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

## Which entity a fact attaches to decides who shares it

Entity derivation already gives four kinds of derived entity, and each is
shared by a different audience. The design principle: *every fact goes on
the entity matching who should share it.*

| Entity derived from        | Who converges on it              | Example                         |
| -------------------------- | -------------------------------- | ------------------------------- |
| `subject` (the repo DID)   | everyone with the repo           | peers, shared edges, "pages that exist" |
| `(subject, name)`          | everyone, per branch             | branch-scoped tracking intent   |
| `(profile, subject)`       | one profile's devices (a replica)| device-local wiring, "active page" |
| `(replica, name)`          | one profile, one branch          | origins (already lives here)    |

`schema::Branch::new(replica: impl AsRef<Entity>, name)` is already generic
over the owner entity, so the `(subject, name)` row costs nothing new — but
the choice must be visible at call sites (two named constructors, not one
polymorphic one), because attaching topology to the `(replica, name)`
entity would make every collaborator write to a different entity and share
nothing. Origins stay on `(replica, name)`; that derivation is what makes
an origin a sequential actor, and nothing here touches it.

The address book hangs off the repository entity, so it is shared by
everyone. A device-local mirror ("this laptop also syncs to my NAS") is the
*same fact shape* attached to the replica entity instead — visible only to
that profile's devices, invisible and irrelevant to collaborators. Shared
and private wiring differ only in which entity the edge hangs off.

## Schema

### Peers: the address book

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
local node; "push remote" is an edge whose source is local. The edge entity
derives from `(source, sink)` so two replicas concurrently adding the same
connection mint the same entity, and their attributes merge instead of
duplicating the edge:

```text
edge = hash(source-node, sink-node)

edge   dialog.flow/source   <node: did, or (did, branch)>
edge   dialog.flow/sink     <node>
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
- **Multiple scopes union.** The scope attribute is cardinality-many and
  the effective scope is the union of the ranges, so concurrent scope
  additions widen the edge; they never conflict. Narrowing is a
  retraction, with the caveat below.
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

Three jobs the memory cells keep, because they cannot move:

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

**Spike result: the sync base is not derivable, and it is two different
things.** The hoped-for derivation (peer's published watermark + local
`Context` → merge base) fails three ways: a watermark comparison yields a
causal *cut*, not a tree, and no `Version → TreeReference` mapping exists
anywhere — deliberately, since a revision record cannot contain the root
of the tree it lives in (`revision_record.rs`); the meet of two contexts
is generally an antichain no actual revision ever materialized, so no tree
ever had that content; and `common_ancestor` yields at best a `Version`,
at O(log gap) fallible verified reads against the cell's infallible O(1).
But the two *roles* of the stored base have different standings:

- **For pull it is a droppable cache.** The merge is correct from the
  empty base ("correct, just unable to skip anything", `pull.rs:56-60`,
  pinned by the empty-base non-resurrection tests). Dropping it costs the
  zero-read fast paths (scenarios 1/3 never fire, graft disabled, deltas
  become whole trees) but never corrupts. So it needs no home in the
  tree: an unregistered local cache, rebuilt by one full pull.
- **For push it is authoritative and non-causal.** It is the last
  observed value of the *remote's* mutable head pointer — the
  compare-and-set token for the non-fast-forward guard and the
  replaced-remote defense — and a residency certificate ("a tree the
  target itself served or accepted") that makes the boundary-missing
  block policy sound and bounds upload volume. No watermark asserts
  either. Epistemically this is the same category as the trust pins
  below: a local observation about the outside world, not a fact about
  the data. It stays local, permanently.

Consequence: upstream *wiring* still moves to the tree, but upstream
entries cannot become pure configuration — each output edge keeps a small
local companion (its CAS/residency observation), and each input edge an
optional cache.

**Why the tree cannot record the pulled revision automatically — and what
it can record instead.** The git-submodule intuition ("just commit what we
pulled") founders on pacing, not on content. A gitlink advances when a
human deliberately bumps it; the sync base advances on every fetch, and
recording a per-fetch value in shared data breaks quiescence: pull P, hit
scenario 2 (nothing new), record "pulled P@R" — that mints a revision,
which is novelty; P absorbs it, P's head moves, the next pull of P hits
scenario 2 again and records again, forever. The propagator network never
settles because firing the propagator changes the data it watches. The
cell version avoids the loop precisely by being invisible to the merge.
What *is* sound is the actual submodule analog: an **explicit pin** — a
deliberate fact "cites P's branch at edition E, tree T", copied from P's
signed head (hence verifiable third-hand), paced by meaning
("checkpoint", "adopted their release") rather than by fetch. Pins
compose with edges (a pinned edge is frozen tracking); neither replaces
the push-side base, which pins nothing — it observes a mutable pointer.

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

- **Input edges (pull) may auto-fire — under the rollup trust model,
  with one insider gap to close.** The deliberate design: the *site*
  enforces the trust boundary by checking a push invocation's delegation
  chain to the subject; the pusher vouches for batch contents; the
  signed head is the batch signature. (Per-fact signing was rejected as
  unscalable, correctly.) This handles **outsiders** completely — nothing
  reaches the rendezvous without a chain. What it cannot catch is an
  *authorized* pusher publishing an invalid batch — in rollup terms, a
  sequencer problem needing validity checking, not authorization. The
  watermark-inflation finding above is exactly that. The validity
  evidence already exists and is already signed: the revision records
  riding a delta. Verifying them before absorbing their versions (today
  `observe_revisions` decodes but never calls `record.verify()`) closes
  the insider hole on the screened and graft paths for a handful of
  Ed25519 checks per pull, no new signing anywhere. Fast-forward-by-root
  is the one path where verbatim context adoption is the point (zero
  reads); there **trust becomes a per-edge attribute** — an edge to your
  own devices or server adopts by root, an edge to an arbitrary
  address-book peer pays the verification reads. The policy finally has a
  natural home because the edge is data.

  The rollup model completes when heads (or revision records) *reference
  their authorizing chain* by envelope hash. Chains already live in the
  tree (`delegations().retain`; the prover assembles proofs from exactly
  these facts), so this is one content-addressed pointer, no
  duplication — and it makes "issuer authorized for subject" checkable by
  **anyone** from tree contents, not just by the site at push time: the
  batch carries its validity proof, so a puller enforces the same
  boundary the site does, which is what makes pulling from arbitrary
  sites safe. Chains change rarely, so verification memoizes by envelope
  hash. This also composes with materialization for free: the access
  branch is already the always-materialized precedent, so the proof
  region and the topology region are one "must be local, must be
  verified" layer. Semantics choice to make early: a retracted delegation
  should invalidate adoption of *future* heads only (revocation does not
  rescind, consistent with everything else here).
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

**Spike result: in-tree claim attribution is conventional, not
cryptographic.** The precise boundary:

- *Cryptographic:* heads (signature over branch, issuer, tree, edition,
  context; origin recomputed from the signed fields, so no head can mint
  into another origin) and revision-level attribution on the read path
  (`TreeHistory::revision_record` verifies signature + slot binding and
  skips planted decoys).
- *Conventional:* claim/history records carry **no issuer and no
  signature** — their origin is an unauthenticated 32-byte key prefix
  chosen by whoever wrote the entry. Nothing on the pull path checks a
  record's claimed origin against any signed artifact, and the merged
  watermark adopts the upstream's published context (fast-forward:
  verbatim; graft/replay: unioned) without verifying that its entries
  about *third-party* origins are legitimate. `observe_revisions` derives
  versions from record contents but never calls `record.verify()`.

Two consequences. First, the design one this section needs: an
origin-scoped read filter ("only facts from my replicas") cannot be
trusted against a hostile collaborator, so replica-scoped facts that must
bear weight use the signed-envelope pattern (signature in the value blob,
facts as the index — exactly `delegation/prove.rs` and the forged-rule
handling in `induce.rs`), and the pin set stays out of the tree entirely.
Second, a pre-existing vulnerability independent of this design, recorded
here because the spike surfaced it: a collaborator can publish a validly
signed head whose context inflates **another origin's** watermark entry
(the ceiling check bounds magnitude, not ownership). Replicas that pull it
merge the inflated entry (per-origin max), after which R1 treats that
origin's genuine future writes as already-observed and silently discards
them, and Tier-0 causality (same-origin ⇒ edition ordering, zero reads)
treats forged same-origin records as sequential supersessions instead of
surfacing conflicts. That is a targeted, quiet censorship primitive
available to anyone whose head you pull, and it deserves a fix regardless
of topology-in-tree — most likely verifying the revision records riding a
delta before absorbing their versions, and bounding context adoption to
verified evidence.

## Winners as read-time policy

The merge deliberately does *less* than a truth-maintenance system: it
converges the log and derives one live set with a fixed deterministic
tie-break (higher stored-byte hash on same-fact collisions). But the log
retains every claim with its provenance — origin, edition, what superseded
what — so a different resolution policy can live **at read time, in the
query layer**, without touching the merge:

- Any policy that is a pure, deterministic function of the merged log
  converges for free: same log everywhere, same answer everywhere. The
  built-in tie-break is just the default policy; an application can query
  the claim records and prefer-by-author, prefer-by-recency-of-edition, or
  surface the contest to a human, and no replica disagrees.
- What must **not** happen is a policy feeding its choice back into the
  merge (a "resolver" that commits winners) unless that write is itself
  keyed by the versions it resolved — otherwise two replicas resolving
  concurrently reintroduce the conflict one level up.
- The part of a TMS this does not give is justification tracking:
  conclusions that retract when their premises die. In dialog that is the
  rules engine's territory — derived facts keyed by the provenance versions
  of their premises, exactly the monotonicity condition the edge-transform
  ban above points at. Same condition, two doors it guards.

So "the query engine is a policy for choosing winners" is sound as long as
choosing stays a read-time view over the log; the staged `prepare`/`commit`
pull is the quarantine point if a policy ever needs to inspect novelty
before it joins.

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

## Sideline: branches as nodes in their own right

If branches were subjects (own DIDs) sharing an archive, remotes and
upstreams collapse further: edges connect nodes, uniformly, and the address
book covers branches too. The merge layer is already indifferent —
watermarks compare across repository boundaries, the router already maps
many DIDs to one provider, and origins are per-(branch, issuer) regardless.
What changes is that a branch stops being free: it costs a keypair and a
provable link to its repository (hierarchical derivation is convenient but
not publicly verifiable for Ed25519; a retained parent-issued attestation
is verifiable and uses existing machinery — do both). This note does not
depend on that unification, but the edge schema deliberately references
nodes as `did` or `(did, branch)` so adopting it later narrows a type
instead of reshaping the relation.

Once arbitrary peers are wired in, the causality between "repository" and
"shared storage" inverts: today the repo decides that its branches share
an archive; with the edge graph as the topology truth, an archive is a
*locality choice* — a pool that pays off (via structural sharing) exactly
where nodes are densely wired with identity-join edges. "Repository"
becomes an emergent label for a tightly-wired, co-located cluster, and
archive sharing could in principle be derived from the wiring rather than
declared. North star, not a commitment.

## Retiring the branch cells

The conservative concrete goal: a change that retires the side-band cells
in favor of the database storing its own topology. Inventorying the cells
shows they have three different fates, and that the chicken-and-egg worry
(if the db stores branches, how do you discover which branches exist?) is
solved rather than created by the move:

- **`branch/{name}/revision` stays, by architecture.** It is the mutable
  pointer *to* the tree — the one thing that cannot live inside what it
  points at. In propagator terms it is the node's identity as a mutable
  location. Likewise the remote-side head cells: they are the rendezvous.
- **`branch/{name}/upstream` and `remote/{name}/address` are the
  retirement targets.** Wiring becomes `dialog.peer/*` + `dialog.flow/*`
  facts; the sync base splits per the spike result above (pull: local
  droppable cache; push: local observation that never had a home in the
  tree to begin with).
- **`branch/{name}/induction` is already replica-local by design** and
  simply stays.

On discovery: there is no branch enumeration *today* — cell addresses are
pure naming convention under the subject's space, so a branch can only be
found by already knowing its name. Moving branch existence into the tree
fixes this. The bootstrap chain becomes: irreducible local knowledge =
`(subject DID, site, well-known branch name)` — and `ACCESS_BRANCH =
"main"` is already that convention. From there: well-known name → its
`revision` cell by convention → its tree → in-tree registry
(`dialog.branch/*` facts on the `(subject, name)` entities: which branches
exist, plus the peers and edges) → every other branch's head cell, again
by naming convention. One entry point, everything else discovered; works
on dumb stores that cannot list keys.

## Staged plan

Each slice is independently shippable and none breaks existing cells.

1. **Peer directory** (`dialog.peer/*`): peers on the repository entity,
   resolution order local-override → tree → bootstrap, cells demoted but
   fully functional. No sync behavior changes. Deliverable: a pulled
   address book — replica B resolves a petname replica A asserted.
2. **Edges** (`dialog.flow/*`) for *input* wiring: which peers a branch
   tracks, on the `(subject, name)` branch entity; pull sync bases and
   head caches become local caches, push bases stay local observations
   (per the spike result).
   Deliverable: a fresh replica self-configures its pulls from the tree.
3. **Output edges + pin set**: retained proofs make edges executable;
   profile-scoped sealed pins gate firing. Deliverable: "clone and it just
   works" — safely.
4. **Always-materialized topology region** (can land with 2).
5. Later, separately argued: scoped edges, notification, branches as
   first-class nodes.

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

Answered by spikes (results inline above): sync-base derivability — **no**
(pull-side cache, push-side authoritative local observation); in-tree claim
attribution — **conventional** (signed-envelope pattern required for
anything tamper-evident; watermark-inflation vulnerability recorded in the
trust section).

Still open:

- Chain-referencing heads: which artifact carries the envelope hash (head
  vs. revision record), and the revocation semantics (proposed: a
  retracted delegation gates future adoption only).
- Verify-on-absorb for revision records riding a delta (the insider
  validity check), and the per-edge trust attribute gating verbatim
  context adoption on fast-forward.
- Reserved region vs. layered topology branch for materialization.
- Which capability authorizes `dialog.peer/*` / `dialog.flow/*` writes —
  ordinary commit authority, or an attenuated topology command?
- Petname ambiguity surface: refuse at resolve time (proposed) vs. refuse
  at merge time (impossible without breaking convergence) vs. rank by
  recency (silent, rejected).
