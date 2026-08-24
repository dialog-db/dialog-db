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

## The short version, in git vocabulary

The whole proposal in one sentence: **this is `.gitmodules` for remotes —
git's two config tables become versioned, replicated facts; every kind of
ref stays exactly where git keeps it.** (The "Mounts" section later
reframes the tracking relation as the `integrate` policy of a more
general mount relation — read this section first, then that one.)

The tables, side by side with git. References are always entities —
names are attributes people edit, never addresses code follows:

```text
# git: [remote "bob"] url = …
bob    dialog.peer/name     "bob"
bob    dialog.peer/did      "did:key:zBlog"
bob    dialog.peer/site     "s3://…"            # a value, not an entity

# the branch registry — a branch is an entity; its name is an attribute
br-a1  dialog.branch/subject  "did:key:zNotes"
br-a1  dialog.branch/name     "main"
br-b7  dialog.branch/subject  "did:key:zBlog"   # Bob's registry facts arrive
br-b7  dialog.branch/name     "main"            # by pulling him; shown as bob/main

# git: [branch "main"] remote = bob, merge = main
rep    dialog.replicate/from  br-b7
rep    dialog.replicate/to    br-a1
```

`rep` needs nothing more: the `from` entity's subject resolves through
the address book to sites, so a peer attribute would be redundant (store
it redundantly-by-design if reverse queries want it). Its entity derives
from the *labeled* relation — `Entity::of(Replicate { from, to })`, the
same tagged-named-fields convention `ReplicaHash`/`BranchHash` already
use in `schema.rs` — so field roles, not positions, disambiguate the
direction, and concurrent identical additions converge instead of
duplicating. The relation is named `replicate` deliberately: it
documents the identity-join-only constraint in its own name (it is not
`transform`, and it is standing intent, not a `merge` event).

Three rules that keep it this small:

- **The hub is a peer whose DID is your own subject.** "origin" = me, at
  another site. Same-subject rendezvous and cross-repo tracking need no
  separate mechanisms — git doesn't distinguish "my origin" from "a fork
  I track" either; both are remotes. Cross-repo *push* is a tracking
  entry sourced from your branch toward a foreign-subject branch, plus a
  retained delegation.
- **Names never unify and never address.** A branch name is local to its
  subject, displayed qualified git-style (`main` is yours, `bob/main` is
  Bob's), and code refers only to entities. Same-name collisions
  surface as ambiguity, exactly like petname conflicts, resolved by the
  same address-book policy.
- **Pulling a peer delivers their registry.** Bob's `dialog.branch/*`
  facts live in Bob's tree; you never mint entities for his branches.

### Branch identity: derived or minted

The one genuine fork this schema surfaces. The protocol already follows
"name but never refer by name" — the head carries the opaque branch
identifier, and `version-control.md` is explicit that the name never
travels — but that identifier is name-*derived* (`hash(replica, name)`),
so the name is baked into identity even though it is never transmitted.

- **Derived** (`hash(subject, name)`, the status quo shape): rename is
  reincarnation — a new branch, and a new origin stream. The virtue is
  convergent creation: two devices creating "main" offline mint the same
  entity and their histories simply merge.
- **Derived + generation** (`hash(subject, name, generation)`, the
  generation derivable by query — count of prior registry entities with
  that name): the middle rung. Fixes reincarnation *in time* — create,
  delete, recreate "main" later, and the new branch does not inherit the
  ghost's history — at no new state, since prior generations are already
  registry facts. It cannot fix *concurrent* creation: two offline
  devices both observe N prior generations, both mint N+1, and converge
  anyway; generation only disambiguates when the predecessors are
  visible.
- **Minted** (opaque id at creation; the name a freely-editable
  attribute): rename is cheap and identity is stable. Two devices
  creating "main" offline mint two branches both named "main" — a
  *visible* ambiguity, like a petname conflict, instead of a silent
  unification. Fixes both the temporal and the concurrent case.

The minted option's cost is arguably correct behavior (silently merging
two independently-created branches because they share a default name is
a bug derived identity commits by construction), and its ambiguity is
handled by policy that must exist anyway. Its endgame is the
branches-as-subjects sideline: **the minted identifier wants to be a
DID**, making branch names petnames in the same address book as peers,
with delegation and cross-repo reference falling out for free. An opaque
minted entity preserves that upgrade without committing to it. What
minting complicates is bootstrap — a well-known name is no longer
computable to an entity — resolved as the retirement plan already does:
the entry-point cell keeps a conventional name-based *address* (names as
rendezvous paths chosen by the publisher are addresses, not references),
and everything in-tree refers by entity from there.

And where every kind of state lives, one row per thing:

| Git                       | Dialog today                    | Proposed                                  |
| ------------------------- | ------------------------------- | ----------------------------------------- |
| `[remote]` config         | `remote/{name}/address` cell    | **facts** (shared, versioned)             |
| `[branch]` tracking config| `upstream` cell (identity half) | **facts** (shared, versioned)             |
| `refs/heads/*`            | `branch/{name}/revision` cell   | cell — **stays** (the mutable pointer)    |
| `refs/remotes/*`          | cached remote-head cells        | local cache — **stays**                   |
| (implicit in remote refs) | sync base                       | local cache (pull) / local observation (push) — **stays** |

Everything below this section is analysis defending that table
(propagator framing, trust, quiescence) or generalizations deliberately
deferred (flows between arbitrary nodes, the ephemeral segment) — not
additional things being built.

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
3. **Sync state — the pointer-paced part only.** The push-side base,
   cached remote heads, and the induction watermark are per-replica
   observations that advance on fetch, independent of whether any
   novelty integrated. As facts they would mint revisions out of
   no-novelty (a scenario-2 advance recorded as a fact is bookkeeping
   the peer absorbs and echoes — see the quiescence analysis below).
   The *novelty-paced* part of sync state — which peer cut a merge
   integrated — belongs in the tree, riding the very commit the
   integration mints; the distinction is worked out below.

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

- **For pull it is a droppable diff-pruning cache.** The merge is
  correct from the empty base ("correct, just unable to skip anything",
  `pull.rs:56-60`, pinned by the empty-base non-resurrection tests), and
  the idle tick survives without it: scenario 2 is gated on watermark
  inclusion alone, base unconsulted, so "nothing new" stays zero-read.
  What dropping it costs is diff pruning when both sides genuinely
  diverged (graft disabled, deltas become whole trees). Never corrupts;
  needs no durable home; rebuilt by one full pull.
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

**Recording where peers are: the clock is already in the tree.** The
git-submodule intuition ("pull is `submodule update`: no change, no
commit; integrated novelty, commit") is correct, and what makes it safe
is that the pacing rule must be *causal*, not *pointer-based*:

- **An integrating pull already commits.** It mints or adopts a revision
  whose watermark is the union — so the data hash always reflects
  integration, and that watermark already records "which origins are
  where, as of this commit." The logical clock this section needs is not
  new machinery; it is the context that every head carries.
- **A peer-position annotation may ride that same commit.** An edge fact
  binding the peer to the integrated cut (peer P, its head's edition,
  its tree root as cited from P's *signed* head — verifiable third-hand)
  costs zero extra revisions when it travels with the merge commit, and
  concurrent recordings from two replicas converge as a per-origin max
  like everything else.
- **Why this does not ping-pong:** when P's movement is only our own
  record echoed back (P adopted our head), P's origins minted nothing,
  the inclusion check reports nothing new, no commit fires, no new
  record. Echoes are never novelty. The loop only afflicts the
  pointer-paced rule — "commit whenever the peer's head *hash* moves" —
  because a scenario-2 advance (peer moved, nothing new for us) would
  then mint novelty out of no-novelty, which the peer absorbs and echoes
  forever. Scenario-2 base advances are the one thing that must never be
  facts.
- **The pull-side stored base is then only a diff-pruning cache** — and a
  weaker one than it first appears: scenario 2 is gated on watermark
  inclusion alone (the base is not consulted), so idle pulls stay
  zero-read with no base at all; scenario 1's base-equality check is a
  micro-shortcut past the watermark comparison. The base earns its keep
  only when both sides genuinely diverged (O(delta) vs O(tree) diffs in
  scenarios 4/5). It can lapse and be rebuilt. (Verify when
  implementing: scenario 3's `tree == base` guard may be redundant with
  `theirs.includes(ours)` — if they have seen everything we have, unseen
  local novelty is nil by definition; if so even scenario 3 is
  watermark-only.)

**The correction the causal rule still needs: mutual annotations are two
clocks counting each other.** The echo argument covers adoption (a peer
adopting our head mints nothing), but an annotation is *not* an echo — it
is genuine novelty from a fresh origin edition. If both ends of an edge
annotate every integration, each side's annotation is data the other must
integrate and would in turn annotate: every round is a real origin
advance, and causal pacing alone never terminates it. This is exactly why
dialog keeps the watermark **on the head rather than in the tree**: the
head (tree root + watermark, signed) is the versioned object that
reflects "state changed", while keeping who-is-where tracking outside the
fixpoint. The rule that lets in-tree annotations exist anyway:
**annotate only when non-annotation novelty integrates.** Operationally:

- **Nothing is ever withheld from broadcast.** Publishing heads is
  untouched; the rule gates only whether the integrating commit *writes
  new annotation facts*.
- **No local-vs-integrated tracking is needed.** The test is on *what*
  the incoming novelty is, not *whose*: does the delta contain anything
  outside the reserved annotation namespace? That is an attribute-range
  test over the delta — the same range-scoped classification the span
  machinery performs. "Peer updates get bundled" falls out: an
  annotation-only delta simply triggers no counter-annotation.
- **The common case self-terminates even without the rule.** A commits
  data → B merges and annotates "A@1" → A pulls: A holds nothing B
  lacks, `theirs.includes(ours)` holds, A **fast-forwards by root,
  minting nothing** → B sees equal heads → settled. Annotations flow
  downstream, and the downstream side adopts rather than merges. The
  rule is a damper for one pathological schedule only: both sides
  holding concurrent novelty and annotating simultaneously, round after
  round. Under the rule each such merge is annotation-free, hence
  fast-forwardable next round, hence settling.
- **The rule is expressible in existing vocabulary.** Inductive rules
  declare `dialog.rule/on` and `dialog.rule/reads`; "a rule that never
  fires on its own conclusions" is a rule whose trigger pattern excludes
  its conclusion attributes — the general discipline against rule
  feedback, not an annotation special case.

Two companion observations:

- **Acknowledgments already exist and are already signed.** "Did B
  integrate my push?" is answered by B's next head: its watermark
  includes my origin at the pushed edition. A separate signed-ack
  artifact would be pointer-paced bookkeeping — banned above. And this
  gives *settled* a concrete representation: **an edge is settled ⇔ the
  two heads mutually include each other's watermarks** — a zero-read
  predicate on two signed values, observable by anyone, surfaceable as
  "in sync with origin".
- **The annotation wants to be an inductive rule conclusion, not a
  `pull.rs` hook.** Pull already owes a commit phase: integrated novelty
  must be screened against `dialog.rule/on` triggers, and the
  replica-local induction watermark exists precisely to pace rule firing
  over novelty without re-firing on one's own conclusions. A rule
  triggered by integration, concluding the edge annotation, inherits
  that discipline — the settling rule and the annotation share one
  mechanism instead of each being bespoke.

Where this leaves the sync-record design — three layers, the middle one
droppable:

1. **Watermarks on signed heads** (exists today): the automatic sync
   record. Acks and settledness come from it alone, loop-free by
   construction because it sits outside the tree.
2. **In-tree peer annotations** (optional): the inductive rule above.
   Buys *queryability* — "origin last integrated us at edition E" as an
   ordinary fact in the address book. If it proves fiddly, drop it;
   layer 1 carries the semantics, layer 2 is UX.
3. **Explicit pins** (deliberate): meaning-paced citations
   ("checkpoint", "adopted their release") that freeze an edge rather
   than track it — the other half of the submodule analogy.

What genuinely stays local shrinks to the push-side pair (the CAS token
on the remote's mutable pointer, and the residency certificate) — which
pin nothing and observe the outside world.

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
  Ed25519 checks per pull, no new signing anywhere — and no
  partial-replication cost, since the records verified are the ones
  riding the delta, their bytes in hand by construction.
  Fast-forward-by-root is the one path where verification would fight
  partial replication (records for regions adopted by reference may not
  be local) and where verbatim context adoption is the point (zero
  reads); there **trust is a per-edge tier** — and the tier need not be a
  new knob: it reads off the delegation graph already stored. Chain
  roots in your own authority (your devices, your operator) → trust
  blindly, adopt by root. A cross-party delegation relationship → trust
  but verify (records checked on absorb). No chain relationship → full
  verification, or no auto-fire. Delegation *is* the trust policy; the
  tiers are its distances.

  Heads *referencing their authorizing chain* (by envelope hash,
  resolving against the chains `delegations().retain` already keeps for
  the prover; the ephemeral profile → session leaf riding the head's
  cell value, replaced with it, never accumulated) is an optional
  extension whose scope must be stated honestly. The site-check and the
  puller-check are the same check from different vantage points, so
  chain-on-head changes not *when* authorization is checked but **who
  can check it and which sites are usable**: it is load-bearing only on
  the untrusted-site tier — a dumb bucket without the invocation gate, a
  mirror, a peer's relay. Where every site enforces pushes (today's
  deployments), it adds nothing, and it is not core machinery.
  Re-verification of *history* is neither possible with it nor a goal
  without it: nothing retains chains today either, and authorization is
  a property of **transfers, not of data at rest** — checked at the
  adoption boundary, like push rights, and consistent with
  revocation-does-not-rescind (a revoked collaborator's old commits
  stand, so their old chains prove nothing actionable). Audit-grade
  attestation, where wanted, is an explicit act — deliberately retaining
  a chain the way a pin retains a citation — never mechanical
  accumulation. Verification of current heads memoizes by envelope hash
  (chains change rarely), and the chains must be materialized to check —
  the same "must be local, must be verified" layer as the access branch.
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

## A generalization deferred: sites, nodes, and flows as entities

An earlier draft factored the model into five first-class relations
(sites, peers, nodes, rendezvous, flows). The short-version form keeps
the substance — branch entities are nodes, tracking entries are edges —
while shedding the extra ontology: sites are values on peers, and the
rendezvous/flow split collapses under the hub-is-a-self-peer rule. This
section is retained as the generalization target: if flows gain scopes
and owners as first-class attributes, the short-version relations widen
into these without reshaping (a tracking entry is a flow whose sink is
local; a peer site is a one-site rendezvous).

Cast: Alice (profile `zAlice`, laptop + phone), her repo `zNotes` with
branch `main`, a hub S3 bucket, Bob's repo `zBlog` she pulls from and may
push to.

**Sites** — where bytes live. Entity = hash of the address.

```text
site₁  dialog.site/address   <serialized SiteAddress for s3://hub…>
```

**Peers** — the address book. Entity = the DID (`did.this()`); the DID
also stored as an attribute for reverse queries (the redundant-by-design
convention `Replica` follows):

```text
bob    dialog.peer/did       "did:key:zBlog"
bob    dialog.peer/petname   "bob"
bob    dialog.peer/site      site₂
```

**Nodes** — the graph's vertices. Entity = `hash(subject, branch-name)`.
For the repo's own branches this relation *is* the branch registry from
the retirement plan: discovery of "which branches exist" and the vertex
set are the same facts.

```text
notes-main   dialog.node/subject  "did:key:zNotes"
notes-main   dialog.node/name     "main"
blog-main    dialog.node/subject  "did:key:zBlog"
blog-main    dialog.node/name     "main"
```

**Rendezvous** — what remote+upstream collapse into for the same-subject
case: a node × site binding, "copies of this node converge here". This
one fact replaces the `remote/{name}/address` cell and the identity half
of the upstream entry; laptop and phone pull it from the tree and know
where to sync.

```text
notes-main   dialog.node/rendezvous   site₁
```

**Flows** — true cross-subject edges. Entity = `hash(owner, source,
sink)`, where `owner` is the repo entity for shared wiring or the
`Replica` entity for device-local wiring — that one argument is the
entire shared/private distinction. A NAS mirror is the same shape with a
replica owner.

```text
flow₁  dialog.flow/source   blog-main          # input: we track Bob
flow₁  dialog.flow/sink     notes-main
flow₁  dialog.flow/of       <zNotes repo entity>

flow₂  dialog.flow/source   notes-main         # output: we may push
flow₂  dialog.flow/sink     blog-main
flow₂  dialog.flow/of       <zNotes repo entity>
flow₂  dialog.flow/proof    blob:bafy…         # Bob's delegation, retained
```

**Peer state, split by pacing** — this part is not deferred; it applies
identically to the two-table form. Most peer state is deliberately not
stored:

| State                                  | Where                                    | Why |
| -------------------------------------- | ---------------------------------------- | --- |
| peer's live head ("where is Bob now")  | local cell (cached fetch) → query-time overlay concept | an observation, refreshed by fetch; `BranchRevision` is the overlay precedent |
| settledness ("in sync with Bob")       | derived, stored nowhere: mutual watermark inclusion | a predicate, not a fact |
| integrated position ("Bob as of this revision") | ephemeral segment (preferred; loop-proof by construction) or an inductive-rule fact | the one tree-worthy piece, novelty-paced |
| push CAS token + residency             | local only, forever                      | per-replica observations; sharing corrupts CAS semantics |
| trust pins                             | local, profile-scoped, sealed            | trust anchors cannot live inside what they protect |

The sync daemon reduces to two queries either way. In two-table terms —
pull sources: sites of peers referenced by my tracking entries (self-DID
peers being the rendezvous case); push targets: tracking entries toward
foreign-DID peers, gated by proof verification and the local pin.

## Mounts: peers as databases, not sources

The reframe that simplifies everything above it. The note to this point
assumed every tracked peer is *integrated* — pulled through the screens
and merged into shared state. The better default is the symlink model:
most peers are **mounted**, not merged. A mount holds the peer's tree by
root reference (the by-reference adoption and lazy hydration machinery,
unchanged) and *exposes* it, read-only, as a named database — the
Datomic shape: a query takes a set of database values (`$local`,
`$bob`), cross-database joins are query expressions, and every fact is
forever attributed to its database. A repository becomes **an address
book plus a set of mounted databases, one of which is local**. The
exposure mechanism exists: `SessionBranch` already layers branches into
one read view; a mount is a foreign branch in that layering.

Merging degrades to what it should have been: an opt-in *convergence
relationship* — for one's own devices (same-subject replicas) and true
collaborators — a policy on a mount rather than the foundation:

```text
mnt   dialog.mount/of      br-b7        # entity = Entity::of(Mount { of, … })
mnt   dialog.mount/policy  "expose"     # or "integrate"
```

(`dialog.replicate/from|to` earlier is then the `integrate` policy; one
relation can carry both.)

What evaporates under mount-by-default: the entire insider-validity
problem — watermark inflation, verify-on-absorb, trust tiers — applies
**only to integrate-mounts**, because it is entirely about absorbing a
peer's claims as shared state. A mounted peer's facts stay theirs; a
signed head plus content addressing is the whole trust story, and no
context is ever absorbed. Most of the address book is mounts.

### The policy spectrum: expose, index, integrate

Federated query costs per mount joined, per query — the honest downside
of `expose`. But integration's performance benefit comes from the
**index**, not the **sharing**; historic integration carried two jobs at
once (building a union index, and converging state with the peer), and
only one's own devices and true collaborators need the second. Split
them and the policy space is three-valued:

- **`expose`** — federated query, nothing materialized. For
  rarely-queried peers.
- **`index`** — materialize a *local* union index over the mounted view,
  incrementally maintained on pull, disposable and rebuildable: a
  replica-local cache, never shared, no claims absorbed, no trust
  obligations, no ping-pong (it is not in the tree at all). This is
  what "integration derives indexed state on pull" was actually buying.
  The machinery is the incremental-view-maintenance shape of `dbsp.md` /
  `incremental-subscriptions.md`.
- **`integrate`** — true shared convergence, reserved for same-subject
  replicas and real collaborators, with the merge semantics and the
  (scoped-down) trust machinery.

### The version-vector view

The automerge correspondence is exact: origin = actor, edition/count =
seq, `Context` = the version vector, and "what a peer thinks each
origin's seq is" is the peer's *published context*, arriving on every
signed head — no new tracking needed. Reconstruction from an empty
store works because dialog is a state-based system that carries its own
op history: the log region holds every claim and revision record, so a
fresh replica pulling the tree receives the full change history, of
which the watermark is the lossless summary (exact because origins are
sequential). Automerge ships ops and derives state; dialog ships state
containing its ops — same information, opposite packaging.

### Lattice-land and fact-land

The principle beneath every ping-pong fix in this note, stated once:
the merge already implements datalog's no-novelty rule (semi-naive
evaluation) — but only in *lattice-land*. `Context::merge` with an
entry ≤ yours is a no-op; R1 screens observed claims; a pull carrying
nothing unobserved mints nothing. Joins are absorbing, so echoes die by
algebra: someone claiming your root is what it is joins to nothing.
Every ping-pong in this note arose the same way — moving
position-tracking into *fact-land* (annotations, pins-as-facts), where
an updated assertion has identity, and identity is novelty.

> Positions and acknowledgments live as lattice values, joined; facts
> are for assertions with identity. Ping-pong is a lattice value
> wearing fact clothing.

This also corrects the determinism story: a derivation's deterministic
input is the **revision**, and a revision is (tree root, edition,
**context**) — the context is fixed and signed at mint time, so rules
may read the watermark deterministically without it being tree facts.
The lockfile always existed; it is on the head. Fact-land pins are
needed only for a reproducible *mount basis* cited by shared
derivations — and there the reflection region is the structural
rendering of the same no-novelty rule (a stratum excluded from
peer-visible novelty), one principle in two forms.

### The reflection region: pins without ping-pong

(Concrete form: the **head map** of the staged plan's phase 2 — a
`{did → signed head}` lattice beside the data tree, its root riding the
head, joined per-key by edition.)

Mounted queries are deterministic only if the revision records *which*
bob — the pin. Three desiderata: pins in the tree hash (determinism),
automatic advancement (no ceremony), no ping-pong. Naively pick two:
auto + in-tree makes mutual mirrors reflect each other forever; in-tree
+ quiet is manual submodule bumps; auto + quiet keeps the basis at the
session layer like Datomic (passed at query time), losing derivation
reproducibility.

Stratification gets all three — the same move the system already made
twice (the head keeps the watermark out of the tree; the ephemeral
segment keeps tip-state out of the log). Designate a **reflection
region**: the part of a tree holding pins of *other* trees' roots.
Define a tree's **reflected root** as its root *excluding its own
reflection region*. Pins always pin reflected roots. Then:

- B updating its pin of A changes B's full root (signed, versioned, in
  B's hash — tree roots do belong in the tree) but not B's reflected
  root, so A observes no change and does nothing. Mutual mounts settle
  in one round, structurally: **a mirror never reflects the other
  mirror's reflection of itself.** No pacing rules, no annotation
  etiquette — the loop is dead by construction.
- Derivations joining `$local` with `$bob` read bob-at-pinned-root — a
  pure function of the local revision. The pin is flake.lock,
  auto-advancing and loop-free.
- Pins do not compose transitively: A's mount of B excludes B's pins,
  so A does not automatically see B's mount of C — like non-recursive
  submodules. A feature: transitive exposure is an explicit act.

Pin advancement is novelty-paced for free: a pin moves only when the
peer's reflected root moved, which is genuine data novelty by
definition. The explicit meaning-paced pin ("checkpoint", "adopted
their release") remains as a *frozen* mount — `policy: pinned` — the
submodule analogy's other half, now unified into the same relation.

## The convergence boundary

The theorem underneath this whole note, found by asking whether two
peers who follow each other could converge on one root. They cannot:
with pins in the tree, mutual convergence requires `R = H(data ∪
pin(R))` — a hash containing itself. Each side must omit its own root,
so their trees differ *definitionally*, before any content difference.
And the generalization is decisive in practice: even a magic fixpoint
for the symmetric pair dies when A and B follow different peer sets —
which is always.

The theorem does not say the design fails; it says **convergence and
following are different relations**, achievable at different boundaries:

- **Within a subject** (replicas of one node), hash equality is
  achievable — precisely because replicas share their follow-set by
  construction (the wiring is shared state) and positions live on the
  head, not in the tree. The head/tree split is the architecture's
  existing dodge of exactly this self-reference: a head is (root,
  edition, context) — data about the tree that could not live inside
  it, placed outside the hash it references. The watermark is on the
  head for the same reason A∪B's root cannot exist.
- **Across subjects**, root equality was never the goal and is
  impossible. The achievable relation is **mutual inclusion** —
  `ours.includes(theirs) ∧ theirs.includes(ours)` — causal equality at
  the lattice level while hashes differ. The "settled" predicate is not
  a convenience; it is the strongest cross-subject relation that can
  exist. Two git repos with identical commits still have different
  `.git` directories.

Corollary, resolving the blurred-boundaries question from early in this
note: names and storage can blur, but the convergence boundary is
derived, not chosen. **A repository is precisely a maximal set of
replicas that can share a root** — identical wiring, self-state
out-of-band. Follow-edges necessarily cross those boundaries; "repo" is
the equivalence class the theorem carves.

Constructively, when a converged A∪B is genuinely wanted: do not try to
make two follows converge — **mint a third subject C that both
integrate into**. C converges because it is one subject with one
follow-set; A and B remain sovereign. "A shared repo" is thereby
derived from first principles, and the user-facing choice becomes
explicit: *follow each other* (mutual inclusion, sovereign roots) or
*share a space* (mint C, converged root) — two primitives with
different guarantees, both honest.

## Determinism at the seams

The worry the split state model raises: peer state changes without the
tree hash reflecting it, and resolvers operate from a specific revision —
does the seam introduce nondeterminism, the way an unpinned fetch does in
a build system?

No — because the seam admits state through exactly one door, and that
door mints a revision. The only way a peer's state affects the tree is
integration: a pull that screens it and commits, after which it is
ordinary in-tree state. Before that it is invisible to everything
revision-scoped. Peer heads are in the same category as a user typing a
fact: **input, not state** — unreflected until commit, and that is what
makes everything downstream of commit reproducible. "Resolvers operate
from a specific revision point" is not a limitation; it is the
determinism guarantee.

The Nix correspondence is exact. Nix does not make the network
deterministic — it confines impurity to fixed-output derivations
(impure fetch, content-verified) and evaluates purely from pinned
inputs:

| Nix                                   | Dialog                                          |
| ------------------------------------- | ----------------------------------------------- |
| flake input spec                      | `dialog.replicate` relation + peer's site       |
| fixed-output fetch (impure, verified) | fetch + signature-verified head                 |
| flake.lock                            | the integrated-position annotation              |
| pure eval from pinned inputs          | rules/queries as pure functions of the revision |
| `nix flake update`                    | pull                                            |

The one question no deterministic system answers from inside a pinned
evaluation is "is upstream current *right now*" — Nix cannot either;
that is what `flake update` is for. Not a leak; the definition of now.

Two consequences:

- **A sharp rule:** derivations read only the revision; observation
  overlays (cached remote heads) are for humans and UI, never rule
  inputs. The query surface is two-tier: *pure* (revision-scoped, what
  rules and resolvers see) and *situated* (joins in observations, what a
  status view shows, non-authoritative by construction).
- **The lockfile already exists — on the head.** (Correcting an earlier
  draft of this bullet that promoted in-tree annotations to lockfile
  status.) The revision is (tree root, edition, context); the context
  is fixed and signed at mint, so a derivation reading the watermark is
  reading its own revision — deterministic without any tree facts. See
  "Lattice-land and fact-land" under Mounts: in-tree position facts are
  needed only as a mount basis cited by shared derivations, and they
  live in the reflection region.

## The ephemeral segment, and commits as invocations

Two ideas that are one idea. If a commit is a UCAN invocation, the
revision record does not grow — it is *replaced* by the invocation
envelope (same issuer, same signature, plus the proof reference), so
record size stays flat while authorization becomes intrinsic to the
commit. And "the chain held only by the tip" generalizes: a tree region
whose entries live in the current revision only — **not retained in the
indexes' history, minting no history records, riding no log** — an
ephemeral segment.

What it buys: invocations at the tip; cursor position, presence, and
other per-moment state that is relevant now and must not accrete; and a
cleaner resolution of the annotation loop than the rule damper — an
ephemeral write mints no history record, hence no novelty, hence nothing
a peer can counter-annotate. Peer positions become loop-proof *by
construction*.

What it costs: the region has no OR-set log behind it, so it needs its
own merge rule — per-origin last-writer-wins is the honest one (exactly
what a CAS cell provides today, now namespaced inside the tree) — and it
must be excluded from watermark and coverage reasoning, since nothing in
it is causally tracked. Conceptually it is the head's payload
generalized into a structured region: the fixpoint-exempt state finally
gets a home *inside* the tree hash without entering the fixpoint.

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

Three phases; value ships at each; none breaks existing cells.

**Phase 1 — config as facts. No new tree machinery.**
`dialog.peer/*` (address book), `dialog.branch/*` (registry; derived
identity `hash(subject, name)` to start, upgradeable later),
`dialog.replicate/*` (what tracks what). Resolution order: local
override → tree → bootstrap cell. The sync daemon stays the imperative
loop it is today — it reads facts instead of cells. Nothing reads peer
*state* from the tree yet, so no lock root is needed for correctness.
Deliverables: replicated versioned config, self-configuring replicas,
branch enumeration, branch cells (except heads) retired. Output push
targets additionally gate on retained proofs plus the local pin set.

**Phase 2 — the head map.** Each branch publishes two roots: the
**data root** (the tree as today — facts and config; what peers pin)
and a **head map** — `{did → signed head}`, one entry per followed
peer, the entry a content-addressed blob of that peer's head — the existing
`Revision` value of `revision.rs`: branch identifier, issuer, tree
root, edition, context, signature (so one reference carries the peer's
root, position, knowledge, and proof of authorship, nothing new
invented). The head
grows one field: `(tree, map, edition, context)`.

The map is a **lattice value**, and that is the load-bearing property.
Merge is per-key: accept an incoming entry for `b` iff its signature
verifies and its edition exceeds the current entry's — per-key max over
each DID's own sequential chain, so the join is commutative,
idempotent, absorbing: `{a→v1, b→v2} ⊔ {b→v3} = {a→v1, b→v3}`, and
receiving `{b→v2}` again is a no-op. By the lattice-land principle,
ping-pong is impossible with no special rules — echoes join to nothing.

It also dodges the convergence theorem legitimately: the theorem
forbids *tree* equality (each tree would contain the other's root); the
map converges because entries reference only data roots and the map
lives outside every data tree — no entry contains the map. So
convergence returns at the meta level: **peers with the same follow-set
converge on the identical map** while their data trees stay sovereign,
and *settled* gets its strongest formulation — the maps are equal.

Free consequences: **transitive gossip** (entries are self-certifying —
b's head signed by b — so `b→v3` can be learned from anyone's map,
verified per-entry, fixing the non-composability of private pins);
**rollback protection** (per-key max: a relayer can withhold but
neither forge nor roll back); **determinism** (a derivation reads its
revision's map snapshot, fixed and signed at mint). One rule makes
unfollow stick: the join is **scoped to the follow-set** — accept and
publish entries only for DIDs you follow — else union-of-keys
resurrects unfollowed peers; with scoping, same-follow-set peers still
converge identically and different follow-sets agree on their
intersection. And one rule keeps the map's own churn out of the
lattice: since the head now carries the map root, a peer's head changes
when only their map changes — so **entries replace only on strictly
greater edition, and updating the map mints no edition** (head-level
metadata, versioned by replacement like the context). A map-only
republication arrives with an unchanged edition and joins to a no-op;
the ripple stops after one hop, and a peer's latest map is still seen
by fetching them directly. This is flake.lock as a convergent, gossipable lattice —
the concrete and final form of the reflection region, and the one
isolated piece of new machinery.

**The map is the context's sibling, and must stay its sibling.** The
context is the same genus — a per-actor version vector, lattice-joined
per-key, riding the head — so the map is a second instance of a kind
that already works, not a new invention. They cannot be one structure,
for three reasons of increasing weight: context keys are *origins*
(one-way hashes, many per peer — not invertible to a DID you can look
up); context values carry no tree roots (nothing in a watermark
dereferences to content, and `did → root` is the map's whole purpose);
and decisively, the context is an *ancestry* summary whose exactness
feeds `observes()` and the R1 screen — extending it with entries for
followed-but-not-integrated peers would make their claims screen as
already-seen and be silently discarded on a later genuine integration,
the watermark-inflation failure mode self-inflicted. The clean split:
**the context answers "what do I contain" (backward-looking, feeds the
screens, exactly ancestral); the map answers "what can I reach"
(outward-looking, feeds mounts and gossip, scoped to follows).** For
integrate-peers they overlap — the earlier "the lockfile was on the
head the whole time" observation; the map adds exactly what the
context structurally cannot: dereferenceable roots, and coverage of
peers mounted without integration.

**Phase 3 — the query surface over mounts.** `$bob` in queries via the
session layering; `expose` first; the `index` policy (local
materialized union view, incrementally maintained, disposable) when
performance demands it. Builds entirely on phases 1–2.

Later, separately argued: scoped replication, notification, minted or
DID branch identity, branches as first-class nodes.

### Phase 1, concretely

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
