# `/ucan/revoke`: artifact, witness path, and what validates what

## The division of labour

`InvocationChain::verify` establishes that an invocation is a valid invocation: signature, `prf` chain (linkage, rooting, attenuation, policy), time bounds, and — since the verification-context work — that no `prf` hop is revoked. All command-agnostic.

For a revocation that means exactly one thing: *whoever signed this was authorized to issue an invocation with `sub` as its subject.* Nothing about `rev` or `pth`, which are opaque arguments to it.

Everything else is `/ucan/revoke`-specific and lives on `RevocationChain`. The failure mode to avoid is re-deriving what `verify` already does: hand-rolled reimplementations of the generic path are where holes come from, each one missing something the shared code already handled.

**Deliberately out of scope:** whether the service holds the subject. That is deployment policy, not artifact validity — the same artifact gets a different answer per deployment.

## `prf` and `pth` answer different questions

- `prf` — may this principal invoke `/ucan/revoke` at all?
- `pth` — why is this principal authorized to revoke *this particular* delegation?

They can rest on entirely different grants, so neither substitutes for the other.

## Shape: `Revocation<S>`

Per `revocation.ipldsch` (the prose README is stale — it still says `do`, `ucan/revoke` without the leading slash, and `revoke`/`path`):

```
type RevocationAction <: Action {
  cmd "/ucan/revoke"
  nnc ""
  arg RevocationArguments
}

type RevocationArguments struct {
  rev &Delegation
  pth [&Delegation]
}
```

`Revocation<S>(Invocation<S>)` with `TryFrom<Invocation<S>>` checks exactly that: the command, the empty nonce, `rev` is a link, `pth` is a *list* of links. Every failure is `MalformedRevocation` — malformed input, never "unauthorized". Authority is a separate question asked later, and only once the shape is known to hold.

Two shape decisions worth naming:

- **A bare link is not a path.** `pth` is `[&Delegation]`; accepting a single link would make the one-hop case shaped differently from every other, which is the sort of special case that later grows a bug.
- **An empty `pth` is shape-valid.** An empty list is a list. Whether it *justifies* anything is `validate`'s question, and it cannot — `rev` is not in an empty path — so this splits cleanly into "well-formed artifact" versus "artifact that proves its authority".

## `RevocationBuilder`: the revoker is the subject

Deriving `sub` from the revoked delegation's subject conflates two different questions in one field: *what the capability was about* and *who is withdrawing it*. The builder takes the revoker explicitly, so `sub` means the revoker. That also keeps `{what}` and `{by}` independent for keying.

## `RevocationChain::validate`

### Four cases need no evidence at all

For these, `pth` is neither required nor examined — supplying one is harmless, supplying garbage is equally harmless:

- **revoker is the target's audience** — refusing what you were handed is not a claim of authority over anyone else, it is declining your own grant
- **revoker is the target's issuer** — you may withdraw what you issued
- **revoker is the target's subject** — it is your capability
- **the target is a powerline** (`Subject::Any`) — its holder can mint a delegation for *any* subject, so no witness could prove anything a forger could not manufacture. Requiring one would be theatre.

### Otherwise `pth` must witness that the revoker *held* the capability

`pth` is a **pool, not an ordered chain**. Carrying more than is relevant is not an error.

The relevant subchain is the walk from a hop issued by the target's subject — the principal whose capability it is — following audience-to-issuer links, until a hop delegates to the revoker. Hops off that walk are ignored.

**The evidence proves possession, not a path to the target.** Whether a chain runs from the revoker down to the delegation being revoked is irrelevant: holding the capability, the revoker could always have created one, so its absence proves nothing. This is why the walk ends at the revoker rather than at the target.

It also means a holder may revoke a hop it descends from. Dave, holding `alice → bob → carol → dave`, may revoke `bob → carol` — cutting off his own authority in the process, which is his to do.

Over that walk:

1. **It must reach the revoker from the subject.** Evidence that never arrives witnesses nothing. Failure is `NoEvidenceOfPossession`, which also covers "no evidence at all" and "evidence rooted somewhere else".
2. **Alignment and time**, via `check_chain` — the same implementation `syntactic_checks` uses, so a witness path cannot drift from a proof chain.
3. **Every hop signed by its claimed issuer.** Without this the walk is a story rather than evidence.

A powerline hop *within* the walk stands in wherever a hop for the subject would, since a powerline implies its own subject.

### Reuse, not reimplementation

Check 3's properties are pure chain properties — they need no invocation. They were previously entangled inside `syntactic_checks` with the two that *do* need one (command attenuation against `self.command`, policy predicates over `args`).

They are now `delegation::chain::check_chain(hops, subject, now)`, called by both.

### Error taxonomy

Three findings, because a caller needs to act on them differently:

```rust
pub enum RevocationError<E> {
    Invalid(String),              // not a valid invocation at all
    Denied(Denial),               // valid invocation, evidence insufficient
    Unavailable { did, detail },  // could not establish either
}

pub enum Denial {
    NoEvidenceOfPossession { subject, revoker },
    Path { source: CheckFailed },
    HopSignature { issuer, detail },
}
```

`RevocationChain::verify` runs both halves and returns this: `InvocationChain::verify` first (yielding `Invalid` on failure), then `validate`. Keeping `Invalid` distinct from `Denied` matters because they are different findings — one says the artifact is not a valid invocation before any question of revocation authority arises.

Separately, `MalformedRevocationChain` says the artifact could not be *read* (bad shape, or a named block absent from the container).

### Why `pth` hops are not screened for revocation

Considered and rejected. Revocation is monotonic and irreversible: if a grant cited in `pth` was itself revoked, everything downstream of it is already dead, so a revocation citing it is redundant rather than dangerous.

The case that *does* matter is already covered: if the revoked grant appears in `prf`, `verify` refuses the whole invocation and it never reaches `validate`.

## Tests

Shape (`revocation::action`) — the builder produces a well-formed revocation with the revoker as subject; another command is not a revocation; a nonce makes it malformed; each argument is required; a bare link is not a path; a non-link argument is malformed; an empty path is shape-valid.

No evidence required (`container::revocation`):
- an audience may refuse its own grant
- an issuer may withdraw what it issued
- a subject may revoke its own capability
- a powerline target needs no evidence
- **a powerline target's path is not even examined** — garbage evidence passes

Evidence required, accepted:
- an intermediary may revoke with a witness path
- **extra hops outside the relevant walk are ignored**
- a powerline hop roots the walk
- **evidence need not reach the target** — a holder may revoke a sibling branch
- **a holder may revoke a hop it descends from**

Evidence required, denied:
- an empty path denies an intermediary
- a path not rooted at the subject
- a stranger on no part of the path
- an expired hop in the walk
- **a forged hop signature**

Error taxonomy:
- **an invalid invocation is distinguishable from a denial**
- a valid invocation with good evidence verifies end to end

Malformed rather than denied: a missing block.

Chain properties (`delegation::chain`): empty chain, linked chain, broken link, wrong root, expired hop.

## Open

- **Delegated revocation.** The spec mentions revocations that are themselves delegated ("is the revocation based on a delegated revocation"). Not handled: check 3 requires the revoker to be the target hop's issuer or audience directly.
