# Deterministic operator derivation

## The problem

An operator is a session-scoped identity derived from a profile with a
context seed (`profile.derive(b"my-app")`). Until now `derive_operator`
took one of two paths depending on whether the profile key could give up
its seed:

```rust
KeyExport::Extractable(seed)  => blake3::derive_key(CTX, seed ++ context)
KeyExport::NonExtractable{..} => blake3::derive_key(CTX, sign(CTX ++ context))
```

The second path uses an Ed25519 signature as a pseudo-random function.
That is not a property Ed25519 guarantees. RFC 8032 specifies a
deterministic nonce, but hedged variants — fresh entropy folded into the
nonce to resist fault injection and side-channel attacks — are a
deliberate, conforming choice, and Apple's CryptoKit, which backs
WebKit's `Ed25519`, makes it. The WebCrypto specification says nothing
about signature determinism either way.

So in Safari the profile signs the same message twice and gets two
different signatures, and the profile derives a different operator on
every page load. Expect the same from any key held in a secure enclave,
an HSM, or a passkey. Safari is not the defect; the assumption was.

### Why a churning operator key is worse than a churning DID

`Origin::derive_from_identifiers([branch_entity, issuer_did])` puts the
operator DID inside the sequential-actor identifier of the version
clock, and an `Origin` must name exactly one sequential actor. A fresh
operator key per page load therefore mints a fresh replica lineage per
page load, and the version vector grows without bound. The identity
churn is the visible symptom; the lineage churn is the damage.

### Two further defects in the same function

- **The two paths disagreed.** The same profile seed derived one
  operator natively and a different one in the browser.
- **The derived private key was a function of a published value.** A
  signature is made to be handed out; the message being signed was a
  fixed, public, non-domain-separated byte string. Anyone who observed
  the profile signing those exact bytes held the operator private key.

## What the operator key actually has to be

Two requirements were bundled into "derive it from the profile key", and
only one of them is real.

1. **Stable for a given (profile, context).** Required, because of
   `Origin`.
2. **Reproducible on any device holding the profile.** Not required.
   Profile keys are device-bound: in the browser they are non-extractable
   `CryptoKey`s that cannot leave the origin, and nothing outside
   `dialog-credentials`' own tests opts into `ExtractableKey`. No second
   device can hold the profile, so there is nothing for cross-device
   reproducibility to buy.

Native profiles are still an extractable seed in a file, which is a
separate matter being addressed on its own terms. Note the consequence
while it lasts: copying a native profile directory to a second machine
gives both machines the same operator key, hence the same `Origin`, hence
colliding `Version`s. That is a property of a copyable profile, not of
this derivation.

## The scheme

Derive from an operation that is deterministic by construction rather
than by implementation choice: Diffie-Hellman.

Every signing key already carries an X25519 agreement key. On native it
derives from the seed on demand; in the browser it is derived once at
generation or import and archived alongside the signing key, because a
non-extractable `CryptoKey` never yields its seed again. Agreement is a
pure function of (secret, peer) on every platform — `deriveBits` cannot
be hedged, because there is no nonce to hedge.

The peer is the profile's **own** agreement public key:

```
shared = X25519(a, a·G) = a²·G
seed   = HKDF-SHA256(shared, info = label ‖ 0x01 ‖ a·G ‖ context)
```

### Why the profile's own public key

The peer point must have an unknown discrete log. If it were `k·G` for
any publicly known `k` — a hash-to-curve point, a nothing-up-my-sleeve
constant — then the shared secret would be `k·(profile agreement public
key)`, which anyone holding the public key could compute. The profile's
own public key is the one point already in hand whose discrete log is the
profile's secret.

The assumption is squaring Diffie-Hellman: given `a·G`, compute `a²·G`.
In a prime-order group that is equivalent to CDH, and X25519's clamping
clears the cofactor, so the agreement lands in Curve25519's prime-order
subgroup.

It also avoids the alternative's real hazard. Hashing to a curve point
means importing an arbitrary u-coordinate through
`importKey("raw", …, "X25519")`, which may land on the twist and which
Safari may or may not accept — and CI only ever launches Chromium
(`flake.nix`), so that is precisely the class of assumption we cannot
test. Self-agreement adds no new import: both `CryptoKey`s are already
held on `AgreementSecretKey`.

### Domain separation

Derivation runs through the same `secret::Context` the sealed-secret path
uses, so the label is a compile-time constant that versions with the
meaning of what is derived. The HKDF `info` for a derivation is tagged
`0x01` where sealing tags `0x00`, which keeps the two domains disjoint by
structure rather than by the labels happening to differ.

The same agreement key now backs both sealed secrets and derivation. An
attacker cannot reach the derivation secret through the sealing path:
`conceal` always generates a fresh random ephemeral key, and forging a
`SealedSecret` whose ephemeral public key is the profile's own would
still require the AEAD key, which is what knowing `a²·G` would have
given them.

### Cost

One `deriveBits` and one HKDF per `build()`, replacing one signature and
one hash.

## What this changes

Both arms of `derive_operator` collapse into one derivation that agrees
across targets, so a profile seed now yields the same operator DID
natively and in the browser. That is a change in derived identity for
every existing profile: the operator DID appears in `revision.issuer`,
in the `Origin` of past revisions, and in the `db:session` provenance
facts, so each profile's lineage forks exactly once at the upgrade. In
Safari that fork is currently happening on every page load, so the
one-time cost is strictly an improvement there.

## Profiles with no agreement key

`KeyExport::NonExtractable.agreement` is optional: it is `None` for
exports written before agreement keys existed (#463, with the wrapped
archive in #473). Such a profile has no seed to re-derive from and
therefore cannot derive an operator under this scheme. It fails with
`AgreementKeyUnavailable` rather than deriving something wrong. The only
remedy for an affected profile is to create a new one; no in-place
migration exists, because the material needed to perform one is exactly
what a non-extractable key withholds.

## Testing

CI runs wasm tests in Chromium only, so no browser test can catch a
relapse into signature-based derivation. The guard is a known-answer
vector instead: a fixed seed derives a fixed operator DID, asserted by
the same test on native and wasm. It fails immediately, on every
platform, if the derivation stops being a pure function of the key — and
it is the reason the native and browser paths cannot silently diverge
again. Do not delete it as redundant with the determinism tests; it is
the structural one.
