# Claim Projection Refactor

## Problem

`Caveat: Serialize + DeserializeOwned` is required on every type in the capability chain (`Effect` -> `Attenuation` -> `Policy` -> `Caveat`). This forces all effect types to be directly serializable, which prevents effects like `access::Claim<P>` from carrying non-serializable fields (e.g., `P::Issuer` which is a signer/keypair that lives in IDB on wasm).

The `#[derive(Claim)]` macro already generates authorization-safe projections (e.g., `Put { content }` -> `PutClaim { checksum }`), but serialization bypasses this and serializes the raw type directly.

## Goal

Serialize capability chains via their `Claim` projections, not raw types. This allows effects to contain non-serializable runtime values (signers, file handles, etc.) as long as their claim projection replaces them with serializable equivalents.

This enables `access::Claim<P> { by: P::Issuer, access, time }` where the claim projection replaces `by` with `Did`. The store receives the real signer and can build full `Authorization` directly, eliminating the two-step ProofChain/claim(signer) flow.

## Current Chain

```
Capability<Get> wraps:
  Constrained<Get, Constrained<Catalog, Constrained<Archive, Subject>>>

Each piece requires: Serialize + DeserializeOwned (via Caveat)
```

## Proposed Chain

Each piece requires `Claim` instead. The `Claim` trait maps each type to its authorization-safe projection:

```
Archive         -> Claim::Claim = Archive          (identity)
Catalog { name} -> Claim::Claim = Catalog { name } (identity)
Get { key }     -> Claim::Claim = Get { key }      (identity)
Put { content } -> Claim::Claim = PutClaim { checksum } (projected)
access::Claim<P> { by: Issuer } -> ClaimClaim<P> { by: Did } (projected)
```

## Changes

### 1. `Claim` impl for `Subject`

```rust
impl Claim for Subject {
    type Claim = Self;
    fn claim(self) -> Self { self }
}
```

### 2. `Claim` impl for `Constrained<C, Of>`

Maps through both constraint and capability:

```rust
impl<C: Claim, Of: Claim> Claim for Constrained<C, Of> {
    type Claim = Constrained<C::Claim, Of::Claim>;
    fn claim(self) -> Self::Claim {
        Constrained {
            constraint: self.constraint.claim(),
            capability: self.capability.claim(),
        }
    }
}
```

### 3. `Capability<T>` serialization

Serialize via claim projection:

```rust
impl Serialize for Capability<T>
where
    T::Capability: Claim,
    <T::Capability as Claim>::Claim: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.can.clone().claim().serialize(serializer)
    }
}
```

### 4. `Caveat` trait

Drop `Serialize + DeserializeOwned` requirement. Use `Claim` instead:

```rust
pub trait Caveat: Claim
where
    <Self as Claim>::Claim: Serialize + DeserializeOwned
{
    fn constrain(&self, builder: &mut impl PolicyBuilder);
}
```

### 5. Drop `Serialize + DeserializeOwned` from

- `Policy` (currently via `Caveat`)
- `Attenuation` (currently via `Caveat`)
- `Effect` (currently via `Caveat`)
- `Ability` (currently direct bound)

### 6. `constrain` uses claim projection

```rust
fn constrain(&self, builder: &mut impl PolicyBuilder) {
    builder.push(&self.clone().claim());
}
```

## Impact

- Most types unaffected: where `Claim::Claim = Self`, behavior is identical
- Types with projections (`Put`, `Set`, `Publish`) already have derives that work
- `access::Claim<P>` can carry `by: P::Issuer`, projected to `Did`
- Eliminates ProofChain/claim(signer) two-step flow
- `Provider<access::Claim<P>>` returns full `Authorization` directly

## Considerations

- **Deserialization**: Produces the claim-projected chain, not the original. Fine for verification but not for re-execution. This matches the intent: deserialized capabilities represent claims, not invocations.
- **Recursive Claim bounds**: `Constrained<C::Claim, Of::Claim>` must be `Serialize + DeserializeOwned`. Need to ensure `Claim::Claim` types satisfy this recursively. For identity projections (`Claim = Self`) this is automatic.
- **Clone overhead**: `clone().claim()` in serialization adds overhead for types with large payloads, but those are exactly the types that benefit from projection (e.g., `Put` with large content becomes `PutClaim` with small checksum).
