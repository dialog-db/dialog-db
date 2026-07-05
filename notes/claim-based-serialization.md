# Claim-Based Serialization: Remove Serialize from Capabilities

## Status

The `Claim` trait and `#[derive(Claim)]` macro are implemented. What remains is removing the `Serialize + DeserializeOwned` requirement from `Caveat` and switching the UCAN parameter collection to use claim projections instead of direct serialization.

## Problem

Every capability type (attenuations, policies, effects) must implement `Serialize + DeserializeOwned` because `Caveat` requires it. This was needed before the `Claim` system existed, as effects were serialized directly for UCAN authorization.

Now that `Claim` generates separate serializable claim types, the requirement on the effects themselves is vestigial. It prevents effects from carrying non-serializable runtime types like `Ed25519Signer` or `CryptoKeyPair`.

## Current State

```
Effect: Sized + Caveat + Claim
Caveat: Serialize + DeserializeOwned       <-- forces effects to be serializable
Claim { type Claim: Serialize + DeserializeOwned }  <-- the actual serializable form

Attenuation: Sized + Caveat                <-- forces attenuations to be serializable
Policy: Sized + Caveat                     <-- forces policies to be serializable
Ability: Sized + Serialize + DeserializeOwned
```

The UCAN path: `parameters(capability)` calls `capability.constrain(&mut builder)` which walks the chain. Each node calls `builder.push(self)` which does `to_ipld(self)`, requiring `Serialize`.

## Proposed Change

Move serialization from the types themselves to their claim representations.

### Step 1: Change `Caveat` to use claims

```rust
// Before (current)
pub trait Caveat: Serialize + DeserializeOwned {
    fn constrain(&self, builder: &mut impl PolicyBuilder);
}

impl<T: Serialize + DeserializeOwned> Caveat for T {
    fn constrain(&self, builder: &mut impl PolicyBuilder) {
        builder.push(self);
    }
}

// After
pub trait Caveat {
    fn constrain(&self, builder: &mut impl PolicyBuilder);
}
```

No blanket impl. Each type implements `constrain()` by pushing its claim representation.

### Step 2: Make `Claim` provide `constrain()`

Merge `Caveat` into `Claim`:

```rust
pub trait Claim {
    type Claim: Serialize + DeserializeOwned;
    fn claim(self) -> Self::Claim;
    fn constrain(&self, builder: &mut impl PolicyBuilder);
}
```

For types where `Claim = Self`, `constrain` just pushes `self`. For types with non-serializable fields, `constrain` pushes the claim type.

### Step 3: Drop `Serialize + DeserializeOwned` from trait bounds

```rust
pub trait Effect: Sized + Claim { ... }
pub trait Attenuation: Sized + Claim { ... }
pub trait Policy: Sized + Claim { ... }
pub trait Ability: Sized { ... }
```

### Step 4: Make `Constrained` and `Capability` conditionally serializable

Only implement `Serialize`/`Deserialize` when all parts are serializable, rather than deriving unconditionally.

### Step 5: Update `parameters()` to use claim projection

Use `Claim::constrain()` instead of `Caveat::constrain()` for UCAN parameter collection.

## Impact

- Most types unaffected: where `Claim::Claim = Self`, behavior is identical
- Types with projections (`Put`, `Set`, `Publish`) already have derive macros that work
- `access::Prove<P>` can carry `by: P::Issuer`, projected to `Did`
- Eliminates the two-step Proof/claim(signer) flow

## Files to Modify

| File | Change |
|------|--------|
| `dialog-capability/src/settings.rs` | Remove `Serialize + DeserializeOwned` from `Caveat`, or merge into `Claim` |
| `dialog-capability/src/claim.rs` | Add `constrain()` method |
| `dialog-capability/src/effect.rs` | Drop `Caveat` from `Effect` bounds |
| `dialog-capability/src/attenuation.rs` | `Caveat` to `Claim` |
| `dialog-capability/src/policy.rs` | `Caveat` to `Claim` |
| `dialog-capability/src/ability.rs` | Drop `Serialize + DeserializeOwned` |
| `dialog-capability/src/constrained.rs` | Conditional `Serialize`/`Deserialize` impl |
| `dialog-capability/src/capability.rs` | Conditional `Serialize`/`Deserialize` impl |
| `dialog-macros/src/lib.rs` | Update `Claim` derive to generate `constrain()` |
| `dialog-ucan/src/scope.rs` | Use `Claim::constrain` instead of `Caveat::constrain` |
