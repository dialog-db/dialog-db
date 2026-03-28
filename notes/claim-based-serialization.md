# Claim-Based Serialization — Remove Serialize from Capabilities

## Problem

Every capability type (attenuations, policies, effects) must implement `Serialize + DeserializeOwned` because `Caveat` requires it. This was needed before the `Claim` system existed — effects were serialized directly for UCAN authorization.

Now that `Claim` generates separate serializable claim types, the requirement on the effects themselves is vestigial. It prevents effects from carrying non-serializable runtime types like `Ed25519Signer` or `CryptoKeyPair`.

## Current State

```
Effect: Sized + Caveat + Claim
Caveat: Serialize + DeserializeOwned       ← forces effects to be serializable
Claim { type Claim: Serialize + DeserializeOwned }  ← the actual serializable form

Attenuation: Sized + Caveat                ← forces attenuations to be serializable
Policy: Sized + Caveat                     ← forces policies to be serializable
Ability: Sized + Serialize + DeserializeOwned

Constrained<P, Of>: derive(Serialize, Deserialize)
Capability<T>: derive(Serialize, Deserialize)
```

The UCAN path: `parameters(capability)` calls `capability.constrain(&mut builder)` which walks the chain. Each node calls `builder.push(self)` which does `to_ipld(self)` — requires `Serialize`.

## Proposed Change

Move serialization from the types themselves to their claim representations.

### Step 1: Change `Caveat` to use claims

```rust
// Before
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

No blanket impl — each type implements `constrain()` by pushing its claim representation.

### Step 2: Make `Claim` provide `constrain()`

```rust
pub trait Claim {
    type Claim: Serialize + DeserializeOwned;
    fn claim(self) -> Self::Claim;

    // New: serialize the claim form for policy collection
    fn constrain(&self, builder: &mut impl PolicyBuilder);
}
```

Or merge `Caveat` into `Claim`:

```rust
pub trait Claim {
    type Claim: Serialize + DeserializeOwned;
    fn claim(self) -> Self::Claim;
    fn constrain(&self, builder: &mut impl PolicyBuilder);
}
```

For types where `Claim = Self` (most attenuations/policies), `constrain` just pushes `self`. For types with non-serializable fields, `constrain` pushes the claim type.

### Step 3: Drop `Serialize + DeserializeOwned` from trait bounds

```rust
pub trait Effect: Sized + Claim { ... }           // drop Caveat
pub trait Attenuation: Sized + Claim { ... }      // Caveat → Claim
pub trait Policy: Sized + Claim { ... }           // Caveat → Claim
pub trait Ability: Sized { ... }                  // drop Serialize + DeserializeOwned
```

### Step 4: Make `Constrained` conditionally serializable

```rust
// Remove derive(Serialize, Deserialize)
pub struct Constrained<P: Policy, Of: Ability> {
    pub constraint: P,
    pub capability: Of,
}

// Manual impl — only when all parts are serializable
impl<P, Of> Serialize for Constrained<P, Of>
where
    P: Policy + Serialize,
    Of: Ability + Serialize,
{ ... }
```

### Step 5: Same for `Capability<T>`

```rust
// Conditional serialization
impl<T: Constraint> Serialize for Capability<T>
where
    T::Capability: Serialize,
{ ... }
```

### Step 6: Update `Claim` derive macro

The `#[derive(Claim)]` macro currently assumes the type is `Serialize`. After the change:
- For simple types (no `#[claim]` attributes): generates `type Claim = Self` and `constrain` pushes `self` (type still needs `Serialize`)
- For types with `#[claim]` fields: generates a separate claim struct and `constrain` pushes the claim (type doesn't need `Serialize`)

### Step 7: Update `parameters()`

Currently calls `capability.constrain(&mut builder)` which relies on `Caveat`. After the change, it calls `Claim::constrain()` instead — same signature, different source.

## Impact

### Types that change nothing
Most existing types have `Claim = Self` and already derive `Serialize + Deserialize`. They continue to work exactly as before — `constrain` pushes `self`.

### Types that benefit
- `repository::Save { credential: Credential }` — `Credential` doesn't need `Serialize`. The claim maps it to a DID.
- Future effects carrying `Ed25519Signer`, `CryptoKeyPair`, or other non-serializable runtime types.

### Breaking changes
- `Caveat` trait changes (or is removed/merged into `Claim`)
- `Ability` drops `Serialize + DeserializeOwned` bound
- Code that relied on `Capability<T>: Serialize` unconditionally would need bounds

## Files to Modify

| File | Change |
|------|--------|
| `dialog-capability/src/settings.rs` | Remove `Serialize + DeserializeOwned` from `Caveat`, or merge into `Claim` |
| `dialog-capability/src/claim.rs` | Add `constrain()` method |
| `dialog-capability/src/effect.rs` | Drop `Caveat` from `Effect` bounds |
| `dialog-capability/src/attenuation.rs` | `Caveat` → `Claim` |
| `dialog-capability/src/policy.rs` | `Caveat` → `Claim` |
| `dialog-capability/src/ability.rs` | Drop `Serialize + DeserializeOwned` |
| `dialog-capability/src/constrained.rs` | Conditional `Serialize`/`Deserialize` impl |
| `dialog-capability/src/capability.rs` | Conditional `Serialize`/`Deserialize` impl |
| `dialog-macros/src/lib.rs` | Update `Claim` derive to generate `constrain()` |
| `dialog-capability/src/ucan/parameters.rs` | Use `Claim::constrain` instead of `Caveat::constrain` |

## Verification

```bash
cargo fmt --all
cargo clippy --all --all-targets --all-features -- -D warnings
cargo test --workspace
cargo test -p dialog-artifacts --target wasm32-unknown-unknown
```
