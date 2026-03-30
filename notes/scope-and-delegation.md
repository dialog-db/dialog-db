# Capability Scoping and Delegation Design

## Problem

Capabilities are always rooted in a `Subject(Did)`. This means you can't express "access to archive/catalog/index" without knowing the repository DID upfront. But delegation scoping needs exactly that — describing *what kind* of access without naming *which resource*.

## Current State

Every capability chain starts with `Subject`:

```
Subject(repo_did) → Archive → Catalog("index") → Get { key }
```

`Attenuation::Of` walks up to `Subject`:
```rust
impl Attenuation for Archive { type Of = Subject; }
impl Attenuation for Catalog { type Of = Archive; }
```

`Constraint` computes the full chain type from the leaf:
```rust
type Capability<Fx> = <Fx as Constraint>::Capability;
// Capability<Get> = Constrained<Get, Constrained<Catalog, Constrained<Archive, Subject>>>
```

`Ability` trait requires `fn subject(&self) -> &Did` — enforces every chain has a subject.

## Proposed Design

### `Any` as Wildcard Root

Introduce `Any` as an alternative chain root alongside `Subject`:

```rust
pub struct Any;  // wildcard — represents any subject
```

### Parameterize `Constraint` by Root

```rust
impl<T: Policy, Root> Constraint<Root> for T {
    type Capability = Constrained<T, <T::Of as Constraint<Root>>::Capability>;
}

impl<Root> Constraint<Root> for Subject {
    type Capability = Root;  // terminates with whatever root was given
}
```

### `Capability` Gains a Root Parameter

`Capability<T>` is already a struct. Add a second type parameter defaulting to `Subject`:

```rust
pub struct Capability<T: Constraint<Root>, Root = Subject> {
    can: <T as Constraint<Root>>::Capability,
}
```

- `Capability<Catalog>` — subject-rooted, invocable (backward compatible)
- `Capability<Catalog, Any>` — wildcard-rooted, delegation scope

Convenience alias for scopes:

```rust
pub type Scope<T> = Capability<T, Any>;
```

Same attenuation types (`Archive`, `Catalog`, etc.) shared between both. The root parameter flows through the chain:

```rust
Capability<Catalog>      = Capability { can: Constrained<Catalog, Constrained<Archive, Subject>> }
Capability<Catalog, Any> = Capability { can: Constrained<Catalog, Constrained<Archive, Any>> }
```

### `invoke`/`perform`/`fork` Only on Rooted Chains

```rust
impl<Fx, Of> Constrained<Fx, Of>
where
    Fx: Effect,
    Of: Ability<Root = Subject>,  // only Subject-rooted chains
{
    pub fn invoke(...) { ... }
    pub async fn perform(...) { ... }
    pub fn fork(...) { ... }
}
```

`Any`-rooted chains compile for building and reading (`.ability()`, `.constrain()`) but cannot be invoked. Compile-time safety preserved.

### `Ability` Changes

```rust
pub trait Ability {
    type Root;  // Subject or Any — propagated from chain root
    fn subject(&self) -> Option<&Did>;  // None for Any-rooted
    fn ability(&self) -> String;
    fn constrain(&self, builder: &mut impl PolicyBuilder);
}

impl Ability for Subject {
    type Root = Subject;
    fn subject(&self) -> Option<&Did> { Some(&self.0) }
    ...
}

impl Ability for Any {
    type Root = Any;
    fn subject(&self) -> Option<&Did> { None }
    ...
}

impl<C: Policy, Of: Ability> Ability for Constrained<C, Of> {
    type Root = Of::Root;  // propagates
    fn subject(&self) -> Option<&Did> { self.capability.subject() }
    ...
}
```

## Usage

### Building Invocable Capabilities (unchanged)

```rust
let cap: Capability<Get> = Subject::from(repo_did)
    .attenuate(Archive)
    .attenuate(Catalog::new("index"))
    .invoke(Get::new(key));

cap.perform(&env).await?;
```

### Building Delegation Scopes (new)

```rust
let scope: Scope<Catalog> = Any
    .attenuate(Archive)
    .attenuate(Catalog::new("index"));

// Use in builder
Builder::default()
    .operator(b"alice")
    .grant(Ucan::delegate(scope))
    .build()
    .await?;
```

### Powerline Delegation

```rust
Builder::default()
    .operator(b"alice")
    .grant(Ucan::unrestricted())
    .build()
    .await?;
```

## Builder `.grant()` API

```rust
impl<Storage> Builder<Storage> {
    /// Grant the operator access via a protocol-specific delegation.
    pub fn grant(mut self, grant: impl Into<Grant>) -> Self {
        self.grants.push(grant.into());
        self
    }
}

// Protocol-specific delegation descriptors
impl Ucan {
    /// Powerline delegation — all commands, any subject.
    pub fn powerline() -> UcanGrant { ... }

    /// Scoped delegation — specific capability path.
    pub fn delegate<Fx>(scope: Scope<Fx>) -> UcanGrant
    where Fx: Constraint { ... }
}
```

At `build()` time, each `Grant` is signed by the profile and stored in the credential store.

## UCAN Mapping

- `Any` root → UCAN `Subject::Any` (powerline delegation)
- `Subject(did)` root → UCAN `Subject::Specific(did)`
- Ability path → UCAN command (e.g., `["archive", "catalog"]`)
- Policy constraints → UCAN policy predicates

## Implementation Steps

1. Add `Any` struct to `dialog-capability`
2. Parameterize `Constraint` trait with `Root` type parameter
3. Change `Ability::subject()` to return `Option<&Did>`
4. Add `Ability::Root` associated type
5. Guard `invoke`/`perform`/`fork` with `Root = Subject` bound
6. Add `type Scope<Fx>` alias
7. Update all `Ability::subject()` callers to handle `Option`
8. Implement `.grant()` on Builder
9. Implement `Ucan::delegate()` and `Ucan::unrestricted()`

## Files to Modify

- `dialog-capability/src/ability.rs` — split `subject()` to `Option`, add `Root` associated type
- `dialog-capability/src/constraint.rs` — parameterize by `Root`
- `dialog-capability/src/constrained.rs` — guard `invoke`/`perform`/`fork`
- `dialog-capability/src/lib.rs` — add `Any`, `Scope` type alias
- `dialog-capability/src/fork.rs` — add `Root = Subject` bounds
- `dialog-capability/src/capability.rs` — update `subject()` callers
- `dialog-capability/src/ucan/claim.rs` — handle `Option<&Did>` from `subject()`
- `dialog-capability/src/ucan/access.rs` — same
- `dialog-artifacts/src/environment/builder.rs` — add `.grant()` method
- All Provider impls that read `subject()` — handle `Option`
