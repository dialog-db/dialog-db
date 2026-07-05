# Capability Scoping and Delegation Design

## Status

This describes future work. Currently `Subject::any()` creates a wildcard subject with `did:_:_`, and the UCAN layer uses `UcanSubject::Any`. The type-level parameterization described below has not been implemented.

## Problem

Capabilities are always rooted in a `Subject(Did)`. This means you can't express "access to archive/catalog/index" without knowing the repository DID upfront. But delegation scoping needs exactly that: describing *what kind* of access without naming *which resource*.

## Current State

Every capability chain starts with `Subject`:

```
Subject(repo_did) -> Archive -> Catalog("index") -> Get { key }
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

`Ability` trait requires `fn subject(&self) -> &Did`. For wildcards, `Subject::any()` returns a subject with `did:_:_`.

## Proposed Design

### `Any` as Wildcard Root

Introduce `Any` as an alternative chain root alongside `Subject`:

```rust
pub struct Any;  // wildcard: represents any subject
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

### Convenience Alias

```rust
pub type Scope<T> = Capability<T, Any>;
```

- `Capability<Catalog>` -- subject-rooted, invocable (backward compatible)
- `Scope<Catalog>` -- wildcard-rooted, delegation scope only

### Compile-Time Safety

`invoke`/`perform`/`fork` only available on subject-rooted chains:

```rust
impl<Fx, Of> Constrained<Fx, Of>
where
    Fx: Effect,
    Of: Ability<Root = Subject>,  // only Subject-rooted chains
{
    pub fn invoke(...) { ... }
    pub async fn perform(...) { ... }
}
```

`Any`-rooted chains compile for building and reading but cannot be invoked.

### `Ability` Changes

```rust
pub trait Ability {
    type Root;  // Subject or Any, propagated from chain root
    fn subject(&self) -> Option<&Did>;  // None for Any-rooted
    fn ability(&self) -> String;
}
```

## Usage

### Building Delegation Scopes

```rust
let scope: Scope<Catalog> = Any
    .attenuate(Archive)
    .attenuate(Catalog::new("index"));

// Use in builder
profile.derive(b"alice")
    .allow(scope)
    .build(storage)
    .await?;
```

### Powerline Delegation (current)

```rust
profile.derive(b"alice")
    .allow(Subject::any())
    .build(storage)
    .await?;
```

## UCAN Mapping

- `Any` root maps to UCAN `Subject::Any` (powerline delegation)
- `Subject(did)` root maps to UCAN `Subject::Specific(did)`
- Ability path maps to UCAN command (e.g., `["archive", "catalog"]`)
- Policy constraints map to UCAN policy predicates
