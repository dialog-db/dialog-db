# Subject Routing: How Effects Find Their Storage

## Problem

When a capability effect arrives (e.g. `archive::Get` with subject `did:key:zRepo`), the system needs to know *where* to read/write data for that subject. The subject DID identifies *who*, but not *where*.

Two things must happen:
1. **Name → DID**: Resolve a human name ("home", "personal") to a subject DID by loading a keypair.
2. **DID → Storage**: Route effects on that DID to the right storage location.

## Option A: Location in Subject

Extend `Subject` to carry an optional storage location alongside the DID:

```rust
pub struct Subject {
    did: Did,
    location: Option<Location>,  // e.g. storage:///home/
}
```

When a profile or repository is opened, the returned Subject includes both the DID and the location where its data lives. Storage providers extract the location from the Subject and resolve paths against it.

**Pros:**
- No secondary lookups — the location travels with the DID
- No shared mutable state (routing table)
- Each Subject is self-contained

**Cons:**
- If someone constructs a Subject from just a DID (`Subject::from(did)`), the location is missing and effects fail silently or with a confusing error
- The location is runtime-only metadata that doesn't serialize — easy to lose across boundaries
- Callers must be careful to propagate the location through every Subject construction

## Option B: Routing Table in the Environment

The operating environment maintains a `Did → Provider` mapping. When a profile or repository is opened, the DID gets registered with its storage provider. All effects route through this table.

```
compositor.mount(did, store);
// later...
archive::Get with subject did → compositor looks up did → dispatches to store
```

**Pros:**
- Doesn't matter how the Subject was constructed — as long as the DID is in the table, effects route correctly
- Supports mixed providers — profile on FileSystem, repo on Volatile, another repo on IndexedDb
- Explicit registration makes the routing visible and debuggable

**Cons:**
- Shared mutable state (the routing table) with interior mutability
- Effects on unregistered DIDs fail — need to handle the "no mount" error path
- Every open/create operation must remember to register, or effects silently fail
- Cloning mounted stores out of a lock on every effect execution

## Option C: Routing by Layout Convention

Instead of explicit routing, use filesystem layout conventions. All subjects live under a shared root, organized by DID:

```
{root}/credential/profile/{name}     → profile keypair
{root}/credential/space/{name}       → repo keypair
{root}/subject/{did}/archive/...     → archive data
{root}/subject/{did}/memory/...      → memory data
{root}/subject/{did}/storage/...     → kv data
```

A single provider (e.g. FileStore) mounted at `{root}` handles all subjects. The DID in the capability chain determines the subdirectory. No routing table needed — the layout *is* the routing.

**Pros:**
- Simplest implementation — one provider, one root, no dispatch logic
- No shared mutable state
- Adding a new subject is just creating a keypair — no registration step
- The filesystem is the source of truth for what subjects exist

**Cons:**
- All subjects must use the same provider type (can't mix FileSystem and Volatile per-subject)
- All subjects must be under the same root (can't spread repos across different directories)
- Profile credentials live on a different root (platform profile directory), so the profile case still needs special handling — the environment needs to know the profile DID to route its effects to the platform root instead of the shared root

## Hybrid Consideration

Options B and C aren't mutually exclusive. The layout convention (C) handles the common case of multiple repos under one root with zero dispatch overhead. The routing table (B) handles the exception cases: profile data on a different root, or (future) repos mounted from different locations.

In practice this might look like: one FileStore for the storage root handles all repo DIDs via layout convention, and the profile DID is the only entry in a small routing table pointing to the platform root.
