# Subject Routing: How Effects Find Their Storage

Historical decision record. The system implemented a hybrid of Options B and C (routing table with layout conventions).

## Problem

When a capability effect arrives (e.g. `archive::Get` with subject `did:key:zRepo`), the system needs to know *where* to read/write data for that subject. The subject DID identifies *who*, but not *where*.

Two things must happen:
1. **Name to DID**: Resolve a human name ("home", "personal") to a subject DID by loading a keypair.
2. **DID to Storage**: Route effects on that DID to the right storage location.

## Option A: Location in Subject

Extend `Subject` to carry an optional storage location alongside the DID.

**Pros:** No secondary lookups; each Subject is self-contained.
**Cons:** If someone constructs a Subject from just a DID, the location is missing and effects fail. The location is runtime-only metadata that doesn't serialize.

## Option B: Routing Table in the Environment

The environment maintains a `Did -> Provider` mapping. When a profile or repository is opened, the DID gets registered with its storage provider.

**Pros:** Doesn't matter how the Subject was constructed. Supports mixed providers.
**Cons:** Shared mutable state (interior mutability). Effects on unregistered DIDs fail.

## Option C: Routing by Layout Convention

Use filesystem layout conventions. All subjects live under a shared root, organized by DID.

**Pros:** Simplest; one provider, no dispatch logic.
**Cons:** All subjects must use the same provider type. Can't spread repos across different directories.

## Decision

The current implementation uses Option B (`Router` in `Storage<S>`) for DID-based dispatch, combined with Option C's convention for layout within each space. The `Loader` creates providers using platform-specific address resolution from a `Location`, then registers them in the `Router` under the space's DID.
