# Credentials

Credentials enable authorization of operations on a repository through a three-layer identity model: **account**, **profile**, and **operator**.

## Identity Layers

### Account

An optional persistent identity backed by a passkey, hardware key, or paper key. It has no storage of its own; it exists purely as a recovery point and delegation target. You may have multiple accounts for different purposes (e.g. work, personal). When recovering access on a new device, you authenticate to the account and selectively delegate from it to that device's profile, choosing which capabilities to grant rather than delegating everything. Account creation can be deferred until you need to sync or share across devices.

### Profile

A named user identity on a specific device. A device may have multiple profiles (e.g. "work" and "personal"), each with its own ed25519 keypair generated on first use. Profiles persist for the lifetime of the device. On first run, a default profile is created automatically.

### Operator

An ephemeral key representing the immediate invoker of a capability in a specific session or process context.

## Authorization Chain

Invoking a capability on a repository requires a delegation chain from the subject back to the operator. The chain always includes the profile layer; the account layer is optional:

### Local

No account is configured. The subject delegates directly to the profile:

```
subject → profile → operator
```

Access is tied to this device. If the device is lost, access cannot be recovered.

### Recovered

An account is present, meaning access has been delegated through a persistent identity that survives device loss:

```
subject → account → profile → operator
```

Or directly, if the repository was created after the account already existed on the device:

```
subject → account → operator
```

The profile is the stable identity on each device. The account, when present, is the anchor for recovery and cross-device delegation.

---

## Opening a Repository

`Credentials` are used to open a repository. The environment is assumed to be bootstrapped with storage capabilities rooted in a `./.dialog/` directory on disk.

The repository identifier is stored in the `meta` store under the key `seed`. To open a repository, the `memory::Get` capability is invoked with the **operator** as subject.

1. If the result is `Ok` with 32 or 64 bytes of content:
   1. The first 32 bytes are interpreted as an ed25519 public key.
   2. The public key is formatted as a `did:key`.
   3. A `Repository` is created with that `did:key` as its subject.

2. If the result is `None`, the repository does not yet exist and is created:
   1. Generate an ed25519 keypair.
   2. Format the public key as a `did:key`.
   3. Invoke `memory::Publish` with the **operator** as subject, targeting the `meta` store under the `seed` key. The value is a 64-byte array: the first 32 bytes are the public key, the last 32 bytes are the ed25519 seed.
   4. Create a `Repository` with the new `did:key` as its subject.

3. If the result is `Ok` but the content is neither 32 nor 64 bytes:
   1. Raise an error: the repository is corrupted.

---

## Authorization

Invoking any capability requires authorization via the `credential::Authorize` capability.

**Input:** the capability the caller wishes to invoke, identified by its subject `did:key` and ability (e.g. `/archive/get`).  
**Output on success:** a signed invocation ready to be dispatched.

When authorization is requested, credentials find any valid delegation chain from the subject to the operator covering the requested ability. The chain length varies depending on whether an account is present; the lookup algorithm is agnostic about path length and walks backward from the operator until it reaches the subject.

If a valid chain is found, the operator signs an invocation carrying the delegation proof chain as authorization. The invocation targets the subject's resource space: the subject originally authorized the operator via delegation, and the operator now acts against the subject's resources using that proof chain.

### Authorization from Subject

If no `subject → profile` delegation exists, one can sometimes be obtained interactively.

If the process is running in the same storage environment where the repository was created, the repository ed25519 seed may be available: reading `meta/seed` and finding 64 bytes means the last 32 bytes are the seed for the repository subject key. Once loaded, that key can be used to issue a `subject → profile` delegation for the approved capabilities.

If the user denies the request, an authorization error is raised.

### Authorization from Account

If a `subject → account` delegation exists but no `account → profile` delegation is found, one may be obtained interactively.

If the user approves, the account key can delegate access through an external process, for example by loading an authorization request in a browser where the account key lives, and on approval producing a UCAN delegation and importing it into credentials.

If the user denies the request, an authorization error is raised. If no response is received, the call blocks indefinitely.

### Authorization Lookup

Delegations are stored and looked up using `storage` capabilities with the **operator** as subject in the `credentials` store. The key layout is audience-first:

```
/ucan/{audience}/{subject}/{cid}       # delegation scoped to a specific subject
/ucan/{audience}/_/{issuer}.{cid}      # powerline delegation (no subject restriction)
```

This layout means chain discovery always starts from what is locally known (the operator) and walks backward toward the subject, without forward enumeration.

To find a chain authorizing `operator` to act on `subject` for a given ability:

1. List `/ucan/{operator}/{subject}/` and `/ucan/{operator}/_/` to find candidate delegations where operator is the audience.
2. For each candidate, load the UCAN and check that it covers the requested ability. Discard those that do not.
3. If a candidate's issuer is the subject, the chain is complete.
4. Otherwise, recurse: treat the candidate's issuer as the new audience and repeat from step 1, carrying a visited set of `(audience, subject)` pairs to prevent cycles.

A powerline delegation (`_`) defers the subject check to the next hop; the issuer must itself have a chain back to the subject. Encoding the issuer into the filename as `{issuer}.{cid}` makes it available without loading the UCAN. Loading is still required to verify `cmd` and `pol` fields, but the issuer prefix allows the recursion to prune dead branches, skipping the load entirely for issuers already known not to be on any viable path (e.g. already in the visited set).

---

## Import

Invoking `credential::Import` adds a UCAN delegation to the credentials store, making it available for authorization lookup.

**Input:** a UCAN delegation.

On import, all relevant path keys are written eagerly, since the delegation's fields are fully known at write time:

1. Validate the delegation's signature and expiry.
2. Determine the storage paths:
   - If the delegation specifies a subject: write to `/ucan/{audience}/{subject}/{cid}`.
   - If the delegation has no subject (powerline): write to `/ucan/{audience}/_/{issuer}.{cid}`.
3. Invoke `memory::Publish` for each path with the **operator** as subject in the `credentials` store.

This front-loads organization cost onto writes so reads can walk a predictable structure without loading UCANs speculatively.

---

## Identification

Invoking `credential::Identify` returns:

- The **operator** public key (as a `did:key`).
- The **profile** public key (as a `did:key`).
- The **account** public key (as a `did:key`), if one is configured.
