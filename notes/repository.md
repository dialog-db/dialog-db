# Credentials

Credentials enable authorization of operations on a repository through a three-layer identity model: **account**, **profile**, and **operator**.

## Identity Layers

### Account

An optional persistent identity backed by a passkey, hardware key, or paper key. It has no storage of its own; it exists purely as a recovery point and delegation target. You may have multiple accounts for different purposes (e.g. work, personal). When recovering access on a new device, you authenticate to the account and selectively delegate from it to that device's profile, choosing which capabilities to grant rather than delegating everything. Account creation can be deferred until you need to sync or share across devices.

### Profile

A named user identity on a specific device. A device may have multiple profiles (e.g. "work" and "personal"), each with its own ed25519 keypair generated on first use. Profiles persist for the lifetime of the device. On first run, a default profile is created automatically.

The profile is represented in the capability chain as:

```
Subject → Profile { profile: Did, account: Option<Did> } → Operator { operator: Did }
```

Where `type Authority = Capability<Operator>` — the authority chain IS the identity.

### Operator

An ephemeral key representing the immediate invoker of a capability in a specific session or process context. Derived from the profile key or generated randomly.

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

## Capability Domains

The system is split into four independent capability domains:

```
Subject → Identify → Result<Authority, Error>
Subject → Profile → Operator → Sign { payload }
Subject → Access → Authorize<Fx, F>
Subject → Credential → Retrieve / Save / List / Import
Subject → Repository → Name → Load / Save
```

- **`authority::Identify`** — returns the current `Authority` chain (effect on Subject)
- **`authority::Sign`** — signs payloads (effect on Operator)
- **`access::Authorize`** — produces authorization proofs (effect on Access)
- **`credential::*`** — credential store for delegation chains
- **`repository::Load / Save`** — named repository discovery and registration

---

## Named Spaces

Profiles and repositories are both named spaces identified by a human-readable name, each containing an ed25519 identity (the "credential").

### Credential

A credential is either:
- **Signer** — full keypair available (owner of the space)
- **Verifier** — public key only (delegate of the space)

### Storage Format

Credentials are stored using multicodec-tagged key material:

- **Signer** (68 bytes): `[0x80 0x26 | secret(32) | 0xed 0x01 | public_key(32)]`
- **Verifier** (34 bytes): `[0xed 0x01 | public_key(32)]`

Where `0x80 0x26` is varint-encoded `0x1300` (ed25519-priv multicodec) and `0xed 0x01` is varint-encoded `0xed` (ed25519-pub multicodec).

On web (IndexedDB), signer credentials are stored as `CryptoKeyPair` objects (non-extractable). Verifier credentials are stored as `Uint8Array`.

### Capability Hierarchy

```
Subject (environment DID)
  └── Repository (ability: /repository)
        └── Name { name: String }
              ├── Load → Result<Option<Credential>, RepositoryError>
              └── Save(Credential) → Result<(), RepositoryError>
```

### Storage Layout

- **Native (FileSystem)**: `{name}/credentials/self`
- **Web (IndexedDB)**: database `{name}`, store `credentials`, key `self`

The `name` scopes the storage on both platforms. Each named space gets its own directory (native) or database (web).

---

## Opening a Repository

Opening a repository is a two-step process using the `Load` and `Save` capabilities:

1. **Load**: `Subject → Repository → Name("home") → Load`
   - If `Some(Credential::Signer(signer))` — owner access, can delegate
   - If `Some(Credential::Verifier(verifier))` — delegate access, read-only unless invited
   - If `None` — repository doesn't exist

2. **Install** (if None):
   1. Generate an ed25519 keypair
   2. `Subject → Repository → Name("home") → Save(Credential::Signer(signer))`
   3. Delegate subject → profile (powerline UCAN delegation)
   4. The `Repository` is created with the new `did:key` as its subject

A higher-level `Repository::open()` helper combines these steps:
- Calls `Load` for the given name
- If `None`, generates a keypair, calls `Save`, delegates
- If `Signer`, uses the DID as subject (can re-delegate if needed)
- If `Verifier`, uses the DID as subject (read-only)

---

## Environment

The environment is built using `Builder`:

```rust
let env = Builder::default()
    .operator(b"alice")
    .grant(Ucan::unrestricted())
    .build()
    .await?;
```

The builder:
1. Opens/creates the profile keypair from storage
2. Derives the operator key (unique or deterministic)
3. Assembles `Credentials` (profile + operator signers)
4. Executes any configured grants (e.g., UCAN delegation)
5. Returns `Environment<Credentials, Storage, Remote>`

Profile keys are stored at the platform data directory:
- **Native**: `~/Library/Application Support/dialog/profile/{name}/key` (32-byte seed)
- **Web**: IndexedDB database `dialog`, store `credentials`, key `profile/{name}` (CryptoKeyPair)

---

## Authorization

Invoking any capability requires authorization via the `access::Authorize` capability.

**Input:** the capability the caller wishes to invoke, identified by its subject `did:key` and ability (e.g. `/archive/get`).
**Output on success:** a signed invocation ready to be dispatched.

When authorization is requested, credentials find any valid delegation chain from the subject to the operator covering the requested ability. The chain length varies depending on whether an account is present; the lookup algorithm is agnostic about path length and walks backward from the operator until it reaches the subject.

If a valid chain is found, the operator signs an invocation carrying the delegation proof chain as authorization. The invocation targets the subject's resource space.

### Authorization from Subject

If no `subject → profile` delegation exists, one can sometimes be obtained interactively.

If the process is running in the same storage environment where the repository was created, the repository credential may be a `Signer` — meaning the seed is available. Once loaded, that key can be used to issue a `subject → profile` delegation for the approved capabilities.

If the user denies the request, an authorization error is raised.

### Authorization from Account

If a `subject → account` delegation exists but no `account → profile` delegation is found, one may be obtained interactively.

If the user approves, the account key can delegate access through an external process, for example by loading an authorization request in a browser where the account key lives, and on approval producing a UCAN delegation and importing it into credentials.

If the user denies the request, an authorization error is raised. If no response is received, the call blocks indefinitely.

### Authorization Lookup

Delegations are stored and looked up using `credential` capabilities with the **operator** as subject in the `credentials` store. The key layout is audience-first:

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

A powerline delegation (`_`) defers the subject check to the next hop; the issuer must itself have a chain back to the subject. Encoding the issuer into the filename as `{issuer}.{cid}` makes it available without loading the UCAN.

---

## Import

Invoking `credential::Import<DelegationChain>` adds a UCAN delegation chain to the credentials store, making it available for authorization lookup. A blanket `Provider<Import<DelegationChain>>` impl exists for any storage that supports `Provider<credential::Save<Vec<u8>>>`.

On import, all relevant path keys are written eagerly:

1. Validate the delegation's signature and expiry.
2. Determine the storage paths:
   - If the delegation specifies a subject: write to `/ucan/{audience}/{subject}/{cid}`.
   - If the delegation has no subject (powerline): write to `/ucan/{audience}/_/{issuer}.{cid}`.
3. Invoke `credential::Save` for each path with the **operator** as subject.

---

## Identification

Invoking `authority::Identify` returns the current `Authority` chain (`Capability<Operator>`), which encodes:

- The **subject** DID (from the chain root)
- The **profile** DID and optional **account** DID (from `Profile` in the chain)
- The **operator** DID (from `Operator` in the chain)

The `Authority` type is `Capability<Operator>` — the identity is the capability chain itself.
