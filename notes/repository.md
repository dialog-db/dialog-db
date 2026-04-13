# Credentials

Credentials enable authorization of operations on a repository through a three-layer identity model: **account**, **profile**, and **operator**.

## Identity Layers

### Account

An optional persistent identity backed by a passkey, hardware key, or paper key. It has no storage of its own; it exists purely as a recovery point and delegation target. You may have multiple accounts for different purposes (e.g. work, personal). When recovering access on a new device, you authenticate to the account and selectively delegate from it to that device's profile, choosing which capabilities to grant rather than delegating everything. Account creation can be deferred until you need to sync or share across devices.

In the type system, Account is represented as an `Option<Did>` field on the `Profile` attenuation, not as a standalone type.

### Profile

A named user identity on a specific device. A device may have multiple profiles (e.g. "work" and "personal"), each with its own ed25519 keypair generated on first use. Profiles persist for the lifetime of the device. On first run, a default profile is created automatically.

The profile is represented in the capability chain as:

```
Subject -> Profile { profile: Did, account: Option<Did> } -> Operator { operator: Did }
```

### Operator

An ephemeral key representing the immediate invoker of a capability in a specific session or process context. Derived from the profile key using a context byte string (same profile + context always yields the same operator key).

## Authorization Chain

Invoking a capability on a repository requires a delegation chain from the subject back to the operator. The chain always includes the profile layer; the account layer is optional:

### Local

No account is configured. The subject delegates directly to the profile:

```
subject -> profile -> operator
```

Access is tied to this device. If the device is lost, access cannot be recovered.

### Recovered

An account is present, meaning access has been delegated through a persistent identity that survives device loss:

```
subject -> account -> profile -> operator
```

Or directly, if the repository was created after the account already existed on the device:

```
subject -> account -> operator
```

The profile is the stable identity on each device. The account, when present, is the anchor for recovery and cross-device delegation.

## Capability Domains

The system is organized into these effect domains (defined in `dialog-effects`):

- **`authority::Identify`** -- returns the current delegation chain as `Capability<Operator>`
- **`archive`** -- content-addressed storage (`Get`, `Put`)
- **`memory`** -- CAS memory cells (`Resolve`, `Publish`, `Retract`)
- **`credential`** -- credential storage (`Save<T>`, `Load<T>`)
- **`space`** -- named space discovery (`Load`, `Create`)
- **`storage`** -- location-based bootstrap (`Load`, `Create`)
- **`access`** -- authorization via protocols (`Prove`, `Retain`, `Authorize`)

## Named Spaces

Profiles and repositories are both named spaces identified by a human-readable name, each containing an ed25519 identity (the "credential").

### Credential

A credential is either:
- **Signer** (`SignerCredential`) -- full keypair available (owner of the space)
- **Verifier** (`VerifierCredential`) -- public key only (delegate of the space)

### Storage Format

Credentials are stored using multicodec-tagged key material:

- **Signer** (68 bytes): `[0x80 0x26 | secret(32) | 0xed 0x01 | public_key(32)]`
- **Verifier** (34 bytes): `[0xed 0x01 | public_key(32)]`

Where `0x80 0x26` is varint-encoded `0x1300` (ed25519-priv multicodec) and `0xed 0x01` is varint-encoded `0xed` (ed25519-pub multicodec).

On web (IndexedDB), signer credentials are stored as `CryptoKeyPair` objects (non-extractable). Verifier credentials are stored as `Uint8Array`.

### Storage Layout

- **Native (FileSystem)**: `{name}/credentials/self`
- **Web (IndexedDB)**: database `{name}`, store `credentials`, key `self`

The `name` scopes the storage on both platforms. Each named space gets its own directory (native) or database (web).

## Opening a Repository

Opening a repository is a two-step process using `space::Load` and `space::Create` capabilities:

1. **Load**: attempts to load an existing credential for the named space
   - If `Some(Credential::Signer(signer))` -- owner access, can delegate
   - If `Some(Credential::Verifier(verifier))` -- delegate access, read-only unless invited
   - If `None` -- repository doesn't exist

2. **Create** (if None):
   1. Generate an ed25519 keypair
   2. Save the credential via `space::Create`
   3. Delegate subject -> profile (powerline UCAN delegation)
   4. The repository is created with the new `did:key` as its subject

Higher-level `Repository::open()` combines these steps. Three modes:
- `.open()` -- loads existing or creates new
- `.load()` -- loads existing, fails if not found
- `.create()` -- creates new, fails if exists

## Environment Setup

The operator is built from a profile:

```rust
let storage = Storage::default();

let profile = Profile::open("alice")
    .perform(&storage)
    .await?;

let operator = profile
    .derive(b"my-app")
    .allow(Subject::any())
    .build(storage)
    .await?;
```

The builder chain:
1. `Profile::open(name)` opens or creates the profile keypair from storage
2. `.derive(context)` starts building an operator with a deterministic key
3. `.allow(capability)` configures the UCAN delegation scope
4. `.network(network)` optionally adds remote network capability
5. `.build(storage)` takes ownership of storage and produces the `Operator`

Profile keys are stored at the platform data directory:
- **Native**: `~/Library/Application Support/dialog/profile/{name}/key` (32-byte seed)
- **Web**: IndexedDB database `dialog`, store `credentials`, key `profile/{name}` (CryptoKeyPair)

## Authorization

Invoking any capability requires authorization. The operator resolves delegation chains using the `access::Prove` capability.

**Input:** the capability the caller wishes to invoke, identified by its subject `did:key` and ability.
**Output on success:** a signed invocation ready to be dispatched.

The authorization flow:
1. The operator's `CertificateStore` is searched for delegation chains from the subject to the operator
2. If a valid chain is found, the operator signs an invocation carrying the proof chain
3. If no chain exists but the subject credential is a `Signer`, a delegation can be issued on the spot

## Delegation Import

Delegations are imported via `profile.access().save(chain).perform(&operator)`. This stores the UCAN delegation chain in the profile's certificate store, making it available for future authorization lookups.

## Identification

Invoking `authority::Identify` returns the current delegation chain as `Capability<Operator>`, which encodes:

- The **subject** DID (from the chain root)
- The **profile** DID and optional **account** DID (from `Profile` in the chain)
- The **operator** DID (from `Operator` in the chain)
