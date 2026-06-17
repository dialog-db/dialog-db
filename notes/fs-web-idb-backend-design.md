# Fs web backend = IDB-wrapped directory handle — design

## Goal
Make the web `Fs` backend self-contained from its address, symmetric with native,
and dissolve "where does the directory handle come from." Remove the Grant
credential and its operator wiring.

## Model

### Address (both targets)
`FsAddress` stays a serializable string. Interpretation per target:
- **native**: a `file:` URL → open the directory directly (today's path).
- **web**: an IndexedDB **database name** → open that db, read the stored
  `FileSystemDirectoryHandle`, wrap as `WebRoot` → `FileSystem`.

Self-contained on both: nothing else needed to resolve the address to a working
`FileSystem`.

### Web registration (one-time, after showDirectoryPicker)
A web-only setup call:
```
dialog_storage::provider::register_web_directory(db_name, handle) -> Result<()>
```
Opens IDB db `db_name`, ensures a `handle` store, `put`s the
`FileSystemDirectoryHandle` (structured-clone). Idempotent / replaceable.
This is the only place the granted handle is captured; afterwards the db is the
durable pointer and survives reload (IDB persists structured-cloned handles).

### Web open (Resource-style)
Web `FileSystem` gains an open-from-db path:
```
FileSystem::open_web(db_name) -> Result<FileSystem, _>
  - open IDB db
  - get the stored FileSystemDirectoryHandle (None -> error "no directory registered")
  - WebRoot::new(db_name, handle).provider()
```
Mirrors native `FileSystemHandle::try_from(file_url) -> FileSystem::from`.

### authorize (both targets, unchanged shape)
```
let fs = resolve_address(address);   // native: from file: URL; web: open_web(db_name)
verify_subject(fs, capability)?;     // dir's credential/key/self DID == invocation subject
attest(FsAuthorization::new(fs))
```
- Native authorize: no env (opens from URL).
- Web authorize: no operator env either — opens the IDB db directly. (No more
  Identify / Load<Grant>.) The IDB db name comes straight from the address.

### verify_subject on web
Opening the web `FileSystem` yields a `WebRoot` over the handle; reading
`credential/key/self` goes through that handle the normal way. So the web
`Load<Credential>` can read the byte-compat credential file through the wrapped
handle — `Credential::identity(bytes)` (DID-only) still used since a web signer
can't be fully imported, but the BYTES come from the directory via the handle,
not from a separate Grant. `credential_web.rs` stays (reads file via handle +
identity()); the byte read works because WebRoot reads files through the handle.

## Removed
- `Grant` type, `Save/Load<Grant>` effects, `CredentialGrantExt`,
  `LoadGrantExt/SaveGrantExt`, prelude entries.
- Grant provider impls in fs/credential.rs, indexeddb/credential.rs,
  volatile/credential.rs.
- Grant in SpaceProvider bound + Space/Storage/Operator `#[provide]` lists.
- `FileSystem::from_grant`.
- Web authorize's Identify + Load<Grant> + profile().credential().site().load_grant()
  path → replaced by FileSystem::open_web(db_name).

## Kept
- Native path entirely (file: URL).
- `Credential::identity` (DID-from-bytes) + web `credential_web.rs`
  `Load<Credential>` — still needed to read the dir's DID on web, but now the
  bytes come through the wrapped handle.
- `verify_subject`, FsAuthorization carrying the resolved FileSystem.

## Open question
- Does authorize need any env on web now? If `FileSystem::open_web` is sync-ish
  (async open IDB), authorize can take `Env: ConditionalSync` only on BOTH
  targets → unifies the two cfg-split authorize impls into one. Likely yes.
- db naming: address string IS the db name. Caller picks it (e.g. space DID).
