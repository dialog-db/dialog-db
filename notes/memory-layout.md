# Repository Memory Layout

Each repository scoped to a subject DID uses the following memory cell layout:

## Local Branches

```
branch/{name}/revision          Cell<Revision>
branch/{name}/upstream          Cell<UpstreamState>
```

- **revision** — the local branch head (latest committed tree + metadata)
- **upstream** — what this branch tracks (`UpstreamState::Remote { name, branch, subject, tree }`)

## Remote Sites

```
remote/{name}/address                     Cell<SiteAddress>
remote/{name}/branch/{branch}/revision    Cell<Revision>
```

- **address** — connection info (`SiteAddress::S3(...)` or `SiteAddress::Ucan(...)`)
- **branch revision** — last fetched revision for a remote branch (updated by fetch, not by pull)

## Operations

### fetch

1. Read `branch/{name}/upstream` to get the remote `{ name, branch, subject }`
2. Load `remote/{remote}/address` to get connection info
3. Resolve remote revision using the address + subject
4. Write result to `remote/{remote}/branch/{branch}/revision`
5. Local `branch/{name}/revision` and `branch/{name}/upstream` are unchanged

### pull

1. Fetch (as above)
2. Read `remote/{remote}/branch/{branch}/revision` — the upstream revision
3. Read `branch/{name}/upstream` tree — the last sync point
4. Three-way merge: upstream vs base vs local
5. Update `branch/{name}/revision` with merged result
6. Update `branch/{name}/upstream` tree to match upstream revision

### push

1. Read `branch/{name}/revision` — local head
2. Read `branch/{name}/upstream` tree — last sync point (base for novelty)
3. Compute novel blocks (diff between base and local)
4. Upload blocks to remote via `remote/{remote}/address`
5. Publish local revision to remote
6. Update `branch/{name}/upstream` tree to match pushed revision
7. Update `remote/{remote}/branch/{branch}/revision` to match

## Type Notes

- `SiteAddress` — enum: `S3(Address)` | `Ucan(UcanAddress)`. Connection info only.
- `UpstreamState` — enum: `Local { branch, tree }` | `Remote { name, branch, subject, tree }`. The `subject` DID in the Remote variant identifies which repository at the site.
- `Revision` — `{ issuer, tree, cause, period, moment }`. The `tree` is a `NodeReference` (blake3 hash of the prolly tree root).
