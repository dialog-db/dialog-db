# Repository Memory Layout

Each repository scoped to a subject DID uses the following memory cell layout:

## Local Branches

```
branch/{name}/revision          Cell<Revision>
branch/{name}/upstream          Cell<Option<UpstreamState>>
```

- **revision** -- the local branch head (latest committed tree + metadata)
- **upstream** -- what this branch tracks (`UpstreamState::Remote { name, branch, tree }`)

## Remote Sites

```
remote/{name}/address                     Cell<RemoteAddress>
remote/{name}/branch/{branch}/revision    Cell<Revision>
```

- **address** -- connection info wrapped in `RemoteAddress { address: SiteAddress, subject: Did }`
- **branch revision** -- last fetched revision for a remote branch (updated by fetch, not by pull)

## Operations

### fetch

1. Read `branch/{name}/upstream` to get the remote `{ name, branch }`
2. Load `remote/{remote}/address` to get connection info and subject DID
3. Resolve remote revision using the address + subject
4. Write result to `remote/{remote}/branch/{branch}/revision`
5. Local `branch/{name}/revision` and `branch/{name}/upstream` are unchanged

### pull

1. Fetch (as above)
2. Read `remote/{remote}/branch/{branch}/revision` -- the upstream revision
3. Read `branch/{name}/upstream` tree -- the last sync point
4. Three-way merge: upstream vs base vs local
5. Update `branch/{name}/revision` with merged result
6. Update `branch/{name}/upstream` tree to match upstream revision

### push

1. Read `branch/{name}/revision` -- local head
2. Read `branch/{name}/upstream` tree -- last sync point (base for novelty)
3. Compute novel blocks (diff between base and local)
4. Upload blocks to remote via `remote/{remote}/address`
5. Publish local revision to remote
6. Update `branch/{name}/upstream` tree to match pushed revision
7. Update `remote/{remote}/branch/{branch}/revision` to match

## Type Notes

- `SiteAddress` -- enum: `S3(Address)` | `Ucan(UcanAddress)`. Connection info only.
- `RemoteAddress` -- wraps `SiteAddress` with a `subject: Did` identifying which repository at the site.
- `UpstreamState` -- enum: `Local { branch, tree }` | `Remote { name, branch, tree }`. The `subject` DID lives on the `RemoteAddress`, not on `UpstreamState`.
- `Revision` -- `{ subject, issuer, authority, tree, cause, period, moment }`. The `tree` is a `NodeReference` (blake3 hash of the prolly tree root).
