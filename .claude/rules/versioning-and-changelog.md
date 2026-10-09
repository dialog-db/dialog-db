# Versioning and the changelog

The workspace is released as one version, tagged `v<version>`
(`RELEASING.md`). Versions follow semver over two surfaces: the Rust API,
and what a replica stores and syncs (stored facts, rule and descriptor
encodings, identities, the wire format). While dialog is 0.x, a change
that breaks either one is a minor bump; anything else is a patch.

## In a pull request that changes behavior

- Add an entry under `## Unreleased` in `CHANGELOG.md`, in the same pull
  request. Never edit a dated `## <version>` section: it is what that
  release shipped.
- Group by the change a reader has to understand, not by commit: one
  `### <the change, stated as what is now true>` per change, newest
  first. Follow it with the pull request link (and the note that argues it
  in full, if there is one), a paragraph saying what the change is, a
  `What this changes for you:` list of the concrete consequences (renamed
  or removed APIs and their replacements, results that differ, errors that
  are new or gone), and a `Why:` paragraph.
- A change to what a replica stores or syncs also gets a bullet under the
  section's `### Compatibility and migration`: what an older replica holds,
  what happens to it on upgrade, whether the older release can read what
  this one writes, and what an embedder has to run (for example
  `Branch::upgrade_rules`). Say so even when the answer is "nothing is
  rewritten".
- Mark a breaking change in the pull request title and the squash commit
  with `!` (`feat!:`, `refactor!:`), whichever surface it breaks. A storage
  or wire change is breaking even when no Rust signature moves.
- Do not change a version number in a feature pull request. Versions move
  only in a release pull request, through `scripts/release.sh prepare`.
- A refactor, test or tooling change with no effect a user of the crates
  could observe needs no entry.

## When a dependent needs a change before it is released

Do not point the dependent (tonk) at a pull request's branch: a rewritten
or deleted branch strands its lockfile. Tag the branch's head as a
pre-release (`v<next>-rc.<n>`, see `RELEASING.md`) and pin the dependent
to that tag, with `version` beside `tag` so Cargo refuses a mismatch.

## Cutting a release

Use the `release` skill.
