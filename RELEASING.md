# Releasing

Agents follow `.claude/rules/versioning-and-changelog.md` when they change
the crates, and the `release` skill (`.claude/skills/release/SKILL.md`) when
they cut a release.

The workspace is released as one version, tagged `v<version>`. Versions
follow semver over the Rust API and over what a replica stores and syncs;
while dialog is 0.x, a change that breaks either bumps the minor version
and anything else bumps the patch (see the top of
[CHANGELOG.md](./CHANGELOG.md)).

## Cutting a release

1. On a branch from `main`, run `scripts/release.sh prepare <version>`. It
   sets `[workspace.package] version` and the version of every crate under
   `[workspace.dependencies]`, refreshes `Cargo.lock`, and turns the
   changelog's `## Unreleased` section into `## <version> (<date>)` under a
   new, empty `## Unreleased`.
2. Read the dated section as someone upgrading would, fix what it misses,
   and open the release pull request.
3. Once it merges, tag the merge commit and push the tag:

   ```sh
   git tag -a v<version> -m v<version> <merge commit>
   git push origin v<version>
   ```

   The `Release` workflow runs `scripts/release.sh check` against the tag
   (the tag, the workspace version, every internal dependency's version and
   the changelog section must agree) and publishes the changelog section as
   the GitHub release.

## Pre-releases

To let a dependent such as tonk build against a change still in review,
tag the pull request's head `v<next>-rc.<n>` (prepare it with
`scripts/release.sh prepare <next>-rc.<n>` on that branch first) and pin
the dependent to the tag rather than to the branch: a tag stays where it
was put when the branch is rewritten or deleted. The `Release` workflow
marks a version with a pre-release suffix as a pre-release.

## Depending on a release

Name the tag and the version together, so Cargo refuses a tag whose
crates say otherwise:

```toml
dialog-repository = { git = "https://github.com/dialog-db/dialog-db.git", tag = "v0.2.0", version = "0.2.0" }
```

## Publishing

The crates are not on crates.io yet. Every internal dependency already
carries the workspace version, which `cargo publish` requires; publishing
still needs each crate's metadata (description, repository) and an order
that publishes a crate after the crates it depends on.
