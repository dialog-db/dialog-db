---
name: release
description: Cut a dialog release or pre-release - choose the version from what changed, prepare it with scripts/release.sh, open the release PR, tag the merge commit, and repoint dependents. Use when asked to release, tag, version or publish dialog, or to give tonk a dialog version to pin.
---

# Cutting a release

Read `RELEASING.md` and the top of `CHANGELOG.md` first; this skill is the
order of operations around them.

1. **Choose the version.** Read `## Unreleased` and the commits since the
   last `v*` tag (`git log $(git describe --tags --match 'v*' --abbrev=0)..origin/main`).
   While 0.x: any `!` commit, or any change to what a replica stores or
   syncs, bumps the minor version; otherwise bump the patch. If the commits
   and the changelog disagree (a `!` commit with no changelog entry, a
   storage change with no compatibility bullet), fix the changelog first.
2. **Prepare.** On a branch from `origin/main`:
   `scripts/release.sh prepare <version>`. It sets the workspace version
   and every internal dependency's version, refreshes `Cargo.lock`, and
   dates the changelog section. Read the dated section as someone
   upgrading from the previous release would and fix what it misses.
   `scripts/release.sh check <version>` must pass. `Cargo.lock` should
   change only the workspace crates' own versions.
3. **Open the release pull request** titled `chore(release): <version>`,
   and wait for it to merge. Do not merge it yourself unless asked.
4. **Tag the merge commit** and push the tag:
   `git tag -a v<version> -m v<version> <merge commit> && git push origin v<version>`.
   The `Release` workflow checks the tag and publishes the GitHub release;
   confirm it went green.
5. **Repoint dependents.** In tonk's `Cargo.toml`, every dialog crate takes
   `tag = "v<version>", version = "<version>"`; refresh the lock and let
   tonk's CI run.

## Pre-release

For a dependent that needs a change still in review: on that pull
request's branch, `scripts/release.sh prepare <next>-rc.<n>` (versions
only; the changelog stays under `Unreleased`), commit, tag the commit
`v<next>-rc.<n>`, push the tag, and pin the dependent to it. The `Release`
workflow publishes it as a pre-release. Increment `<n>` for each new head
the dependent needs.
