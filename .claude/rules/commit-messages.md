# Squash commit messages are short

A pull request lands as one squash commit, and `git log` on `main` is what
people skim. Its message is not the pull request body.

When you merge or squash a pull request, set the commit message yourself:

- **Title:** the pull request's conventional title with its number, such as
  `perf(query): pipeline the winner checks of a value-pinned scan (#658)`.
  Mark a breaking change with `!` (see `versioning-and-changelog.md`).
- **Body:** at most three short lines saying what changed and why, then
  `Fixes #N` or `Refs #N` when there is one. An empty body is fine.
- Leave out tables, measurements, file lists, test lists and the review
  history. They stay in the pull request, which the title's number links.

Never accept GitHub's default squash message, which pastes the body or every
commit's message.
