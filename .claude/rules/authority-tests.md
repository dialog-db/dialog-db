# Tests at authority boundaries

A claim in a PR body or a doc comment of the form "A cannot X", "X is
refused for Y", or "X requires Y" is pinned by a test, and the test:

- performs X *as* the principal, mode, or subject the claim says is refused
  (a `Session`, a peer with no grant, another peer sharing the storage), not
  as a stand-in;
- asserts the specific error variant (`Withheld`, `Unauthorized`,
  `Containment`, `NotFound`, ...) with `matches!`, never `is_err()`;
- checks that nothing was written as a side effect of the refusal when the
  path writes on success.

Storage-layer tests (cells, paths, names, listing) run on the filesystem
provider as well as the volatile one. The volatile provider keys by the raw
string and cannot see path-shaped bugs.

A test that narrows its platform (`#[cfg(not(target_arch = "wasm32"))]`) to
get CI green says in its doc comment what the other platform does instead
and names the test that covers it there. Narrowing a test is never the fix
for a failure.
