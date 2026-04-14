---
globs: "*.rs"
---

Use `foo.rs` + `foo/submodule.rs` module structure instead of `foo/mod.rs`.

- CORRECT: `environment.rs` as the module root with `environment/storage.rs`, `environment/test.rs` as submodules
- INCORRECT: `environment/mod.rs` with `environment/storage.rs`
