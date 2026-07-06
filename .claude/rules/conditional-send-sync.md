---
globs: "*.rs"
---

Use `ConditionalSend` and `ConditionalSync` from `dialog_common` instead of raw `Send` and `Sync` bounds. This ensures wasm32 compatibility where `Send`/`Sync` are not available.
