---
globs: "*.rs"
---

NEVER use "// ===" style section separator comments. This includes any variation like:
- `// =========`
- `// ===== Section Name =====`
- `// =========================`

If section headers are needed, use regular comments without decorative equals signs. Better yet, organize code to not need section separators at all.
