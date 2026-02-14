#!/usr/bin/env bash
#
# Build the Dialog Inspector browser extension.
#
# Produces a loadable extension directory at dist/extension/ containing:
#
#   manifest.json          — MV3 extension manifest
#   devtools.html/js       — devtools panel registration
#   content_loader.js      — thin JS loader for content script WASM
#   panel.html + panel_*.js + panel_*_bg.wasm  — Leptos UI (Trunk output)
#   content.js + content_bg.wasm               — content script (wasm-bindgen)
#
# Prerequisites (available via `nix develop`):
#   - trunk
#   - wasm-bindgen (CLI, must match Cargo.lock version)
#   - wasm-opt (from binaryen, optional)
#
# Usage:
#   cd rust/dialog-inspector
#   ./build-extension.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
OUT="$SCRIPT_DIR/dist/extension"

# ── Preflight checks ────────────────────────────────────────────────
for cmd in trunk wasm-bindgen cargo; do
  if ! command -v "$cmd" &>/dev/null; then
    echo "error: $cmd not found. Run 'nix develop' first." >&2
    exit 1
  fi
done

echo "==> Cleaning output directory"
rm -rf "$OUT"
mkdir -p "$OUT"

# ── 1. Build panel UI with Trunk ────────────────────────────────────
echo "==> Building panel (Trunk)"
(cd "$SCRIPT_DIR" && trunk build --release)

# Trunk outputs to dist/ — copy its output, renaming index.html to panel.html
cp "$SCRIPT_DIR/dist/index.html" "$OUT/panel.html"
# Copy all generated JS and WASM assets
cp "$SCRIPT_DIR"/dist/panel-*.js "$OUT/" 2>/dev/null || true
cp "$SCRIPT_DIR"/dist/panel-*_bg.wasm "$OUT/" 2>/dev/null || true
# Also copy any other trunk-generated assets (CSS gets inlined by trunk)
for f in "$SCRIPT_DIR"/dist/*.wasm "$SCRIPT_DIR"/dist/*.js; do
  [ -f "$f" ] && cp "$f" "$OUT/" 2>/dev/null || true
done

# ── 2. Build content script with cargo + wasm-bindgen ───────────────
echo "==> Building content script (cargo + wasm-bindgen)"

CONTENT_WASM="$WORKSPACE_ROOT/target/wasm32-unknown-unknown/wasm-release/content.wasm"

cargo build \
  --bin content \
  -p dialog-inspector \
  --target wasm32-unknown-unknown \
  --profile wasm-release \
  --manifest-path "$WORKSPACE_ROOT/Cargo.toml"

wasm-bindgen \
  --target web \
  --out-dir "$OUT" \
  --out-name content \
  "$CONTENT_WASM"

# Optimize WASM if wasm-opt is available
if command -v wasm-opt &>/dev/null; then
  echo "==> Optimizing content WASM (wasm-opt)"
  wasm-opt -Os -o "$OUT/content_bg.wasm" "$OUT/content_bg.wasm"
fi

# ── 3. Copy extension static files ─────────────────────────────────
echo "==> Copying extension files"
cp "$SCRIPT_DIR/extension/manifest.json" "$OUT/"
cp "$SCRIPT_DIR/extension/devtools.html" "$OUT/"
cp "$SCRIPT_DIR/extension/devtools.js" "$OUT/"
cp "$SCRIPT_DIR/extension/content_loader.js" "$OUT/"

echo ""
echo "Extension built at: $OUT"
echo ""
echo "To load in Chrome:"
echo "  1. Open chrome://extensions"
echo "  2. Enable 'Developer mode'"
echo "  3. Click 'Load unpacked'"
echo "  4. Select: $OUT"
