#!/usr/bin/env bash
# Run the same coverage checks CI runs, locally.
#
# Usage:
#   scripts/coverage.sh           # both Rust + JS
#   scripts/coverage.sh rust      # Rust only
#   scripts/coverage.sh js        # JS only
#
# Requires:
#   - cargo-llvm-cov (cargo install cargo-llvm-cov)
#   - llvm-tools-preview component (rustup component add llvm-tools-preview)
#   - npm install in js/ for the JS half

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

mode="${1:-all}"

run_rust() {
  echo "==> Rust coverage"
  if ! command -v cargo-llvm-cov >/dev/null; then
    echo "cargo-llvm-cov not installed. Install with:" >&2
    echo "  cargo install cargo-llvm-cov" >&2
    exit 1
  fi
  # `--summary-only` makes the threshold check + the printed table use the
  # same filtered set; lcov.info is generated via the second pass.
  local IGNORE='(_tests\.rs|/tests/|target/|crates/rsqlite-wasm/)'
  cargo llvm-cov --workspace --summary-only \
    --ignore-filename-regex "$IGNORE" \
    --fail-under-lines 75
  cargo llvm-cov report --lcov --output-path lcov.info \
    --ignore-filename-regex "$IGNORE"
}

run_js() {
  echo "==> JS coverage"
  cd "$ROOT/js"
  npm run test:coverage
}

case "$mode" in
  all)  run_rust; run_js ;;
  rust) run_rust ;;
  js)   run_js ;;
  *)    echo "unknown mode: $mode (expected: all | rust | js)" >&2; exit 2 ;;
esac
