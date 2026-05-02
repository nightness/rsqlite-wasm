import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    // The Node-target wasm-pack output uses CommonJS and synchronous WASM
    // initialization, which works under Node without any browser shims.
    environment: "node",
    include: ["test/**/*.test.ts"],
    // Each test file gets a fresh WASM module — keeps state isolated.
    isolate: true,
    testTimeout: 10000,
    coverage: {
      provider: "v8",
      // The .ts entry points are user-facing surface; the wasm glue under
      // dist/ is generated and excluded by default.
      include: ["src/**/*.ts"],
      exclude: ["dist/**", "scripts/**", "test/**", "**/*.d.ts"],
      reporter: ["text", "lcov", "html"],
      reportsDirectory: "coverage",
      // No threshold. The Node test target loads the wasm-bindgen output
      // directly and exercises the Rust surface (where the real coverage
      // gate is — see `scripts/coverage.sh rust`). The src/*.ts wrappers
      // (Database, WorkerDatabase) are thin JS shims around web APIs
      // (dynamic import, Worker) that don't run under node, so a
      // threshold here would be aspirational rather than meaningful.
      // Coverage is reported as an artifact for inspection.
    },
  },
});
