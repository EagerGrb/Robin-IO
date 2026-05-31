import { defineConfig } from "vitest/config"

export default defineConfig({
  test: {
    environment: "node",
    include: ["packages/**/*.test.ts"]
  },
  resolve: {
    alias: {
      "@robbin-io/core": new URL("./packages/core/src/index.ts", import.meta.url).pathname,
      "@robbin-io/codec-jsonl": new URL("./packages/codec-jsonl/src/index.ts", import.meta.url).pathname,
      "@robbin-io/source-memory": new URL("./packages/source-memory/src/index.ts", import.meta.url).pathname,
      "@robbin-io/sink-memory": new URL("./packages/sink-memory/src/index.ts", import.meta.url).pathname,
      "@robbin-io/node": new URL("./packages/node/src/index.ts", import.meta.url).pathname,
      "@robbin-io/browser": new URL("./packages/browser/src/index.ts", import.meta.url).pathname,
      "@robbin-io/source-file": new URL("./packages/source-file/src/index.ts", import.meta.url).pathname,
      "@robbin-io/sink-file": new URL("./packages/sink-file/src/index.ts", import.meta.url).pathname,
      "@robbin-io/codec-csv": new URL("./packages/codec-csv/src/index.ts", import.meta.url).pathname,
      "@robbin-io/plugin-testing": new URL("./packages/plugin-testing/src/index.ts", import.meta.url).pathname,
      "@robbin-io/transform-fields": new URL("./packages/transform-fields/src/index.ts", import.meta.url).pathname,
      "@robbin-io/validation-zod": new URL("./packages/validation-zod/src/index.ts", import.meta.url).pathname
    }
  }
})
