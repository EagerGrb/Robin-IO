import { defineConfig } from "vite"

export default defineConfig({
  resolve: {
    alias: {
      "@robbin-io/browser": new URL("../../packages/browser/src/index.ts", import.meta.url).pathname,
      "@robbin-io/codec-csv": new URL("../../packages/codec-csv/src/index.ts", import.meta.url).pathname,
      "@robbin-io/core": new URL("../../packages/core/src/index.ts", import.meta.url).pathname,
      "@robbin-io/sink-memory": new URL("../../packages/sink-memory/src/index.ts", import.meta.url).pathname
    }
  }
})
