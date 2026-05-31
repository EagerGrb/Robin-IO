import { defineConfig } from "vite"

export default defineConfig({
  resolve: {
    alias: {
      "@robbin-io/core": new URL("../../packages/core/src/index.ts", import.meta.url).pathname,
      "@robbin-io/browser": new URL("../../packages/browser/src/index.ts", import.meta.url).pathname,
      "@robbin-io/sink-memory": new URL("../../packages/sink-memory/src/index.ts", import.meta.url).pathname,
      "@robbin-io/source-memory": new URL("../../packages/source-memory/src/index.ts", import.meta.url).pathname
    }
  }
})
