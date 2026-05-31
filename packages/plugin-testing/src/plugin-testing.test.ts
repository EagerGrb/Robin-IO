import { describe, expect, it } from "vitest"
import { map } from "@robbin-io/core"
import { memorySource } from "@robbin-io/source-memory"
import { collectSource, runTransform } from "./index"

describe("plugin testing helpers", () => {
  it("collects sources and runs transforms", async () => {
    await expect(collectSource(memorySource([1, 2, 3]))).resolves.toEqual([1, 2, 3])
    await expect(
      runTransform(
        [1, 2],
        map((value: number) => value + 1)
      )
    ).resolves.toEqual([2, 3])
  })
})
