import { describe, expect, it } from "vitest"
import { z } from "zod"
import { pipeline } from "@robbin-io/core"
import { memorySink } from "@robbin-io/sink-memory"
import { memorySource } from "@robbin-io/source-memory"
import { validateWithZod } from "./index"

describe("validateWithZod", () => {
  it("validates and returns parsed data", async () => {
    const sink = memorySink<{ id: string; age: number }>()
    const schema = z.object({
      id: z.string(),
      age: z.coerce.number()
    })

    const result = await pipeline()
      .from(memorySource([{ id: "u1", age: "42" }]))
      .through(validateWithZod(schema))
      .to(sink)
      .run()

    expect(result.ok).toBe(true)
    expect(sink.getItems()).toEqual([{ id: "u1", age: 42 }])
  })

  it("reports serializable validation issues", async () => {
    const sink = memorySink()
    const schema = z.object({
      id: z.string().min(1)
    })

    const result = await pipeline()
      .from(memorySource([{ id: "" }]))
      .through(validateWithZod(schema))
      .to(sink)
      .run({ errorMode: "skip-and-collect" })

    expect(result.ok).toBe(false)
    expect(sink.getItems()).toEqual([])
    expect(result.errors[0]?.code).toBe("ZOD_VALIDATION_ERROR")
    expect(JSON.parse(JSON.stringify(result.errors[0]?.metadata))).toMatchObject({
      issues: [
        {
          path: ["id"]
        }
      ]
    })
  })
})
