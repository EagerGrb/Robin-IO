import { describe, expect, it } from "vitest"
import { pipeline } from "@robbin-io/core"
import { memorySink } from "@robbin-io/sink-memory"
import { memorySource } from "@robbin-io/source-memory"
import { mapFields } from "./index"

describe("mapFields", () => {
  it("renames and transforms fields", async () => {
    const sink = memorySink<Record<string, unknown>>()

    const result = await pipeline()
      .from(
        memorySource([
          {
            "User ID": "42",
            "Full Name": " Ada Lovelace ",
            profile: { email: "ada@example.com" }
          }
        ])
      )
      .through(
        mapFields({
          id: { from: "User ID", transform: (value) => String(value) },
          name: { from: "Full Name", transform: (value) => String(value).trim() },
          email: "profile.email"
        })
      )
      .to(sink)
      .run()

    expect(result.ok).toBe(true)
    expect(sink.getItems()).toEqual([
      {
        id: "42",
        name: "Ada Lovelace",
        email: "ada@example.com"
      }
    ])
  })

  it("supports defaults", async () => {
    const sink = memorySink<Record<string, unknown>>()

    const result = await pipeline()
      .from(
        memorySource([
          {
            status: "active"
          }
        ])
      )
      .through(
        mapFields([
          { from: "status", to: "state" },
          { from: "missing", to: "fallback", default: "n/a" }
        ])
      )
      .to(sink)
      .run()

    expect(result.ok).toBe(true)
    expect(sink.getItems()).toEqual([
      {
        state: "active",
        fallback: "n/a"
      }
    ])
  })

  it("reports missing required fields", async () => {
    const sink = memorySink<Record<string, unknown>>()

    const result = await pipeline()
      .from(memorySource([{ status: "active" }]))
      .through(
        mapFields([
          { from: "status", to: "state" },
          { from: "missingRequired", to: "required", required: true }
        ])
      )
      .to(sink)
      .run({ errorMode: "skip-and-collect" })

    expect(result.ok).toBe(false)
    expect(sink.getItems()).toEqual([])
    expect(result.errors[0]?.code).toBe("FIELD_MAPPING_MISSING")
  })
})
