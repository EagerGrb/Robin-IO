import { describe, expect, it } from "vitest"
import { RuntimeError, createDeadLetterRecord } from "@robbin-io/core"
import { deadLetterSink, memorySink } from "./index"

describe("memory sinks", () => {
  it("collects memory sink items", () => {
    const sink = memorySink<number>()

    sink.write({
      id: 0,
      items: [1, 2],
      size: 2,
      createdAt: Date.now()
    })

    expect(sink.getItems()).toEqual([1, 2])
  })

  it("collects serializable dead-letter records", () => {
    const sink = deadLetterSink()
    const record = createDeadLetterRecord(
      new RuntimeError("bad record", {
        code: "VALIDATION_ERROR",
        stage: "validate",
        input: { id: "" },
        metadata: { recordIndex: 1 }
      })
    )

    sink.write({
      id: 0,
      items: [record],
      size: 1,
      createdAt: Date.now()
    })

    expect(sink.getRecords()).toHaveLength(1)
    expect(JSON.parse(JSON.stringify(sink.getRecords()[0]))).toMatchObject({
      code: "VALIDATION_ERROR",
      message: "bad record",
      stage: "validate",
      input: { id: "" },
      metadata: { recordIndex: 1 }
    })
  })
})
