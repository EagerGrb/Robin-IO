import { map, pipeline, progressBehavior } from "@robbin-io/core"
import { csvDecoder } from "@robbin-io/codec-csv"
import { jsonlEncoder } from "@robbin-io/codec-jsonl"
import { fsFileSink, fsFileSource } from "@robbin-io/node"

const [, , input = "input.csv", output = "output.jsonl"] = process.argv
const progress = progressBehavior()

const result = await pipeline()
  .from(fsFileSource(input))
  .through(csvDecoder({ header: true }))
  .through(
    map((row: any) => ({
      ...row,
      name: String(row.name ?? "").trim()
    }))
  )
  .through(jsonlEncoder())
  .batch({ size: 1000 })
  .to(fsFileSink(output, { createParentDirectories: true }))
  .run({ runtime: "node", behaviors: [progress], errorMode: "skip-and-collect" })

console.log({ ok: result.ok, progress: progress.getSnapshot(), errors: result.errors.length })
