import { exposeWorkerTransform } from "@robbin-io/browser"

interface CsvRow {
  readonly id: string
  readonly name: string
  readonly amount: string
  readonly note: string
}

interface NormalizedRow {
  readonly id: number
  readonly name: string
  readonly amountCents: number
  readonly noteLength: number
}

exposeWorkerTransform<CsvRow, NormalizedRow>((input) => ({
  id: Number(input.id),
  name: input.name.toUpperCase(),
  amountCents: Math.round(Number(input.amount) * 100),
  noteLength: input.note.length
}))
