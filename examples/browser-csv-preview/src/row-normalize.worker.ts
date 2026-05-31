import { exposeWorkerTransform } from "@robbin-io/browser"

exposeWorkerTransform<Record<string, string>, Record<string, string>>((row) => {
  const normalized: Record<string, string> = {}
  for (const [key, value] of Object.entries(row)) {
    normalized[key.trim()] = String(value ?? "").trim()
  }
  return normalized
})
