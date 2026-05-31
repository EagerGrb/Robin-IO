import { existsSync } from "node:fs"
import { readFile } from "node:fs/promises"
import { resolve } from "node:path"

const requiredDocs = [
  "docs/release/checklist.md",
  "docs/release/readiness.md",
  "docs/release/rehearsal.md",
  "docs/release/ci-matrix.md",
  "docs/release/candidate-record.md",
  "docs/release/api-surface.core.json"
]

const requiredMentions = new Map([
  [
    "docs/release/checklist.md",
    [
      "docs/release/readiness.md",
      "docs/release/rehearsal.md",
      "docs/release/ci-matrix.md",
      "docs/release/candidate-record.md"
    ]
  ],
  [
    "docs/release/readiness.md",
    ["CORE_RUNTIME_ERROR_CODES", "npm run api:check", "docs/release/ci-matrix.md", "docs/release/candidate-record.md"]
  ],
  [
    "docs/release/rehearsal.md",
    ["npm run verify", "npm run bench:browser-worker", "docs/release/ci-matrix.md", "docs/release/candidate-record.md"]
  ],
  [
    "docs/release/ci-matrix.md",
    [
      "npm run verify",
      "npm run bench",
      "npm run bench:browser-worker",
      "CHROME_PATH",
      "docs/release/candidate-record.md"
    ]
  ],
  [
    "docs/release/candidate-record.md",
    ["npm run verify", "npm run bench", "npm run bench:browser-worker", "npm run changeset:status"]
  ]
])

const failures = []

for (const doc of requiredDocs) {
  if (!existsSync(resolve(doc))) {
    failures.push(`missing ${doc}`)
  }
}

for (const [doc, mentions] of requiredMentions) {
  const path = resolve(doc)
  if (!existsSync(path)) {
    continue
  }
  const content = await readFile(path, "utf8")
  for (const mention of mentions) {
    if (!content.includes(mention)) {
      failures.push(`${doc}: missing mention ${mention}`)
    }
  }
}

if (failures.length > 0) {
  console.error("Release docs check failed:")
  for (const failure of failures) {
    console.error(`- ${failure}`)
  }
  process.exit(1)
}

console.log("Release docs check passed.")
