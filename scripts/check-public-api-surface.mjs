import { readFile } from "node:fs/promises"
import { resolve } from "node:path"
import { pathToFileURL } from "node:url"

const surfaces = [await readSurface("api-surface.core.json")]
const failures = []

for (const surface of surfaces) {
  const packageDir = packageDirFor(surface.package)
  const packageJson = JSON.parse(await readFile(resolve(packageDir, "package.json"), "utf8"))
  const importPath = resolve(packageDir, packageJson.exports["."].import)
  const module = await import(pathToFileURL(importPath).href)
  const actual = Object.keys(module).sort()
  const expected = [...surface.stable, ...surface.experimental].sort()

  for (const name of expected) {
    if (!actual.includes(name)) {
      failures.push(`${surface.package}: missing expected root value export ${name}`)
    }
  }

  for (const name of actual) {
    if (!expected.includes(name)) {
      failures.push(`${surface.package}: unexpected root value export ${name}`)
    }
  }
}

if (failures.length > 0) {
  console.error("Public API surface check failed:")
  for (const failure of failures) {
    console.error(`- ${failure}`)
  }
  process.exit(1)
}

console.log(`Public API surface check passed for ${surfaces.length} package(s).`)

async function readSurface(name) {
  return JSON.parse(await readFile(resolve("docs", "release", name), "utf8"))
}

function packageDirFor(packageName) {
  const name = packageName.replace(/^@robbin-io\//, "")
  return resolve("packages", name)
}
