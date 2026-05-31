import { existsSync } from "node:fs"
import { readdir, readFile } from "node:fs/promises"
import { resolve } from "node:path"

const configPath = resolve(".changeset", "config.json")
const readmePath = resolve(".changeset", "README.md")

if (!existsSync(configPath)) {
  throw new Error("Missing .changeset/config.json")
}

if (!existsSync(readmePath)) {
  throw new Error("Missing .changeset/README.md")
}

const config = JSON.parse(await readFile(configPath, "utf8"))
const requiredFields = ["changelog", "access", "baseBranch", "updateInternalDependencies"]

for (const field of requiredFields) {
  if (config[field] === undefined) {
    throw new Error(`Missing changeset config field: ${field}`)
  }
}

if (!Array.isArray(config.ignore)) {
  throw new Error("Changeset config field `ignore` must be an array")
}

const ignored = new Set(config.ignore)
const examplesRoot = resolve("examples")
const exampleDirs = await readdir(examplesRoot, { withFileTypes: true })

for (const dirent of exampleDirs) {
  if (!dirent.isDirectory()) {
    continue
  }
  const packageJson = JSON.parse(await readFile(resolve(examplesRoot, dirent.name, "package.json"), "utf8"))
  if (packageJson.private !== true) {
    throw new Error(`${packageJson.name}: examples must be private`)
  }
  if (!ignored.has(packageJson.name)) {
    throw new Error(`${packageJson.name}: private example workspace must be ignored by changesets`)
  }
}

const packagesRoot = resolve("packages")
const packageDirs = await readdir(packagesRoot, { withFileTypes: true })

for (const dirent of packageDirs) {
  if (!dirent.isDirectory()) {
    continue
  }
  const packageJson = JSON.parse(await readFile(resolve(packagesRoot, dirent.name, "package.json"), "utf8"))
  if (packageJson.private === true) {
    continue
  }
  if (ignored.has(packageJson.name)) {
    throw new Error(`${packageJson.name}: publishable package must not be ignored by changesets`)
  }
}

console.log("Changeset config check passed.")
