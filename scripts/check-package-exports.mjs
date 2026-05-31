import { existsSync } from "node:fs"
import { readdir, readFile } from "node:fs/promises"
import { resolve } from "node:path"
import { pathToFileURL } from "node:url"

const packagesRoot = resolve("packages")
const releaseReadinessPath = resolve("docs", "release", "readiness.md")
const releaseMatrixPath = resolve("docs", "release", "ci-matrix.md")
const packageDirs = await readdir(packagesRoot, { withFileTypes: true })
const failures = []

if (!existsSync(releaseReadinessPath)) {
  failures.push("missing docs/release/readiness.md")
}

if (!existsSync(releaseMatrixPath)) {
  failures.push("missing docs/release/ci-matrix.md")
}

for (const dirent of packageDirs) {
  if (!dirent.isDirectory()) {
    continue
  }

  const packageDir = resolve(packagesRoot, dirent.name)
  const packageJsonPath = resolve(packageDir, "package.json")
  if (!existsSync(packageJsonPath)) {
    continue
  }

  const packageJson = JSON.parse(await readFile(packageJsonPath, "utf8"))
  if (packageJson.license !== "MIT") {
    failures.push(`${packageJson.name}: expected license MIT`)
  }

  if (packageJson.sideEffects !== false) {
    failures.push(`${packageJson.name}: expected sideEffects false`)
  }

  if (!Array.isArray(packageJson.files) || !packageJson.files.includes("dist")) {
    failures.push(`${packageJson.name}: expected files to include dist`)
  }

  if (packageJson.publishConfig?.access !== "public") {
    failures.push(`${packageJson.name}: expected publishConfig.access public`)
  }

  if (packageJson.engines?.node !== ">=24") {
    failures.push(`${packageJson.name}: expected engines.node >=24`)
  }

  const packageExports = packageJson.exports
  const rootExport = packageExports?.["."]
  if (!rootExport) {
    failures.push(`${packageJson.name}: missing exports["."]`)
    continue
  }

  for (const [exportName, exportEntry] of Object.entries(packageExports)) {
    const importPath = resolve(packageDir, exportEntry.import)
    const typesPath = resolve(packageDir, exportEntry.types)

    if (!existsSync(importPath)) {
      failures.push(`${packageJson.name}: missing import entry ${exportName} -> ${exportEntry.import}`)
    }

    if (!existsSync(typesPath)) {
      failures.push(`${packageJson.name}: missing types entry ${exportName} -> ${exportEntry.types}`)
    }

    if (existsSync(importPath)) {
      try {
        await import(pathToFileURL(importPath).href)
      } catch (error) {
        failures.push(`${packageJson.name}: failed to import ${exportName} -> ${exportEntry.import}: ${error.message}`)
      }
    }
  }

  const importPath = resolve(packageDir, rootExport.import)
  if (existsSync(importPath)) {
    try {
      const module = await import(pathToFileURL(importPath).href)
      if (packageJson.name === "@robbin-io/core" && !module.CORE_RUNTIME_ERROR_CODES) {
        failures.push(`${packageJson.name}: missing CORE_RUNTIME_ERROR_CODES export`)
      }
    } catch (error) {
      failures.push(`${packageJson.name}: failed to import ${rootExport.import}: ${error.message}`)
    }
  }
}

if (failures.length > 0) {
  console.error("Package export check failed:")
  for (const failure of failures) {
    console.error(`- ${failure}`)
  }
  process.exit(1)
}

console.log(`Package export check passed for ${packageDirs.filter((entry) => entry.isDirectory()).length} packages.`)
