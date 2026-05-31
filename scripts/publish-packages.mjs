import { spawn } from "node:child_process"
import { existsSync } from "node:fs"
import { readdir, readFile } from "node:fs/promises"
import { resolve } from "node:path"

const packagesRoot = resolve("packages")
const npmCommand = resolveNpmCommand()
const dryRun = process.argv.includes("--dry-run") || process.env.NPM_PUBLISH_DRY_RUN === "1"
const packages = await readPackages()
const ordered = orderPackages(packages)

for (const packageInfo of ordered) {
  const args = ["publish", packageInfo.dir, "--access", "public"]
  if (dryRun) {
    args.push("--dry-run")
  } else {
    args.push("--provenance")
  }
  await runNpm(args)
}

console.log(`${dryRun ? "Dry-run published" : "Published"} ${ordered.length} package(s).`)

async function readPackages() {
  const entries = await readdir(packagesRoot, { withFileTypes: true })
  const packages = []
  for (const entry of entries) {
    if (!entry.isDirectory()) {
      continue
    }
    const dir = resolve(packagesRoot, entry.name)
    const packageJsonPath = resolve(dir, "package.json")
    if (!existsSync(packageJsonPath)) {
      continue
    }
    const packageJson = JSON.parse(await readFile(packageJsonPath, "utf8"))
    if (packageJson.private === true) {
      continue
    }
    packages.push({ dir, packageJson })
  }
  return packages
}

function orderPackages(packages) {
  const byName = new Map(packages.map((packageInfo) => [packageInfo.packageJson.name, packageInfo]))
  const visited = new Set()
  const visiting = new Set()
  const output = []

  for (const packageInfo of packages) {
    visit(packageInfo)
  }

  return output

  function visit(packageInfo) {
    const name = packageInfo.packageJson.name
    if (visited.has(name)) {
      return
    }
    if (visiting.has(name)) {
      throw new Error(`Package dependency cycle includes ${name}`)
    }
    visiting.add(name)
    for (const dependencyName of internalDependencies(packageInfo.packageJson)) {
      const dependency = byName.get(dependencyName)
      if (dependency) {
        visit(dependency)
      }
    }
    visiting.delete(name)
    visited.add(name)
    output.push(packageInfo)
  }
}

function internalDependencies(packageJson) {
  return Object.keys({
    ...packageJson.dependencies,
    ...packageJson.peerDependencies,
    ...packageJson.optionalDependencies
  }).filter((name) => name.startsWith("@robbin-io/"))
}

async function runNpm(args) {
  await new Promise((resolve, reject) => {
    const child = spawn(npmCommand.command, [...npmCommand.argsPrefix, ...args], {
      stdio: "inherit",
      windowsHide: true
    })
    child.on("error", reject)
    child.on("exit", (code) => {
      if (code === 0) {
        resolve()
        return
      }
      reject(new Error(`npm ${args.join(" ")} failed with exit code ${code}`))
    })
  })
}

function resolveNpmCommand() {
  if (process.env.npm_execpath) {
    return { command: process.execPath, argsPrefix: [process.env.npm_execpath] }
  }
  if (process.platform === "win32") {
    return { command: process.env.ComSpec ?? "cmd.exe", argsPrefix: ["/d", "/s", "/c", "npm"] }
  }
  return { command: "npm", argsPrefix: [] }
}
