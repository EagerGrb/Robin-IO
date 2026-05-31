import { spawn } from "node:child_process"
import { existsSync } from "node:fs"
import { readdir, readFile } from "node:fs/promises"
import { resolve } from "node:path"

const packagesRoot = resolve("packages")
const packageDirs = await readdir(packagesRoot, { withFileTypes: true })
const failures = []
const npmCommand = resolveNpmCommand()
let checked = 0

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
  const packed = await npmPackDryRun(packageJson.name)
  checked += 1

  const files = new Set(packed.files.map((file) => normalizePackagePath(file.path)))
  if (!files.has("package.json")) {
    failures.push(`${packageJson.name}: tarball missing package.json`)
  }

  for (const [exportName, exportEntry] of Object.entries(packageJson.exports ?? {})) {
    for (const field of ["import", "types"]) {
      const target = normalizePackagePath(exportEntry[field])
      if (!files.has(target)) {
        failures.push(`${packageJson.name}: tarball missing export ${exportName} ${field} target ${target}`)
      }
    }
  }

  for (const file of files) {
    if (isForbiddenTarballPath(file)) {
      failures.push(`${packageJson.name}: tarball includes forbidden development file ${file}`)
    }
  }
}

if (failures.length > 0) {
  console.error("Package tarball check failed:")
  for (const failure of failures) {
    console.error(`- ${failure}`)
  }
  process.exit(1)
}

console.log(`Package tarball check passed for ${checked} packages.`)

async function npmPackDryRun(workspace) {
  const output = await runNpm(["pack", "--dry-run", "--json", "--workspace", workspace])
  const parsed = JSON.parse(output)
  const packed = parsed[0]
  if (!packed?.files) {
    throw new Error(`npm pack --dry-run returned no file list for ${workspace}: ${output}`)
  }
  return packed
}

function normalizePackagePath(path) {
  return path.replace(/\\/g, "/").replace(/^\.\//, "")
}

function isForbiddenTarballPath(path) {
  return (
    path.startsWith("src/") ||
    path.includes(".test.") ||
    path.endsWith(".tsbuildinfo") ||
    path === "tsconfig.json" ||
    path === "vite.config.ts"
  )
}

async function runNpm(args) {
  return await new Promise((resolve, reject) => {
    let stdout = ""
    let stderr = ""
    const child = spawn(npmCommand.command, [...npmCommand.argsPrefix, ...args], {
      windowsHide: true,
      stdio: ["ignore", "pipe", "pipe"]
    })
    child.stdout.on("data", (chunk) => {
      stdout += String(chunk)
    })
    child.stderr.on("data", (chunk) => {
      stderr += String(chunk)
    })
    child.on("error", reject)
    child.on("exit", (code) => {
      if (code === 0) {
        resolve(stdout)
        return
      }
      reject(new Error(`npm ${args.join(" ")} failed with exit code ${code}: ${stderr}${stdout}`))
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
