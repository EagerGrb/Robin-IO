import { spawn } from "node:child_process"
import { existsSync } from "node:fs"
import { mkdtemp, rm } from "node:fs/promises"
import { createServer } from "node:net"
import { tmpdir } from "node:os"
import { join } from "node:path"

const npmCommand = resolveNpmCommand()

export async function runNpm(args) {
  await run(npmCommand.command, [...npmCommand.argsPrefix, ...args])
}

export async function getFreePort() {
  const server = createServer()
  await new Promise((resolve, reject) => {
    server.once("error", reject)
    server.listen(0, "127.0.0.1", resolve)
  })
  const address = server.address()
  await new Promise((resolve, reject) => server.close((error) => (error ? reject(error) : resolve())))
  if (!address || typeof address === "string") {
    throw new Error("Unable to allocate a free local port")
  }
  return address.port
}

export function startPreview({ workspace, port }) {
  let stdout = ""
  let stderr = ""
  let stopping = false
  const child = spawn(
    npmCommand.command,
    [...npmCommand.argsPrefix, "run", "preview", "-w", workspace, "--", "--port", String(port), "--strictPort"],
    {
      stdio: ["ignore", "pipe", "pipe"],
      windowsHide: true
    }
  )
  child.stdout.on("data", (chunk) => {
    stdout += String(chunk)
  })
  child.stderr.on("data", (chunk) => {
    stderr += String(chunk)
  })
  child.on("error", (error) => {
    throw error
  })
  child.on("exit", (code) => {
    if (!stopping && code !== 0 && code !== null) {
      console.error(`Preview process for ${workspace} exited with ${code}: ${stdout}${stderr}`)
    }
  })
  return {
    child,
    getOutput: () => `${stdout}${stderr}`,
    async stop() {
      stopping = true
      await killProcessTree(child)
    }
  }
}

export async function waitForPreviewReady(preview) {
  const deadline = Date.now() + 15_000
  while (Date.now() < deadline) {
    const output = stripAnsi(preview.getOutput())
    if (/Local:\s+http:\/\/127\.0\.0\.1:\d+\//.test(output) || /Local:\s+http:\/\/localhost:\d+\//.test(output)) {
      return
    }
    if (preview.child.exitCode !== null) {
      throw new Error(`Preview process exited before ready: ${output}`)
    }
    await delay(100)
  }
  throw new Error(`Timed out waiting for preview readiness. Output: ${preview.getOutput()}`)
}

export async function waitForHttp(targetUrl) {
  const deadline = Date.now() + 15_000
  let lastError
  while (Date.now() < deadline) {
    try {
      const response = await fetch(targetUrl)
      if (response.ok) {
        return
      }
    } catch (error) {
      lastError = error
    }
    await delay(100)
  }
  throw new Error(
    `Timed out waiting for ${targetUrl}: ${lastError instanceof Error ? lastError.message : String(lastError)}`
  )
}

export async function runPageInChrome({ executablePath, targetUrl, resultExpression, timeoutMs = 15_000 }) {
  const userDataDir = await mkdtemp(join(tmpdir(), "io-browser-worker-"))
  const chrome = spawn(
    executablePath,
    [
      "--headless=new",
      "--disable-gpu",
      "--no-first-run",
      "--no-default-browser-check",
      `--user-data-dir=${userDataDir}`,
      "--remote-debugging-port=0",
      "about:blank"
    ],
    { stdio: ["ignore", "pipe", "pipe"], windowsHide: true }
  )

  try {
    const browserWsUrl = await waitForDevToolsUrl(chrome)
    const pageWsUrl = await createPage(browserWsUrl)
    const client = await connectCdp(pageWsUrl)
    try {
      await client.send("Runtime.enable")
      await client.send("Page.enable")
      await client.send("Page.navigate", { url: targetUrl })
      return await waitForPageResult(client, resultExpression, timeoutMs)
    } finally {
      client.close()
    }
  } finally {
    chrome.kill()
    await waitForExit(chrome)
    await rm(userDataDir, { recursive: true, force: true, maxRetries: 5, retryDelay: 200 }).catch(() => undefined)
  }
}

export function findChromePath() {
  if (process.platform === "win32") {
    const candidates = [
      `${process.env.ProgramFiles}\\Google\\Chrome\\Application\\chrome.exe`,
      `${process.env["ProgramFiles(x86)"]}\\Google\\Chrome\\Application\\chrome.exe`,
      `${process.env.LOCALAPPDATA}\\Google\\Chrome\\Application\\chrome.exe`,
      `${process.env.ProgramFiles}\\Microsoft\\Edge\\Application\\msedge.exe`,
      `${process.env["ProgramFiles(x86)"]}\\Microsoft\\Edge\\Application\\msedge.exe`
    ]
    return candidates.find((candidate) => candidate && existsSync(candidate))
  }
  if (process.platform === "darwin") {
    const candidate = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
    return existsSync(candidate) ? candidate : undefined
  }
  return "google-chrome"
}

async function run(command, args) {
  await new Promise((resolve, reject) => {
    const child = spawn(command, args, { stdio: "inherit", windowsHide: true })
    child.on("error", reject)
    child.on("exit", (code) => {
      if (code === 0) {
        resolve()
        return
      }
      reject(new Error(`${command} ${args.join(" ")} failed with exit code ${code}`))
    })
  })
}

async function waitForExit(child) {
  if (child.exitCode !== null || child.killed) {
    await delay(250)
    return
  }
  await Promise.race([new Promise((resolve) => child.once("exit", resolve)), delay(5000)])
}

async function killProcessTree(child) {
  if (child.exitCode !== null) {
    return
  }
  if (process.platform === "win32" && child.pid) {
    await new Promise((resolve) => {
      const killer = spawn("taskkill", ["/pid", String(child.pid), "/t", "/f"], {
        stdio: "ignore",
        windowsHide: true
      })
      killer.on("exit", resolve)
      killer.on("error", resolve)
    })
    await waitForExit(child)
    return
  }
  child.kill()
  await waitForExit(child)
}

export async function stopProcessTree(child) {
  await killProcessTree(child)
}

function waitForDevToolsUrl(child) {
  return new Promise((resolve, reject) => {
    const deadline = setTimeout(() => {
      cleanup()
      reject(new Error("Timed out waiting for Chrome DevTools URL"))
    }, 15_000)

    const onData = (chunk) => {
      const match = String(chunk).match(/DevTools listening on (ws:\/\/[^\s]+)/)
      if (match) {
        cleanup()
        resolve(match[1])
      }
    }
    const onExit = (code) => {
      cleanup()
      reject(new Error(`Chrome exited before DevTools was ready with code ${code}`))
    }
    const cleanup = () => {
      clearTimeout(deadline)
      child.stderr.off("data", onData)
      child.stdout.off("data", onData)
      child.off("exit", onExit)
    }

    child.stderr.on("data", onData)
    child.stdout.on("data", onData)
    child.on("exit", onExit)
  })
}

async function createPage(browserWsUrl) {
  const browserUrl = new URL(browserWsUrl)
  const response = await fetch(`http://${browserUrl.host}/json/new?about:blank`, { method: "PUT" })
  if (!response.ok) {
    throw new Error(`Unable to create Chrome page: ${response.status} ${response.statusText}`)
  }
  const page = await response.json()
  if (!page.webSocketDebuggerUrl) {
    throw new Error(`Chrome page response did not include webSocketDebuggerUrl: ${JSON.stringify(page)}`)
  }
  return page.webSocketDebuggerUrl
}

async function connectCdp(pageWsUrl) {
  const socket = new WebSocket(pageWsUrl)
  const pending = new Map()
  let nextId = 1

  await new Promise((resolve, reject) => {
    socket.addEventListener("open", resolve, { once: true })
    socket.addEventListener("error", () => reject(new Error("Chrome DevTools WebSocket failed to open")), {
      once: true
    })
  })

  socket.addEventListener("message", (event) => {
    const message = JSON.parse(String(event.data))
    if (!message.id) {
      return
    }
    const request = pending.get(message.id)
    if (!request) {
      return
    }
    pending.delete(message.id)
    if (message.error) {
      request.reject(new Error(`${message.error.code}: ${message.error.message}`))
      return
    }
    request.resolve(message.result)
  })

  return {
    send(method, params = {}) {
      const id = nextId++
      socket.send(JSON.stringify({ id, method, params }))
      return new Promise((resolve, reject) => {
        pending.set(id, { resolve, reject })
      })
    },
    close() {
      socket.close()
    }
  }
}

async function waitForPageResult(client, expression, timeoutMs) {
  const deadline = Date.now() + timeoutMs
  let lastValue
  while (Date.now() < deadline) {
    const result = await client.send("Runtime.evaluate", {
      expression,
      returnByValue: true
    })
    if (result.result?.value) {
      return result.result.value
    }
    const status = await client.send("Runtime.evaluate", {
      expression: "document.body?.innerText",
      returnByValue: true
    })
    lastValue = status.result?.value
    await delay(100)
  }
  throw new Error(`Timed out waiting for page result: ${expression}; last page text: ${lastValue ?? "<empty>"}`)
}

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

function stripAnsi(value) {
  return value.replace(/\u001b\[[0-9;]*m/g, "")
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
