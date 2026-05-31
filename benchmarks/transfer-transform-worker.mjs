import { parentPort } from "node:worker_threads"

if (!parentPort) {
  throw new Error("transfer transform benchmark worker requires parentPort")
}

parentPort.on("message", (message) => {
  if (!message || message.type !== "io:transform") {
    return
  }

  const input = message.input
  parentPort.postMessage({
    type: "io:transform-result",
    id: message.id,
    ok: true,
    output: input.byteLength
  })
})
