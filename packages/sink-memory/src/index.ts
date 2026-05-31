import type { Batch, DeadLetterRecord, Sink } from "@robbin-io/core"

export interface MemorySink<T> extends Sink<T> {
  getItems(): readonly T[]
  clear(): void
}

export function memorySink<T>(name = "memory-sink"): MemorySink<T> {
  const items: T[] = []

  return {
    kind: "sink",
    name,
    write(batch: Batch<T>) {
      items.push(...batch.items)
    },
    getItems() {
      return [...items]
    },
    clear() {
      items.length = 0
    }
  }
}

export interface DeadLetterSink extends Sink<DeadLetterRecord> {
  getRecords(): readonly DeadLetterRecord[]
}

export function deadLetterSink(name = "dead-letter-sink"): DeadLetterSink {
  const records: DeadLetterRecord[] = []

  return {
    kind: "sink",
    name,
    write(batch) {
      records.push(...batch.items)
    },
    getRecords() {
      return [...records]
    }
  }
}
