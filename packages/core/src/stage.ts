import type { BatchOptions, PipelineStage, Sink, Source, Transform } from "./types.js"

export type InternalStageKind = "source" | "decoder" | "transform" | "encoder" | "batch" | "sink"

export interface InternalStage {
  readonly id: string
  readonly name: string
  readonly kind: InternalStageKind
  readonly concurrency: number
  readonly preserveOrder: boolean
  readonly maxInFlight?: number
}

export interface BuildInternalStagesOptions {
  readonly source: Source<unknown>
  readonly stages: readonly PipelineStage[]
  readonly batchOptions?: BatchOptions
  readonly sink: Sink<unknown>
}

export function buildInternalStages(options: BuildInternalStagesOptions): readonly InternalStage[] {
  const ids = new StageIdAllocator()
  const stages: InternalStage[] = [createInternalStage(ids, options.source.name, "source")]

  for (const stage of options.stages) {
    stages.push(createPipelineStage(ids, stage))
  }

  if (options.batchOptions) {
    stages.push(createInternalStage(ids, "batch", "batch"))
  }

  stages.push(createInternalStage(ids, options.sink.name, "sink"))
  return stages
}

function createPipelineStage(ids: StageIdAllocator, stage: PipelineStage): InternalStage {
  if (stage.kind === "transform") {
    const transform = stage as Transform<unknown, unknown>
    return createInternalStage(ids, transform.name, "transform", {
      concurrency: Math.max(1, transform.concurrency ?? 1),
      preserveOrder: transform.preserveOrder ?? false,
      maxInFlight: transform.maxInFlight
    })
  }

  return createInternalStage(ids, stage.name, stage.kind)
}

function createInternalStage(
  ids: StageIdAllocator,
  name: string,
  kind: InternalStageKind,
  options: Pick<InternalStage, "concurrency" | "preserveOrder" | "maxInFlight"> = {
    concurrency: 1,
    preserveOrder: true
  }
): InternalStage {
  return {
    id: ids.next(name),
    name,
    kind,
    concurrency: options.concurrency,
    preserveOrder: options.preserveOrder,
    maxInFlight: options.maxInFlight
  }
}

class StageIdAllocator {
  private readonly counts = new Map<string, number>()

  next(name: string): string {
    const safeName = name.trim() || "stage"
    const count = (this.counts.get(safeName) ?? 0) + 1
    this.counts.set(safeName, count)
    return `${safeName}#${count}`
  }
}
