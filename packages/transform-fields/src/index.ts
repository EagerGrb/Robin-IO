import { RuntimeError, type RuntimeContext, type Transform } from "@robbin-io/core"

export interface FieldMappingRule<I extends Record<string, unknown> = Record<string, unknown>, O = unknown> {
  readonly from?: string
  readonly to: string
  readonly required?: boolean
  readonly default?: O | ((input: I) => O)
  readonly transform?: (value: unknown, input: I, ctx: RuntimeContext) => O
}

export interface MapFieldsOptions {
  readonly includeUndefined?: boolean
}

export type FieldMappingObject<I extends Record<string, unknown>, O extends Record<string, unknown>> = {
  [K in keyof O]: string | Omit<FieldMappingRule<I, O[K]>, "to">
}

export function mapFields<I extends Record<string, unknown>, O extends Record<string, unknown>>(
  mapping: FieldMappingObject<I, O>,
  options?: MapFieldsOptions
): Transform<I, O>
export function mapFields<I extends Record<string, unknown>>(
  mapping: readonly FieldMappingRule<I, unknown>[],
  options?: MapFieldsOptions
): Transform<I, Record<string, unknown>>
export function mapFields<I extends Record<string, unknown>>(
  mapping: readonly FieldMappingRule<I, unknown>[] | FieldMappingObject<I, Record<string, unknown>>,
  options: MapFieldsOptions = {}
): Transform<I, Record<string, unknown>> {
  const rules = Array.isArray(mapping)
    ? [...mapping]
    : Object.entries(mapping).map(([to, spec]) => normalizeRule(to, spec))

  return {
    kind: "transform",
    name: "map-fields",
    handle(input, ctx) {
      const output: Record<string, unknown> = {}
      for (const rule of rules) {
        const fromPath = rule.from ?? rule.to
        const rawValue = getPath(input, fromPath)
        const value = resolveValue(rawValue, input, ctx, rule)

        if (value === undefined && !options.includeUndefined) {
          continue
        }

        setPath(output, rule.to, value)
      }
      return output
    }
  }
}

function normalizeRule<I extends Record<string, unknown>, O>(
  to: string,
  spec: string | Omit<FieldMappingRule<I, O>, "to">
): FieldMappingRule<I, O> {
  if (typeof spec === "string") {
    return { from: spec, to }
  }

  return {
    ...spec,
    to,
    from: spec.from ?? to
  }
}

function resolveValue<I extends Record<string, unknown>, O>(
  rawValue: unknown,
  input: I,
  ctx: RuntimeContext,
  rule: FieldMappingRule<I, O>
): O | undefined {
  let value = rawValue

  if (value === undefined) {
    if (rule.default !== undefined) {
      value = typeof rule.default === "function" ? (rule.default as (input: I) => O)(input) : rule.default
    } else if (rule.required) {
      throw new RuntimeError(`Missing required field "${rule.from ?? rule.to}"`, {
        code: "FIELD_MAPPING_MISSING",
        stage: "map-fields",
        input,
        metadata: {
          from: rule.from ?? rule.to,
          to: rule.to
        }
      })
    } else {
      return undefined
    }
  }

  if (rule.transform) {
    try {
      return rule.transform(value, input, ctx)
    } catch (error) {
      throw new RuntimeError(`Failed to map field "${rule.from ?? rule.to}" to "${rule.to}"`, {
        code: "FIELD_MAPPING_ERROR",
        stage: "map-fields",
        input,
        cause: error,
        metadata: {
          from: rule.from ?? rule.to,
          to: rule.to
        }
      })
    }
  }

  return value as O
}

function getPath(input: unknown, path: string): unknown {
  if (input === null || input === undefined) {
    return undefined
  }

  const segments = path.split(".")
  let current: any = input
  for (const segment of segments) {
    if (current === null || current === undefined) {
      return undefined
    }
    current = current[segment]
  }
  return current
}

function setPath(target: Record<string, unknown>, path: string, value: unknown): void {
  const segments = path.split(".")
  let current: Record<string, unknown> = target

  for (let index = 0; index < segments.length - 1; index += 1) {
    const segment = segments[index]
    const existing = current[segment]
    if (existing && typeof existing === "object" && !Array.isArray(existing)) {
      current = existing as Record<string, unknown>
      continue
    }

    const next: Record<string, unknown> = {}
    current[segment] = next
    current = next
  }

  current[segments[segments.length - 1]] = value
}
