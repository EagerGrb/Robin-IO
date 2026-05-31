import { RuntimeError, type Transform } from "@robbin-io/core"
import { ZodError, type ZodType } from "zod"

export interface ZodValidationOptions {
  readonly name?: string
}

export function validateWithZod<I, O>(schema: ZodType<O, I>, options: ZodValidationOptions = {}): Transform<I, O> {
  return {
    kind: "transform",
    name: options.name ?? "validate-zod",
    handle(input) {
      const result = schema.safeParse(input)
      if (result.success) {
        return result.data
      }

      throw new RuntimeError("Zod validation failed", {
        code: "ZOD_VALIDATION_ERROR",
        stage: options.name ?? "validate-zod",
        input,
        cause: result.error,
        metadata: {
          issues: serializeIssues(result.error)
        }
      })
    }
  }
}

function serializeIssues(error: ZodError): Array<Record<string, unknown>> {
  return error.issues.map((issue) => ({
    code: issue.code,
    path: issue.path,
    message: issue.message
  }))
}
