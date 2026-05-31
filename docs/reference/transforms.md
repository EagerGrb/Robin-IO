# Transform API / Transform API 参�?
Transform packages provide reusable record-level behavior without binding core to business rules.

Transform 包提供可复用的记录级行为，但不会把业务规则绑定到 core�?
## Field Mapping / 字段映射

```ts
import { mapFields } from "@robbin-io/transform-fields"

pipeline().through(
  mapFields({
    id: "User ID",
    name: { from: "Full Name", transform: (value) => String(value).trim() }
  })
)
```

`mapFields()` supports renaming, nested paths, defaults, required fields, and per-field transforms.

`mapFields()` 支持重命名、嵌套路径、默认值、必填字段和字段级转�?�?
## Zod Validation / Zod 校验

```ts
import { z } from "zod"
import { validateWithZod } from "@robbin-io/validation-zod"

const schema = z.object({
  id: z.string(),
  age: z.coerce.number()
})

pipeline().through(validateWithZod(schema))
```

Validation failures become `RuntimeError` records with `code: "ZOD_VALIDATION_ERROR"` and serializable issue metadata.

校验失败会变�?`RuntimeError`，错误码�?`ZOD_VALIDATION_ERROR`，并带有可序列化�?issue metadata�?