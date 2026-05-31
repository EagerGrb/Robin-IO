# Transform API

Transform packages provide reusable record-level behavior without binding core to business rules.

## Field Mapping

```ts
import { mapFields } from "@robbin-io/transform-fields"

pipeline().through(
  mapFields({
    id: "User ID",
    name: { from: "Full Name", transform: (value) => String(value).trim() }
  })
)
```

`mapFields()` supports:

- renaming;
- nested paths;
- defaults;
- required fields;
- per-field transforms.

## Zod Validation

```ts
import { z } from "zod"
import { validateWithZod } from "@robbin-io/validation-zod"

const schema = z.object({
  id: z.string(),
  age: z.coerce.number()
})

pipeline().through(validateWithZod(schema))
```

Validation failures become `RuntimeError` values with `code: "ZOD_VALIDATION_ERROR"` and serializable issue metadata.
