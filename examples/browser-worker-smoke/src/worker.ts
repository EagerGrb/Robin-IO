import { exposeWorkerTransform } from "@robbin-io/browser"

exposeWorkerTransform<ArrayBuffer, number>((input) => input.byteLength)
