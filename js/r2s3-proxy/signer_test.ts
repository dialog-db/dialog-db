import { sign } from "./signer.ts"
import { assertEquals } from "@std/assert"

Deno.test("test s3 sign", () => {
  const url = sign({
    time: new Date("2025-05-07T05:48:59Z"),
    credentials: {
      accessKeyId: "my-id",
      secretAccessKey: "top secret",
    },
    bucket: "pale",
    region: "auto",
    key: "file/path",
  })

  assertEquals(
    url.href,
    "https://pale.s3.auto.amazonaws.com/file/path?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Content-Sha256=UNSIGNED-PAYLOAD&X-Amz-Credential=my-id%2F20250507%2Fauto%2Fs3%2Faws4_request&X-Amz-Date=20250507T054859Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host&X-Amz-Signature=09cdc9df7d3590e098888b3663c7e417f6720543da1b35f57e15aed24d438bff"
  )
})

Deno.test("test r2 sign", () => {
  const url = sign({
    time: new Date("2025-05-07T05:48:59Z"),
    credentials: {
      accessKeyId: "my-id",
      secretAccessKey: "top secret",
    },
    bucket: "pale",
    region: "auto",
    key: "file/path",
    endpoint:
      "https://2c5a882977b89ac2fc7ca2f958422366.r2.cloudflarestorage.com",
  })

  assertEquals(
    url.href,
    "https://pale.2c5a882977b89ac2fc7ca2f958422366.r2.cloudflarestorage.com/file/path?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Content-Sha256=UNSIGNED-PAYLOAD&X-Amz-Credential=my-id%2F20250507%2Fauto%2Fs3%2Faws4_request&X-Amz-Date=20250507T054859Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host&X-Amz-Signature=e0363a29790c09a3f0eb52fa12aa1c0dcf6166312c82473d9076178e330afaf9"
  )
})

Deno.test("test s3 with checksum", () => {
  const url = sign({
    time: new Date("2025-05-07T05:48:59Z"),
    credentials: {
      accessKeyId: "my-id",
      secretAccessKey: "top secret",
    },
    checksum: "kgGGxxs9Hqpv0UdShU0CxA4hIU1zaNBpTFMfy4P2ZYs=",
    bucket: "pale",
    region: "auto",
    key: "file/path",
  })

  assertEquals(
    url.href,
    "https://pale.s3.auto.amazonaws.com/file/path?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Content-Sha256=UNSIGNED-PAYLOAD&X-Amz-Credential=my-id%2F20250507%2Fauto%2Fs3%2Faws4_request&X-Amz-Date=20250507T054859Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host%3Bx-amz-checksum-sha256&X-Amz-Signature=2932f4085c638682dbb368529ef59c9da3ecafb4f524533a5e07355a20038555"
  )
})
