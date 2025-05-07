import * as Signer from "./signer.ts"
import { assertEquals } from "@std/assert"

Deno.test("test s3 sign", () => {
  const auth = Signer.authorize({
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
    auth.href,
    "https://pale.s3.auto.amazonaws.com/file/path?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Content-Sha256=UNSIGNED-PAYLOAD&X-Amz-Credential=my-id%2F20250507%2Fauto%2Fs3%2Faws4_request&X-Amz-Date=20250507T054859Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host&X-Amz-Signature=09cdc9df7d3590e098888b3663c7e417f6720543da1b35f57e15aed24d438bff"
  )

  assertEquals(
    auth.signingKey,
    new Uint8Array([
      79, 106, 222, 178, 108, 52, 104, 178, 205, 22, 58, 104, 193, 109, 221, 37,
      179, 183, 58, 87, 9, 22, 242, 56, 155, 133, 98, 156, 239, 136, 247, 8,
    ])
  )

  assertEquals(
    auth.payloadHeader,
    `AWS4-HMAC-SHA256
20250507T054859Z
20250507/auto/s3/aws4_request`
  )

  assertEquals(
    auth.payloadBody,
    `PUT
/file/path
X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Content-Sha256=UNSIGNED-PAYLOAD&X-Amz-Credential=my-id%2F20250507%2Fauto%2Fs3%2Faws4_request&X-Amz-Date=20250507T054859Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host
host:pale.s3.auto.amazonaws.com

host
UNSIGNED-PAYLOAD`
  )
  assertEquals(
    auth.signingPayload,
    `AWS4-HMAC-SHA256
20250507T054859Z
20250507/auto/s3/aws4_request
5c93f6200b90cd7dcb4e1e90256531a3f24ed6dc2c54e2837b0b9804456e7ca7`
  )

  assertEquals(
    auth.signature,
    `09cdc9df7d3590e098888b3663c7e417f6720543da1b35f57e15aed24d438bff`
  )
})

Deno.test("test r2 sign", () => {
  const auth = Signer.authorize({
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
    auth.href,
    "https://pale.2c5a882977b89ac2fc7ca2f958422366.r2.cloudflarestorage.com/file/path?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Content-Sha256=UNSIGNED-PAYLOAD&X-Amz-Credential=my-id%2F20250507%2Fauto%2Fs3%2Faws4_request&X-Amz-Date=20250507T054859Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host&X-Amz-Signature=e0363a29790c09a3f0eb52fa12aa1c0dcf6166312c82473d9076178e330afaf9"
  )

  assertEquals(
    auth.signingKey,
    new Uint8Array([
      79, 106, 222, 178, 108, 52, 104, 178, 205, 22, 58, 104, 193, 109, 221, 37,
      179, 183, 58, 87, 9, 22, 242, 56, 155, 133, 98, 156, 239, 136, 247, 8,
    ])
  )

  assertEquals(
    auth.payloadHeader,
    `AWS4-HMAC-SHA256
20250507T054859Z
20250507/auto/s3/aws4_request`
  )

  assertEquals(
    auth.payloadBody,
    `PUT
/file/path
X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Content-Sha256=UNSIGNED-PAYLOAD&X-Amz-Credential=my-id%2F20250507%2Fauto%2Fs3%2Faws4_request&X-Amz-Date=20250507T054859Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host
host:pale.2c5a882977b89ac2fc7ca2f958422366.r2.cloudflarestorage.com

host
UNSIGNED-PAYLOAD`
  )
  assertEquals(
    auth.signingPayload,
    `AWS4-HMAC-SHA256
20250507T054859Z
20250507/auto/s3/aws4_request
ff151cc91640c650163866371ddca4fd268b05c9bb71e5703e7b4c9663696d41`
  )

  assertEquals(
    auth.signature,
    `e0363a29790c09a3f0eb52fa12aa1c0dcf6166312c82473d9076178e330afaf9`
  )
})

Deno.test("test s3 with checksum", () => {
  const auth = Signer.authorize({
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
    auth.href,
    "https://pale.s3.auto.amazonaws.com/file/path?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Content-Sha256=UNSIGNED-PAYLOAD&X-Amz-Credential=my-id%2F20250507%2Fauto%2Fs3%2Faws4_request&X-Amz-Date=20250507T054859Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host%3Bx-amz-checksum-sha256&X-Amz-Signature=2932f4085c638682dbb368529ef59c9da3ecafb4f524533a5e07355a20038555"
  )

  assertEquals(
    auth.signingKey,
    new Uint8Array([
      79, 106, 222, 178, 108, 52, 104, 178, 205, 22, 58, 104, 193, 109, 221, 37,
      179, 183, 58, 87, 9, 22, 242, 56, 155, 133, 98, 156, 239, 136, 247, 8,
    ])
  )

  assertEquals(
    auth.payloadHeader,
    `AWS4-HMAC-SHA256
20250507T054859Z
20250507/auto/s3/aws4_request`
  )

  assertEquals(
    auth.payloadBody,
    `PUT
/file/path
X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Content-Sha256=UNSIGNED-PAYLOAD&X-Amz-Credential=my-id%2F20250507%2Fauto%2Fs3%2Faws4_request&X-Amz-Date=20250507T054859Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host%3Bx-amz-checksum-sha256
host:pale.s3.auto.amazonaws.com
x-amz-checksum-sha256:kgGGxxs9Hqpv0UdShU0CxA4hIU1zaNBpTFMfy4P2ZYs=

host;x-amz-checksum-sha256
UNSIGNED-PAYLOAD`
  )
  assertEquals(
    auth.signingPayload,
    `AWS4-HMAC-SHA256
20250507T054859Z
20250507/auto/s3/aws4_request
9fff0936a02aed12e49bd03e2bd7678c7be7b8252433848e5a3a76d887983e5f`
  )

  assertEquals(
    auth.signature,
    `2932f4085c638682dbb368529ef59c9da3ecafb4f524533a5e07355a20038555`
  )
})

Deno.test("test r2 with checksum", () => {
  const auth = Signer.authorize({
    time: new Date("2025-05-07T05:48:59Z"),
    credentials: {
      accessKeyId: "my-id",
      secretAccessKey: "top secret",
    },
    endpoint:
      "https://2c5a882977b89ac2fc7ca2f958422366.r2.cloudflarestorage.com",
    checksum: "kgGGxxs9Hqpv0UdShU0CxA4hIU1zaNBpTFMfy4P2ZYs=",
    bucket: "pale",
    region: "auto",
    key: "file/path",
  })

  assertEquals(
    auth.href,
    "https://pale.2c5a882977b89ac2fc7ca2f958422366.r2.cloudflarestorage.com/file/path?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Content-Sha256=UNSIGNED-PAYLOAD&X-Amz-Credential=my-id%2F20250507%2Fauto%2Fs3%2Faws4_request&X-Amz-Date=20250507T054859Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host%3Bx-amz-checksum-sha256&X-Amz-Signature=8dc119745d387770784b234ad3a6f5e5afa13b04c9a99e777418bd3380c228cc"
  )

  assertEquals(
    auth.signingKey,
    new Uint8Array([
      79, 106, 222, 178, 108, 52, 104, 178, 205, 22, 58, 104, 193, 109, 221, 37,
      179, 183, 58, 87, 9, 22, 242, 56, 155, 133, 98, 156, 239, 136, 247, 8,
    ])
  )

  assertEquals(
    auth.payloadHeader,
    `AWS4-HMAC-SHA256
20250507T054859Z
20250507/auto/s3/aws4_request`
  )

  assertEquals(
    auth.payloadBody,
    `PUT
/file/path
X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Content-Sha256=UNSIGNED-PAYLOAD&X-Amz-Credential=my-id%2F20250507%2Fauto%2Fs3%2Faws4_request&X-Amz-Date=20250507T054859Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host%3Bx-amz-checksum-sha256
host:pale.2c5a882977b89ac2fc7ca2f958422366.r2.cloudflarestorage.com
x-amz-checksum-sha256:kgGGxxs9Hqpv0UdShU0CxA4hIU1zaNBpTFMfy4P2ZYs=

host;x-amz-checksum-sha256
UNSIGNED-PAYLOAD`
  )
  assertEquals(
    auth.signingPayload,
    `AWS4-HMAC-SHA256
20250507T054859Z
20250507/auto/s3/aws4_request
d1f95bef0508d5a1d77d74b88b6928990bdc43322b0ca015be521793a3edf2ba`
  )

  assertEquals(
    auth.signature,
    `8dc119745d387770784b234ad3a6f5e5afa13b04c9a99e777418bd3380c228cc`
  )
})
