import { sign } from "./signer.ts"
import { sha256 } from "multiformats/hashes/sha2"
import { base64pad } from "multiformats/bases/base64"
import * as Raw from "multiformats/codecs/raw"
import * as Link from "multiformats/link"
import * as Blake3 from "blake3-multihash"

const config = {
  credentials: {
    accessKeyId: Deno.env.get("R2S3_ACCESS_KEY_ID") ?? "",
    secretAccessKey: Deno.env.get("R2S3_SECRET_ACCESS_KEY") ?? "",
  },
  bucket: Deno.env.get("R2S3_BUCKET") ?? "",
  expiry: parseInt(Deno.env.get("R2S3_EXPIRY") ?? "86400"),
  publicRead: !Deno.env.has("R2S3_PRIVATE_READ"),
  region: Deno.env.get("R2S3_REGION") ?? "auto",
  endpoint: Deno.env.get("R2S3_ENDPOINT") ?? "",
}

Deno.serve(request => {
  switch (request.method) {
    case "PUT": {
      return put(request)
    }
    case "GET": {
      return get(request)
    }
    default: {
      return new Response("Not supported", {
        status: 405,
      })
    }
  }
})

const put = async (request: Request): Promise<Response> => {
  const body = new Uint8Array(await request.bytes())
  const digest = await sha256.digest(body)
  const link = Link.create(Raw.code, await Blake3.digest(body))
  const key = `${link}.blob`
  const checksum = base64pad.baseEncode(digest.digest)

  const url = sign({
    credentials: config.credentials,
    endpoint: config.endpoint,
    bucket: config.bucket,
    publicRead: config.publicRead,
    key,
    region: "auto",
    checksum,
    headers: new Headers({
      "content-length": `${body.byteLength}`,
    }),
  })

  console.log(`PUT: ${url}`)

  return await fetch(url.href, {
    method: "PUT",
    body,
    headers: {
      "x-amz-checksum-sha256": checksum,
      "content-length": String(body.byteLength),
    },
  })
}

const get = async (request: Request): Promise<Response> => {
  const { pathname, searchParams } = new URL(request.url)

  const url = sign({
    credentials: config.credentials,
    endpoint: config.endpoint,
    bucket: config.bucket,
    publicRead: config.publicRead,
    key: pathname.slice(1),
    region: "auto",
    method: "GET",
  })

  for (const [key, value] of searchParams) {
    url.searchParams.set(key, value)
  }

  console.log(`GET: ${url}`)

  return await fetch(url.href, {
    method: "GET",
  })
}
