import { hmac } from "@noble/hashes/hmac"
import { sha256 } from "@noble/hashes/sha2"
import { bytesToHex as toHex } from "@noble/hashes/utils"

export const UNSIGNED_PAYLOAD = "UNSIGNED-PAYLOAD"
export const SHA256_HEADER = "X-Amz-Content-Sha256"
export const ALGORITHM_QUERY_PARAM = "X-Amz-Algorithm"
export const CREDENTIAL_QUERY_PARAM = "X-Amz-Credential"
export const AMZ_DATE_QUERY_PARAM = "X-Amz-Date"
export const SIGNED_HEADERS_QUERY_PARAM = "X-Amz-SignedHeaders"
export const EXPIRES_QUERY_PARAM = "X-Amz-Expires"
export const HOST_HEADER = "host"
export const ALGORITHM_IDENTIFIER = "AWS4-HMAC-SHA256"
export const CHECKSUM_SHA256 = "x-amz-checksum-sha256"
export const KEY_TYPE_IDENTIFIER = "aws4_request"
export const AMZ_SECURITY_TOKEN_QUERY_PARAM = "X-Amz-Security-Token"

// For some reason this header MUST be lower case or it is not respected.
export const AMZ_ACL_QUERY_PARAM = "x-amz-acl"

export const AMZ_SIGNATURE_QUERY_PARAM = "X-Amz-Signature"

export interface Credentials {
  accessKeyId: string
  secretAccessKey: string
}

export interface Sign {
  credentials: Credentials
  region: string
  bucket: string

  /**
   * Key under which content should be stored.
   */
  key: string

  /**
   * The sha256 checksum of the object in base64pad encoding.
   */
  checksum?: string

  /**
   * HTTP endpoint url found in your R2 console. Can be omitted for S3.
   */
  endpoint?: string

  /**
   * The expiration time of signed URL in seconds. Defaults to 86400
   */
  expires?: number

  method?: string

  /**
   * Optional time to be used for the date.
   */
  time?: Date

  /**
   * The temporary session token for AWS.
   */
  sessionToken?: string

  headers?: Headers

  /**
   * Should the stored object be public-read.
   */
  publicRead?: boolean

  service?: string
}
export const sign = ({
  credentials,
  endpoint,
  region,
  bucket,
  key,
  checksum,
  headers = new Headers(),
  method = "PUT",
  time = new Date(),
  expires = 86400, // 24 hours
  service = "s3",
  sessionToken,
  publicRead,
}: Sign) => {
  const date = time.toISOString().replace(/[:-]|\.\d{3}/g, "")
  const url = endpoint
    ? new URL(`https://${bucket}.${new URL(endpoint).host}/${key}`)
    : new URL(`https://${bucket}.s3.${region}.amazonaws.com/${key}`)

  headers.set(HOST_HEADER, url.host)
  // const signedHeaders = [HOST_HEADER]
  // const canonicalHeaders = [`${HOST_HEADER}:${url.host}`]

  // add checksum headers
  if (checksum) {
    headers.set(CHECKSUM_SHA256, checksum)
    // signedHeaders.push(CHECKSUM_SHA256)
    // canonicalHeaders.push(`${CHECKSUM_SHA256}:${checksum}`)
  }

  // Set query string
  const params = url.searchParams
  params.set(ALGORITHM_QUERY_PARAM, ALGORITHM_IDENTIFIER)
  params.set(SHA256_HEADER, UNSIGNED_PAYLOAD)

  const scope = deriveScope({ date, region, service })
  params.set(CREDENTIAL_QUERY_PARAM, `${credentials.accessKeyId}/${scope}`)
  params.set(AMZ_DATE_QUERY_PARAM, date)
  params.set(EXPIRES_QUERY_PARAM, `${expires}`)

  if (sessionToken) {
    params.set(AMZ_SECURITY_TOKEN_QUERY_PARAM, sessionToken)
  }

  if (publicRead) {
    params.set(AMZ_ACL_QUERY_PARAM, "public-read")
  }

  // NEED X-Amz-SignedHeaders so we set this first.
  params.set(
    SIGNED_HEADERS_QUERY_PARAM,
    [...headers.keys()].sort().join(";")
    // signedHeaders.join(";")
  )

  const signature = toHex(
    hmac(
      sha256,
      deriveSigningKey(credentials, {
        date: toShortDate(date),
        region,
        service,
      }),
      derivePayload({ url, scope, method, headers })
    )
  )

  params.set(AMZ_SIGNATURE_QUERY_PARAM, signature)

  return url
}

const derivePayload = ({
  url,
  scope,
  headers,
  method,
}: {
  url: URL
  scope: string
  method: string
  headers: Headers
}) =>
  `${ALGORITHM_IDENTIFIER}\n${url.searchParams.get(
    AMZ_DATE_QUERY_PARAM
  )}\n${scope}\n${toHex(sha256(deriveCanonicalString(url, headers, method)))}`

const deriveCanonicalString = (url: URL, headers: Headers, method: string) => {
  const path = encodeURIComponent(url.pathname).replace(/%2F/g, "/")
  const query = deriveQuery(url.searchParams)

  return `${method}\n${path}\n${query}\n${formatHeaders(
    headers
  )}\n\n${url.searchParams.get(
    SIGNED_HEADERS_QUERY_PARAM
  )}\n${UNSIGNED_PAYLOAD}`
}

const formatHeaders = (headers: Headers) => {
  const lines = []
  for (const [key, ...values] of headers) {
    lines.push(`${key}:${values.join(";")}`)
  }
  return lines.join("\n")
}

const deriveQuery = (params: URLSearchParams) => {
  // params.set("x-id", "PutObject")
  // Encode query string to be signed
  const seenKeys = new Set()
  return [...params]
    .filter(([k]) => {
      if (!k) return false // no empty keys
      if (seenKeys.has(k)) return false // first val only for S3
      seenKeys.add(k)
      return true
    })
    .map(pair => pair.map(p => encodeURIComponent(p)))
    .sort(([k1, v1], [k2, v2]) =>
      // eslint-disable-next-line no-nested-ternary
      k1 < k2 ? -1 : k1 > k2 ? 1 : v1 < v2 ? -1 : v1 > v2 ? 1 : 0
    )
    .map(pair => pair.join("="))
    .join("&")
}

const deriveScope = ({
  date,
  region,
  service,
}: {
  date: string
  region: string
  service: string
}) => `${toShortDate(date)}/${region}/${service}/aws4_request`

const deriveSigningKey = (
  credentials: Credentials,
  {
    date,
    region,
    service = "s3",
    keyTypeID = KEY_TYPE_IDENTIFIER,
  }: {
    date: string
    region: string
    service?: string
    keyTypeID?: string
  }
) => {
  let key: string | Uint8Array = `AWS4${credentials.secretAccessKey}`
  for (const signable of [date, region, service, keyTypeID]) {
    key = hmac(sha256, key, signable)
  }

  return key as Uint8Array
}

const toShortDate = (time: string) => time.slice(0, 8)
