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
export const PUBLIC_READ = "public-read"

export const AMZ_SIGNATURE_QUERY_PARAM = "X-Amz-Signature"

export interface Credentials {
  accessKeyId: string
  secretAccessKey: string
}

export interface KeyMaterial {
  credentials: Credentials
  date: string
  region: string
  service: string

  keyType?: string
}

export interface ScopeOptions {
  /**
   * Time to be used for the date.
   */
  date: string

  /**
   * R2/S3 Region option.
   */
  region: string

  /**
   * Service identifier
   */
  service: string
}

export interface Access {
  /**
   * Credentials to me used.
   */
  credentials: Credentials

  /**
   * Key under which content should be stored.
   */
  key?: string

  /**
   * The sha256 checksum of the object in base64pad encoding.
   */
  checksum?: string

  /**
   * Time to be used for the date.
   */
  time?: Date

  /**
   * HTTP endpoint url found in your R2 console. Can be omitted for S3.
   */
  endpoint?: string

  headers?: Headers

  /**
   * The temporary session token for AWS.
   */
  sessionToken?: string

  /**
   * Should the stored object be public-read.
   */
  publicRead?: boolean

  /**
   * The expiration time of signed URL in seconds. Defaults to 86400
   */
  expires?: number

  method?: string
  bucket: string
  region: string

  service?: string
}

export const authorize = (source: Access) => Authorization.from(source)

interface SearchParamOptions {
  credentials: Credentials
  scope: string
  timestamp: string
  expires: number
  signedHeaders: Headers
  sessionToken?: string
}

interface HeaderOptions {
  host: string
  headers?: Headers
  checksum?: string
}

const deriveHeaders = (options: HeaderOptions) => {
  // Populate headers
  const headers = new Headers(options.headers)
  headers.set(HOST_HEADER, options.host)

  if (options.checksum) {
    headers.set(CHECKSUM_SHA256, options.checksum)
  }

  return headers
}

const deriveSearchParams = ({
  credentials,
  timestamp,
  scope,
  expires,
  sessionToken,
  signedHeaders,
}: SearchParamOptions) => {
  const searchParams = new URLSearchParams()
  searchParams.set(ALGORITHM_QUERY_PARAM, ALGORITHM_IDENTIFIER)
  searchParams.set(SHA256_HEADER, UNSIGNED_PAYLOAD)
  searchParams.set(
    CREDENTIAL_QUERY_PARAM,
    `${credentials.accessKeyId}/${scope}`
  )
  searchParams.set(AMZ_DATE_QUERY_PARAM, timestamp)
  searchParams.set(EXPIRES_QUERY_PARAM, `${expires}`)

  if (sessionToken) {
    searchParams.set(AMZ_SECURITY_TOKEN_QUERY_PARAM, sessionToken)
  }

  searchParams.set(
    SIGNED_HEADERS_QUERY_PARAM,
    formatSignedHeaders(signedHeaders)
  )

  return searchParams
}

class Authorization {
  static from({
    credentials,
    bucket,
    region,
    endpoint,
    sessionToken,
    checksum,
    publicRead = false,
    key = "",
    headers = new Headers(),
    method = "PUT",
    service = "s3",
    time = new Date(),
    expires = 86400,
  }: Access) {
    const host = deriveHost({ bucket, region, endpoint })
    const { pathname } = new URL(`https://${host}/${key}`)
    const timestamp = formatTimestamp(time ?? new Date())
    const date = timestamp.slice(0, 8)
    const scope = deriveScope({ date, region, service })

    return new this(
      service,
      credentials,
      method,
      host,
      pathname,
      headers,
      timestamp,
      date,
      region,
      bucket,
      expires,
      scope,
      checksum,
      sessionToken,
      publicRead
    )
  }
  constructor(
    public service: string,
    public credentials: Credentials,
    public method: string,
    public host: string,
    public pathname: string,
    public baseHeaders: Headers,
    public timestamp: string,
    public date: string,

    public region: string,
    public bucket: string,
    public expires: number,
    public scope: string,
    public checksum: string | undefined,
    public sessionToken: string | undefined,
    public publicRead: boolean
  ) {}

  get searchParams() {
    return deriveSearchParams(this)
  }

  get signingKey() {
    return deriveSigningKey(this)
  }

  get signedHeaders() {
    return deriveHeaders(this)
  }

  get payloadHeader() {
    return derivePayloadHeader(this)
  }
  get payloadBody() {
    return derivePayloadBody(this)
  }
  get signingPayload() {
    return deriveSigningPayload(this)
  }

  get signature() {
    return toHex(hmac(sha256, this.signingKey, this.signingPayload))
  }

  get url() {
    const url = new URL(`https://${this.host}${this.pathname}`)
    for (const [name, ...value] of this.searchParams.entries()) {
      url.searchParams.set(name, value.join(";"))
    }
    url.searchParams.set(AMZ_SIGNATURE_QUERY_PARAM, this.signature)

    return url
  }

  get href() {
    return this.url.href
  }

  toString() {
    return this.href
  }
}

export interface HostOptions {
  bucket: string
  region: string
  endpoint?: string
}

const deriveHost = ({ bucket, endpoint, region }: HostOptions) =>
  endpoint
    ? `${bucket}.${new URL(endpoint).host}`
    : `${bucket}.s3.${region}.amazonaws.com`

export const formatSignedHeaders = (headers: Headers) =>
  [...headers.keys()].sort().join(";")

export const formatTimestamp = (time: Date) =>
  time.toISOString().replace(/[:-]|\.\d{3}/g, "")

interface PayloadMeterial {
  scope: string
  timestamp: string
}

interface PayloadMeterial extends PayloadHeaderMaterial, PayloadBodyMeterial {}

interface PayloadHeaderMaterial {
  scope: string
  timestamp: string
}

export const deriveSigningPayload = (source: PayloadMeterial) =>
  `${derivePayloadHeader(source)}
${toHex(sha256(derivePayloadBody(source)))}`

interface PayloadBodyMeterial {
  pathname: string
  method: string
  signedHeaders: Headers
  searchParams: URLSearchParams
}

interface PayloadHeaderMaterial {
  scope: string
  timestamp: string
}

export const derivePayloadHeader = ({
  scope,
  timestamp,
}: PayloadHeaderMaterial) =>
  `${ALGORITHM_IDENTIFIER}
${timestamp}
${scope}`

export const derivePayloadBody = ({
  pathname,
  method,
  signedHeaders,
  searchParams,
}: PayloadBodyMeterial) =>
  `${method}
${formatPath(pathname)}
${formatSearch(searchParams)}
${formatHeaders(signedHeaders)}

${formatSignedHeaders(signedHeaders)}
${UNSIGNED_PAYLOAD}`

const formatPath = (pathname: string) =>
  encodeURIComponent(pathname).replace(/%2F/g, "/")

export const formatHeaders = (headers: Headers) => {
  const lines = []
  for (const [key, ...values] of headers) {
    lines.push(`${key}:${values.join(";")}`)
  }
  return lines.join("\n")
}

export const formatSearch = (params: URLSearchParams) => {
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

export const deriveScope = ({ date, region, service }: ScopeOptions) =>
  `${date}/${region}/${service}/aws4_request`

export const deriveSigningKey = ({
  credentials,
  date,
  region,
  service,
  keyType = KEY_TYPE_IDENTIFIER,
}: KeyMaterial) => {
  let key: string | Uint8Array = `AWS4${credentials.secretAccessKey}`
  for (const signable of [date, region, service, keyType]) {
    key = hmac(sha256, key, signable)
  }

  return key as Uint8Array
}
