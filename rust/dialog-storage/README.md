# dialog-storage

Generalized API for constructing content addressed storage from different backends and encoding schemes.

## Storage Backends

### Memory Storage Backend

In-memory HashMap-based storage. All data is kept in memory and never persisted.

```rust
use dialog_storage::MemoryStorageBackend;

let backend = MemoryStorageBackend::<Vec<u8>, Vec<u8>>::default();
```

**Use cases**: Testing, caching, temporary storage

### File System Storage Backend

Stores values as files in a directory, with filenames based on base58-encoded keys.

```rust
use dialog_storage::FileSystemStorageBackend;
use std::path::Path;

let backend = FileSystemStorageBackend::<Vec<u8>, Vec<u8>>::new(
    Path::new("/path/to/storage")
).await?;
```

**Use cases**: Local persistence, desktop applications, server-side storage

**Platform**: Not available on WASM

### IndexedDB Storage Backend

Browser-based persistent storage using IndexedDB.

```rust
use dialog_storage::IndexedDbStorageBackend;

let backend = IndexedDbStorageBackend::<Vec<u8>, Vec<u8>>::new(
    "my-database",
    "my-store"
).await?;
```

**Use cases**: Web applications

**Platform**: WASM only

### REST Storage Backend

Flexible HTTP-based storage supporting multiple authentication methods.

#### No Authentication

For publicly accessible endpoints that don't require authentication.

```rust
use dialog_storage::{RestStorageBackend, RestStorageConfig, AuthMethod};

let config = RestStorageConfig {
    endpoint: "https://api.example.com".to_string(),
    auth_method: AuthMethod::None,
    bucket: Some("my-bucket".to_string()),      // Optional
    key_prefix: Some("data".to_string()),        // Optional
    ..Default::default()
};

let backend = RestStorageBackend::<Vec<u8>, Vec<u8>>::new(config)?;
```

**URL Structure**: Keys are base64-encoded and the URL format depends on configuration:
- With bucket and prefix: `{endpoint}/{bucket}/{prefix}/{base64_key}`
- With bucket only: `{endpoint}/{bucket}/{base64_key}`
- With prefix only: `{endpoint}/{prefix}/{base64_key}`
- Neither: `{endpoint}/{base64_key}`

**HTTP Methods**:
- `GET` - Retrieve a value (returns 404 if not found)
- `PUT` - Store a value

**Note**: The endpoint must allow public read/write access.

#### Bearer Token

```rust
use dialog_storage::{RestStorageBackend, RestStorageConfig, AuthMethod};

let config = RestStorageConfig {
    endpoint: "https://api.example.com".to_string(),
    auth_method: AuthMethod::Bearer("your-api-token".to_string()),
    bucket: Some("my-bucket".to_string()),      // Optional
    key_prefix: Some("data".to_string()),        // Optional
    headers: vec![("X-Custom-Header".to_string(), "value".to_string())],
    ..Default::default()
};

let backend = RestStorageBackend::<Vec<u8>, Vec<u8>>::new(config)?;
```

**URL Structure**: Same as No Authentication (keys are base64-encoded):
- With bucket and prefix: `{endpoint}/{bucket}/{prefix}/{base64_key}`
- With bucket only: `{endpoint}/{bucket}/{base64_key}`
- With prefix only: `{endpoint}/{prefix}/{base64_key}`
- Neither: `{endpoint}/{base64_key}`

**HTTP Methods**: All requests include `Authorization: Bearer {token}` header
- `GET` - Retrieve a value (returns 404 if not found)
- `PUT` - Store a value

#### AWS S3

First, create an IAM user with S3 read/write permissions for your bucket, then generate an access key:

```rust
use dialog_storage::{RestStorageBackend, RestStorageConfig, AuthMethod, S3Credentials};

let config = RestStorageConfig {
    endpoint: "https://s3.amazonaws.com".to_string(),
    auth_method: AuthMethod::S3(S3Credentials {
        access_key_id: std::env::var("AWS_ACCESS_KEY_ID")?,
        secret_access_key: std::env::var("AWS_SECRET_ACCESS_KEY")?,
        region: "us-east-1".to_string(),
        expires: 3600,
        ..Default::default()
    }),
    bucket: Some("my-bucket".to_string()),
    key_prefix: Some("my-app-data".to_string()),
    ..Default::default()
};

let mut backend = RestStorageBackend::<Vec<u8>, Vec<u8>>::new(config)?;
backend.set(b"key".to_vec(), b"value".to_vec()).await?;
```

#### Cloudflare R2

First, create an R2 API token with object read/write permissions for your bucket. Use the S3 API credentials (not the R2 Auth token):

```rust
use dialog_storage::{RestStorageBackend, RestStorageConfig, AuthMethod, S3Credentials};

let config = RestStorageConfig {
    endpoint: "https://YOUR_ACCOUNT_ID.r2.cloudflarestorage.com".to_string(),
    auth_method: AuthMethod::S3(S3Credentials {
        access_key_id: std::env::var("R2_ACCESS_KEY_ID")?,
        secret_access_key: std::env::var("R2_SECRET_ACCESS_KEY")?,
        region: "auto".to_string(),  // R2 uses "auto" as the region
        expires: 3600,
        ..Default::default()
    }),
    bucket: Some("my-bucket".to_string()),
    key_prefix: Some("data".to_string()),
    ..Default::default()
};

let mut backend = RestStorageBackend::<Vec<u8>, Vec<u8>>::new(config)?;
backend.set(b"key".to_vec(), b"value".to_vec()).await?;
```

## Testing

```bash
# Unit tests
cargo test --lib

# HTTP integration tests (mockito)
cargo test --lib --features http_tests

# Local S3 tests (in-memory s3s server)
cargo test --lib --features s3_integration_tests local_s3

# Real S3/R2 tests (requires env vars)
R2S3_HOST=https://account.r2.cloudflarestorage.com \
  R2S3_REGION=auto \
  R2S3_BUCKET=my-bucket \
  R2S3_ACCESS_KEY_ID=xxx \
  R2S3_SECRET_ACCESS_KEY=yyy \
  cargo test --lib --features s3_integration_tests -- --ignored
```
