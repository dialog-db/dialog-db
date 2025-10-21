# dialog-storage

Generalized API for constructing content addressed storage from different
backends and encoding schemes.

## Storage Backends

### REST Storage Backend with S3/R2 Support

The `RestStorageBackend` provides a flexible HTTP-based storage backend that supports:
- Generic REST APIs
- AWS S3-compatible storage
- Cloudflare R2
- Any S3-compatible service

#### Features

- **Multiple Authentication Methods**:
  - None (public endpoints)
  - Bearer token authentication
  - AWS SigV4 signed URLs (S3/R2)

- **S3/R2 Capabilities**:
  - Automatic SHA-256 checksum calculation and verification
  - Pre-signed URL generation
  - Bucket and key prefix support
  - Custom endpoint configuration

- **Configurable Options**:
  - Custom HTTP headers
  - Request timeouts
  - Bucket organization
  - Key prefixes

#### Usage Examples

##### Basic REST API (No Auth)

```rust
use dialog_storage::{RestStorageBackend, RestStorageConfig, AuthMethod};

let config = RestStorageConfig {
    endpoint: "https://api.example.com".to_string(),
    auth_method: AuthMethod::None,
    ..Default::default()
};

let backend = RestStorageBackend::<Vec<u8>, Vec<u8>>::new(config)?;
```

##### Bearer Token Authentication

```rust
use dialog_storage::{RestStorageBackend, RestStorageConfig, AuthMethod};

let config = RestStorageConfig {
    endpoint: "https://api.example.com".to_string(),
    auth_method: AuthMethod::Bearer("your-api-token".to_string()),
    headers: vec![
        ("X-Custom-Header".to_string(), "value".to_string())
    ],
    ..Default::default()
};

let backend = RestStorageBackend::<Vec<u8>, Vec<u8>>::new(config)?;
```

##### AWS S3

```rust
use dialog_storage::{
    RestStorageBackend, RestStorageConfig, AuthMethod, S3Credentials
};

let s3_creds = S3Credentials {
    access_key_id: std::env::var("AWS_ACCESS_KEY_ID")?,
    secret_access_key: std::env::var("AWS_SECRET_ACCESS_KEY")?,
    session_token: None,
    region: "us-east-1".to_string(),
    public_read: false,
    expires: 3600, // URL expiration in seconds
};

let config = RestStorageConfig {
    endpoint: "https://s3.amazonaws.com".to_string(),
    auth_method: AuthMethod::S3(s3_creds),
    bucket: Some("my-bucket".to_string()),
    key_prefix: Some("my-app-data".to_string()),
    ..Default::default()
};

let mut backend = RestStorageBackend::<Vec<u8>, Vec<u8>>::new(config)?;

// Store data
backend.set(b"key1".to_vec(), b"value1".to_vec()).await?;

// Retrieve data
let value = backend.get(&b"key1".to_vec()).await?;
```

##### Cloudflare R2

```rust
use dialog_storage::{
    RestStorageBackend, RestStorageConfig, AuthMethod, S3Credentials
};

let r2_creds = S3Credentials {
    access_key_id: std::env::var("R2_ACCESS_KEY_ID")?,
    secret_access_key: std::env::var("R2_SECRET_ACCESS_KEY")?,
    session_token: None,
    region: "auto".to_string(), // R2 uses "auto" as the region
    public_read: false,
    expires: 3600,
};

let config = RestStorageConfig {
    endpoint: "https://YOUR_ACCOUNT_ID.r2.cloudflarestorage.com".to_string(),
    auth_method: AuthMethod::S3(r2_creds),
    bucket: Some("my-r2-bucket".to_string()),
    key_prefix: Some("data".to_string()),
    ..Default::default()
};

let backend = RestStorageBackend::<Vec<u8>, Vec<u8>>::new(config)?;
```

## Testing

### Unit Tests

Run all unit tests:

```bash
cargo test --lib
```

### HTTP Integration Tests (Mockito)

These tests use mockito to mock HTTP responses:

```bash
cargo test --lib --features http_tests
```

### S3 Integration Tests

Real S3/R2 integration tests require environment variables. These tests are marked with `#[ignore]` and must be explicitly run.

#### Local End-to-End Tests (using s3s)

The crate includes a local in-memory S3 server for end-to-end testing without external dependencies:

```bash
cargo test --lib --features s3_integration_tests local_s3
```

These tests spin up a local S3-compatible server and test the full request/response cycle including S3 signing.

#### Using MinIO (Local S3)

1. Start MinIO:
```bash
docker run -p 9000:9000 -p 9001:9001 \
  -e "MINIO_ROOT_USER=minioadmin" \
  -e "MINIO_ROOT_PASSWORD=minioadmin" \
  minio/minio server /data --console-address ":9001"
```

2. Create a bucket via the MinIO console at http://localhost:9001 or using the `mc` client

3. Run tests:
```bash
R2S3_HOST=http://localhost:9000 \
  R2S3_REGION=us-east-1 \
  R2S3_BUCKET=test-bucket \
  R2S3_ACCESS_KEY_ID=minioadmin \
  R2S3_SECRET_ACCESS_KEY=minioadmin \
  cargo test --lib --features s3_integration_tests -- --ignored
```

#### Using Cloudflare R2

```bash
R2S3_HOST=https://YOUR_ACCOUNT_ID.r2.cloudflarestorage.com \
  R2S3_REGION=auto \
  R2S3_BUCKET=your-bucket \
  R2S3_ACCESS_KEY_ID=your-r2-access-key \
  R2S3_SECRET_ACCESS_KEY=your-r2-secret-key \
  cargo test --lib --features s3_integration_tests -- --ignored
```

#### Using AWS S3

```bash
R2S3_HOST=https://s3.amazonaws.com \
  R2S3_REGION=us-east-1 \
  R2S3_BUCKET=your-bucket \
  R2S3_ACCESS_KEY_ID=your-aws-access-key \
  R2S3_SECRET_ACCESS_KEY=your-aws-secret-key \
  cargo test --lib --features s3_integration_tests -- --ignored
```