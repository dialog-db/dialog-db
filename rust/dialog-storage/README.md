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

### S3 Storage Backend

S3-compatible storage backend supporting AWS S3, Cloudflare R2, MinIO, and other S3-compatible services.

#### Public Access (No Authentication)

For publicly accessible buckets that don't require authentication:

```rust
use dialog_storage::s3::{S3, Session};

let mut backend = S3::<Vec<u8>, Vec<u8>>::open(
    "https://s3.amazonaws.com",
    "my-bucket",
    Session::Public
).with_prefix("data");

backend.set(b"key".to_vec(), b"value".to_vec()).await?;
let value = backend.get(&b"key".to_vec()).await?;
```

#### AWS S3

First, create an IAM user with S3 read/write permissions for your bucket, then generate an access key:

```rust
use dialog_storage::s3::{S3, Credentials, Service, Session};

let credentials = Credentials {
    access_key_id: std::env::var("AWS_ACCESS_KEY_ID")?,
    secret_access_key: std::env::var("AWS_SECRET_ACCESS_KEY")?,
    session_token: None,
};

let service = Service::s3("us-east-1"); // Your AWS region
let session = Session::new(&credentials, &service, 3600); // 1 hour expiry

let mut backend = S3::<Vec<u8>, Vec<u8>>::open(
    "https://s3.us-east-1.amazonaws.com",
    "my-bucket",
    session
).with_prefix("data");

backend.set(b"key".to_vec(), b"value".to_vec()).await?;
```

#### Cloudflare R2

First, create an R2 API token with object read/write permissions for your bucket. Use the S3 API credentials (not the R2 Auth token):

```rust
use dialog_storage::s3::{S3, Credentials, Service, Session};

let credentials = Credentials {
    access_key_id: std::env::var("R2_ACCESS_KEY_ID")?,
    secret_access_key: std::env::var("R2_SECRET_ACCESS_KEY")?,
    session_token: None,
};

let service = Service::s3("auto"); // R2 uses "auto" as the region
let session = Session::new(&credentials, &service, 3600);

let mut backend = S3::<Vec<u8>, Vec<u8>>::open(
    "https://YOUR_ACCOUNT_ID.r2.cloudflarestorage.com",
    "my-bucket",
    session
).with_prefix("data");

backend.set(b"key".to_vec(), b"value".to_vec()).await?;
```

#### Key Encoding

Keys are automatically encoded to be S3-safe:
- Safe characters (`a-z`, `A-Z`, `0-9`, `-`, `_`, `.`) are kept as-is
- Unsafe characters or binary data are base58-encoded with a `!` prefix
- Path separators (`/`) in keys create S3 key hierarchies

```rust
// These keys will be stored as:
// "simple-key"           -> "simple-key"
// "path/to/key"          -> "path/to/key"
// "key with spaces"      -> "!<base58>"
// "safe/user@email.com"  -> "safe/!<base58>"
```

#### Additional Options

```rust
use dialog_storage::s3::{S3, Hasher, Session};

let mut backend = S3::<Vec<u8>, Vec<u8>>::open(
    "https://s3.amazonaws.com",
    "my-bucket",
    Session::Public
)
.with_prefix("my-prefix")      // Optional key prefix
.with_hasher(Hasher::Sha256);  // Checksum algorithm (default: SHA256)
```

#### Operations

```rust
// Set a value
backend.set(key.clone(), value.clone()).await?;

// Get a value
let value = backend.get(&key).await?; // Returns Option<Vec<u8>>

// Delete a value
backend.delete(&key).await?;

// List objects (returns S3 keys with prefix)
let result = backend.list(None).await?;
for key in result.keys {
    println!("{}", key);
}
```

## Testing

```bash
# Run all unit tests
cargo test --lib

# Run local S3 tests (uses in-memory s3s server)
cargo test --lib local_s3

# Run real S3/R2 integration tests (requires env vars)
R2S3_HOST=https://YOUR_ACCOUNT_ID.r2.cloudflarestorage.com \
R2S3_REGION=auto \
R2S3_BUCKET=my-bucket \
R2S3_ACCESS_KEY_ID=xxx \
R2S3_SECRET_ACCESS_KEY=yyy \
cargo test --lib --features s3_integration_tests
```

## R2 Configuration

### API Token

You need to set up an API token with the following settings:

#### Permissions

Object Read & Write: Allows the ability to read, write, and list objects in specific buckets.

#### Specify bucket(s)

Allow access to the buckets you want to enable.

### CORS Policy

To make it usable for web clients, you need to set up a CORS policy as follows:

```json
[
  {
    "AllowedOrigins": [
      "*"
    ],
    "AllowedMethods": [
      "GET",
      "POST",
      "PUT",
      "DELETE",
      "HEAD"
    ],
    "AllowedHeaders": [
      "*"
    ],
    "ExposeHeaders": [
      "ETag",
      "x-amz-checksum-sha256"
    ]
  }
]
```
