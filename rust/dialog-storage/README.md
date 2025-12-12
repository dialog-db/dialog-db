# dialog-storage

Generalized API for constructing content addressed storage from different backends and encoding schemes.

## Storage Backends

| Backend | Use Case | Native | WASM |
|---------|----------|:------:|:----:|
| `MemoryStorageBackend` | Testing, caching, temporary storage | ✓ | ✓ |
| `FileSystemStorageBackend` | Local persistence, desktop/server apps | ✓ | ✗ |
| `IndexedDbStorageBackend` | Browser persistent storage | ✗ | ✓ |
| `S3` | Cloud storage (AWS S3, Cloudflare R2, MinIO) | ✓ | ✓ |

## R2 Configuration

### API Token

Create an R2 API token with the following settings:

**Permissions**: Object Read & Write (allows reading, writing, and listing objects)

**Bucket Access**: Specify the buckets you want to enable access for

### CORS Policy

For web clients, configure a CORS policy on your bucket:

```json
[
  {
    "AllowedOrigins": ["*"],
    "AllowedMethods": ["GET", "POST", "PUT", "DELETE", "HEAD"],
    "AllowedHeaders": ["*"],
    "ExposeHeaders": ["ETag", "x-amz-checksum-sha256"]
  }
]
```
