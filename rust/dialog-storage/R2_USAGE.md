# Using the REST Storage Backend with R2

This guide explains how to use the REST storage backend with Cloudflare R2 or other S3-compatible storage services.

## Configuration

To use the REST backend with R2, you need to create a configuration:

```rust
use dialog_storage::{RestStorageBackend, RestStorageConfig};

fn create_r2_backend() -> Result<RestStorageBackend<Vec<u8>, Vec<u8>>, Error> {
    let config = RestStorageConfig {
        // R2 endpoint URL
        endpoint: "https://<account-id>.r2.cloudflarestorage.com".to_string(),
        
        // R2 API token or access key
        api_key: Some("<your-api-key>".to_string()),
        
        // R2 bucket name
        bucket: Some("<your-bucket>".to_string()),
        
        // Optional key prefix for organizing your data
        key_prefix: Some("my-app/data/".to_string()),
        
        // Optional custom headers
        headers: vec![
            // Add any custom headers required by your R2 setup
            // For S3-compatible authentication, you might need to add
            // signature headers here or use a proper AWS SDK
        ],
        
        // Timeout in seconds
        timeout_seconds: Some(30),
        
        ..Default::default()
    };
    
    RestStorageBackend::new(config)
        .map_err(|e| e.into())
}
```

## Basic Operations

Once you have created a backend, you can use it just like any other storage backend:

```rust
// Write a value
let key = vec![1, 2, 3];
let value = vec![4, 5, 6];
backend.set(key.clone(), value.clone()).await?;

// Read a value
let retrieved = backend.get(&key).await?;
assert_eq!(retrieved, Some(value));
```

## Integration Tests

This crate includes integration tests for R2 that will run when the required environment variables are set:

- `R2_KEY`: R2 access key
- `R2_SECRET`: R2 secret key (not actually used in the simplified implementation)
- `R2_URL`: R2 endpoint URL
- `R2_BUCKET`: R2 bucket name (optional, will use "dialog-test-bucket" if not set)

To run the tests:

```bash
export R2_KEY=your_access_key
export R2_SECRET=your_secret_key
export R2_URL=https://your-account.r2.cloudflarestorage.com
export R2_BUCKET=your-bucket-name
cargo test --package dialog-storage -- storage::backend::r2_tests
```

> **Note**: These tests will create objects in your R2 bucket with keys prefixed with `dialog-test-{timestamp}`. You might want to set up a lifecycle rule to automatically delete these test objects.

## Using with AWS S3 or MinIO

The same configuration approach works with AWS S3 or MinIO, but you might need to customize the authentication headers or use a more comprehensive S3 client implementation for production use.

For full S3 compatibility including authentication, consider enhancing the REST backend with AWS Signature V4 support or using the AWS SDK in parallel with this implementation.

## Limitations

The current implementation is simplified and has the following limitations:

1. No built-in support for AWS Signature V4 authentication
2. No delete operation (though this could be added)
3. No handling of multipart uploads for large files (>5GB)
4. No support for S3/R2-specific features like bucket policies, ACLs, etc.

These limitations can be addressed by extending the implementation or using a proper S3 client alongside this backend.