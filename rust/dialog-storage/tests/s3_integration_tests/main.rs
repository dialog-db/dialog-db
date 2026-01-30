//! S3 Integration Tests
//!
//! These tests run against a real S3/R2/MinIO endpoint and require the following
//! environment variables to be set:
//!
//! - R2S3_ENDPOINT: The S3-compatible endpoint
//! - R2S3_REGION: AWS region (e.g., "us-east-1" or "auto" for R2)
//! - R2S3_BUCKET: Bucket name
//! - R2S3_ACCESS_KEY_ID: Access key ID
//! - R2S3_SECRET_ACCESS_KEY: Secret access key
//!
//! Run with:
//! ```bash
//! cargo test -p dialog-storage --features s3-integration-tests --test s3_integration_tests
//! ```

mod bucket;
mod storage;
mod transactional_memory;
