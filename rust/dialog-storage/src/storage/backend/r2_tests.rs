//! Integration tests for R2 storage backend
//! 
//! These tests will only run when the following environment variables are set:
//! - R2_KEY: R2 access key
//! - R2_SECRET: R2 secret key
//! - R2_URL: R2 endpoint URL
//! - R2_BUCKET: R2 bucket name (optional, will use "dialog-test-bucket" if not set)
//!
//! To run these tests specifically:
//! ```
//! export R2_KEY=your_access_key
//! export R2_SECRET=your_secret_key
//! export R2_URL=https://your-account.r2.cloudflarestorage.com
//! export R2_BUCKET=your-bucket-name
//! cargo test --package dialog-storage -- storage::backend::r2_tests
//! ```

use std::env;

use anyhow::Result;

use crate::{
    DialogStorageError, RestStorageBackend, RestStorageConfig, StorageBackend, StorageSink,
};

/// Check if the required environment variables are set
fn r2_env_vars_present() -> bool {
    env::var("R2_KEY").is_ok() && env::var("R2_SECRET").is_ok() && env::var("R2_URL").is_ok()
}

/// Create a test prefix to isolate test data
fn test_prefix() -> String {
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    format!("dialog-test-{}", timestamp)
}

/// Set up the R2 storage backend with credentials from environment variables
fn setup_r2_backend() -> Result<RestStorageBackend<Vec<u8>, Vec<u8>>, DialogStorageError> {
    let r2_key = env::var("R2_KEY").expect("R2_KEY environment variable not set");
    // Secret is checked but not used in this simplified version
    // In a real implementation, we would use a proper S3/R2 client with the secret
    let _r2_secret = env::var("R2_SECRET").expect("R2_SECRET environment variable not set");
    let r2_url = env::var("R2_URL").expect("R2_URL environment variable not set");
    let r2_bucket = env::var("R2_BUCKET").unwrap_or_else(|_| "dialog-test-bucket".to_string());
    let prefix = test_prefix();
    
    // Use AWS Signature v4 credentials in custom headers
    // For R2, we would typically use the AWS SDK for full S3-compatible access
    // This is a simplified approach for testing the basic REST functionality
    let mut headers = Vec::new();
    headers.push(("X-Amz-Access-Key".to_string(), r2_key.clone()));
    
    let config = RestStorageConfig {
        endpoint: r2_url,
        api_key: Some(r2_key),
        bucket: Some(r2_bucket),
        key_prefix: Some(prefix),
        headers,
        ..Default::default()
    };
    
    // Convert the error type
    match RestStorageBackend::new(config) {
        Ok(backend) => Ok(backend),
        Err(e) => Err(DialogStorageError::StorageBackend(e.to_string())),
    }
}

/// Test setup and a basic write operation
#[tokio::test]
async fn test_r2_setup_and_basic_write() -> Result<()> {
    if !r2_env_vars_present() {
        println!("Skipping R2 tests as environment variables are not set");
        return Ok(());
    }
    
    let mut backend = setup_r2_backend()?;
    
    // Test writing to R2
    let test_key = vec![1, 2, 3];
    let test_value = vec![4, 5, 6];
    let result = backend.set(test_key.clone(), test_value.clone()).await;
    
    assert!(result.is_ok(), "Failed to write to R2: {:?}", result);
    
    // Clean up
    // Note: In a real test, we would delete the object, but this simplified implementation
    // doesn't implement delete functionality yet.
    
    Ok(())
}

/// Test reading data from R2
#[tokio::test]
async fn test_r2_read() -> Result<()> {
    if !r2_env_vars_present() {
        println!("Skipping R2 tests as environment variables are not set");
        return Ok(());
    }
    
    let mut backend = setup_r2_backend()?;
    
    // Write a test value
    let test_key = vec![10, 11, 12];
    let test_value = vec![13, 14, 15];
    backend.set(test_key.clone(), test_value.clone()).await?;
    
    // Read it back
    let retrieved = backend.get(&test_key).await?;
    
    assert_eq!(retrieved, Some(test_value), "Retrieved value doesn't match what was written");
    
    Ok(())
}

/// Test reading a non-existent key from R2
#[tokio::test]
async fn test_r2_read_nonexistent() -> Result<()> {
    if !r2_env_vars_present() {
        println!("Skipping R2 tests as environment variables are not set");
        return Ok(());
    }
    
    let backend = setup_r2_backend()?;
    
    // Generate a random key that we haven't written to
    let nonexistent_key = vec![100, 101, 102];
    
    // Try to read it
    let retrieved = backend.get(&nonexistent_key).await?;
    
    assert_eq!(retrieved, None, "Expected None for nonexistent key");
    
    Ok(())
}

/// Test bulk operations using the StorageSink trait
#[tokio::test]
async fn test_r2_bulk_operations() -> Result<()> {
    if !r2_env_vars_present() {
        println!("Skipping R2 tests as environment variables are not set");
        return Ok(());
    }
    
    let mut backend = setup_r2_backend()?;
    
    // Create a stream of test data
    use async_stream::try_stream;
    
    let source_stream = try_stream! {
        for i in 0..3 {
            yield (vec![i, i+1, i+2], vec![i+3, i+4, i+5]);
        }
    };
    
    // Write the data in bulk
    backend.write(source_stream).await?;
    
    // Verify the data was written
    for i in 0..3 {
        let key = vec![i, i+1, i+2];
        let expected_value = vec![i+3, i+4, i+5];
        let retrieved = backend.get(&key).await?;
        
        assert_eq!(retrieved, Some(expected_value), 
            "Retrieved value doesn't match what was written for key {:?}", key);
    }
    
    Ok(())
}

/// Test more complex R2 operations with larger data
#[tokio::test]
async fn test_r2_larger_data() -> Result<()> {
    if !r2_env_vars_present() {
        println!("Skipping R2 tests as environment variables are not set");
        return Ok(());
    }
    
    let mut backend = setup_r2_backend()?;
    
    // Create a larger test value (~10KB)
    let test_key = vec![50, 51, 52];
    let mut test_value = Vec::with_capacity(10_000);
    for i in 0..10_000 {
        test_value.push((i % 256) as u8);
    }
    
    // Write the large value
    backend.set(test_key.clone(), test_value.clone()).await?;
    
    // Read it back
    let retrieved = backend.get(&test_key).await?;
    
    assert_eq!(retrieved, Some(test_value), "Retrieved large value doesn't match what was written");
    
    Ok(())
}

/// Helper test to clean up test data
/// 
/// This test isn't automatically run, but can be executed manually:
/// ```
/// cargo test --package dialog-storage -- storage::backend::r2_tests::cleanup_test_data --exact --nocapture
/// ```
#[tokio::test]
#[ignore]
async fn cleanup_test_data() -> Result<()> {
    if !r2_env_vars_present() {
        println!("Skipping R2 tests as environment variables are not set");
        return Ok(());
    }
    
    // This would require implementing a delete method or using the AWS SDK directly.
    // For simplicity, we'll just log that this would clean up data.
    println!("NOTE: This test would normally clean up test data, but the current implementation doesn't support delete operations.");
    println!("To clean up test data manually, check the R2 console for objects with the 'dialog-test-' prefix.");
    
    Ok(())
}