//! S3 ListObjectsV2 operations.
//!
//! This module provides the [`ListResult`] response type for listing objects
//! in an S3 bucket using the [ListObjectsV2] API, as well as the
//! `Provider<storage::List>` implementation for [`S3`].
//!
//! [ListObjectsV2]: https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html

use async_trait::async_trait;
use dialog_capability::{Authority, Capability, Provider, Subject};
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_s3_credentials::capability::storage::List as AuthorizeList;
use serde::Deserialize;

use super::{Bucket, RequestDescriptorExt, S3, S3StorageError};
use crate::capability::storage;

/// Response from S3 ListObjectsV2 API.
#[derive(Debug)]
pub struct ListResult {
    /// Object keys returned in this response.
    pub keys: Vec<String>,
    /// If true, there are more results to fetch.
    pub is_truncated: bool,
    /// Token to use for fetching the next page of results.
    pub next_continuation_token: Option<String>,
}

/// Root element of ListObjectsV2 XML response.
#[derive(Debug, Deserialize)]
#[serde(rename = "ListBucketResult")]
struct ListBucketResult {
    #[serde(rename = "IsTruncated", default)]
    is_truncated: bool,
    #[serde(rename = "Contents", default)]
    contents: Vec<Contents>,
    #[serde(rename = "NextContinuationToken")]
    next_continuation_token: Option<String>,
}

/// Individual object entry in the listing.
#[derive(Debug, Deserialize)]
struct Contents {
    #[serde(rename = "Key")]
    key: String,
}

/// S3 error response XML structure.
///
/// S3 returns `<Error>` XML responses for bucket-level errors like `NoSuchBucket`
/// or `AccessDenied`. We parse these to provide more informative error messages
/// than just treating them as serialization errors.
#[derive(Debug, Deserialize)]
#[serde(rename = "Error")]
struct S3Error {
    #[serde(rename = "Code")]
    code: String,
    #[serde(rename = "Message")]
    message: Option<String>,
}

impl<Issuer> Bucket<Issuer>
where
    Issuer: Authority + ConditionalSend + ConditionalSync + Clone,
    super::S3<Issuer>: Provider<storage::List>,
{
    /// List objects in the bucket with the configured path prefix.
    ///
    /// Returns object keys (encoded S3 keys, not decoded).
    /// Use `continuation_token` for pagination when `is_truncated` is true.
    ///
    /// # Prefix behavior
    ///
    /// S3 treats `prefix` as a filter, not a path. Listing with a non-existent prefix
    /// returns 200 OK with an empty `ListBucketResult` (zero keys). This is standard
    /// S3 behavior - the prefix simply filters which keys are returned.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use async_trait::async_trait;
    /// use dialog_capability::{Authority, DialogCapabilitySignError, Did, Principal};
    /// use dialog_storage::s3::{S3, S3Credentials, Address, Bucket};
    ///
    /// #[derive(Clone)]
    /// struct Issuer(Did);
    /// impl Principal for Issuer {
    ///     fn did(&self) -> &Did { &self.0 }
    /// }
    /// # #[cfg_attr(not(target_arch = "wasm32"), async_trait)]
    /// # #[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
    /// impl Authority for Issuer {
    ///     async fn sign(&mut self, _: &[u8]) -> Result<Vec<u8>, DialogCapabilitySignError> { Ok(Vec::new()) }
    ///     fn secret_key_bytes(&self) -> Option<[u8; 32]> { None }
    /// }
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let address = Address::new("http://localhost:9000", "us-east-1", "my-bucket");
    /// let credentials = S3Credentials::public(address)?;
    /// let issuer = Issuer(Did::from("did:key:zMyIssuer"));
    /// let s3 = S3::from_s3(credentials, issuer);
    /// let bucket = Bucket::new(s3, "did:key:zMySubject", "my-store");
    ///
    /// // List all objects in the store
    /// let result = bucket.list(None).await?;
    /// for key in result.keys {
    ///     println!("Found key: {}", key);
    /// }
    ///
    /// // Handle pagination
    /// if result.is_truncated {
    ///     let next_page = bucket.list(result.next_continuation_token.as_deref()).await?;
    ///     // Process next_page...
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn list(
        &self,
        continuation_token: Option<&str>,
    ) -> Result<ListResult, S3StorageError> {
        // Build the list capability
        let capability = Subject::from(self.subject().to_string())
            .attenuate(storage::Storage)
            .attenuate(storage::Store::new(self.path()))
            .invoke(storage::List::new(continuation_token.map(String::from)));

        // Execute the capability using the Provider trait
        let result = capability
            .perform(&mut self.bucket.clone())
            .await
            .map_err(|e| S3StorageError::ServiceError(e.to_string()))?;

        Ok(ListResult {
            keys: result.keys,
            is_truncated: result.is_truncated,
            next_continuation_token: result.next_continuation_token,
        })
    }
}

// Provider<storage::List> implementation for S3
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Issuer> Provider<storage::List> for S3<Issuer>
where
    Issuer: Authority + ConditionalSend + ConditionalSync,
{
    async fn execute(
        &mut self,
        input: Capability<storage::List>,
    ) -> Result<storage::ListResult, storage::StorageError> {
        // Build the authorization capability
        let store: &storage::Store = input.policy();
        let list: &storage::List = input.policy();
        let capability = Subject::from(input.subject().to_string())
            .attenuate(storage::Storage)
            .attenuate(store.clone())
            .invoke(AuthorizeList::new(list.continuation_token.clone()));

        // Acquire authorization and perform
        let authorized = capability
            .acquire(self)
            .await
            .map_err(|e| storage::StorageError::Storage(e.to_string()))?;

        let authorization = authorized
            .perform(self)
            .await
            .map_err(|e| storage::StorageError::Storage(format!("{:?}", e)))?;

        let client = reqwest::Client::new();
        let builder = authorization.into_request(&client);
        let response = builder
            .send()
            .await
            .map_err(|e| storage::StorageError::Storage(e.to_string()))?;

        if response.status().is_success() {
            let body = response
                .text()
                .await
                .map_err(|e| storage::StorageError::Storage(e.to_string()))?;

            // Parse the XML response
            parse_list_response(&body)
                .map(|result| storage::ListResult {
                    keys: result.keys,
                    is_truncated: result.is_truncated,
                    next_continuation_token: result.next_continuation_token,
                })
                .map_err(|e| storage::StorageError::Storage(e.to_string()))
        } else {
            Err(storage::StorageError::Storage(format!(
                "Failed to list objects: {}",
                response.status()
            )))
        }
    }
}

/// Parse the S3 ListObjectsV2 XML response.
///
/// Returns an error if the XML is an S3 error response (e.g., NoSuchBucket, AccessDenied)
/// or if the XML doesn't have the expected root element.
pub(crate) fn parse_list_response(xml: &str) -> Result<ListResult, S3StorageError> {
    // First, try to parse as an S3 error response
    if let Ok(error) = quick_xml::de::from_str::<S3Error>(xml) {
        let message = error.message.unwrap_or_default();
        return Err(S3StorageError::ServiceError(format!(
            "{}: {}",
            error.code, message
        )));
    }

    // Check that we have the expected root element.
    // quick-xml is lenient and will parse any XML as defaults, so we need to validate.
    if !xml.contains("<ListBucketResult") {
        return Err(S3StorageError::SerializationError(
            "Unexpected XML response: missing ListBucketResult element".into(),
        ));
    }

    // Parse as ListBucketResult
    let result: ListBucketResult = quick_xml::de::from_str(xml)
        .map_err(|e| S3StorageError::SerializationError(format!("Failed to parse XML: {}", e)))?;

    Ok(ListResult {
        keys: result.contents.into_iter().map(|c| c.key).collect(),
        is_truncated: result.is_truncated,
        next_continuation_token: result.next_continuation_token,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[dialog_common::test]
    fn it_parses_empty_list_response() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
            <ListBucketResult>
                <IsTruncated>false</IsTruncated>
            </ListBucketResult>"#;

        let result = parse_list_response(xml).unwrap();
        assert!(result.keys.is_empty());
        assert!(!result.is_truncated);
        assert!(result.next_continuation_token.is_none());
    }

    #[dialog_common::test]
    fn it_parses_list_response_with_keys() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
            <ListBucketResult>
                <IsTruncated>false</IsTruncated>
                <Contents>
                    <Key>prefix/key1</Key>
                    <Size>100</Size>
                </Contents>
                <Contents>
                    <Key>prefix/key2</Key>
                    <Size>200</Size>
                </Contents>
            </ListBucketResult>"#;

        let result = parse_list_response(xml).unwrap();
        assert_eq!(result.keys.len(), 2);
        assert_eq!(result.keys[0], "prefix/key1");
        assert_eq!(result.keys[1], "prefix/key2");
        assert!(!result.is_truncated);
    }

    #[dialog_common::test]
    fn it_parses_truncated_list_response() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
            <ListBucketResult>
                <IsTruncated>true</IsTruncated>
                <NextContinuationToken>abc123</NextContinuationToken>
                <Contents>
                    <Key>key1</Key>
                </Contents>
            </ListBucketResult>"#;

        let result = parse_list_response(xml).unwrap();
        assert_eq!(result.keys.len(), 1);
        assert!(result.is_truncated);
        assert_eq!(result.next_continuation_token, Some("abc123".to_string()));
    }

    #[dialog_common::test]
    fn it_errors_on_malformed_xml() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
            <ListBucketResult>
                <IsTruncated>true</IsTruncated>
                <Contents>
                    <Key>key1</Key>
                <!-- missing closing tags -->"#;

        let result = parse_list_response(xml);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, S3StorageError::SerializationError(_)));
    }

    #[dialog_common::test]
    fn it_errors_on_unexpected_xml_structure() {
        // XML is valid but doesn't have the expected ListBucketResult root element.
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
            <SomeUnknownElement>
                <Foo>bar</Foo>
            </SomeUnknownElement>"#;

        let result = parse_list_response(xml);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, S3StorageError::SerializationError(ref msg) if msg.contains("ListBucketResult")),
            "Expected error about missing ListBucketResult, got: {:?}",
            err
        );
    }

    #[dialog_common::test]
    fn it_errors_on_wrong_root_element() {
        // Valid XML structure but wrong root element name - should error.
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
            <WrongRootElement>
                <IsTruncated>false</IsTruncated>
                <Contents>
                    <Key>key1</Key>
                </Contents>
            </WrongRootElement>"#;

        let result = parse_list_response(xml);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, S3StorageError::SerializationError(ref msg) if msg.contains("ListBucketResult")),
            "Expected error about missing ListBucketResult, got: {:?}",
            err
        );
    }

    #[dialog_common::test]
    fn it_errors_on_non_xml_input() {
        // Not XML at all - should error
        let xml = "this is not xml at all { json: maybe? }";

        let result = parse_list_response(xml);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, S3StorageError::SerializationError(_)));
    }

    #[dialog_common::test]
    fn it_parses_no_such_bucket_error() {
        // S3 returns an Error XML when bucket doesn't exist.
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
            <Error>
                <Code>NoSuchBucket</Code>
                <Message>The specified bucket does not exist</Message>
                <BucketName>nonexistent-bucket</BucketName>
                <RequestId>ABC123</RequestId>
                <HostId>xyz</HostId>
            </Error>"#;

        let result = parse_list_response(xml);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, S3StorageError::ServiceError(ref msg) if msg.contains("NoSuchBucket")),
            "Expected NoSuchBucket error, got: {:?}",
            err
        );
    }

    #[dialog_common::test]
    fn it_parses_access_denied_error() {
        // S3 returns an Error XML when access is denied.
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
            <Error>
                <Code>AccessDenied</Code>
                <Message>Access Denied</Message>
                <RequestId>ABC123</RequestId>
                <HostId>xyz</HostId>
            </Error>"#;

        let result = parse_list_response(xml);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, S3StorageError::ServiceError(ref msg) if msg.contains("AccessDenied")),
            "Expected AccessDenied error, got: {:?}",
            err
        );
    }
}
