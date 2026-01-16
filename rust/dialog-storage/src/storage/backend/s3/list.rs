//! S3 ListObjectsV2 operations.
//!
//! This module provides the [`ListResult`] response type for listing objects
//! in an S3 bucket using the [ListObjectsV2] API.
//!
//! [ListObjectsV2]: https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html

use dialog_common::ConditionalSync;
use serde::Deserialize;

use super::{Access, Bucket, Precondition, S3StorageError, StorageAuthorizer, storage};

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

/// Build a list URL with query parameters.
fn build_list_url(
    base_url: url::Url,
    prefix: Option<&str>,
    continuation_token: Option<&str>,
) -> url::Url {
    let mut url = base_url;
    url.query_pairs_mut().append_pair("list-type", "2");
    if let Some(prefix) = prefix {
        url.query_pairs_mut().append_pair("prefix", prefix);
    }
    if let Some(token) = continuation_token {
        url.query_pairs_mut()
            .append_pair("continuation-token", token);
    }
    url
}

impl<Key, Value, C> Bucket<Key, Value, C>
where
    Key: AsRef<[u8]> + Clone + ConditionalSync,
    Value: AsRef<[u8]> + From<Vec<u8>> + Clone + ConditionalSync,
    C: StorageAuthorizer,
{
    /// List objects in the bucket with the configured prefix.
    ///
    /// Returns an iterator over object keys (encoded S3 keys, not decoded).
    /// Use `continuation_token` for pagination when `is_truncated` is true.
    ///
    /// # Prefix behavior
    ///
    /// S3 treats `prefix` as a filter, not a path. Listing with a non-existent prefix
    /// returns 200 OK with an empty `ListBucketResult` (zero keys). This is standard
    /// S3 behavior - the prefix simply filters which keys are returned.
    pub async fn list(
        &self,
        continuation_token: Option<&str>,
    ) -> Result<ListResult, S3StorageError> {
        // Build the list effect with prefix
        let effect = Access(storage::List {
            prefix: self.prefix_path(),
            continuation_token: continuation_token.map(String::from),
        });

        let descriptor = self
            .credentials
            .execute(effect)
            .await
            .map_err(S3StorageError::from)?;

        let response = self
            .send_request(descriptor, None, Precondition::None)
            .await?;

        let status = response.status();

        if !status.is_success() {
            // Try to parse the error body for a more informative message
            let body = response.text().await.unwrap_or_default();
            if let Ok(error) = quick_xml::de::from_str::<S3Error>(&body) {
                let message = error.message.unwrap_or_default();
                return Err(S3StorageError::ServiceError(format!(
                    "{}: {}",
                    error.code, message
                )));
            }
            return Err(S3StorageError::ServiceError(format!(
                "Failed to list objects: {}",
                status
            )));
        }

        let body = response
            .text()
            .await
            .map_err(|e| S3StorageError::TransportError(e.to_string()))?;

        // Parse the XML response
        Self::parse_list_response(&body)
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
        let result: ListBucketResult = quick_xml::de::from_str(xml).map_err(|e| {
            S3StorageError::SerializationError(format!("Failed to parse XML: {}", e))
        })?;

        Ok(ListResult {
            keys: result.contents.into_iter().map(|c| c.key).collect(),
            is_truncated: result.is_truncated,
            next_continuation_token: result.next_continuation_token,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::super::{Bucket, Public, S3StorageError};
    use super::*;
    use url::Url;

    // Type alias for tests that need a concrete Bucket type
    type TestBucket = Bucket<Vec<u8>, Vec<u8>, Public>;

    #[dialog_common::test]
    fn it_builds_list_url_with_prefix() {
        let url = Url::parse("https://s3.amazonaws.com/bucket").unwrap();
        let list_url = build_list_url(url, Some("prefix/"), None);

        assert!(list_url.as_str().contains("list-type=2"));
        assert!(list_url.as_str().contains("prefix=prefix%2F"));
    }

    #[dialog_common::test]
    fn it_builds_list_url_with_continuation_token() {
        let url = Url::parse("https://s3.amazonaws.com/bucket").unwrap();
        let list_url = build_list_url(url, None, Some("token123"));

        assert!(list_url.as_str().contains("continuation-token=token123"));
    }

    #[dialog_common::test]
    fn it_parses_empty_list_response() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
            <ListBucketResult>
                <IsTruncated>false</IsTruncated>
            </ListBucketResult>"#;

        let result = TestBucket::parse_list_response(xml).unwrap();
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

        let result = TestBucket::parse_list_response(xml).unwrap();
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

        let result = TestBucket::parse_list_response(xml).unwrap();
        assert_eq!(result.keys.len(), 1);
        assert!(result.is_truncated);
        assert_eq!(result.next_continuation_token, Some("abc123".to_string()));
    }

    #[dialog_common::test]
    fn it_builds_virtual_hosted_path() {
        // Non-IP endpoints use virtual-hosted style by default
        use super::super::{Address, Public};
        let address = Address::new("https://s3.amazonaws.com", "us-east-1", "bucket");
        let authorizer = Public::new(address).unwrap();
        let backend = Bucket::<Vec<u8>, Vec<u8>, _>::open(authorizer).unwrap();

        // encode_path creates a path that gets combined with the bucket URL
        let path = backend.encode_path(b"key");
        assert_eq!(path, "key");
    }

    #[dialog_common::test]
    fn it_builds_path_with_prefix() {
        // IP/localhost endpoints use path style by default
        use super::super::{Address, Public};
        let address = Address::new("http://localhost:9000", "us-east-1", "bucket");
        let authorizer = Public::new(address).unwrap();
        let backend = Bucket::<Vec<u8>, Vec<u8>, _>::open(authorizer).unwrap().at("prefix");

        let path = backend.encode_path(b"key");
        assert_eq!(path, "prefix/key");
    }

    #[dialog_common::test]
    fn it_errors_on_malformed_xml() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
            <ListBucketResult>
                <IsTruncated>true</IsTruncated>
                <Contents>
                    <Key>key1</Key>
                <!-- missing closing tags -->"#;

        let result = TestBucket::parse_list_response(xml);
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

        let result = TestBucket::parse_list_response(xml);
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

        let result = TestBucket::parse_list_response(xml);
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

        let result = TestBucket::parse_list_response(xml);
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

        let result = TestBucket::parse_list_response(xml);
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

        let result = TestBucket::parse_list_response(xml);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, S3StorageError::ServiceError(ref msg) if msg.contains("AccessDenied")),
            "Expected AccessDenied error, got: {:?}",
            err
        );
    }
}
