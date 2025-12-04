//! S3 ListObjectsV2 operations.
//!
//! This module provides the [`List`] request type and [`ListResult`] response type
//! for listing objects in an S3 bucket using the ListObjectsV2 API.

use serde::Deserialize;
use url::Url;

use super::{Invocation, Request, S3, S3StorageError};
use dialog_common::ConditionalSync;

/// A GET request to list objects in a bucket.
///
/// Uses the S3 ListObjectsV2 API to retrieve object keys.
#[derive(Debug, Clone)]
pub struct List {
    url: Url,
}

impl List {
    /// Create a new list request for the given bucket URL with optional prefix.
    ///
    /// The URL should be the bucket root (e.g., `https://s3.amazonaws.com/bucket`)
    /// with query parameters for `list-type=2` and optionally `prefix`.
    pub fn new(mut url: Url, prefix: Option<&str>, continuation_token: Option<&str>) -> Self {
        url.query_pairs_mut().append_pair("list-type", "2");
        if let Some(prefix) = prefix {
            url.query_pairs_mut().append_pair("prefix", prefix);
        }
        if let Some(token) = continuation_token {
            url.query_pairs_mut()
                .append_pair("continuation-token", token);
        }
        Self { url }
    }
}

impl Invocation for List {
    fn method(&self) -> &'static str {
        "GET"
    }

    fn url(&self) -> &Url {
        &self.url
    }
}
impl Request for List {}

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

impl<Key, Value> S3<Key, Value>
where
    Key: AsRef<[u8]> + Clone + ConditionalSync,
    Value: AsRef<[u8]> + From<Vec<u8>> + Clone + ConditionalSync,
{
    /// Build the bucket URL (for listing operations).
    fn bucket_url(&self) -> Result<Url, S3StorageError> {
        let base_url = self.endpoint.trim_end_matches('/');
        let url_str = format!("{base_url}/{}", self.bucket);

        Url::parse(&url_str)
            .map_err(|e| S3StorageError::ServiceError(format!("Invalid URL: {}", e)))
    }

    /// List objects in the bucket with the configured prefix.
    ///
    /// Returns an iterator over object keys (encoded S3 keys, not decoded).
    /// Use `continuation_token` for pagination when `is_truncated` is true.
    pub async fn list(
        &self,
        continuation_token: Option<&str>,
    ) -> Result<ListResult, S3StorageError> {
        let bucket_url = self.bucket_url()?;
        let list_request = List::new(bucket_url, self.prefix.as_deref(), continuation_token);
        let response = list_request.perform(self).await?;

        if !response.status().is_success() {
            return Err(S3StorageError::ServiceError(format!(
                "Failed to list objects: {}",
                response.status()
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
    pub(crate) fn parse_list_response(xml: &str) -> Result<ListResult, S3StorageError> {
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
    use super::*;
    use crate::s3::Session;

    #[test]
    fn test_list_request() {
        let url = Url::parse("https://s3.amazonaws.com/bucket").unwrap();
        let request = List::new(url.clone(), Some("prefix/"), None);

        assert_eq!(request.method(), "GET");
        assert!(request.url().as_str().contains("list-type=2"));
        assert!(request.url().as_str().contains("prefix=prefix%2F"));
    }

    #[test]
    fn test_list_request_with_continuation_token() {
        let url = Url::parse("https://s3.amazonaws.com/bucket").unwrap();
        let request = List::new(url.clone(), None, Some("token123"));

        assert!(
            request
                .url()
                .as_str()
                .contains("continuation-token=token123")
        );
    }

    #[test]
    fn test_parse_list_response_empty() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
            <ListBucketResult>
                <IsTruncated>false</IsTruncated>
            </ListBucketResult>"#;

        let result = S3::<Vec<u8>, Vec<u8>>::parse_list_response(xml).unwrap();
        assert!(result.keys.is_empty());
        assert!(!result.is_truncated);
        assert!(result.next_continuation_token.is_none());
    }

    #[test]
    fn test_parse_list_response_with_keys() {
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

        let result = S3::<Vec<u8>, Vec<u8>>::parse_list_response(xml).unwrap();
        assert_eq!(result.keys.len(), 2);
        assert_eq!(result.keys[0], "prefix/key1");
        assert_eq!(result.keys[1], "prefix/key2");
        assert!(!result.is_truncated);
    }

    #[test]
    fn test_parse_list_response_truncated() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
            <ListBucketResult>
                <IsTruncated>true</IsTruncated>
                <NextContinuationToken>abc123</NextContinuationToken>
                <Contents>
                    <Key>key1</Key>
                </Contents>
            </ListBucketResult>"#;

        let result = S3::<Vec<u8>, Vec<u8>>::parse_list_response(xml).unwrap();
        assert_eq!(result.keys.len(), 1);
        assert!(result.is_truncated);
        assert_eq!(result.next_continuation_token, Some("abc123".to_string()));
    }

    #[test]
    fn test_bucket_url() {
        let backend =
            S3::<Vec<u8>, Vec<u8>>::open("https://s3.amazonaws.com", "bucket", Session::Public);

        let url = backend.bucket_url().unwrap();
        assert_eq!(url.as_str(), "https://s3.amazonaws.com/bucket");
    }

    #[test]
    fn test_parse_list_response_malformed_xml() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
            <ListBucketResult>
                <IsTruncated>true</IsTruncated>
                <Contents>
                    <Key>key1</Key>
                <!-- missing closing tags -->"#;

        let result = S3::<Vec<u8>, Vec<u8>>::parse_list_response(xml);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, S3StorageError::SerializationError(_)));
    }

    #[test]
    fn test_parse_list_response_unexpected_structure() {
        // XML is valid but doesn't match expected ListBucketResult structure.
        // quick-xml with serde returns defaults (empty keys, not truncated)
        // for mismatched structures - this is acceptable as it won't crash.
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
            <Error>
                <Code>NoSuchBucket</Code>
                <Message>The specified bucket does not exist</Message>
            </Error>"#;

        let result = S3::<Vec<u8>, Vec<u8>>::parse_list_response(xml);
        // quick-xml is lenient and returns defaults for wrong root element
        assert!(result.is_ok());
        let list = result.unwrap();
        assert!(list.keys.is_empty());
        assert!(!list.is_truncated);
    }

    #[test]
    fn test_parse_list_response_wrong_root_element() {
        // Valid XML structure but wrong root element name.
        // quick-xml with serde still parses nested elements if they match,
        // even when the root element name differs. This is acceptable behavior
        // as it means the parser is lenient about the wrapper element.
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
            <WrongRootElement>
                <IsTruncated>false</IsTruncated>
                <Contents>
                    <Key>key1</Key>
                </Contents>
            </WrongRootElement>"#;

        let result = S3::<Vec<u8>, Vec<u8>>::parse_list_response(xml);
        // quick-xml parses nested elements even with wrong root element
        assert!(result.is_ok());
        let list = result.unwrap();
        // The key is still parsed because Contents/Key structure matches
        assert_eq!(list.keys.len(), 1);
        assert_eq!(list.keys[0], "key1");
        assert!(!list.is_truncated);
    }

    #[test]
    fn test_parse_list_response_not_xml() {
        // Not XML at all - should error
        let xml = "this is not xml at all { json: maybe? }";

        let result = S3::<Vec<u8>, Vec<u8>>::parse_list_response(xml);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, S3StorageError::SerializationError(_)));
    }
}
