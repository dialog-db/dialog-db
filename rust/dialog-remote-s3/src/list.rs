//! S3 ListObjectsV2 operations.
//!
//! This module provides the [`ListResult`] response type for listing objects
//! in an S3 bucket using the [ListObjectsV2] API, as well as XML parsing utilities.
//!
//! [ListObjectsV2]: https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html

use serde::Deserialize;

use crate::s3::S3StorageError;

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

#[derive(Debug, Deserialize)]
struct Contents {
    #[serde(rename = "Key")]
    key: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename = "Error")]
struct S3Error {
    #[serde(rename = "Code")]
    code: String,
    #[serde(rename = "Message")]
    message: Option<String>,
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
        let xml = "this is not xml at all { json: maybe? }";

        let result = parse_list_response(xml);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, S3StorageError::SerializationError(_)));
    }

    #[dialog_common::test]
    fn it_parses_no_such_bucket_error() {
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
