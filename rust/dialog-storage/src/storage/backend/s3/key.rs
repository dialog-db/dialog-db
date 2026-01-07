//! S3 key encoding and decoding.
//!
//! Keys are automatically encoded to be S3-safe. Keys are treated as `/`-delimited
//! paths, and each segment is encoded independently:
//! - Segments containing only safe characters (`a-z`, `A-Z`, `0-9`, `-`, `_`, `.`) are kept as-is
//! - Segments containing unsafe characters or binary data are base58-encoded with a `!` prefix
//! - Path separators (`/`) preserve the S3 key hierarchy

use base58::{FromBase58, ToBase58};

use super::S3StorageError;

/// S3-safe key encoding that preserves path structure.
///
/// Keys are treated as `/`-delimited paths. Each path component is checked:
/// - If it contains only safe characters (alphanumeric, `-`, `_`, `.`), it's kept as-is
/// - Otherwise, it's base58-encoded and prefixed with `!`
///
/// The `!` character is used as a prefix marker because it's in AWS S3's
/// "safe for use" list and unlikely to appear at the start of path components.
///
/// See [Object key naming guidelines] for more information about S3 key requirements.
///
/// # Examples
///
/// - `remote/main` → `remote/main` (all components safe)
/// - `remote/user@example` → `remote/!<base58>` (@ is unsafe, encode component)
/// - `foo/bar/baz` → `foo/bar/baz` (all safe)
///
/// [Object key naming guidelines]: https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html
pub fn encode(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes)
        .split('/')
        .map(|component| {
            // Check if component contains only safe characters
            let is_safe = component.bytes().all(|b| {
                matches!(b,
                    b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'-' | b'_' | b'.'
                )
            });

            if is_safe && !component.is_empty() {
                component.to_string()
            } else {
                // Base58 encode and prefix with !
                format!("!{}", component.as_bytes().to_base58())
            }
        })
        .collect::<Vec<String>>()
        .join("/")
}

/// Decode an S3-encoded key back to bytes.
///
/// Path components starting with `!` are base58-decoded.
/// Other components are used as-is.
pub fn decode(encoded: &str) -> Result<Vec<u8>, S3StorageError> {
    // Decode each path component: `!`-prefixed ones are base58, others are plain text
    let components = encoded
        .split('/')
        .map(|component| {
            if let Some(encoded_part) = component.strip_prefix('!') {
                encoded_part.from_base58().map_err(|e| {
                    S3StorageError::SerializationError(format!(
                        "Invalid base58 encoding in component '{}': {:?}",
                        component, e
                    ))
                })
            } else {
                Ok(component.as_bytes().to_vec())
            }
        })
        .collect::<Result<Vec<Vec<u8>>, S3StorageError>>()?;

    // Join decoded components with `/` separator
    Ok(components.join(&b'/'))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_encodes_safe_chars() {
        assert_eq!(encode(b"simple-key"), "simple-key");
        assert_eq!(encode(b"with_underscore"), "with_underscore");
        assert_eq!(encode(b"with.dot"), "with.dot");
        assert_eq!(encode(b"CamelCase123"), "CamelCase123");
    }

    #[test]
    fn it_encodes_path_structure() {
        assert_eq!(encode(b"path/to/key"), "path/to/key");
        assert_eq!(encode(b"a/b/c"), "a/b/c");
    }

    #[test]
    fn it_encodes_unsafe_chars() {
        let encoded = encode(b"user@example");
        assert!(encoded.starts_with('!'));

        let encoded = encode(b"has space");
        assert!(encoded.starts_with('!'));
    }

    #[test]
    fn it_encodes_binary() {
        let encoded = encode(&[0x01, 0x02, 0x03]);
        assert!(encoded.starts_with('!'));
    }

    #[test]
    fn it_decodes_safe_chars() {
        assert_eq!(decode("simple-key").unwrap(), b"simple-key");
        assert_eq!(decode("path/to/key").unwrap(), b"path/to/key");
    }

    #[test]
    fn it_roundtrips() {
        let original = b"test-key";
        let encoded = encode(original);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded, original);

        let binary = vec![1, 2, 3, 4, 5];
        let encoded = encode(&binary);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded, binary);

        let path = b"safe/!encoded/also-safe";
        let encoded = encode(path);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded, path);
    }

    #[test]
    fn it_errors_on_invalid_base58() {
        let result = decode("!invalid@@base58");
        assert!(result.is_err());
    }
}
