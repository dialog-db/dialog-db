//! The cause key of a create, which a client gives as the HTTP header
//! `Idempotency-Key` does, so that a create sent again makes nothing new. See
//! PDD007, "Each Cause Runs Once".

/// The longest cause key, in bytes.
pub const MAX: usize = 128;

/// Whether `text` is a cause key: from 1 to [`MAX`] bytes, each an ASCII
/// letter, a digit, `-`, or `_`.
pub fn is_key(text: &str) -> bool {
    let allowed = |byte: u8| byte.is_ascii_alphanumeric() || b"-_".contains(&byte);
    !text.is_empty() && text.len() <= MAX && text.bytes().all(allowed)
}

/// Why `text` is not a cause key, for the message of a refusal.
pub const RULE: &str =
    "a cause key has from 1 to 128 bytes, each an ASCII letter, a digit, `-`, or `_`";

#[cfg(test)]
mod tests {
    use super::is_key;

    #[test]
    fn it_tells_a_cause_key() {
        for key in ["k", "0f3a-b_C", &"a".repeat(128)] {
            assert!(is_key(key), "{key:?}");
        }
        for key in ["", "has space", "é", &"a".repeat(129), "a/b"] {
            assert!(!is_key(key), "{key:?}");
        }
    }
}
