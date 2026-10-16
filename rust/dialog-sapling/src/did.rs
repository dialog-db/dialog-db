//! The DID of a key: an Ed25519 `did:key`, as each key of Sapling is. See
//! Sapling's PDD002, "Keys and Authority".

use dialog_credentials::Ed25519Verifier;
pub use dialog_varsig::Did;

/// The DID that `text` names, if it is the `did:key` of a key whose
/// signatures Dialog DB verifies here: an Ed25519 key, as every key of
/// Sapling is.
pub fn parse_key(text: &str) -> Option<Did> {
    text.parse::<Ed25519Verifier>().ok()?;
    text.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::parse_key;

    #[test]
    fn it_takes_the_did_key_of_an_ed25519_key_alone() {
        let did = "did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK";
        assert_eq!(
            parse_key(did).map(|did| did.to_string()),
            Some(did.to_owned())
        );
        for text in ["", "did:key:", "did:web:example.com", "did:key:z6Mk"] {
            assert!(parse_key(text).is_none(), "{text:?}");
        }
    }
}
