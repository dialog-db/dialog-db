use serde::Serialize;
use serde::de::DeserializeOwned;

/// Trait for effect types that can produce an authorization-safe representation.
///
/// During authorization, payload fields (like content bytes) should be
/// represented as their checksum rather than raw data. The `Claim` trait
/// defines this projection: `claim()` returns the authorization shape
/// of the effect.
///
/// For effects without payload fields, `type Claim = Self` — the
/// authorization shape is identical to the execution shape.
///
/// For effects with payload fields (e.g., `archive::Put` with `content`),
/// `type Claim` is a generated struct where payload fields are replaced
/// with their checksums.
///
/// # Deriving
///
/// Use `#[derive(Claim)]` to generate the implementation automatically.
/// Fields that need projection are annotated with `#[claim(into = TargetType)]`
/// (uses `From` conversion) or `#[claim(into = Type, with = path)]` (calls a
/// custom function). Use `rename` to change the field name in the claim struct:
///
/// ```no_run
/// # use serde::{Serialize, Deserialize};
/// # use dialog_macros::Claim;
/// # use dialog_common::Checksum;
/// #[derive(Debug, Clone, Serialize, Deserialize, Claim)]
/// pub struct Put {
///     pub digest: Vec<u8>,
///     #[claim(into = Checksum, with = Checksum::sha256, rename = checksum)]
///     pub content: Vec<u8>,
/// }
/// // Generates:
/// // - `PutClaim { digest: Vec<u8>, checksum: Checksum }`
/// // - `impl Claim for Put { type Claim = PutClaim; ... }`
/// ```
pub trait Claim {
    /// The authorization-safe representation of this effect.
    ///
    /// For effects without payload fields, this is `Self`.
    /// For effects with payload fields, this is a generated struct
    /// where payloads are replaced with checksums.
    type Claim: Serialize + DeserializeOwned;

    /// Project this effect into its authorization claim form.
    fn claim(self) -> Self::Claim;
}

#[cfg(test)]
mod tests {
    use super::*;
    use dialog_macros::Claim;
    use serde::{Deserialize, Serialize};

    // A mock checksum type for testing projections.
    // Uses byte length as a stand-in for hashing — just enough to verify
    // that the derive macro calls `From` rather than copying raw bytes.
    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct Checksum(usize);

    impl From<Vec<u8>> for Checksum {
        fn from(bytes: Vec<u8>) -> Self {
            Checksum(bytes.len())
        }
    }

    // Unit struct — Claim = Self
    #[derive(Debug, Clone, Copy, Serialize, Deserialize, Claim)]
    struct Ping;

    #[test]
    fn it_derives_claim_for_unit_struct() {
        let ping = Ping;
        let claim = ping.claim();
        // Unit struct claim is identity.
        assert_eq!(std::mem::size_of_val(&claim), 0);
    }

    // Named struct without projections — Claim = Self
    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Claim)]
    struct Get {
        key: Vec<u8>,
    }

    #[test]
    fn it_derives_identity_claim_for_plain_struct() {
        let get = Get {
            key: b"hello".to_vec(),
        };
        let claim = get.claim();
        assert_eq!(claim.key, b"hello");
    }

    // Named struct with projection — generates PutClaim
    #[derive(Debug, Clone, Serialize, Deserialize, Claim)]
    struct Put {
        digest: Vec<u8>,
        #[claim(into = Checksum)]
        content: Vec<u8>,
    }

    #[test]
    fn it_projects_content_to_checksum() {
        let put = Put {
            digest: vec![1, 2, 3],
            content: vec![4, 5, 6],
        };
        let claim = put.claim();
        assert_eq!(claim.digest, vec![1, 2, 3]);
        assert_eq!(claim.content, Checksum(3));
    }

    // Named struct with projection + rename
    #[derive(Debug, Clone, Serialize, Deserialize, Claim)]
    struct Publish {
        when: Option<u64>,
        #[claim(into = Checksum, rename = checksum)]
        content: Vec<u8>,
    }

    #[test]
    fn it_renames_projected_field() {
        let publish = Publish {
            when: Some(42),
            content: vec![7, 8, 9],
        };
        let claim = publish.claim();
        assert_eq!(claim.when, Some(42));
        assert_eq!(claim.checksum, Checksum(3));
    }

    #[test]
    fn it_generates_claim_struct_with_renamed_field() {
        let publish = Publish {
            when: None,
            content: vec![1, 2],
        };
        let claim: PublishClaim = publish.claim();
        // The generated struct has `checksum` (not `content`).
        assert_eq!(claim.checksum, Checksum(2));
        assert_eq!(claim.when, None);
    }

    // Custom conversion function for testing `with`.
    fn hash_len(data: Vec<u8>) -> Checksum {
        Checksum(data.len() * 10)
    }

    // Named struct with `with` — uses custom function instead of From
    #[derive(Debug, Clone, Serialize, Deserialize, Claim)]
    struct Upload {
        key: String,
        #[claim(into = Checksum, with = hash_len, rename = checksum)]
        payload: Vec<u8>,
    }

    #[test]
    fn it_uses_custom_with_function() {
        let upload = Upload {
            key: "test".to_string(),
            payload: vec![1, 2, 3],
        };
        let claim = upload.claim();
        assert_eq!(claim.key, "test");
        // hash_len multiplies length by 10
        assert_eq!(claim.checksum, Checksum(30));
    }
}
