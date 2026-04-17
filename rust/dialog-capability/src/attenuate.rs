use serde::Serialize;
use serde::de::DeserializeOwned;

/// Trait for effect types that can produce an attenuation shape suitable
/// for embedding in delegations and invocations.
///
/// Delegated capabilities and invocations carry their effect parameters as
/// constraints. Those constraints travel over the wire and live inside
/// signed capability chains, so they need a wire-safe shape — e.g. a
/// payload byte buffer is projected to its checksum. The `Attenuate`
/// trait defines this projection: `attenuate()` returns the attenuation
/// shape of the effect.
///
/// For effects without payload fields, `type Attenuation = Self` — the
/// attenuation shape is identical to the execution shape.
///
/// For effects with payload fields (e.g., `archive::Put` with `content`),
/// `type Attenuation` is a generated struct where payload fields are
/// replaced with their checksums.
///
/// # Deriving
///
/// Use `#[derive(Attenuate)]` to generate the implementation automatically.
/// Fields that need projection are annotated with
/// `#[attenuate(into = TargetType)]` (uses `From` conversion) or
/// `#[attenuate(into = Type, with = path)]` (calls a custom function).
/// Use `rename` to change the field name in the attenuation struct:
///
/// ```no_run
/// # use serde::{Serialize, Deserialize};
/// # use dialog_macros::Attenuate;
/// # use dialog_common::Checksum;
/// #[derive(Debug, Clone, Serialize, Deserialize, Attenuate)]
/// pub struct Put {
///     pub digest: Vec<u8>,
///     #[attenuate(into = Checksum, with = Checksum::sha256, rename = checksum)]
///     pub content: Vec<u8>,
/// }
/// // Generates:
/// // - `PutAttenuation { digest: Vec<u8>, checksum: Checksum }`
/// // - `impl Attenuate for Put { type Attenuation = PutAttenuation; ... }`
/// ```
pub trait Attenuate {
    /// The attenuation-safe representation of this effect.
    ///
    /// For effects without payload fields, this is `Self`.
    /// For effects with payload fields, this is a generated struct
    /// where payloads are replaced with checksums.
    type Attenuation: Serialize + DeserializeOwned;

    /// Project this effect into its attenuation form.
    fn attenuate(self) -> Self::Attenuation;
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use dialog_macros::Attenuate;
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

    // Unit struct — Attenuation = Self
    #[derive(Debug, Clone, Copy, Serialize, Deserialize, Attenuate)]
    struct Ping;

    #[test]
    fn it_derives_attenuation_for_unit_struct() {
        let ping = Ping;
        let attenuation = ping.attenuate();
        // Unit struct attenuation is identity.
        assert_eq!(std::mem::size_of_val(&attenuation), 0);
    }

    // Named struct without projections — Attenuation = Self
    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Attenuate)]
    struct Get {
        key: Vec<u8>,
    }

    #[test]
    fn it_derives_identity_attenuation_for_plain_struct() {
        let get = Get {
            key: b"hello".to_vec(),
        };
        let attenuation = get.attenuate();
        assert_eq!(attenuation.key, b"hello");
    }

    // Named struct with projection — generates PutAttenuation
    #[derive(Debug, Clone, Serialize, Deserialize, Attenuate)]
    struct Put {
        digest: Vec<u8>,
        #[attenuate(into = Checksum)]
        content: Vec<u8>,
    }

    #[test]
    fn it_projects_content_to_checksum() {
        let put = Put {
            digest: vec![1, 2, 3],
            content: vec![4, 5, 6],
        };
        let attenuation = put.attenuate();
        assert_eq!(attenuation.digest, vec![1, 2, 3]);
        assert_eq!(attenuation.content, Checksum(3));
    }

    // Named struct with projection + rename
    #[derive(Debug, Clone, Serialize, Deserialize, Attenuate)]
    struct Publish {
        when: Option<u64>,
        #[attenuate(into = Checksum, rename = checksum)]
        content: Vec<u8>,
    }

    #[test]
    fn it_renames_projected_field() {
        let publish = Publish {
            when: Some(42),
            content: vec![7, 8, 9],
        };
        let attenuation = publish.attenuate();
        assert_eq!(attenuation.when, Some(42));
        assert_eq!(attenuation.checksum, Checksum(3));
    }

    #[test]
    fn it_generates_attenuation_struct_with_renamed_field() {
        let publish = Publish {
            when: None,
            content: vec![1, 2],
        };
        let attenuation: PublishAttenuation = publish.attenuate();
        // The generated struct has `checksum` (not `content`).
        assert_eq!(attenuation.checksum, Checksum(2));
        assert_eq!(attenuation.when, None);
    }

    // Custom conversion function for testing `with`.
    fn hash_len(data: Vec<u8>) -> Checksum {
        Checksum(data.len() * 10)
    }

    // Named struct with `with` — uses custom function instead of From
    #[derive(Debug, Clone, Serialize, Deserialize, Attenuate)]
    struct Upload {
        key: String,
        #[attenuate(into = Checksum, with = hash_len, rename = checksum)]
        payload: Vec<u8>,
    }

    #[test]
    fn it_uses_custom_with_function() {
        let upload = Upload {
            key: "test".to_string(),
            payload: vec![1, 2, 3],
        };
        let attenuation = upload.attenuate();
        assert_eq!(attenuation.key, "test");
        // hash_len multiplies length by 10
        assert_eq!(attenuation.checksum, Checksum(30));
    }

    // Generic struct — Attenuation = Self
    #[derive(Debug, Clone, Serialize, Deserialize, Attenuate)]
    struct GenericEffect<T>(std::marker::PhantomData<T>);

    #[test]
    fn it_derives_attenuation_for_generic_struct() {
        let effect = GenericEffect::<String>(std::marker::PhantomData);
        let _attenuation = effect.attenuate();
    }
}
