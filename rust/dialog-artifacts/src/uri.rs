use std::{fmt::Display, io::Write, ops::Deref, str::FromStr};

use serde::{Deserialize, Serialize};
use url::Url;

use base58::ToBase58;
use ed25519_dalek::SigningKey;

use crate::{DialogArtifactsError, ENTITY_LENGTH, make_reference, mutable_slice};

/// A [`Uri`] is a helper type that helps validate and reliably convert between
/// plain string URIs (which typically represent an [`Entity`]) and their other
/// representations such as their byte representation when used as a component
/// of an index key.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct Uri(Url);

impl Uri {
    /// Generate a globally unique URI. The raw format will be an ed25519 DID
    /// Key.
    pub fn unique() -> Result<Self, DialogArtifactsError> {
        const PREFIX: &str = "z6Mk";

        let key = [
            PREFIX,
            SigningKey::generate(&mut rand::thread_rng())
                .verifying_key()
                .as_bytes()
                .as_ref()
                .to_base58()
                .as_str(),
        ]
        .concat();

        format!("did:key:{key}")
            .parse()
            .map(Self)
            .map_err(|error| DialogArtifactsError::InvalidEntity(format!("{error}")))
    }

    /// Convert this [`Uri`] to the byte representation expected for use as part
    /// of an index key.
    ///
    /// The layout is 64 bytes wide. The first 32 bytes contain the first 32 bytes
    /// of the UTF-8-encoded URI string; the last 32 bytes are the hash of any
    /// remaining bytes in the URI string (or else all zeroes).
    pub fn key_bytes(&self) -> Result<[u8; ENTITY_LENGTH], DialogArtifactsError> {
        let format = |bytes: &[u8]| {
            let mut key_bytes = [0u8; 64];

            if let Some((l, r)) = bytes.split_at_checked(32) {
                let rest = make_reference(r);

                mutable_slice!(key_bytes, 0, 32).write_all(l)?;
                mutable_slice!(key_bytes, 32, 32).write_all(rest.as_ref())?;
            } else {
                mutable_slice!(key_bytes, 0, 32).write_all(bytes)?;
            }

            Ok(key_bytes) as Result<[u8; 64], std::io::Error>
        };

        format(self.0.as_str().as_bytes()).map_err(|error| {
            DialogArtifactsError::InvalidEntity(format!("Could not format as key bytes: {error}"))
        })
    }
}

impl Display for Uri {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", **self)
    }
}

impl From<Uri> for String {
    fn from(value: Uri) -> Self {
        (*value).to_string()
    }
}

impl Deref for Uri {
    type Target = Url;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl FromStr for Uri {
    type Err = DialogArtifactsError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(s.parse().map_err(|error| {
            DialogArtifactsError::InvalidUri(format!("{error}"))
        })?))
    }
}

impl TryFrom<String> for Uri {
    type Error = DialogArtifactsError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

#[cfg(test)]
mod tests {
    use crate::Entity;
    use anyhow::Result;

    #[test]
    fn it_can_convert_to_key_bytes() -> Result<()> {
        let entity: Entity = "https://google.com".parse()?;

        println!("\n{entity}");
        println!("{:?}", entity.key_bytes());
        assert_eq!(
            entity.key_bytes(),
            &[
                104, 116, 116, 112, 115, 58, 47, 47, 103, 111, 111, 103, 108, 101, 46, 99, 111,
                109, 47, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
            ]
        );

        let entity: Entity = "https://www.reddit.com/r/Wellington/".parse()?;

        println!("\n{entity}");
        println!("{:?}", entity.key_bytes());
        assert_eq!(
            entity.key_bytes(),
            &[
                104, 116, 116, 112, 115, 58, 47, 47, 119, 119, 119, 46, 114, 101, 100, 100, 105,
                116, 46, 99, 111, 109, 47, 114, 47, 87, 101, 108, 108, 105, 110, 103, 174, 109, 72,
                16, 228, 74, 156, 26, 71, 116, 75, 44, 178, 112, 196, 124, 85, 229, 151, 72, 94,
                42, 78, 114, 123, 226, 181, 252, 47, 68, 96, 188
            ]
        );

        let entity: Entity = "did:web:cdata.earth".parse()?;

        println!("\n{entity}");
        println!("{:?}", entity.key_bytes());
        assert_eq!(
            entity.key_bytes(),
            &[
                100, 105, 100, 58, 119, 101, 98, 58, 99, 100, 97, 116, 97, 46, 101, 97, 114, 116,
                104, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
            ]
        );

        let entity: Entity = "did:web:anubiarts.neocities.org".parse()?;

        println!("\n{entity}");
        println!("{:?}", entity.key_bytes());
        assert_eq!(
            entity.key_bytes(),
            &[
                100, 105, 100, 58, 119, 101, 98, 58, 97, 110, 117, 98, 105, 97, 114, 116, 115, 46,
                110, 101, 111, 99, 105, 116, 105, 101, 115, 46, 111, 114, 103, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
            ]
        );

        let entity: Entity = "did:key:z6Mk2WiNvjBbuWZ8jYNmFzh4uFyt8iqwpDND6ymg6KnKzchw".parse()?;

        println!("\n{entity}");
        println!("{:?}", entity.key_bytes());
        assert_eq!(
            entity.key_bytes(),
            &[
                100, 105, 100, 58, 107, 101, 121, 58, 122, 54, 77, 107, 50, 87, 105, 78, 118, 106,
                66, 98, 117, 87, 90, 56, 106, 89, 78, 109, 70, 122, 104, 52, 145, 178, 200, 111,
                186, 28, 163, 145, 181, 81, 20, 47, 75, 48, 26, 200, 30, 45, 131, 111, 84, 186,
                185, 89, 166, 62, 252, 15, 108, 30, 140, 116
            ]
        );

        Ok(())
    }
}
