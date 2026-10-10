//! The stored form of a rule body: the dag-cbor of its descriptor in an
//! envelope that names the format it is written in.
//!
//! ```text
//! { "format": 2, "rule": { "deduce": ..., "when": [...] } }
//! ```
//!
//! Format 2 writes every constant tagged by the entity naming its type
//! (`{"text:": "foo:"}`), so a constant decodes as the type it was
//! written as. Format 1 (0.2.0) and format 0 (the bare descriptor the
//! release before formats wrote) wrote constants bare, and are still
//! read: a bare constant reads as the first type its payload fits. A
//! body in a later format than this release reads is refused by name
//! rather than read as something it is not.

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

/// The rule body format this release writes.
pub const FORMAT: u64 = 2;

#[derive(Serialize)]
struct Envelope<'a, T> {
    format: u64,
    rule: &'a T,
}

#[derive(Deserialize)]
struct Stamped<T> {
    rule: T,
}

/// Only the stamp of a body: every other key is skipped.
#[derive(Deserialize)]
struct Stamp {
    #[serde(default)]
    format: Option<u64>,
}

/// `rule` as a stored body in [`FORMAT`].
pub(crate) fn encode<T: Serialize>(rule: &T) -> Option<Vec<u8>> {
    serde_ipld_dagcbor::to_vec(&Envelope {
        format: FORMAT,
        rule,
    })
    .ok()
}

/// The descriptor a stored body holds, in [`FORMAT`] or any earlier.
pub(crate) fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, String> {
    let format = serde_ipld_dagcbor::from_slice::<Stamp>(bytes)
        .ok()
        .and_then(|stamp| stamp.format);
    match format {
        None => serde_ipld_dagcbor::from_slice::<T>(bytes),
        Some(format) if format > FORMAT => {
            return Err(format!(
                "the rule body is in format {format}, and this release reads up to {FORMAT}"
            ));
        }
        Some(_) => serde_ipld_dagcbor::from_slice::<Stamped<T>>(bytes).map(|body| body.rule),
    }
    .map_err(|error| format!("dag-cbor decode failed: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Body {
        when: Vec<String>,
    }

    /// A body is written in the current format and read back; the bare
    /// body an earlier release wrote is read as format 0.
    #[dialog_common::test]
    fn it_reads_every_format_up_to_its_own() {
        let body = Body {
            when: vec!["a".into()],
        };
        let stamped = encode(&body).expect("encodes");
        assert_eq!(
            decode::<Body>(&stamped),
            Ok(Body {
                when: vec!["a".into()]
            })
        );

        let bare = serde_ipld_dagcbor::to_vec(&body).expect("encodes");
        assert_eq!(decode::<Body>(&bare), Ok(body));
    }

    /// A body in a later format is refused by name.
    #[dialog_common::test]
    fn it_refuses_a_later_format() {
        let later = serde_ipld_dagcbor::to_vec(&Envelope {
            format: FORMAT + 1,
            rule: &Body { when: Vec::new() },
        })
        .expect("encodes");
        let error = decode::<Body>(&later).expect_err("a later format is refused");
        assert!(error.contains(&format!("format {}", FORMAT + 1)), "{error}");
    }
}
