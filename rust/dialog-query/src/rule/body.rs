//! The stored form of a rule body: the dag-cbor of its descriptor in an
//! envelope that names the format it is written in.
//!
//! ```text
//! { "format": 3, "rule": { "deduce": ..., "when": [...] } }
//! ```
//!
//! Format 3 writes the rule's canonical spelling (see
//! [`canonical`](crate::rule::canonical)): the locals and the head's
//! fields renamed by the labeling, a premise's fields named by what
//! they read, the premises sorted, and no descriptions. The body then
//! holds exactly what the rule's identity holds, so every spelling of
//! one rule stores the same bytes, and a rule's body is found by
//! encoding the rule. What the author wrote is not stored.
//!
//! Format 2 wrote the authored descriptor, with every constant tagged
//! by the entity naming its type (`{"text:": "foo:"}`), so a constant
//! decodes as the type it was written as. Format 1 (0.2.0) and format
//! 0 (the bare descriptor the release before formats wrote) wrote
//! constants bare, and a bare constant reads as the first type its
//! payload fits. All three are still read, and `Branch::upgrade_rules`
//! rewrites them in format 3. A body in a later format than this
//! release reads is refused by name rather than read as something it
//! is not.
//!
//! dag-cbor holds integers of at most 64 bits and finite floats, so a
//! rule whose constants exceed that is refused when it compiles (see
//! [`TypeError::UnencodableConstant`](crate::error::TypeError::UnencodableConstant)),
//! never when it is stored.

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

/// The rule body format this release writes.
pub const FORMAT: u64 = 3;

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

/// `rule`, a canonical descriptor, as a stored body in [`FORMAT`].
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

    fn body() -> Body {
        Body {
            when: vec!["a".into()],
        }
    }

    /// A body is written in the current format and read back, and the
    /// bodies earlier releases wrote are still read: the format 2
    /// envelope and the bare descriptor of format 0.
    #[dialog_common::test]
    fn it_reads_every_format_up_to_its_own() {
        let current = encode(&body()).expect("encodes");
        let stamp: Stamp = serde_ipld_dagcbor::from_slice(&current).expect("stamped");
        assert_eq!(stamp.format, Some(FORMAT));
        assert_eq!(decode::<Body>(&current), Ok(body()));

        let earlier = serde_ipld_dagcbor::to_vec(&Envelope {
            format: 2,
            rule: &body(),
        })
        .expect("encodes");
        assert_eq!(decode::<Body>(&earlier), Ok(body()));

        let bare = serde_ipld_dagcbor::to_vec(&body()).expect("encodes");
        assert_eq!(decode::<Body>(&bare), Ok(body()));
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
