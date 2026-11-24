//! The stored form of a rule body: the dag-cbor of its descriptor in an
//! envelope that names the format it is written in.
//!
//! ```text
//! { "format": 5, "rule": { "deduce": ..., "when": [...] } }
//! ```
//!
//! Format 5 is format 4 with a pattern where a parameter stands: a map
//! or a list of terms, written under `match` (see
//! [`Pattern`](crate::Pattern)). A body is written in the lowest format
//! that can say it, so a rule that holds a pattern is written in format
//! 5 and every other rule in format 4 ([`TERMS_FORMAT`]), byte for byte
//! as the release before wrote it. A stored rule then keeps its body,
//! is found and retracted by encoding the rule, and is read by a
//! release that reads up to format 4. Only a rule that holds a pattern
//! is refused by such a release, by the number of its format, and is
//! inert there. A body in a format below 5 that holds a pattern is not
//! one any release wrote, and is refused when it is decoded.
//!
//! Format 4 writes the rule's canonical spelling with each attribute
//! under its cardinality (`one`, unwritten, or `many`). Format 3 wrote the
//! same spelling with each attribute under a `pick`, which this release
//! refuses by name (see
//! [`AttributeDescriptor`](crate::attribute::AttributeDescriptor)), so a
//! format 3 body spelling one decodes to an error and the rule is inert
//! until `Branch::upgrade_rules` rewrites it. A format 3 body spelling no
//! pick decodes as it was written.
//!
//! Format 3 wrote the rule's canonical spelling, as formats 4 and 5 do
//! (see [`canonical`](crate::rule::canonical)): the locals and the
//! head's fields renamed by the labeling, a premise's fields named by
//! what they read, the premises sorted, and no descriptions. The body
//! then holds exactly what the rule's identity holds, so every spelling
//! of one rule stores the same bytes, and a rule's body is found by
//! encoding the rule. What the author wrote is not stored.
//!
//! Format 2 wrote the authored descriptor, with every constant tagged
//! by the entity naming its type (`{"text:": "foo:"}`), so a constant
//! decodes as the type it was written as. Format 1 (0.2.0) and format
//! 0 (the bare descriptor the release before formats wrote) wrote
//! constants bare, and a bare constant reads as the first type its
//! payload fits. All three are still read, as format 3 is, and
//! `Branch::upgrade_rules` rewrites them in format 4. A body in a later
//! format than this release reads is refused by name rather than read
//! as something it is not.
//!
//! dag-cbor holds integers of at most 64 bits and finite floats, so a
//! rule whose constants exceed that is refused when it compiles (see
//! [`TypeError::UnencodableConstant`](crate::error::TypeError::UnencodableConstant)),
//! never when it is stored.

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

/// The latest rule body format this release reads and writes: the one
/// a rule that holds a pattern is written in.
pub const FORMAT: u64 = 5;

/// The format a rule that holds no pattern is written in: format 4, as
/// the release before wrote it.
pub const TERMS_FORMAT: u64 = 4;

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

/// The format a stored body names, `0` for the bare descriptor the
/// release before formats wrote, or `None` when the bytes are no
/// dag-cbor map.
pub fn format_of(bytes: &[u8]) -> Option<u64> {
    serde_ipld_dagcbor::from_slice::<Stamp>(bytes)
        .ok()
        .map(|stamp| stamp.format.unwrap_or(0))
}

/// The format a body is written in: [`FORMAT`] when the rule holds a
/// pattern, and [`TERMS_FORMAT`] when it holds none.
pub fn format_for(holds_pattern: bool) -> u64 {
    if holds_pattern { FORMAT } else { TERMS_FORMAT }
}

/// `rule`, a canonical descriptor, as a stored body in the format
/// [`format_for`] gives.
pub(crate) fn encode<T: Serialize>(holds_pattern: bool, rule: &T) -> Option<Vec<u8>> {
    serde_ipld_dagcbor::to_vec(&Envelope {
        format: format_for(holds_pattern),
        rule,
    })
    .ok()
}

/// `rule` in `format`, as a release that wrote it stored it.
#[cfg(test)]
pub(crate) fn encode_as<T: Serialize>(format: u64, rule: &T) -> Option<Vec<u8>> {
    serde_ipld_dagcbor::to_vec(&Envelope { format, rule }).ok()
}

/// Refuses a body that holds a pattern in a format that has none: only
/// format 5 writes one.
pub(crate) fn check_pattern(bytes: &[u8], held: Option<String>) -> Result<(), String> {
    match (held, format_of(bytes)) {
        (Some(pattern), Some(format)) if format < FORMAT => Err(format!(
            "the rule body is in format {format}, which writes no pattern, and holds the pattern {pattern}"
        )),
        _ => Ok(()),
    }
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

    /// A body is written in format 5 when its rule holds a pattern and
    /// in format 4 when it holds none, both are read back, and the
    /// bodies earlier releases wrote are still read: the envelopes of
    /// formats 3 and 2 and the bare descriptor of format 0.
    #[dialog_common::test]
    fn it_reads_every_format_up_to_its_own() {
        for (holds_pattern, format) in [(true, FORMAT), (false, TERMS_FORMAT)] {
            let current = encode(holds_pattern, &body()).expect("encodes");
            assert_eq!(format_of(&current), Some(format));
            assert_eq!(decode::<Body>(&current), Ok(body()));
        }
        assert_eq!((FORMAT, TERMS_FORMAT), (5, 4));

        for earlier in [3, 2] {
            let earlier = encode_as(earlier, &body()).expect("encodes");
            assert_eq!(decode::<Body>(&earlier), Ok(body()));
        }

        let bare = serde_ipld_dagcbor::to_vec(&body()).expect("encodes");
        assert_eq!(decode::<Body>(&bare), Ok(body()));
    }

    /// A key of the envelope this release does not know is skipped,
    /// and the body is read as its number says. So a release cannot be
    /// told what a body holds by a key beside the number: only a later
    /// number makes it refuse. A design that names what a body uses
    /// has to move the number too.
    #[dialog_common::test]
    fn it_reads_past_an_envelope_key_it_does_not_know() {
        #[derive(Serialize)]
        struct Extended<'a> {
            format: u64,
            uses: Vec<&'a str>,
            rule: &'a Body,
        }
        let extended = serde_ipld_dagcbor::to_vec(&Extended {
            format: FORMAT,
            uses: vec!["pattern"],
            rule: &body(),
        })
        .expect("encodes");
        assert_eq!(decode::<Body>(&extended), Ok(body()));
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
