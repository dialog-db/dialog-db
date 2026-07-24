//! Pattern matching for artifacts against selectors.
//!
//! This module provides functionality for matching artifacts and index entries
//! against artifact selectors during query operations.
//!
//! A spilled value's key carries the order-preserving encoding of its leading
//! raw bytes plus the whole-value hash, so most predicates decide from the
//! key alone: equality compares the prefix and hash (the reader can encode
//! and hash its candidate value), and prefix/range predicates decide whenever
//! the answer lies within the in-key prefix. Only an order predicate whose
//! answer lies beyond the prefix returns [`SelectorMatch::NeedsValue`], and
//! the scan loads the block and re-checks semantically via
//! [`value_predicates_admit`].

use std::cmp::Ordering;

use crate::{
    ArtifactSelector, Value, ValueDataType,
    artifacts::selector::Constrained,
    decode_bytes_cow, decode_value,
    key::value_payload,
    key::varkey::{KeyRef, ValuePayload, ValueRef},
};
use dialog_search_tree::Manifest;

/// The verdict of matching one scanned key against a selector.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SelectorMatch {
    /// Every constraint is satisfied, decided from the key alone.
    Matches,
    /// Some constraint fails; the entry is not part of the result.
    Excluded,
    /// A value predicate's answer lies beyond a spilled value's in-key
    /// prefix: the caller must load the value and re-check the value
    /// predicates semantically ([`value_predicates_admit`]).
    NeedsValue,
}

/// How a spilled value's in-key prefix relates to a probe byte string, given
/// that the true value strictly extends the prefix (a value spills only when
/// it is longer than the prefix the key keeps).
enum TruncatedOrder {
    /// The true value is decidably below the probe.
    Below,
    /// The true value is decidably above the probe.
    Above,
    /// The probe extends the stored prefix: the answer lies beyond the
    /// prefix and only the loaded value can decide.
    Undecided,
}

/// Compares a value known only by its leading `stored` bytes (the true value
/// strictly extends them) against a complete `probe`.
fn truncated_order(stored: &[u8], probe: &[u8]) -> TruncatedOrder {
    let shared = stored.len().min(probe.len());
    match stored[..shared].cmp(&probe[..shared]) {
        Ordering::Less => TruncatedOrder::Below,
        Ordering::Greater => TruncatedOrder::Above,
        Ordering::Equal if stored.len() >= probe.len() => {
            // The probe is a prefix of (or equals) the stored bytes: the true
            // value extends past the probe, so it sorts above it.
            TruncatedOrder::Above
        }
        // The stored bytes are a proper prefix of the probe.
        Ordering::Equal => TruncatedOrder::Undecided,
    }
}

/// Checks whether an already-parsed [`KeyRef`] matches a selector's
/// constraints, used on the scan hot path so an entry's key is parsed once
/// (in the scan) and matched without re-splitting.
///
/// Entity and attribute are stored losslessly, so every comparison (exact and
/// prefix) is exact against the key bytes.
///
/// `manifest` is the format of the tree the scanned keys were WRITTEN under.
/// A value constraint is decided by re-encoding the selector's value through
/// the same inline-vs-spill decision (`inline_n`) and the same spilled-prefix
/// width (`spill_prefix`) the key was built with, so reading under a different
/// manifest would make an equality match on a boundary-sized value silently
/// fail.
pub fn match_selector_and_key_ref(
    selector: &ArtifactSelector<Constrained>,
    key: &KeyRef<'_>,
    manifest: &Manifest,
) -> SelectorMatch {
    let mut verdict = SelectorMatch::Matches;

    if let Some(entity) = selector.entity()
        && entity.as_str().as_bytes() != key.entity.as_ref()
    {
        return SelectorMatch::Excluded;
    }

    if let Some(attribute) = selector.attribute()
        && attribute.as_str().as_bytes() != key.attribute.as_ref()
    {
        return SelectorMatch::Excluded;
    }

    if let Some(value) = selector.value() {
        if value.data_type() != key.value_type {
            return SelectorMatch::Excluded;
        }
        // Compare by the same encoding the key was built with. Equality never
        // needs the block: the reader holds the candidate value, so it can
        // recompute the spilled prefix and whole-value hash and compare pure
        // bytes. The inline/spilled shape must agree too: the same logical
        // value always keys the same way, so a shape mismatch is a non-match.
        let expected = value_payload(value, manifest);
        let equal = match (&expected, &key.value) {
            (ValuePayload::Inline(a), ValueRef::Inline(b)) => a.as_slice() == *b,
            (
                ValuePayload::Spilled { prefix, hash },
                ValueRef::Spilled {
                    prefix: key_prefix,
                    hash: key_hash,
                },
            ) => prefix.as_slice() == *key_prefix && hash.as_slice() == *key_hash,
            _ => false,
        };
        if !equal {
            return SelectorMatch::Excluded;
        }
    }

    if let Some(prefix) = selector.attribute_prefix() {
        let bytes = prefix.as_bytes();
        let segment = key.attribute.as_ref();
        if bytes.len() > segment.len() || &segment[..bytes.len()] != bytes {
            return SelectorMatch::Excluded;
        }
    }

    if let Some(prefix) = selector.entity_prefix() {
        let bytes = prefix.as_bytes();
        let segment = key.entity.as_ref();
        if bytes.len() > segment.len() || &segment[..bytes.len()] != bytes {
            return SelectorMatch::Excluded;
        }
    }

    if let Some(prefix) = selector.value_prefix() {
        // A prefix predicate is a STRING predicate. An inline string decides
        // directly against its order-preserving payload (raw UTF-8 for a
        // NUL-free prefix); a spilled string decides from its in-key prefix
        // unless the probe extends past it.
        if key.value_type != ValueDataType::String {
            return SelectorMatch::Excluded;
        }
        let bytes = prefix.as_bytes();
        match &key.value {
            ValueRef::Inline(payload) => {
                if bytes.len() > payload.len() || &payload[..bytes.len()] != bytes {
                    return SelectorMatch::Excluded;
                }
            }
            ValueRef::Spilled { prefix: stored, .. } => {
                let Some((stored, rest)) = decode_bytes_cow(stored) else {
                    return SelectorMatch::Excluded;
                };
                if !rest.is_empty() {
                    return SelectorMatch::Excluded;
                }
                let stored = stored.as_ref();
                if stored.len() >= bytes.len() {
                    if !stored.starts_with(bytes) {
                        return SelectorMatch::Excluded;
                    }
                } else if bytes.starts_with(stored) {
                    verdict = SelectorMatch::NeedsValue;
                } else {
                    return SelectorMatch::Excluded;
                }
            }
        }
    }

    // Value range bounds compare against the decoded value semantically, so
    // exclusivity (`>`/`<`) and the exact bound value are handled precisely
    // (the key range is a superset that includes the boundary; this drops it
    // when the bound is exclusive). A spilled value decides from its in-key
    // prefix unless the bound extends past it.
    if selector.value_lower().is_some() || selector.value_upper().is_some() {
        match &key.value {
            ValueRef::Inline(payload) => {
                // An undecodable inline payload is corrupt: fail closed
                // rather than admit it.
                let Some((value, rest)) = decode_value(key.value_type, payload) else {
                    return SelectorMatch::Excluded;
                };
                if !rest.is_empty() {
                    return SelectorMatch::Excluded;
                }
                // Compare only within the bound's type: `Value`'s derived
                // `PartialOrd` orders across variants by declaration order,
                // not semantically, so a cross-type value must be excluded
                // rather than variant-ordered. The key range already brackets
                // the bound's band, so a differing type here is a spurious
                // neighbor at the band edge.
                if let Some(bound) = selector.value_lower() {
                    if value.data_type() != bound.value.data_type() {
                        return SelectorMatch::Excluded;
                    }
                    match value.partial_cmp(&bound.value) {
                        Some(Ordering::Greater) => {}
                        Some(Ordering::Equal) if bound.inclusive => {}
                        _ => return SelectorMatch::Excluded,
                    }
                }
                if let Some(bound) = selector.value_upper() {
                    if value.data_type() != bound.value.data_type() {
                        return SelectorMatch::Excluded;
                    }
                    match value.partial_cmp(&bound.value) {
                        Some(Ordering::Less) => {}
                        Some(Ordering::Equal) if bound.inclusive => {}
                        _ => return SelectorMatch::Excluded,
                    }
                }
            }
            ValueRef::Spilled { prefix: stored, .. } => {
                let Some((stored, rest)) = decode_bytes_cow(stored) else {
                    return SelectorMatch::Excluded;
                };
                if !rest.is_empty() {
                    return SelectorMatch::Excluded;
                }
                let stored = stored.as_ref();
                // Byte order equals semantic order for the variable-length
                // types (the only ones that spill); a bound of a different
                // type never matches, exactly as for inline values.
                if let Some(bound) = selector.value_lower() {
                    if key.value_type != bound.value.data_type() {
                        return SelectorMatch::Excluded;
                    }
                    match truncated_order(stored, &bound.value.to_bytes()) {
                        TruncatedOrder::Above => {}
                        TruncatedOrder::Below => return SelectorMatch::Excluded,
                        TruncatedOrder::Undecided => verdict = SelectorMatch::NeedsValue,
                    }
                }
                if let Some(bound) = selector.value_upper() {
                    if key.value_type != bound.value.data_type() {
                        return SelectorMatch::Excluded;
                    }
                    match truncated_order(stored, &bound.value.to_bytes()) {
                        TruncatedOrder::Below => {}
                        TruncatedOrder::Above => return SelectorMatch::Excluded,
                        TruncatedOrder::Undecided => verdict = SelectorMatch::NeedsValue,
                    }
                }
            }
        }
    }

    verdict
}

/// Semantically re-checks the VALUE predicates (prefix and range bounds) of a
/// selector against a fully-loaded value: the scan's post-filter for entries
/// whose key-side verdict was [`SelectorMatch::NeedsValue`].
pub(crate) fn value_predicates_admit(
    selector: &ArtifactSelector<Constrained>,
    value: &Value,
) -> bool {
    if let Some(prefix) = selector.value_prefix() {
        match value {
            Value::String(content) => {
                if !content.as_bytes().starts_with(prefix.as_bytes()) {
                    return false;
                }
            }
            _ => return false,
        }
    }
    if let Some(bound) = selector.value_lower() {
        if value.data_type() != bound.value.data_type() {
            return false;
        }
        match value.partial_cmp(&bound.value) {
            Some(Ordering::Greater) => {}
            Some(Ordering::Equal) if bound.inclusive => {}
            _ => return false,
        }
    }
    if let Some(bound) = selector.value_upper() {
        if value.data_type() != bound.value.data_type() {
            return false;
        }
        match value.partial_cmp(&bound.value) {
            Some(Ordering::Less) => {}
            Some(Ordering::Equal) if bound.inclusive => {}
            _ => return false,
        }
    }
    true
}
