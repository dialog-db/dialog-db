//! Deterministically biased fractional positions for ordered relations.
//!
//! A port of the synopsys POC
//! (`commontoolsinc/synopsys/src/position`) — see
//! `notes/ordered-relations.md` for the evaluation. A position is an
//! ASCII string that sorts in plain byte order, designed to live as an
//! attribute predicate (`todo.item/<position>`) so an ordered
//! collection's members come out of one contiguous EAV range scan
//! already sorted. Structure:
//!
//! ```text
//! position = major (1 byte, A–Z: A–M negative, N–Z positive)
//!          ‖ minor (capacity(major) bytes, base62 — integer part)
//!          ‖ patch (variable, base62 — fractional tail)
//! ```
//!
//! The major encodes the minor's width, smallest in the middle
//! (`a`/`Z` = 1 digit) and growing toward the edges (`z`/`A` = 26), so
//! repeated appends or prepends step through integer headroom and
//! positions grow *logarithmically* at the ends; the fractional patch
//! only grows on repeated insertion between the same neighbors. New
//! positions take their fractional tail from a **bias** derived from
//! the inserted member's identity, so the same member inserted between
//! the same neighbors converges to byte-identical positions on every
//! replica, while distinct members disperse instead of colliding.
//! Colliding positions stay benign: the EAV key totals the order as
//! `(position, member)`.
//!
//! Deliberate departures from the JS POC (see the note):
//!
//! - **Bias is truncated** to [`BIAS_DIGITS`] digits rather than the
//!   full re-encoded identifier (~43 digits for a 32-byte reference):
//!   positions must fit an attribute (64 bytes with the namespace),
//!   and a handful of uniform digits already makes same-slot
//!   collisions vanishingly rare. Truncation is deterministic, so the
//!   convergence property is unaffected.
//! - **No out-of-alphabet sentinels**: the POC models open bounds with
//!   bytes outside base62 (`/`, `{`); `/` is the attribute namespace
//!   separator, so here open bounds are explicit (`Bound::Open`) and
//!   sentinel bytes can never reach a stored position.
//! - **Exhaustion is an error**: inserting before the absolute minimum
//!   returns [`PositionError::Exhausted`] instead of silently reusing
//!   the neighbor's position.

use std::fmt::{self, Display};
use std::iter::repeat_n;
use std::ops::{Bound, RangeBounds};
use std::str::from_utf8;

use crate::DialogArtifactsError;

/// Number of base62 digits a bias contributes to freshly derived
/// positions. Six digits ≈ 35 bits of the member's identity: enough to
/// make same-slot collisions negligible, small enough to respect the
/// attribute-length budget.
pub const BIAS_DIGITS: usize = 6;

/// Base62 digit ranges in byte order: `0-9 < A-Z < a-z`.
const B62: [(u8, u8); 3] = [(b'0', b'9'), (b'A', b'Z'), (b'a', b'z')];
/// Major digit ranges in byte order: `A-M` (negative side) and `N-Z`
/// (positive side). Majors are deliberately UPPERCASE-ONLY: symbols
/// (the user-named attribute halves, per the dictionary-concepts
/// work) must start with a lowercase letter, so a name half is
/// self-discriminating by its first byte — lowercase is a symbol,
/// uppercase is a position — with no tag byte and no ambiguity
/// between positions and ordinary words. Thirteen length classes per
/// side still give base62^13 (≈ 2^77) integer headroom.
const MAJORS: [(u8, u8); 2] = [(b'A', b'M'), (b'N', b'Z')];

const B62_MIN: u8 = b'0';
const B62_MAX: u8 = b'z';
/// Median base62 digit (by sorted code index), matching the POC.
const B62_MEDIAN: u8 = b'V';

/// Failure modes of position derivation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PositionError {
    /// No position exists in the supported range (inserting before the
    /// absolute minimum position).
    Exhausted,
    /// The input does not parse as a position.
    Invalid(String),
}

impl Display for PositionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PositionError::Exhausted => {
                write!(f, "no position exists in the requested range")
            }
            PositionError::Invalid(reason) => {
                write!(f, "invalid position: {reason}")
            }
        }
    }
}

impl From<PositionError> for DialogArtifactsError {
    fn from(error: PositionError) -> Self {
        DialogArtifactsError::InvalidValue(error.to_string())
    }
}

/// A fractional position: a non-empty ASCII string over `0-9A-Za-z`
/// whose leading byte is a major (`A-Z`), ordered by plain byte
/// comparison. Canonical: trailing minimum digits are trimmed, so
/// logically equal positions are byte-identical.
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(into = "String", try_from = "String")]
pub struct Position(Vec<u8>);

impl Position {
    /// The position an empty collection's first member lands on when
    /// inserted with no bounds and no patch: the zero major.
    pub fn origin() -> Self {
        Position(vec![major::ZERO])
    }

    /// The position's bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// The position as a string slice (always ASCII).
    pub fn as_str(&self) -> &str {
        // Validated ASCII on construction.
        from_utf8(&self.0).expect("positions are ASCII")
    }
}

impl Display for Position {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl From<Position> for String {
    fn from(position: Position) -> Self {
        position.as_str().to_owned()
    }
}

impl TryFrom<String> for Position {
    type Error = PositionError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Position::try_from(value.as_str())
    }
}

impl TryFrom<&str> for Position {
    type Error = PositionError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let bytes = value.as_bytes();
        let Some(&head) = bytes.first() else {
            return Err(PositionError::Invalid("empty".into()));
        };
        if !in_ranges(head, &MAJORS) {
            return Err(PositionError::Invalid(format!(
                "leading byte {:?} is not a major (A-Z)",
                head as char
            )));
        }
        if let Some(&byte) = bytes[1..].iter().find(|byte| !in_ranges(**byte, &B62)) {
            return Err(PositionError::Invalid(format!(
                "byte {:?} is outside base62",
                byte as char
            )));
        }
        // Canonical form never ends with the minimum digit (trailing
        // minimums are trimmed at construction). Accepting one here
        // would let a logically equal peer-supplied position exist as
        // a distinct byte string, splitting convergence.
        if bytes.last() == Some(&B62_MIN) {
            return Err(PositionError::Invalid(
                "non-canonical: trailing minimum digit".into(),
            ));
        }
        Ok(Position(bytes.to_vec()))
    }
}

/// The bias a member's identity contributes to derived positions:
/// [`BIAS_DIGITS`] base62 digits of the identity bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Bias(Vec<u8>);

impl Bias {
    /// No bias: derived positions fall back to median digits. Only
    /// appropriate when the member has no stable identity.
    pub fn none() -> Self {
        Bias(Vec::new())
    }

    /// Derive the bias from a member's identity bytes (its entity
    /// reference): the leading [`BIAS_DIGITS`] digits of the bytes
    /// re-encoded into base62. Deterministic, so every replica derives
    /// the same bias for the same member.
    pub fn derive(identity: &[u8]) -> Self {
        let mut digits = to_base62(identity);
        digits.truncate(BIAS_DIGITS);
        Bias(digits)
    }
}

/// Derive the position for a member inserted into `range` — the
/// bounds name the *neighbors*, and the derived position falls
/// strictly between them, so range syntax reads as the insertion
/// itself:
///
/// ```no_run
/// # use dialog_artifacts::position::{insert, Bias, Position};
/// # fn demo(first: &Position, second: &Position) {
/// let bias = Bias::derive(b"member");
/// let head = insert(&bias, ..).unwrap();               // empty collection
/// let appended = insert(&bias, first..).unwrap();      // after the last
/// let prepended = insert(&bias, ..first).unwrap();     // before the first
/// let wedged = insert(&bias, first..second).unwrap();  // between neighbors
/// # }
/// ```
///
/// Since a position can only fall *between* neighbors, inclusive and
/// exclusive bounds are equivalent here (`a..b` ≡ `a..=b`). Swapped
/// bounds are normalized. With both sides unbounded the position is
/// the zero major carrying the bias as its fractional tail.
/// Deterministic: the same `(range, bias)` produces the same position
/// on every replica.
///
/// Inserting between two *equal* positions returns that position
/// unchanged — a collision, which the `(position, member)` total order
/// absorbs. Inserting before the absolute minimum position fails with
/// [`PositionError::Exhausted`].
pub fn insert<Range>(bias: &Bias, range: Range) -> Result<Position, PositionError>
where
    Range: RangeBounds<Position>,
{
    fn neighbor(bound: Bound<&Position>) -> Option<&Position> {
        match bound {
            Bound::Unbounded => None,
            Bound::Included(position) | Bound::Excluded(position) => Some(position),
        }
    }
    match (neighbor(range.start_bound()), neighbor(range.end_bound())) {
        (Some(low), Some(high)) => {
            // Normalize swapped bounds: derivation is order-agnostic.
            if low.0 > high.0 {
                between(bias, high, low)
            } else {
                between(bias, low, high)
            }
        }
        (Some(low), None) => Ok(after_position(bias, low)),
        (None, Some(high)) => before_position(bias, high),
        (None, None) => Ok(create(major::ZERO, &[], &bias.0)),
    }
}

/// Assemble a canonical position: trim trailing minimum digits from
/// the patch, and, when the patch is empty, from the minor too.
///
/// When the patch is non-empty the minor is instead RE-PADDED to the
/// major's full capacity: minor digits arrive min-trimmed from the
/// digit arithmetic, and without padding the patch's first digits
/// would occupy minor byte positions and break byte ordering (a
/// `a‖patch:Y…` would read as minor `Y` and sort above `a‖minor:1`).
/// This is a deliberate fix over the JS POC, which exhibits exactly
/// that mis-ordering when a trimmed minor meets a non-empty patch.
fn create(major: u8, minor: &[u8], patch: &[u8]) -> Position {
    let patch = trim(patch);
    let capacity = major::capacity(major);
    let mut bytes = Vec::with_capacity(1 + capacity + patch.len());
    bytes.push(major);
    if patch.is_empty() {
        bytes.extend_from_slice(trim(minor));
    } else {
        let width = minor.len().min(capacity);
        bytes.extend_from_slice(&minor[..width]);
        bytes.extend(repeat_n(B62_MIN, capacity - width));
        bytes.extend_from_slice(patch);
    }
    Position(bytes)
}

/// A position sorting after `position`, biased by `bias`.
fn after_position(bias: &Bias, position: &Position) -> Position {
    let major = major::of(position);
    let minor = minor::of(position);
    // Step the integer part first: increment the minor…
    if let Some(minor) = digits_increment(&minor, &B62) {
        return create(major, &minor, &bias.0);
    }
    // …or move into the next (larger) major class…
    if let Some(major) = digit_increment(major, &MAJORS) {
        let capacity = major::capacity(major);
        return create(major, &vec![B62_MIN; capacity], &bias.0);
    }
    // …or, at the maximum major, grow the fractional tail (always
    // possible: patches resize).
    let patch = patch::increment(&patch::of(position), &bias.0);
    create(major, &minor, &patch)
}

/// A position sorting before `position`, biased by `bias`. Fails with
/// [`PositionError::Exhausted`] at the absolute minimum.
fn before_position(bias: &Bias, position: &Position) -> Result<Position, PositionError> {
    let major = major::of(position);
    let minor = minor::of(position);
    if let Some(minor) = digits_decrement(&minor, &B62) {
        return Ok(create(major, &minor, &bias.0));
    }
    if let Some(major) = digit_decrement(major, &MAJORS) {
        let capacity = major::capacity(major);
        return Ok(create(major, &vec![B62_MAX; capacity], &bias.0));
    }
    match patch::decrement(&patch::of(position), &bias.0) {
        Some(patch) => Ok(create(major, &minor, &patch)),
        None => Err(PositionError::Exhausted),
    }
}

/// A position sorting strictly between `low` and `high` when room
/// exists; equal inputs come back unchanged.
fn between(bias: &Bias, low: &Position, high: &Position) -> Result<Position, PositionError> {
    let low_major = major::of(low);
    let high_major = major::of(high);

    match digit_intermediate(low_major, high_major, &MAJORS) {
        Digit::Some(major) => {
            let capacity = major::capacity(major);
            Ok(create(major, &vec![B62_MIN; capacity], &bias.0))
        }
        Digit::Equal => {
            let low_minor = minor::of(low);
            let high_minor = minor::of(high);
            match digits_intermediate(&low_minor, &high_minor, &B62) {
                Digits::Some(minor) => Ok(create(low_major, &minor, &bias.0)),
                Digits::Equal => {
                    match patch::intermediate(&patch::of(low), Some(&patch::of(high)), &bias.0) {
                        Some(patch) => Ok(create(low_major, &low_minor, &patch)),
                        // No room: low and high are the same position.
                        None => Ok(low.clone()),
                    }
                }
                Digits::Consecutive => {
                    // `high` stripped of its patch is the most compact
                    // position in the gap, when it has one to strip.
                    if patch::decrement(&patch::of(high), &bias.0).is_some() {
                        return Ok(create(high_major, &high_minor, &[]));
                    }
                    // Otherwise extend `low`'s fractional tail.
                    let patch = patch::next(&patch::of(low), &bias.0);
                    Ok(create(low_major, &low_minor, &patch))
                }
            }
        }
        Digit::Consecutive => {
            // Consecutive majors: prefer stepping `low`'s minor up…
            if let Some(minor) = digits_increment(&minor::of(low), &B62) {
                return Ok(create(low_major, &minor, &bias.0));
            }
            // …or `high`'s minor down…
            if let Some(minor) = digits_decrement(&minor::of(high), &B62) {
                return Ok(create(high_major, &minor, &bias.0));
            }
            // …or `high` without its patch…
            if patch::decrement(&patch::of(high), &bias.0).is_some() {
                return Ok(create(high_major, &minor::of(high), &[]));
            }
            // …falling back to growing `low`'s fractional tail.
            let patch = patch::next(&patch::of(low), &bias.0);
            Ok(create(low_major, &minor::of(low), &patch))
        }
    }
}

/// Major-component arithmetic: the single leading byte encoding the
/// minor's width class.
mod major {
    use super::{MAJORS, Position};

    /// The zero major: the innermost positive class (`N`, one-digit
    /// minor).
    pub const ZERO: u8 = b'N';

    /// The major byte of a position.
    pub fn of(position: &Position) -> u8 {
        position.0[0]
    }

    /// Width of the minor component the major denotes: 1 at the inner
    /// edge of each side (`a`, `Z`), growing outward to 26 (`z`, `A`).
    /// Bytes outside the recommended range denote a zero-width minor.
    pub fn capacity(major: u8) -> usize {
        let [
            (outer_negative, inner_negative),
            (inner_positive, outer_positive),
        ] = MAJORS;
        if (inner_positive..=outer_positive).contains(&major) {
            (major - inner_positive + 1) as usize
        } else if (outer_negative..=inner_negative).contains(&major) {
            (inner_negative - major + 1) as usize
        } else {
            0
        }
    }
}

/// Minor-component access: the fixed-width (per major) integer part.
mod minor {
    use super::{B62_MIN, Position, major};

    /// The minor digits of a position, padded with minimum digits to
    /// the major's capacity (canonical positions trim trailing
    /// minimums when the patch is empty).
    pub fn of(position: &Position) -> Vec<u8> {
        let capacity = major::capacity(major::of(position));
        let stored = &position.0[1..(1 + capacity).min(position.0.len())];
        let mut minor = vec![B62_MIN; capacity];
        minor[..stored.len()].copy_from_slice(stored);
        minor
    }
}

/// Patch-component arithmetic: the unbounded fractional tail.
mod patch {
    use super::{
        B62, B62_MEDIAN, Digits, Position, digits_decrement, digits_increment, major, trim,
    };

    /// The patch digits of a position (everything past the minor).
    pub fn of(position: &Position) -> Vec<u8> {
        let start = 1 + major::capacity(major::of(position));
        position.0.get(start..).unwrap_or_default().to_vec()
    }

    /// Append `extra` to `digits`.
    fn append(digits: &[u8], extra: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(digits.len() + extra.len());
        out.extend_from_slice(digits);
        out.extend_from_slice(extra);
        out
    }

    /// An intermediate patch strictly between `low` and `high` (`None`
    /// for `high` = the open upper bound), or `None` when they are
    /// equal. Consecutive patches grow: the bias (or the median digit)
    /// is appended to `low`; a found intermediate has its tie-break
    /// digit nudged to the bias's head when that fits the gap.
    pub fn intermediate(low: &[u8], high: Option<&[u8]>, bias: &[u8]) -> Option<Vec<u8>> {
        match super::digits_intermediate_bounded(low, high, &B62) {
            Digits::Equal => None,
            Digits::Consecutive => {
                let tail: &[u8] = if bias.is_empty() { &[B62_MEDIAN] } else { bias };
                Some(append(low, tail))
            }
            Digits::Some(digits) => {
                let Some(&head) = bias.first() else {
                    return Some(digits);
                };
                // Nudge the tie-break digit (the last one) to the
                // bias's head when the head fits strictly inside the
                // gap at that offset. The ceiling binds only while the
                // found digits still share the bound's prefix: past a
                // divergence every continuation sorts below the bound,
                // so its digit at this offset is no constraint.
                let offset = digits.len() - 1;
                let ceiling = high.and_then(|high| {
                    let shared = (0..offset).all(|index| {
                        digits[index] == high.get(index).copied().unwrap_or(super::B62_MIN)
                    });
                    shared.then(|| high.get(offset).copied()).flatten()
                });
                let fits = match (low.get(offset), ceiling) {
                    (Some(&floor), Some(ceiling)) => floor < head && head < ceiling,
                    // The POC compares against absent digits as
                    // "false" (undefined comparisons), so a missing
                    // bound never admits the nudge.
                    _ => false,
                };
                let keep = if fits { offset } else { offset + 1 };
                let mut out = Vec::with_capacity(keep + bias.len());
                out.extend_from_slice(&digits[..keep]);
                out.extend_from_slice(bias);
                Some(out)
            }
        }
    }

    /// A patch strictly after `digits` (toward the open upper bound):
    /// an intermediate against the open bound, or the bias appended.
    pub fn next(digits: &[u8], bias: &[u8]) -> Vec<u8> {
        intermediate(digits, None, bias).unwrap_or_else(|| append(digits, bias))
    }

    /// Shorten a patch to its last non-minimum digit, when doing so
    /// leaves at least one digit. (Only reachable for non-canonical
    /// inputs; kept for parity with the POC.)
    fn decrease(digits: &[u8]) -> Option<Vec<u8>> {
        let mut offset = digits.len().checked_sub(1)?;
        while offset > 0 && digits[offset] == super::B62_MIN {
            offset -= 1;
        }
        if offset == 0 {
            None
        } else {
            Some(digits[..=offset].to_vec())
        }
    }

    /// A patch sorting after `patch`, biased. Always succeeds:
    /// patches resize.
    pub fn increment(patch: &[u8], bias: &[u8]) -> Vec<u8> {
        match digits_increment(patch, &B62) {
            None => append(patch, &[B62_MEDIAN]),
            // The bias substitutes only when it sorts at or above the
            // whole incremented string: a head-digit tie says nothing
            // about the tail (`b0…` sorts BELOW `ba`).
            Some(digits) => match bias.first() {
                None => digits,
                Some(_) if bias >= digits.as_slice() => bias.to_vec(),
                Some(_) => append(&digits, bias),
            },
        }
    }

    /// A patch sorting before `patch`, biased, or `None` when no
    /// smaller patch exists.
    pub fn decrement(patch: &[u8], bias: &[u8]) -> Option<Vec<u8>> {
        let digits = digits_decrement(patch, &B62).or_else(|| decrease(patch))?;
        if bias.is_empty() {
            return Some(digits);
        }
        // The bias substitutes only when it sorts at or below the whole
        // decremented string (a head-digit tie says nothing about the
        // tail), and even the appended form can overshoot the original
        // when the decrement trimmed digits off the end — the stepped
        // string alone is the always-sound fallback.
        let candidate = if bias <= digits.as_slice() {
            bias.to_vec()
        } else {
            append(&digits, bias)
        };
        Some(if candidate.as_slice() < patch {
            candidate
        } else {
            digits
        })
    }

    /// Trim trailing minimum digits.
    #[allow(dead_code)]
    pub fn canonical(digits: &[u8]) -> Vec<u8> {
        trim(digits).to_vec()
    }
}

/// Whether `byte` falls inside any of the digit `ranges`.
fn in_ranges(byte: u8, ranges: &[(u8, u8)]) -> bool {
    ranges
        .iter()
        .any(|(from, to)| (*from..=*to).contains(&byte))
}

/// Result of single-digit intermediate search.
enum Digit {
    Some(u8),
    Equal,
    Consecutive,
}

/// Result of digit-string arithmetic.
enum Digits {
    Some(Vec<u8>),
    Equal,
    Consecutive,
}

/// Increment one digit within `ranges`, rounding gaps up; `None` past
/// the maximum.
fn digit_increment(digit: u8, ranges: &[(u8, u8)]) -> Option<u8> {
    let next = digit.checked_add(1)?;
    for &(from, to) in ranges {
        if next < from {
            return Some(from);
        }
        if next <= to {
            return Some(next);
        }
    }
    None
}

/// Decrement one digit within `ranges`, rounding gaps down; `None`
/// below the minimum.
fn digit_decrement(digit: u8, ranges: &[(u8, u8)]) -> Option<u8> {
    let previous = digit.checked_sub(1)?;
    for &(from, to) in ranges.iter().rev() {
        if previous > to {
            return Some(to);
        }
        if previous >= from {
            return Some(previous);
        }
    }
    None
}

/// An intermediate digit strictly between `from` and `to` that falls
/// inside `ranges`, or the equal/consecutive verdict. Digits at or
/// beyond the range bounds are treated exactly as the POC does:
/// rounded into the nearest admissible bound when that bound is
/// strictly inside the gap.
fn digit_intermediate(from: u8, to: u8, ranges: &[(u8, u8)]) -> Digit {
    let (bottom, top) = if from > to { (to, from) } else { (from, to) };
    if bottom == top {
        return Digit::Equal;
    }
    if top - bottom == 1 {
        return Digit::Consecutive;
    }

    // Round-half-up average, matching JS `Math.round`.
    let digit = (bottom as u16 + top as u16).div_ceil(2) as u8;

    let mut last: Option<u8> = None;
    for &(low, high) in ranges {
        if digit < low {
            if bottom < low && low < top {
                return Digit::Some(low);
            }
            if let Some(last) = last
                && bottom < last
                && last < high
            {
                return Digit::Some(last);
            }
            return Digit::Consecutive;
        }
        if low <= digit && digit <= high {
            return Digit::Some(digit);
        }
        last = Some(high);
    }
    if let Some(last) = last
        && bottom < last
        && last < top
    {
        return Digit::Some(last);
    }
    Digit::Consecutive
}

/// Increment a fixed-width digit string within `ranges`; the result is
/// trimmed of the trailing digits the carry reset. `None` when every
/// digit carries (the string is at capacity).
fn digits_increment(source: &[u8], ranges: &[(u8, u8)]) -> Option<Vec<u8>> {
    let mut digits = source.to_vec();
    for offset in (0..digits.len()).rev() {
        match digit_increment(digits[offset], ranges) {
            None => digits[offset] = ranges[0].0,
            Some(digit) => {
                digits[offset] = digit;
                digits.truncate(offset + 1);
                return Some(digits);
            }
        }
    }
    None
}

/// Decrement a fixed-width digit string within `ranges`, trimmed of
/// trailing minimums. `None` when every digit borrows.
fn digits_decrement(source: &[u8], ranges: &[(u8, u8)]) -> Option<Vec<u8>> {
    let mut digits = source.to_vec();
    let max = ranges[ranges.len() - 1].1;
    for offset in (0..digits.len()).rev() {
        match digit_decrement(digits[offset], ranges) {
            None => digits[offset] = max,
            Some(digit) => {
                digits[offset] = digit;
                return Some(trim(&digits).to_vec());
            }
        }
    }
    None
}

/// An intermediate digit string between `begin` and `end`, where
/// digits missing off either end read as the minimum (canonical
/// trimming makes them implicit).
fn digits_intermediate(begin: &[u8], end: &[u8], ranges: &[(u8, u8)]) -> Digits {
    digits_intermediate_bounded(begin, Some(end), ranges)
}

/// [`digits_intermediate`] with an optionally *open* upper bound:
/// `None` for `end` reads every missing high digit as one past the
/// maximum, replacing the POC's out-of-alphabet sentinel.
fn digits_intermediate_bounded(begin: &[u8], end: Option<&[u8]>, ranges: &[(u8, u8)]) -> Digits {
    let min = ranges[0].0;
    let max = ranges[ranges.len() - 1].1;
    let end_len = end.map(|end| end.len()).unwrap_or(0);
    let length = begin.len().max(end_len).max(usize::from(end.is_none()));
    let mut digits = vec![0u8; length];

    // Copy the shared prefix. An open upper bound shares nothing (its
    // first digit is already past the maximum).
    let mut offset = 0;
    if let Some(end) = end {
        while offset < digits.len() {
            let lower = begin.get(offset).copied().unwrap_or(min);
            let upper = end.get(offset).copied().unwrap_or(min);
            if lower == upper {
                digits[offset] = lower;
                offset += 1;
            } else {
                break;
            }
        }
        if offset == digits.len() {
            return Digits::Equal;
        }
    }

    // Orient the bounds at the first divergent digit.
    let reversed = match end {
        Some(end) => {
            let lower = begin.get(offset).copied().unwrap_or(min);
            let upper = end.get(offset).copied().unwrap_or(min);
            lower > upper
        }
        None => false,
    };
    let (from, to): (&[u8], Option<&[u8]>) = if reversed {
        (end.expect("reversed implies a bound"), Some(begin))
    } else {
        (begin, end)
    };

    // Walk forward copying `from` until a non-consecutive gap admits
    // an intermediate digit. Missing high digits read one past the
    // maximum so a shorter (or open) upper bound leaves room below it.
    //
    // Once a digit of `from` is committed strictly below the bound's
    // (the consecutive verdict), the bound's remaining digits are
    // irrelevant: every continuation of the committed prefix already
    // sorts below the bound, so the walk continues against the open
    // bound. Consulting them instead — as the JS POC does — hands back
    // "intermediates" BELOW `from` whenever the bound's tail digits
    // sort below `from`'s (e.g. between `N0Az` and `N0B1`).
    let mut bounded = true;
    while offset < digits.len() {
        let low = from.get(offset).copied().unwrap_or(min);
        let high = if bounded {
            to.and_then(|to| to.get(offset).copied()).unwrap_or(max + 1)
        } else {
            max + 1
        };
        match digit_intermediate(low, high, ranges) {
            Digit::Equal | Digit::Consecutive => {
                bounded = bounded && low == high;
                digits[offset] = low;
                offset += 1;
            }
            Digit::Some(digit) => {
                digits[offset] = digit;
                digits.truncate(offset + 1);
                return Digits::Some(digits);
            }
        }
    }
    Digits::Consecutive
}

/// Trim trailing minimum digits (they are implicit in canonical form).
fn trim(digits: &[u8]) -> &[u8] {
    let mut length = digits.len();
    while length > 0 && digits[length - 1] == B62_MIN {
        length -= 1;
    }
    &digits[..length]
}

/// Re-encode bytes into base62 digit characters (most significant
/// first), preserving leading zero bytes as leading zero digits.
fn to_base62(bytes: &[u8]) -> Vec<u8> {
    const CODES: &[u8; 62] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    let mut leading_zeros = 0usize;
    let mut counting = true;
    let mut encoding: Vec<u32> = Vec::new();
    for &byte in bytes {
        if counting && byte == 0 {
            leading_zeros += 1;
            continue;
        }
        counting = false;
        let mut carry = byte as u32;
        let mut index = 0;
        while carry != 0 || index < encoding.len() {
            if index == encoding.len() {
                encoding.push(0);
            }
            carry += encoding[index] << 8;
            encoding[index] = carry % 62;
            carry /= 62;
            index += 1;
        }
    }
    let mut digits = Vec::with_capacity(leading_zeros + encoding.len());
    digits.extend(repeat_n(CODES[0], leading_zeros));
    digits.extend(encoding.iter().rev().map(|&digit| CODES[digit as usize]));
    digits
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;

    fn position(text: &str) -> Position {
        Position::try_from(text).expect("position parses")
    }

    fn bias(seed: &[u8]) -> Bias {
        Bias::derive(seed)
    }

    /// Insert with no bounds lands on the zero major carrying the
    /// bias; without bias it is the bare origin.
    #[dialog_common::test]
    fn it_derives_first_positions() {
        let first = insert(&Bias::none(), ..).expect("derives");
        assert_eq!(first.as_str(), "N");
        let biased = insert(&bias(b"member-1"), ..).expect("derives");
        assert!(
            biased.as_str().starts_with("N0"),
            "major + padded minor: {biased}"
        );
        assert!(biased.as_str().len() <= 2 + BIAS_DIGITS);
        assert!(biased > first, "bias sorts above the bare origin");
    }

    /// Order invariants: after > input, before < input, between falls
    /// strictly inside when room exists.
    #[dialog_common::test]
    fn it_preserves_order() {
        let b = bias(b"member");
        let origin = position("N");
        let after = insert(&b, &origin..).expect("after");
        assert!(after > origin, "{after} > {origin}");
        let before = insert(&b, ..&origin).expect("before");
        assert!(before < origin, "{before} < {origin}");
        let mid = insert(&b, &before..&after).expect("between");
        assert!(before < mid && mid < after, "{before} < {mid} < {after}");
    }

    /// Repeated appends grow logarithmically through the integer
    /// headroom rather than linearly through the fraction.
    #[dialog_common::test]
    fn it_appends_compactly() {
        let mut positions = vec![insert(&Bias::none(), ..).expect("first")];
        for _ in 0..1000 {
            let last = positions.last().expect("non-empty");
            let next = insert(&Bias::none(), last..).expect("append");
            assert!(&next > last);
            positions.push(next);
        }
        let longest = positions.iter().map(|p| p.as_bytes().len()).max().unwrap();
        assert!(longest <= 4, "1000 appends stay compact, got {longest}");
        let mut sorted = positions.clone();
        sorted.sort();
        assert_eq!(sorted, positions, "byte order is insertion order");
    }

    /// Repeated prepends stay compact through the negative majors.
    #[dialog_common::test]
    fn it_prepends_compactly() {
        let mut positions = vec![insert(&Bias::none(), ..).expect("first")];
        for _ in 0..1000 {
            let first = positions.last().expect("non-empty");
            let next = insert(&Bias::none(), ..first).expect("prepend");
            assert!(&next < first);
            positions.push(next);
        }
        let longest = positions.iter().map(|p| p.as_bytes().len()).max().unwrap();
        assert!(longest <= 4, "1000 prepends stay compact, got {longest}");
    }

    /// Repeated same-slot insertion (the pathological case) grows the
    /// fractional tail but stays within the attribute budget for
    /// realistic depths.
    #[dialog_common::test]
    fn it_bounds_midpoint_growth() {
        let b = bias(b"wedge");
        let mut low = position("N");
        let high = insert(&Bias::none(), &low..).expect("high");
        for _ in 0..24 {
            let mid = insert(&b, &low..&high).expect("between");
            assert!(low < mid && mid < high, "{low} < {mid} < {high}");
            low = mid;
        }
        assert!(low.as_bytes().len() < 40, "24 wedges fit: {low}");
    }

    /// Determinism and convergence: the same (bounds, member) derive
    /// byte-identical positions; distinct members disperse.
    #[dialog_common::test]
    fn it_converges_and_disperses() {
        let low = position("N");
        let high = position("O");
        let milk_here = insert(&bias(b"milk"), &low..&high).expect("derives");
        let milk_there = insert(&bias(b"milk"), &low..&high).expect("derives");
        assert_eq!(milk_here, milk_there, "replicas converge");

        let bread = insert(&bias(b"bread"), &low..&high).expect("derives");
        assert_ne!(milk_here, bread, "distinct members disperse");
    }

    /// Equal bounds return the shared position (a benign collision);
    /// swapped bounds are normalized.
    #[dialog_common::test]
    fn it_handles_degenerate_bounds() {
        let at = position("R");
        let same = insert(&bias(b"x"), &at..&at).expect("derives");
        assert_eq!(same, at);

        let low = position("P");
        let high = position("T");
        let forward = insert(&bias(b"x"), &low..&high).expect("derives");
        let swapped = insert(&bias(b"x"), &high..&low).expect("derives");
        assert_eq!(forward, swapped, "bounds normalize");
    }

    /// Canonicality: derived positions never carry trailing minimum
    /// digits and always re-parse.
    #[dialog_common::test]
    fn it_stays_canonical() {
        let b = bias(b"member");
        let mut low = position("N");
        let high = position("O");
        for _ in 0..12 {
            let mid = insert(&b, &low..&high).expect("between");
            assert_ne!(mid.as_bytes().last(), Some(&b'0'), "no trailing min: {mid}");
            Position::try_from(mid.as_str()).expect("round-trips");
            low = mid;
        }
    }

    /// Every byte of every derived position stays inside the alphabet
    /// (no sentinel leakage) and inside the attribute charset.
    #[dialog_common::test]
    fn it_never_leaks_sentinels() {
        let mut low = position("N");
        let high = position("O");
        for seed in 0..64u8 {
            let mid = insert(&bias(&[seed]), &low..&high).expect("between");
            for &byte in mid.as_bytes() {
                assert!(
                    in_ranges(byte, &B62),
                    "byte {byte:#x} escapes base62 in {mid}"
                );
                assert_ne!(byte, b'/');
            }
            low = mid;
        }
    }

    /// The absolute minimum cannot be preceded: exhaustion is an
    /// explicit error, not a silent duplicate.
    #[dialog_common::test]
    fn it_reports_exhaustion() {
        // "A" (major at the negative edge) with an all-minimum minor is
        // the smallest canonical position.
        let floor = position("A");
        let result = insert(&Bias::none(), ..&floor);
        assert_eq!(result, Err(PositionError::Exhausted));
    }

    /// base62 re-encoding preserves byte-order (leading digits of the
    /// encoding order like the bytes they encode) and leading zeros.
    #[dialog_common::test]
    fn it_encodes_bias_digits() {
        assert_eq!(to_base62(&[]), Vec::<u8>::new());
        assert_eq!(to_base62(&[0, 0]), b"00".to_vec());
        let digits = to_base62(&[255; 8]);
        assert!(digits.iter().all(|&digit| in_ranges(digit, &B62)));
        assert_eq!(Bias::derive(&[7; 32]).0.len(), BIAS_DIGITS);
    }

    /// Wedging between every adjacent pair of an append-built list must
    /// stay strictly inside the gap. The JS POC's digit walk kept
    /// consulting the upper bound's tail digits after the prefixes had
    /// already diverged, handing back "intermediates" BELOW the lower
    /// neighbor for gaps like `O0z..O1` — the exact shape plain appends
    /// produce.
    #[dialog_common::test]
    fn it_wedges_between_appended_neighbors() {
        let mut positions = vec![insert(&Bias::none(), ..).expect("first")];
        for seed in 0..300u32 {
            let last = positions.last().expect("non-empty");
            let next = insert(&bias(&seed.to_be_bytes()), last..).expect("append");
            positions.push(next);
        }
        for (index, pair) in positions.windows(2).enumerate() {
            let (low, high) = (&pair[0], &pair[1]);
            for member in [b"wedge".as_slice(), &[index as u8]] {
                let mid = insert(&Bias::derive(member), low..high).expect("wedge");
                assert!(
                    low < &mid && &mid < high,
                    "gap {index}: {low} < {mid} < {high} violated"
                );
            }
        }
    }

    /// Randomized order invariant: inserting at random gaps with random
    /// biases keeps every derived position strictly inside its gap and
    /// the whole list byte-sorted in insertion order.
    #[dialog_common::test]
    fn it_stays_ordered_under_random_insertion() {
        let mut state = 0x9E37_79B9_7F4A_7C15u64;
        let mut random = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };

        let mut positions = vec![insert(&Bias::none(), ..).expect("first")];
        for _ in 0..600 {
            let draw = random();
            let member = bias(&draw.to_be_bytes());
            let gap = (random() as usize) % (positions.len() + 1);
            let derived = if gap == 0 {
                let first = &positions[0];
                match insert(&member, ..first) {
                    Ok(position) => position,
                    // The absolute floor is a legal dead end.
                    Err(PositionError::Exhausted) => continue,
                    Err(error) => panic!("prepend failed: {error:?}"),
                }
            } else if gap == positions.len() {
                let last = &positions[gap - 1];
                insert(&member, last..).expect("append")
            } else {
                let (low, high) = (&positions[gap - 1], &positions[gap]);
                let mid = insert(&member, low..high).expect("between");
                // Equal neighbors absorb collisions; strict betweenness
                // is only owed for a real gap.
                if low < high {
                    assert!(low < &mid && &mid < high, "{low} < {mid} < {high} violated");
                }
                mid
            };
            positions.insert(gap, derived);
            let mut sorted = positions.clone();
            sorted.sort();
            assert_eq!(sorted, positions, "byte order must track list order");
        }
    }

    /// The parse boundary rejects non-canonical spellings: a trailing
    /// minimum digit would let a logically equal peer-supplied position
    /// exist as a distinct byte string and split convergence.
    #[dialog_common::test]
    fn it_rejects_non_canonical_input() {
        for bad in ["N0", "O000", "N0z0"] {
            assert!(
                Position::try_from(bad).is_err(),
                "{bad:?} must not parse as canonical"
            );
        }
        for good in ["N", "O1z", "A1"] {
            Position::try_from(good).expect("canonical spelling parses");
        }
    }

    /// The biased patch steps substitute the bias only when the WHOLE
    /// bias sorts past the stepped string: a head-digit tie says
    /// nothing about the tail.
    #[dialog_common::test]
    fn it_keeps_biased_patch_steps_ordered() {
        let after = patch::increment(b"ba", b"b0xy");
        assert!(
            after.as_slice() > b"ba".as_slice(),
            "increment must sort after: {after:?}"
        );
        let before = patch::decrement(b"b1", b"b8xy").expect("decrements");
        assert!(
            before.as_slice() < b"b1".as_slice(),
            "decrement must sort before: {before:?}"
        );
    }
}
