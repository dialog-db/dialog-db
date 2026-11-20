//! Frames another process could send that the writer never would. The
//! reader must refuse each with an [`Error`], leave the session as it was,
//! and read nothing it did not check.

use crate::{Dictionary, Error, Kind, MAGIC, Scalar, Session};

/// Lays out a frame by hand: `tokens` as (tag, bytes), `columns` as token
/// ids, and one (rows, ids) entry per section with the lanes column-major.
fn frame(
    kind: u8,
    base: u32,
    tokens: &[(u8, &[u8])],
    columns: &[u32],
    sections: &[(u32, &[u32])],
) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(MAGIC);
    out.push(kind);
    out.extend_from_slice(&base.to_le_bytes());
    out.extend_from_slice(&(tokens.len() as u32).to_le_bytes());
    for (tag, bytes) in tokens {
        out.push(*tag);
        out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        out.extend_from_slice(bytes);
    }
    out.extend_from_slice(&(columns.len() as u16).to_le_bytes());
    for id in columns {
        out.extend_from_slice(&id.to_le_bytes());
    }
    for (rows, ids) in sections {
        out.extend_from_slice(&rows.to_le_bytes());
        for id in *ids {
            out.extend_from_slice(&id.to_le_bytes());
        }
    }
    out
}

/// The session's first frame as the writer lays it out: one column `x`
/// and one row.
fn first_valid_frame() -> Vec<u8> {
    let mut session = Session::default();
    let mut frame = session.frame(Kind::Snapshot, &["x"]);
    frame.row([Some(Scalar::Text("one"))]);
    frame.finish()
}

/// A frame refused at its column names teaches the dictionary nothing:
/// its token count is unchanged and the session's real next frame, whose
/// `base` is that count, is still accepted.
///
/// The frame below has one well-formed token, a natural number, and names
/// it as its only column, which must be text. The refusal comes after the
/// frame's tokens were already learned.
#[dialog_common::test]
fn it_learns_nothing_from_a_frame_refused_at_its_columns() {
    let natural = 7u128.to_le_bytes();
    let bad = frame(0, 0, &[(6, &natural)], &[0], &[(0, &[])]);
    let mut dictionary = Dictionary::default();
    let refused = dictionary.accept(bad.into());
    assert!(
        matches!(refused, Err(Error::Malformed(0))),
        "expected Malformed(0) for a number used as a column name, found {refused:?}"
    );
    let tokens = dictionary.len();
    let next = dictionary.accept(first_valid_frame().into());
    assert!(
        tokens == 0 && next.is_ok(),
        "expected the refused frame to leave 0 tokens and the next frame to be accepted; \
         found {tokens} tokens and {next:?}"
    );
}

/// A column id the session never sent is refused as [`Error::Unknown`],
/// as a lane id the session never sent is, and the refusal teaches the
/// dictionary nothing.
#[dialog_common::test]
fn it_refuses_a_column_id_it_never_received_as_unknown() {
    let bad = frame(0, 0, &[(1, b"x")], &[7], &[(0, &[])]);
    let mut dictionary = Dictionary::default();
    let refused = dictionary.accept(bad.into());
    let tokens = dictionary.len();
    assert!(
        matches!(refused, Err(Error::Unknown(7))) && tokens == 0,
        "expected Unknown(7) with 0 tokens learned; found {refused:?} with {tokens} tokens learned"
    );
}

/// A header that declares far more tokens than the frame holds is refused
/// as [`Error::Truncated`].
///
/// Declaring `u32::MAX` tokens included: the reader reserves no more
/// tokens than the frame's bytes could hold, so the count alone allocates
/// nothing. What it reserves is measured by `tests/reservation.rs`.
#[dialog_common::test]
fn it_refuses_a_header_that_declares_more_tokens_than_it_holds() {
    for count in [1u32 << 22, u32::MAX] {
        let mut header = Vec::new();
        header.extend_from_slice(MAGIC);
        header.push(0);
        header.extend_from_slice(&0u32.to_le_bytes());
        header.extend_from_slice(&count.to_le_bytes());
        let mut dictionary = Dictionary::default();
        let refused = dictionary.accept(header.into());
        assert!(
            matches!(refused, Err(Error::Truncated)),
            "expected Truncated for {count} declared tokens, found {refused:?}"
        );
        assert!(dictionary.is_empty());
    }
}

/// A column index past the answer's columns reads no value. In a delta
/// the asserted section's lanes are followed by the retracted section, so
/// an index one or two past the last column lands on the retracted
/// section's row count and lane, and reads them as token ids.
#[dialog_common::test]
fn it_reads_no_value_for_a_column_the_answer_does_not_have() {
    let mut session = Session::default();
    let mut delta = session.frame(Kind::Delta, &["x"]);
    delta.row([Some(Scalar::Text("asserted"))]);
    delta.retracted();
    delta.row([Some(Scalar::Text("retracted"))]);
    let mut dictionary = Dictionary::default();
    let answer = dictionary.accept(delta.finish().into()).unwrap();
    assert_eq!(answer.columns().count(), 1);
    let row = answer.section(0).row(0);
    let past = [row.get(&dictionary, 1), row.get(&dictionary, 2)];
    assert_eq!(
        past,
        [None, None],
        "expected no value for columns 1 and 2 of a one-column answer, found {past:?}"
    );
}
