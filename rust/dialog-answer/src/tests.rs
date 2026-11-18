#![allow(clippy::unused_async)]

use crate::{Cell, Dictionary, Error, Kind, Scalar, Session};

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

#[dialog_common::test]
fn it_round_trips_every_scalar() {
    let values = [
        Scalar::Text("Alice"),
        Scalar::Entity("did:key:z6Mk"),
        Scalar::Symbol("person/name"),
        Scalar::Bytes(&[1, 2, 3]),
        Scalar::Record(&[9]),
        Scalar::Natural(u128::MAX),
        Scalar::Integer(-7),
        Scalar::Float(2.5),
        Scalar::Boolean(true),
    ];
    let mut session = Session::default();
    let mut frame = session.frame(Kind::Snapshot, &["v"]);
    for value in values {
        frame.row([Some(value)]);
    }
    let mut dictionary = Dictionary::default();
    let answer = dictionary.accept(frame.finish().into()).unwrap();
    let rows = answer.section(0);
    assert_eq!(rows.len(), values.len());
    for (row, value) in rows.iter().zip(values) {
        assert_eq!(row.get(&dictionary, 0), Some(value));
    }
}

/// A token one frame sent is referenced, not written again, by the
/// session's later frames: a delta that repeats the snapshot's values
/// carries only its lanes.
#[dialog_common::test]
fn it_sends_each_token_once_per_session() {
    let mut session = Session::default();
    let mut dictionary = Dictionary::default();
    let mut snapshot = session.frame(Kind::Snapshot, &["this", "status"]);
    snapshot.row([Some(Scalar::Entity("issue:1")), Some(Scalar::Text("open"))]);
    snapshot.row([Some(Scalar::Entity("issue:2")), Some(Scalar::Text("open"))]);
    let snapshot = snapshot.finish();
    dictionary.accept(snapshot.clone().into()).unwrap();
    assert_eq!(
        dictionary.len(),
        5,
        "two column names, two entities, one status"
    );

    let mut delta = session.frame(Kind::Delta, &["this", "status"]);
    delta.row([Some(Scalar::Entity("issue:2")), Some(Scalar::Text("open"))]);
    delta.retracted();
    delta.row([Some(Scalar::Entity("issue:1")), Some(Scalar::Text("open"))]);
    let delta = delta.finish();
    let answer = dictionary.accept(delta.clone().into()).unwrap();
    assert_eq!(dictionary.len(), 5, "the delta sent no token");
    assert!(delta.len() < snapshot.len());
    let retracted = answer.section(1);
    assert_eq!(
        retracted.row(0).get(&dictionary, 0),
        Some(Scalar::Entity("issue:1"))
    );
}

#[dialog_common::test]
fn it_tells_absent_from_unbound() {
    let mut session = Session::default();
    let mut frame = session.frame(Kind::Snapshot, &["a", "b", "c"]);
    frame.row_with([Some(Some(Scalar::Natural(1))), Some(None), None]);
    let mut dictionary = Dictionary::default();
    let answer = dictionary.accept(frame.finish().into()).unwrap();
    let row = answer.section(0).row(0);
    assert_eq!(row.cell(&dictionary, 0), Cell::Value(Scalar::Natural(1)));
    assert_eq!(row.cell(&dictionary, 1), Cell::Absent);
    assert_eq!(row.cell(&dictionary, 2), Cell::Unbound);
}

/// A frame out of the session's order is refused, not misread: its ids
/// would name the wrong tokens.
#[dialog_common::test]
fn it_refuses_a_frame_out_of_sequence() {
    let mut session = Session::default();
    let mut first = session.frame(Kind::Snapshot, &["x"]);
    first.row([Some(Scalar::Text("one"))]);
    let _lost = first.finish();
    let mut second = session.frame(Kind::Snapshot, &["x"]);
    second.row([Some(Scalar::Text("two"))]);
    let mut dictionary = Dictionary::default();
    let refused = dictionary.accept(second.finish().into());
    assert!(matches!(refused, Err(Error::Sequence { expected: 0, .. })));
    assert!(dictionary.is_empty(), "a refused frame teaches nothing");
}

#[dialog_common::test]
fn it_refuses_truncated_and_malformed_frames() {
    let mut session = Session::default();
    let mut frame = session.frame(Kind::Snapshot, &["x"]);
    frame.row([Some(Scalar::Text("ok"))]);
    let bytes = frame.finish();
    let mut dictionary = Dictionary::default();
    for cut in 0..bytes.len() {
        assert!(
            dictionary.accept(bytes[..cut].into()).is_err(),
            "cut at {cut}"
        );
    }
    let mut corrupt = bytes.clone();
    // The column name's text: make it invalid UTF-8.
    corrupt[13 + 5] = 0xFF;
    assert!(matches!(
        dictionary.accept(corrupt.into()),
        Err(Error::Malformed(_))
    ));
    assert!(dictionary.accept(bytes.into()).is_ok());
}

mod generated;
mod hostile;
