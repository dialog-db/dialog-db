//! Frames from a fixed-seed generator: every scalar kind and its edge
//! values, written with a [`Session`] and read back with a [`Dictionary`].

use crate::{Cell, Dictionary, Kind, Scalar, Session};

/// xorshift64: deterministic, so a failure names the same frame every run.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

/// A scalar the test owns, so a frame's expected cells outlive its
/// writing.
#[derive(Debug, Clone)]
enum Value {
    Text(String),
    Entity(String),
    Symbol(String),
    Bytes(Vec<u8>),
    Record(Vec<u8>),
    Natural(u128),
    Integer(i128),
    Float(f64),
    Boolean(bool),
}

impl Value {
    fn scalar(&self) -> Scalar<'_> {
        match self {
            Value::Text(text) => Scalar::Text(text),
            Value::Entity(text) => Scalar::Entity(text),
            Value::Symbol(text) => Scalar::Symbol(text),
            Value::Bytes(bytes) => Scalar::Bytes(bytes),
            Value::Record(bytes) => Scalar::Record(bytes),
            Value::Natural(n) => Scalar::Natural(*n),
            Value::Integer(n) => Scalar::Integer(*n),
            Value::Float(n) => Scalar::Float(*n),
            Value::Boolean(b) => Scalar::Boolean(*b),
        }
    }
}

/// Equal as stored: floats by their bits, so NaN equals itself and -0.0
/// does not equal 0.0.
fn same(left: Scalar<'_>, right: Scalar<'_>) -> bool {
    match (left, right) {
        (Scalar::Float(left), Scalar::Float(right)) => left.to_bits() == right.to_bits(),
        (left, right) => left == right,
    }
}

#[derive(Debug, Clone)]
enum Expected {
    Unbound,
    Absent,
    Value(Value),
}

/// A pool of values with every kind's edges and short random strings
/// drawn from a small alphabet, so rows repeat values.
fn pool(rng: &mut Rng) -> Vec<Value> {
    let mut pool = vec![
        Value::Text(String::new()),
        Value::Entity(String::new()),
        Value::Symbol(String::new()),
        Value::Bytes(Vec::new()),
        Value::Record(Vec::new()),
        Value::Text("é\u{1F600}".into()),
        Value::Natural(0),
        Value::Natural(u128::MAX),
        Value::Integer(i128::MIN),
        Value::Integer(i128::MAX),
        Value::Integer(-1),
        Value::Float(f64::NAN),
        Value::Float(-0.0),
        Value::Float(0.0),
        Value::Float(f64::INFINITY),
        Value::Float(f64::MIN_POSITIVE),
        Value::Boolean(false),
        Value::Boolean(true),
    ];
    let alphabet = ['a', 'b', 'é', ':'];
    for _ in 0..24 {
        let text: String = (0..rng.below(4))
            .map(|_| alphabet[rng.below(alphabet.len())])
            .collect();
        let bytes: Vec<u8> = (0..rng.below(4)).map(|_| rng.next() as u8).collect();
        pool.push(match rng.below(5) {
            0 => Value::Text(text),
            1 => Value::Entity(format!("did:key:{text}")),
            2 => Value::Symbol(text),
            3 => Value::Bytes(bytes),
            _ => Value::Record(bytes),
        });
    }
    pool
}

/// One frame to write: its kind, columns and the rows of each section,
/// a row possibly shorter than the columns (the writer pads it unbound).
struct Plan {
    kind: Kind,
    columns: Vec<String>,
    sections: Vec<Vec<Vec<Expected>>>,
}

fn rows(rng: &mut Rng, pool: &[Value], columns: usize, count: usize) -> Vec<Vec<Expected>> {
    (0..count)
        .map(|_| {
            let width = if columns > 0 && rng.below(8) == 0 {
                rng.below(columns)
            } else {
                columns
            };
            (0..width)
                .map(|_| match rng.below(10) {
                    0 => Expected::Unbound,
                    1 => Expected::Absent,
                    _ => Expected::Value(pool[rng.below(pool.len())].clone()),
                })
                .collect()
        })
        .collect()
}

fn plans(rng: &mut Rng, pool: &[Value]) -> Vec<Plan> {
    let names = |names: &[&str]| names.iter().map(|name| name.to_string()).collect();
    vec![
        Plan {
            kind: Kind::Snapshot,
            columns: Vec::new(),
            sections: vec![rows(rng, pool, 0, 3)],
        },
        Plan {
            kind: Kind::Snapshot,
            columns: names(&["this", "name"]),
            sections: vec![Vec::new()],
        },
        Plan {
            kind: Kind::Snapshot,
            columns: names(&["this", "name", "a", "", "é"]),
            sections: vec![rows(rng, pool, 5, 40)],
        },
        Plan {
            kind: Kind::Delta,
            columns: names(&["this", "name", "a", "", "é"]),
            sections: vec![rows(rng, pool, 5, 7), rows(rng, pool, 5, 5)],
        },
        Plan {
            kind: Kind::Delta,
            columns: names(&["this"]),
            sections: vec![Vec::new(), rows(rng, pool, 1, 4)],
        },
        Plan {
            kind: Kind::Delta,
            columns: Vec::new(),
            sections: vec![Vec::new(), Vec::new()],
        },
        Plan {
            kind: Kind::Snapshot,
            columns: names(&["name", "this"]),
            sections: vec![rows(rng, pool, 2, 20)],
        },
    ]
}

fn write(session: &mut Session, plan: &Plan) -> Vec<u8> {
    let columns: Vec<&str> = plan.columns.iter().map(String::as_str).collect();
    let mut frame = session.frame(plan.kind, &columns);
    for (at, section) in plan.sections.iter().enumerate() {
        if at == 1 {
            frame.retracted();
        }
        for row in section {
            frame.row_with(row.iter().map(|cell| match cell {
                Expected::Unbound => None,
                Expected::Absent => Some(None),
                Expected::Value(value) => Some(Some(value.scalar())),
            }));
        }
    }
    frame.finish()
}

/// The session's frames, in order, with what each must read back as.
fn session_frames(seed: u64) -> (Vec<Plan>, Vec<Vec<u8>>) {
    let mut rng = Rng(seed);
    let pool = pool(&mut rng);
    let plans = plans(&mut rng, &pool);
    let mut session = Session::default();
    let frames = plans.iter().map(|plan| write(&mut session, plan)).collect();
    (plans, frames)
}

/// Every frame a session writes reads back cell for cell: every scalar
/// kind, empty texts and buffers, NaN and -0.0, the extreme integers,
/// absent and unbound cells and rows shorter than their columns, repeated
/// values, zero rows, zero columns, deltas with either or both sections
/// empty, and later frames that reuse earlier frames' tokens.
#[dialog_common::test]
fn it_round_trips_a_generated_session() {
    for seed in [0x9E37_79B9_7F4A_7C15, 1, 0xDEAD_BEEF] {
        let (plans, frames) = session_frames(seed);
        let mut dictionary = Dictionary::default();
        let mut sent = Session::default();
        for (plan, bytes) in plans.iter().zip(&frames) {
            // Replays the writer's token count without keeping the frames.
            write(&mut sent, plan);
            let answer = dictionary.accept(bytes.as_slice().into()).unwrap();
            assert_eq!(dictionary.len(), sent.len(), "seed {seed:#x}");
            assert_eq!(answer.kind(), plan.kind);
            let columns: Vec<&str> = answer.columns().collect();
            assert_eq!(columns, plan.columns);
            for (at, section) in plan.sections.iter().enumerate() {
                let rows = answer.section(at);
                assert_eq!(rows.len(), section.len(), "seed {seed:#x} section {at}");
                for (index, expected) in section.iter().enumerate() {
                    let row = rows.row(index);
                    for column in 0..plan.columns.len() {
                        let cell = row.cell(&dictionary, column);
                        let matches = match (expected.get(column), cell) {
                            (None | Some(Expected::Unbound), Cell::Unbound) => true,
                            (Some(Expected::Absent), Cell::Absent) => true,
                            (Some(Expected::Value(value)), Cell::Value(read)) => {
                                same(value.scalar(), read)
                            }
                            _ => false,
                        };
                        assert!(
                            matches,
                            "seed {seed:#x} section {at} row {index} column {column}: \
                             expected {:?}, read {cell:?}",
                            expected.get(column)
                        );
                    }
                }
            }
        }
    }
}

/// Reads every column name and every cell of every section an accepted
/// answer has. Only the native-only mutation test uses it.
#[cfg(not(target_arch = "wasm32"))]
fn read_everything(dictionary: &Dictionary, answer: &crate::Answer) {
    let columns = answer.columns().count();
    let sections = match answer.kind() {
        Kind::Snapshot => 1,
        Kind::Delta => 2,
    };
    for at in 0..sections {
        for row in answer.section(at).iter() {
            for column in 0..columns {
                let _ = row.cell(dictionary, column);
            }
        }
    }
}

/// Every byte of every generated frame flipped, cleared or set, and every
/// frame cut at every length: `accept` returns an error or an answer whose
/// every cell reads, and never panics.
///
/// The high byte of the header's token count (offset 12) is not mutated:
/// setting it makes the frame declare billions of tokens, which the
/// reader reserves room for before reading any, and that allocation aborts
/// the process rather than panicking. `tests/reservation.rs` measures it.
///
/// Native only: on `wasm32-unknown-unknown` a panic aborts, so
/// `catch_unwind` cannot turn one into a failure that names the mutation.
/// On wasm, `it_round_trips_a_generated_session` covers the same frames
/// unmutated.
#[cfg(not(target_arch = "wasm32"))]
#[dialog_common::test]
fn it_never_panics_on_a_mutated_frame() {
    use std::panic::{AssertUnwindSafe, catch_unwind};

    let (_, frames) = session_frames(0x9E37_79B9_7F4A_7C15);
    let mut panics = Vec::new();
    let mut attempt = |at: usize, label: String, mutated: Vec<u8>| {
        let outcome = catch_unwind(AssertUnwindSafe(|| {
            let mut dictionary = Dictionary::default();
            for earlier in &frames[..at] {
                dictionary.accept(earlier.as_slice().into()).unwrap();
            }
            if let Ok(answer) = dictionary.accept(mutated.into()) {
                read_everything(&dictionary, &answer);
            }
        }));
        if outcome.is_err() {
            panics.push(format!("frame {at}: {label}"));
        }
    };
    for (at, bytes) in frames.iter().enumerate() {
        for cut in 0..bytes.len() {
            attempt(at, format!("cut at {cut}"), bytes[..cut].to_vec());
        }
        for position in (0..bytes.len()).filter(|position| *position != 12) {
            for (name, mutate) in [
                ("xor 0x01", (|b: u8| b ^ 0x01) as fn(u8) -> u8),
                ("xor 0x80", |b| b ^ 0x80),
                ("set 0x00", |_| 0x00),
                ("set 0xFF", |_| 0xFF),
            ] {
                let mut mutated = bytes.clone();
                mutated[position] = mutate(mutated[position]);
                attempt(at, format!("{name} at {position}"), mutated);
            }
        }
    }
    assert!(
        panics.is_empty(),
        "accept or a read panicked on: {panics:#?}"
    );
}
