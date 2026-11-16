//! How a row holds a binding: a value handed to it, or a component of a
//! row a scan read, materialized the first time something reads it as a
//! [`Value`].

use std::borrow::Cow;
use std::cmp::Ordering;
use std::sync::{Arc, OnceLock};

use dialog_corpus::{Cause, CheckedRow, ValueView};

use super::Binding;
use crate::Claim;
use crate::attribute::Relation;
use crate::corpus::Value;

/// Which component of a scanned row a slot binds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Part {
    /// The attribute, as a symbol.
    The,
    /// The entity.
    Of,
    /// The value.
    Is,
}

/// A scanned row as every slot and citation that binds it shares it: the
/// checked row, and each component materialized at most once, however
/// many rows the slots are copied into.
#[derive(Debug)]
pub(crate) struct Scanned {
    row: CheckedRow,
    /// Boxed: a scan seldom binds its attribute, and an inline cell costs
    /// every row its size.
    the: OnceLock<Box<Binding>>,
    of: OnceLock<Binding>,
    is: OnceLock<Binding>,
    claim: OnceLock<Arc<Claim>>,
}

impl Scanned {
    /// The scanned row's entity, as the key stores it.
    pub(crate) fn of_str(&self) -> &str {
        self.row.of_str()
    }

    /// Shares `row`, its value read as `viewed` when the scan's kind reads
    /// the stored value as another (a natural as a float).
    pub(crate) fn new(row: CheckedRow, viewed: Option<Value>) -> Arc<Self> {
        let is = OnceLock::new();
        if let Some(value) = viewed {
            let _ = is.set(Binding::Present(value));
        }
        Arc::new(Scanned {
            row,
            the: OnceLock::new(),
            of: OnceLock::new(),
            is,
            claim: OnceLock::new(),
        })
    }

    /// `part`'s binding, materialized on first read.
    #[cfg_attr(feature = "trace-hydration", track_caller)]
    #[inline(never)]
    pub(crate) fn binding(&self, part: Part) -> &Binding {
        #[cfg(feature = "trace-hydration")]
        let site = std::panic::Location::caller();
        let make = || {
            #[cfg(feature = "trace-hydration")]
            super::hydration::record(
                site,
                match part {
                    Part::The => "the",
                    Part::Of => "of",
                    Part::Is => "is",
                },
            );
            Binding::Present(materialize(&self.row, part))
        };
        match part {
            Part::The => self.the.get_or_init(|| Box::new(make())),
            Part::Of => self.of.get_or_init(make),
            Part::Is => self.is.get_or_init(make),
        }
    }
}

/// A binding as a row stores it.
///
/// A scan binds the components of the row it read as [`Slot::Scanned`]:
/// the row is shared by every slot it binds and by the claim it cites,
/// and it holds the key arena's handle, so binding costs no copy. The
/// value is materialized once, into `cell`, the first time something
/// reads it as a [`Binding`]; a reader that only hands it on (an answer's
/// encoder, a consistency check against another scanned row) reads it in
/// place and never pays for it.
#[derive(Clone, Debug)]
pub(crate) enum Slot {
    /// A binding handed to the row.
    Bound(Binding),
    /// A component of a scanned row, checked when it was read
    /// ([`dialog_corpus::FactView::checked`]), so it materializes.
    Scanned { row: Arc<Scanned>, part: Part },
}

impl From<Binding> for Slot {
    fn from(binding: Binding) -> Self {
        Slot::Bound(binding)
    }
}

impl Slot {
    /// Binds `part` of `row`.
    pub(crate) fn scanned(row: &Arc<Scanned>, part: Part) -> Self {
        Slot::Scanned {
            row: Arc::clone(row),
            part,
        }
    }

    /// The binding, materialized on first read. Inlined: most reads are
    /// of a value already bound, and pay only the match.
    #[cfg_attr(feature = "trace-hydration", track_caller)]
    #[inline]
    pub(crate) fn binding(&self) -> &Binding {
        match self {
            Slot::Bound(binding) => binding,
            Slot::Scanned { row, part } => row.binding(*part),
        }
    }

    /// Whether this slot is `Present`, without materializing it.
    pub(crate) fn is_present(&self) -> bool {
        match self {
            Slot::Bound(binding) => binding.is_present(),
            Slot::Scanned { .. } => true,
        }
    }

    /// The value read in place: `None` for [`Binding::Absent`]. Borrows
    /// from the scanned row's key, or from the value it materialized into
    /// if something already did.
    pub(crate) fn view(&self) -> Option<ValueView<'_>> {
        match self {
            Slot::Bound(Binding::Present(value)) => Some(ValueView::from(value)),
            Slot::Bound(Binding::Absent) => None,
            // A value read through a view is what the cell holds.
            Slot::Scanned {
                row,
                part: Part::Is,
            } if row.is.get().is_some() => row
                .is
                .get()
                .and_then(Binding::as_value)
                .map(ValueView::from),
            Slot::Scanned { row, part } => Some(read(&row.row, *part)),
        }
    }

    /// Whether two slots hold the same value, when their bytes settle it
    /// without materializing either: `None` when the values have to be
    /// compared as values (a number read through another number type's
    /// lens, say).
    pub(crate) fn agrees(&self, other: &Slot) -> Option<bool> {
        match (self, other) {
            (Slot::Bound(Binding::Absent), Slot::Bound(Binding::Absent)) => Some(true),
            (Slot::Bound(Binding::Absent), _) | (_, Slot::Bound(Binding::Absent)) => Some(false),
            (Slot::Bound(_), Slot::Bound(_)) => None,
            _ => {
                let (left, right) = (self.view()?, other.view()?);
                match (&left, &right) {
                    (ValueView::Entity(a), ValueView::Entity(b))
                    | (ValueView::String(a), ValueView::String(b))
                    | (ValueView::Symbol(a), ValueView::Symbol(b)) => Some(a == b),
                    (ValueView::Bytes(a), ValueView::Bytes(b))
                    | (ValueView::Record(a), ValueView::Record(b)) => Some(a == b),
                    (ValueView::Boolean(a), ValueView::Boolean(b)) => Some(a == b),
                    // Numbers and mixed types agree as values do.
                    _ => None,
                }
            }
        }
    }
}

/// `part` of `row`, read in place.
fn read(row: &CheckedRow, part: Part) -> ValueView<'_> {
    match part {
        Part::The => ValueView::Symbol(row.the_str().into()),
        Part::Of => ValueView::Entity(row.of_str().into()),
        Part::Is => row.value_view(),
    }
}

/// `part` of `row`, materialized.
fn materialize(row: &CheckedRow, part: Part) -> Value {
    match part {
        Part::The => Value::from(row.relation()),
        Part::Of => Value::Entity(row.entity()),
        Part::Is => row.value(),
    }
}

/// A claim as a row cites it: one handed to it, or the scanned row it was
/// read from, made into a [`Claim`] the first time something proves it.
#[derive(Clone, Debug)]
pub(crate) enum Cited {
    /// A claim handed to the row.
    Claim(Arc<Claim>),
    /// A scanned row, checked when it was cited.
    Scanned(Arc<Scanned>),
}

impl Cited {
    /// Cites `row`.
    pub(crate) fn scanned(row: &Arc<Scanned>) -> Self {
        Cited::Scanned(Arc::clone(row))
    }

    /// The claim, made on first read.
    #[cfg_attr(feature = "trace-hydration", track_caller)]
    pub(crate) fn claim(&self) -> &Arc<Claim> {
        #[cfg(feature = "trace-hydration")]
        let site = std::panic::Location::caller();
        match self {
            Cited::Claim(claim) => claim,
            Cited::Scanned(scanned) => scanned.claim.get_or_init(|| {
                #[cfg(feature = "trace-hydration")]
                super::hydration::record(site, "claim");
                let row = &scanned.row;
                Arc::new(Claim {
                    the: Relation::from(row.relation()),
                    of: row.entity(),
                    is: match scanned.is.get() {
                        Some(Binding::Present(value)) => value.clone(),
                        _ => row.value(),
                    },
                    cause: row.cause().cloned().unwrap_or(Cause([0; 32])),
                })
            }),
        }
    }
}

impl PartialEq for Cited {
    fn eq(&self, other: &Self) -> bool {
        self.claim() == other.claim()
    }
}

/// A row's join value, ordered as the index orders it (the
/// order-preserving encoding [`dialog_corpus::encode_value_owned`]
/// writes), without materializing or encoding a scanned one.
///
/// A scanned entity, text or symbol that borrows from its key is
/// escape-free, so it holds no zero byte, and its encoding is its bytes
/// and the terminator: comparing the bytes compares the encodings.
#[derive(Clone, Debug)]
pub struct JoinKey(Key);

#[derive(Clone, Debug)]
enum Key {
    /// A scanned component that borrows from its key, read in place.
    Scanned(Arc<Scanned>, Part),
    /// A value's encoding.
    Encoded(Vec<u8>),
}

/// The text a borrowed view holds.
fn borrowed<'a>(view: &ValueView<'a>) -> Option<&'a str> {
    match view {
        ValueView::Entity(Cow::Borrowed(text))
        | ValueView::String(Cow::Borrowed(text))
        | ValueView::Symbol(Cow::Borrowed(text)) => Some(text),
        _ => None,
    }
}

impl JoinKey {
    pub(crate) fn of(slot: &Slot) -> Option<JoinKey> {
        if let Slot::Scanned { row, part } = slot
            && borrowed(&read(&row.row, *part)).is_some()
        {
            return Some(JoinKey(Key::Scanned(Arc::clone(row), *part)));
        }
        let value = slot.binding().as_value()?;
        Some(JoinKey(Key::Encoded(dialog_corpus::encode_value_owned(
            value,
        ))))
    }

    /// The bytes of a scanned key, or the encoding of any other.
    fn bytes(&self) -> (&[u8], bool) {
        match &self.0 {
            Key::Scanned(row, part) => (
                borrowed(&read(&row.row, *part))
                    .expect("a scanned join key borrows")
                    .as_bytes(),
                true,
            ),
            Key::Encoded(bytes) => (bytes, false),
        }
    }
}

impl Ord for JoinKey {
    fn cmp(&self, other: &Self) -> Ordering {
        let ((left, left_raw), (right, right_raw)) = (self.bytes(), other.bytes());
        // A raw key's encoding is its bytes and a terminator.
        let terminated = |bytes: &'_ [u8], raw: bool| {
            bytes
                .iter()
                .copied()
                .chain(raw.then_some(0u8))
                .collect::<Vec<u8>>()
        };
        if left_raw == right_raw {
            left.cmp(right)
        } else {
            terminated(left, left_raw).cmp(&terminated(right, right_raw))
        }
    }
}

impl PartialOrd for JoinKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for JoinKey {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}

impl Eq for JoinKey {}
