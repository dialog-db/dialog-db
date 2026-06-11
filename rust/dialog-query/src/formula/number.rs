//! Numeric scheme machinery for generic formulas.
//!
//! A generic formula like `Sum<N: Number>` is polymorphic over the
//! numeric value types. The pieces:
//!
//! - [`SchemeBound`] carries the lattice bound a scheme variable
//!   ranges over, as data the `#[derive(Formula)]` macro can emit
//!   into the formula's cells.
//! - [`Number`] is the bound trait for numeric schemes: conversion
//!   to and from [`Value`] plus *fallible* arithmetic. Fallibility
//!   is where the no-implicit-promotion semantics lives — an
//!   operation that cannot produce a value of the same type (a
//!   mixed-type pair, an integer overflow, an integer division by
//!   zero) yields `None`, and the formula yields no rows for that
//!   input: a non-match, never an error and never a coercion (see
//!   `notes/formula-schemes.md`).
//! - [`Numeric`] is the *dynamic* number: an enum over the numeric
//!   [`Value`] variants that itself implements [`Number`] by
//!   delegating same-variant operations and refusing mixed ones.
//!   The engine registers the canonical instantiation
//!   `Formula<Numeric>`, so dynamic evaluation is just another
//!   monomorphization; typed Rust callers use `u64`/`i64`/`f64`.

use crate::artifact::{ArtifactTypeError, Type as ValueType, Value};
use crate::type_system::{Primitive, Type as Kind};
use crate::types::{Scalar, TypeDescriptor, Typed};
use std::cmp::Ordering;
use std::fmt::{self, Display};

/// The lattice bound a scheme variable ranges over.
///
/// Every type usable as a formula scheme instantiation reports the
/// *bound of its scheme*, not its own type: all [`Number`]
/// implementors report [`Primitive::NUMERIC`]. This uniformity is
/// load-bearing — a generic formula's cells are built once in a
/// static shared across instantiations, so the bound must not vary
/// by instantiation.
pub trait SchemeBound {
    /// The set of primitive types the scheme variable ranges over.
    const BOUND: Primitive;
}

/// The bound trait for numeric formula schemes.
///
/// Arithmetic is *fallible*: `None` means "no value of this type is
/// the result" — a mixed-type pair (on [`Numeric`]), an integer
/// overflow, or an integer division/remainder by zero. A formula
/// computing over a `Number` turns `None` into zero output rows.
/// Floating point follows IEEE-754 and is total (division by zero
/// is infinity, not `None`).
pub trait Number:
    SchemeBound
    + Scalar
    + PartialEq
    + TryFrom<Value, Error = ArtifactTypeError>
    + dialog_common::ConditionalSync
{
    /// Addition; `None` on overflow or mixed types.
    fn add(self, other: Self) -> Option<Self>;
    /// Subtraction; `None` on overflow or mixed types.
    fn subtract(self, other: Self) -> Option<Self>;
    /// Multiplication; `None` on overflow or mixed types.
    fn multiply(self, other: Self) -> Option<Self>;
    /// Division; `None` on division by zero (integers), overflow, or
    /// mixed types.
    fn divide(self, other: Self) -> Option<Self>;
    /// Remainder; `None` on a zero divisor (integers), overflow, or
    /// mixed types.
    fn remainder(self, other: Self) -> Option<Self>;
}

macro_rules! impl_integer_number {
    ($ty:ty) => {
        impl SchemeBound for $ty {
            const BOUND: Primitive = Primitive::NUMERIC;
        }

        impl Number for $ty {
            fn add(self, other: Self) -> Option<Self> {
                self.checked_add(other)
            }
            fn subtract(self, other: Self) -> Option<Self> {
                self.checked_sub(other)
            }
            fn multiply(self, other: Self) -> Option<Self> {
                self.checked_mul(other)
            }
            fn divide(self, other: Self) -> Option<Self> {
                self.checked_div(other)
            }
            fn remainder(self, other: Self) -> Option<Self> {
                self.checked_rem(other)
            }
        }
    };
}

impl_integer_number!(u64);
impl_integer_number!(i64);
impl_integer_number!(u128);
impl_integer_number!(i128);

impl SchemeBound for f64 {
    const BOUND: Primitive = Primitive::NUMERIC;
}

impl Number for f64 {
    fn add(self, other: Self) -> Option<Self> {
        Some(self + other)
    }
    fn subtract(self, other: Self) -> Option<Self> {
        Some(self - other)
    }
    fn multiply(self, other: Self) -> Option<Self> {
        Some(self * other)
    }
    fn divide(self, other: Self) -> Option<Self> {
        Some(self / other)
    }
    fn remainder(self, other: Self) -> Option<Self> {
        Some(self % other)
    }
}

/// The dynamic number: a numeric [`Value`] with the non-numeric
/// variants excluded.
///
/// Implements [`Number`] by delegating same-variant operations and
/// refusing mixed-variant ones (`None`), which is the strict
/// no-promotion semantics: a row pairing an unsigned integer with a
/// float in one scheme variable is a non-match. The engine registers
/// generic formulas at their `Numeric` instantiation.
#[derive(Clone, Copy, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
pub enum Numeric {
    /// An unsigned 128-bit integer (the width [`Value`] stores).
    UnsignedInt(u128),
    /// A signed 128-bit integer (the width [`Value`] stores).
    SignedInt(i128),
    /// A 64-bit IEEE-754 float.
    Float(f64),
}

impl Numeric {
    fn binary(
        self,
        other: Self,
        u: impl FnOnce(u128, u128) -> Option<u128>,
        i: impl FnOnce(i128, i128) -> Option<i128>,
        f: impl FnOnce(f64, f64) -> Option<f64>,
    ) -> Option<Self> {
        match (self, other) {
            (Numeric::UnsignedInt(a), Numeric::UnsignedInt(b)) => u(a, b).map(Numeric::UnsignedInt),
            (Numeric::SignedInt(a), Numeric::SignedInt(b)) => i(a, b).map(Numeric::SignedInt),
            (Numeric::Float(a), Numeric::Float(b)) => f(a, b).map(Numeric::Float),
            // Mixed variants: no value of a single type is the
            // result — strict, no promotion.
            _ => None,
        }
    }
}

impl Numeric {
    /// Convert into the given numeric type *losslessly*: `None` when
    /// the value is not exactly representable there. The literal
    /// adaptation primitive: a polymorphic literal instantiates to a
    /// row's type through this, so a literal never changes value and
    /// data is never coerced.
    ///
    /// Float literals stay float (the author wrote float syntax);
    /// integer literals adapt wherever they fit exactly, including
    /// to float below 2^53.
    pub fn instantiate(self, target: ValueType) -> Option<Numeric> {
        match (self, target) {
            (n @ Numeric::UnsignedInt(_), ValueType::UnsignedInt) => Some(n),
            (n @ Numeric::SignedInt(_), ValueType::SignedInt) => Some(n),
            (n @ Numeric::Float(_), ValueType::Float) => Some(n),
            (Numeric::UnsignedInt(v), ValueType::SignedInt) => {
                i128::try_from(v).ok().map(Numeric::SignedInt)
            }
            (Numeric::SignedInt(v), ValueType::UnsignedInt) => {
                u128::try_from(v).ok().map(Numeric::UnsignedInt)
            }
            (Numeric::UnsignedInt(v), ValueType::Float) => {
                let f = v as f64;
                (f.is_finite() && f as u128 == v && f >= 0.0).then_some(Numeric::Float(f))
            }
            (Numeric::SignedInt(v), ValueType::Float) => {
                let f = v as f64;
                (f.is_finite() && f as i128 == v).then_some(Numeric::Float(f))
            }
            _ => None,
        }
    }

    /// The numeric [`ValueType`] of this variant.
    pub fn value_type(&self) -> ValueType {
        match self {
            Numeric::UnsignedInt(_) => ValueType::UnsignedInt,
            Numeric::SignedInt(_) => ValueType::SignedInt,
            Numeric::Float(_) => ValueType::Float,
        }
    }

    /// Same-variant ordering: `None` for mixed variants (the strict
    /// no-promotion semantics, exactly like the arithmetic) and for
    /// incomparable floats (NaN).
    pub fn compare(self, other: Self) -> Option<Ordering> {
        match (self, other) {
            (Numeric::UnsignedInt(a), Numeric::UnsignedInt(b)) => Some(a.cmp(&b)),
            (Numeric::SignedInt(a), Numeric::SignedInt(b)) => Some(a.cmp(&b)),
            (Numeric::Float(a), Numeric::Float(b)) => a.partial_cmp(&b),
            _ => None,
        }
    }

    /// The set of numeric types this value can instantiate to
    /// losslessly — the kind a polymorphic literal contributes to
    /// inference (so `1` does not pin a scheme, while `1.5` pins it
    /// to Float).
    pub fn admissible(&self) -> Primitive {
        let mut set = Primitive::EMPTY;
        for target in [
            ValueType::UnsignedInt,
            ValueType::SignedInt,
            ValueType::Float,
        ] {
            if self.instantiate(target).is_some() {
                set = set.union(Primitive::singleton(target));
            }
        }
        set
    }
}

impl TryFrom<Value> for Numeric {
    type Error = ArtifactTypeError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::UnsignedInt(v) => Ok(Numeric::UnsignedInt(v)),
            Value::SignedInt(v) => Ok(Numeric::SignedInt(v)),
            Value::Float(v) => Ok(Numeric::Float(v)),
            other => Err(ArtifactTypeError::TypeMismatch(
                ValueType::Float,
                other.data_type(),
            )),
        }
    }
}

impl From<Numeric> for Value {
    fn from(n: Numeric) -> Self {
        match n {
            Numeric::UnsignedInt(v) => Value::UnsignedInt(v),
            Numeric::SignedInt(v) => Value::SignedInt(v),
            Numeric::Float(v) => Value::Float(v),
        }
    }
}

impl Display for Numeric {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Numeric::UnsignedInt(v) => write!(f, "{v}"),
            Numeric::SignedInt(v) => write!(f, "{v}"),
            Numeric::Float(v) => write!(f, "{v}"),
        }
    }
}

/// Descriptor for the dynamic [`Numeric`] type: no single storage
/// tag, kind is the NUMERIC set.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct NumericDescriptor;

impl TypeDescriptor for NumericDescriptor {
    const TYPE: Option<ValueType> = None;

    fn kind(&self) -> Option<Kind> {
        Some(Kind::primitive_set(Primitive::NUMERIC))
    }
}

impl Typed for Numeric {
    type Descriptor = NumericDescriptor;
}

impl Scalar for Numeric {}

impl SchemeBound for Numeric {
    const BOUND: Primitive = Primitive::NUMERIC;
}

impl Number for Numeric {
    fn add(self, other: Self) -> Option<Self> {
        self.binary(other, u128::checked_add, i128::checked_add, |a, b| {
            Some(a + b)
        })
    }
    fn subtract(self, other: Self) -> Option<Self> {
        self.binary(other, u128::checked_sub, i128::checked_sub, |a, b| {
            Some(a - b)
        })
    }
    fn multiply(self, other: Self) -> Option<Self> {
        self.binary(other, u128::checked_mul, i128::checked_mul, |a, b| {
            Some(a * b)
        })
    }
    fn divide(self, other: Self) -> Option<Self> {
        self.binary(other, u128::checked_div, i128::checked_div, |a, b| {
            Some(a / b)
        })
    }
    fn remainder(self, other: Self) -> Option<Self> {
        self.binary(other, u128::checked_rem, i128::checked_rem, |a, b| {
            Some(a % b)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[dialog_common::test]
    fn it_computes_within_one_variant() {
        let a = Numeric::UnsignedInt(2);
        let b = Numeric::UnsignedInt(3);
        assert_eq!(a.add(b), Some(Numeric::UnsignedInt(5)));

        let a = Numeric::Float(2.0);
        let b = Numeric::Float(1.5);
        assert_eq!(a.add(b), Some(Numeric::Float(3.5)));
    }

    /// Mixed variants are a non-match, never a promotion: dialog's
    /// value lattice has no type that holds both ranges losslessly.
    #[dialog_common::test]
    fn it_refuses_mixed_variants() {
        let a = Numeric::UnsignedInt(2);
        let b = Numeric::Float(3.5);
        assert_eq!(a.add(b), None);
        assert_eq!(b.multiply(a), None);
    }

    /// Integer edge cases produce no value instead of wrapping or
    /// trapping: overflow and zero division are non-matches.
    #[dialog_common::test]
    fn it_refuses_overflow_and_zero_division() {
        assert_eq!(u64::MAX.add(1), None);
        assert_eq!(0u64.subtract(1), None);
        assert_eq!(1u64.divide(0), None);
        assert_eq!(
            Numeric::SignedInt(i128::MIN).divide(Numeric::SignedInt(-1)),
            None
        );
        // IEEE float is total.
        assert_eq!(1f64.divide(0.0), Some(f64::INFINITY));
    }

    /// The dynamic descriptor reports the NUMERIC set as its kind.
    #[dialog_common::test]
    fn it_reports_the_numeric_kind() {
        let kind = NumericDescriptor.kind().expect("kind");
        assert_eq!(kind.primitive_part(), Primitive::NUMERIC);
    }

    /// Lossless instantiation: integer literals adapt wherever they
    /// fit exactly; floats stay float; nothing ever changes value.
    #[dialog_common::test]
    fn it_instantiates_losslessly() {
        let one = Numeric::UnsignedInt(1);
        assert_eq!(
            one.instantiate(ValueType::SignedInt),
            Some(Numeric::SignedInt(1))
        );
        assert_eq!(one.instantiate(ValueType::Float), Some(Numeric::Float(1.0)));

        // Above 2^53 a float is no longer exact.
        let big = Numeric::UnsignedInt((1u128 << 53) + 1);
        assert_eq!(big.instantiate(ValueType::Float), None);
        assert!(big.instantiate(ValueType::SignedInt).is_some());

        // Negatives have no unsigned form.
        let neg = Numeric::SignedInt(-1);
        assert_eq!(neg.instantiate(ValueType::UnsignedInt), None);
        assert_eq!(
            neg.instantiate(ValueType::Float),
            Some(Numeric::Float(-1.0))
        );

        // Float literals stay float.
        let frac = Numeric::Float(1.5);
        assert_eq!(frac.instantiate(ValueType::UnsignedInt), None);
        assert_eq!(frac.instantiate(ValueType::SignedInt), None);
    }

    /// The admissible set is what a polymorphic literal contributes
    /// to inference.
    #[dialog_common::test]
    fn it_reports_admissible_sets() {
        assert_eq!(Numeric::UnsignedInt(1).admissible(), Primitive::NUMERIC);
        assert_eq!(
            Numeric::Float(1.5).admissible(),
            Primitive::singleton(ValueType::Float)
        );
        let neg = Numeric::SignedInt(-1).admissible();
        assert!(!neg.contains(ValueType::UnsignedInt));
        assert!(neg.contains(ValueType::SignedInt));
        assert!(neg.contains(ValueType::Float));
    }

    /// Round trip through Value preserves the variant.
    #[dialog_common::test]
    fn it_round_trips_values() -> anyhow::Result<()> {
        for v in [
            Value::UnsignedInt(7),
            Value::SignedInt(-7),
            Value::Float(0.5),
        ] {
            let n = Numeric::try_from(v.clone())?;
            assert_eq!(Value::from(n), v);
        }
        assert!(Numeric::try_from(Value::String("x".into())).is_err());
        Ok(())
    }
}
