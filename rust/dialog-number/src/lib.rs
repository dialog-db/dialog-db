#![warn(missing_docs)]
#![warn(clippy::absolute_paths)]
#![warn(clippy::default_trait_access)]
#![warn(clippy::fallible_impl_from)]
#![warn(clippy::panicking_unwrap)]
#![deny(clippy::partial_pub_fields)]
#![deny(clippy::unnecessary_self_imports)]
#![cfg_attr(not(test), deny(clippy::panic))]

//! Dialog's numbers: one exact decimal domain with one canonical,
//! order-preserving encoding.
//!
//! A number is a decimal, `±0.d₁d₂…dₙ × 100^e` with base-100 digits, or zero,
//! an infinity or NaN. Every way a number reaches dialog lands in this one
//! domain:
//!
//! - a natural or an integer is exact at any size;
//! - a float is the shortest decimal that reads back as the same float, so a
//!   float and the decimal it prints as are one number (`0.1` is `0.1`), and
//!   float → number → float loses nothing;
//! - equal numbers are one value whatever held them: `3`, `3.0` and the float
//!   `3.0` are one number, `-0.0` is `0`, and NaN is one value.
//!
//! The encoding ([`encode`]) is SQLite4's numeric key encoding: a class byte
//! carrying the sign and the size of the exponent, the exponent itself where
//! the class cannot hold it, and the base-100 digits, so that comparing
//! encodings byte by byte orders numbers by value. It is canonical, so it
//! serves as both an index key and a number's identity content.
//!
//! A float's own binary value is not kept: the float `1e23`, exactly
//! `99999999999999991611392`, is the number `10²³`. Results computed in
//! floats are approximations of the numbers they print as.
//!
//! ```no_run
//! use dialog_number::{Number, encode_f64, encode_i128};
//!
//! let mut three = Vec::new();
//! encode_i128(3, &mut three);
//! let mut float = Vec::new();
//! encode_f64(3.0, &mut float);
//! assert_eq!(three, float);
//!
//! let number = Number::decode(&three).unwrap();
//! assert_eq!(number.to_i128(), Some(3));
//! ```

mod convert;
mod key;
mod number;
mod varint;

pub use key::{encode, encode_f64, encode_i128, encode_u128};
pub use number::{DecodeError, Number, decode_f64, decode_i128, encoded_len};
