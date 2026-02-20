//! Built-in formulas for common data transformations
//!
//! - Mathematical operations (sum, difference, product, quotient, modulo)
//! - String operations (concatenate, length, uppercase, lowercase)
//! - Type conversions (to_string, parse_number)
//! - Boolean logic (and, or, not)

/// Type conversion formulas (to_string, parse_number)
pub mod conversions;
/// Boolean logic formulas (and, or, not)
pub mod logic;
/// Mathematical operation formulas (sum, difference, product, quotient, modulo)
pub mod math;
/// String manipulation formulas (concatenate, length, uppercase, lowercase, like)
pub mod string;

pub use conversions::{ParseNumber, ToString};
pub use logic::{And, Not, Or};
pub use math::{Difference, Modulo, Product, Quotient, Sum};
pub use string::{Concatenate, Length, Like, Lowercase, Uppercase};
