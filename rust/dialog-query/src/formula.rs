//! Built-in formulas for common data transformations
//!
//! - Mathematical operations (sum, difference, product, quotient, modulo)
//! - String operations (concatenate, length, uppercase, lowercase)
//! - Type conversions (to_string, parse_number)
//! - Boolean logic (and, or, not)

pub mod conversions;
pub mod logic;
pub mod math;
pub mod string;

pub use conversions::{ParseNumber, ToString};
pub use logic::{And, Not, Or};
pub use math::{Difference, Modulo, Product, Quotient, Sum};
pub use string::{Concatenate, Is, Length, Lowercase, Uppercase};
