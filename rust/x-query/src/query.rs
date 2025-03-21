mod variable;
pub use variable::*;

mod pattern;
pub use pattern::*;

mod frame;
pub use frame::*;

mod r#match;
pub use r#match::*;

mod scope;
pub use scope::*;

pub mod pull;
pub mod push;

use crate::{Value, XQueryError};
use x_common::ConditionalSend;

pub trait Query: Clone + ConditionalSend {
    fn scope(&self, scope: &Scope) -> Self;
    fn substitute(&self, variable: &Variable, constant: &Value) -> Result<Self, XQueryError>;
}
