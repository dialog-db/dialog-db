use std::fmt::Display;

use base58::ToBase58;

use crate::make_seed;

#[derive(Clone)]
pub struct Scope(String);

impl Default for Scope {
    fn default() -> Self {
        Self::new()
    }
}

impl Scope {
    pub fn new() -> Self {
        Scope(make_seed().to_base58())
    }
}

impl Display for Scope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
