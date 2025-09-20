pub mod fact;
pub use crate::artifact::{Artifact, Attribute, Instruction};

pub enum Claim {
    Fact(fact::Claim),
}

impl From<Claim> for Vec<Instruction> {
    fn from(claim: Claim) -> Self {
        match claim {
            Claim::Fact(claim) => claim.into(),
        }
    }
}

impl From<Claim> for Instruction {
    fn from(claim: Claim) -> Self {
        match claim {
            Claim::Fact(claim) => claim.into(),
        }
    }
}
