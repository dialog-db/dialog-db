pub mod fact;
pub use crate::artifact::{Artifact, Attribute, Instruction};
use futures_util::Stream;
use std::pin::Pin;
use std::task::{Context, Poll};

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

/// Implement IntoIterator for Claim to iterate over its instructions
impl IntoIterator for Claim {
    type Item = Instruction;
    type IntoIter = std::vec::IntoIter<Instruction>;

    fn into_iter(self) -> Self::IntoIter {
        let instructions: Vec<Instruction> = self.into();
        instructions.into_iter()
    }
}

/// A newtype wrapper around Vec<Claim> that implements Stream<Item = Instruction>
pub struct Claims {
    inner: std::vec::IntoIter<Instruction>,
}

impl From<Vec<Claim>> for Claims {
    fn from(claims: Vec<Claim>) -> Self {
        let instructions: Vec<Instruction> = claims
            .into_iter()
            .flat_map(|claim| claim.into_iter())
            .collect();
        Claims {
            inner: instructions.into_iter(),
        }
    }
}

impl From<Claim> for Claims {
    fn from(claim: Claim) -> Self {
        let instructions: Vec<Instruction> = claim.into_iter().collect();
        Claims {
            inner: instructions.into_iter(),
        }
    }
}

/// Implement Stream for Claims to stream instructions from all claims
impl Stream for Claims {
    type Item = Instruction;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(self.inner.next())
    }
}

/// Implement IntoIterator for Claims to iterate over instructions
impl IntoIterator for Claims {
    type Item = Instruction;
    type IntoIter = std::vec::IntoIter<Instruction>;

    fn into_iter(self) -> Self::IntoIter {
        self.inner
    }
}

// Claims automatically implements ConditionalSend since std::vec::IntoIter<Instruction> is Send
