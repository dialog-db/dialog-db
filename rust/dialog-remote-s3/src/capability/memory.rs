//! Memory S3 request implementations.
//!
//! This module provides `S3Request` implementations for memory (CAS cell)
//! capabilities, enabling them to be translated into presigned S3 URLs.

use super::{Precondition, S3Request};
use crate::Checksum;
use dialog_capability::{Capability, Policy};
use dialog_effects::memory::{self, Cell, Space};

impl S3Request for Capability<memory::Resolve> {
    fn method(&self) -> &'static str {
        "GET"
    }
    fn path(&self) -> String {
        format!(
            "{}/{}/{}",
            self.subject(),
            &Space::of(self).space,
            &Cell::of(self).cell
        )
    }
}

impl S3Request for Capability<memory::Publish> {
    fn method(&self) -> &'static str {
        "PUT"
    }
    fn path(&self) -> String {
        format!(
            "{}/{}/{}",
            self.subject(),
            &Space::of(self).space,
            &Cell::of(self).cell
        )
    }
    fn checksum(&self) -> Option<Checksum> {
        Some(crate::Hasher::Sha256.checksum(&memory::Publish::of(self).content))
    }
    fn precondition(&self) -> Precondition {
        match &memory::Publish::of(self).when {
            Some(edition) => Precondition::IfMatch(String::from_utf8_lossy(edition).to_string()),
            None => Precondition::IfNoneMatch,
        }
    }
}

impl S3Request for Capability<memory::PublishClaim> {
    fn method(&self) -> &'static str {
        "PUT"
    }
    fn path(&self) -> String {
        format!(
            "{}/{}/{}",
            self.subject(),
            &Space::of(self).space,
            &Cell::of(self).cell
        )
    }
    fn checksum(&self) -> Option<Checksum> {
        Some(memory::PublishClaim::of(self).checksum.clone())
    }
    fn precondition(&self) -> Precondition {
        match &memory::PublishClaim::of(self).when {
            Some(edition) => Precondition::IfMatch(edition.clone()),
            None => Precondition::IfNoneMatch,
        }
    }
}

impl S3Request for Capability<memory::Retract> {
    fn method(&self) -> &'static str {
        "DELETE"
    }
    fn path(&self) -> String {
        format!(
            "{}/{}/{}",
            self.subject(),
            &Space::of(self).space,
            &Cell::of(self).cell
        )
    }
    fn precondition(&self) -> Precondition {
        Precondition::IfMatch(String::from_utf8_lossy(&memory::Retract::of(self).when).to_string())
    }
}

impl S3Request for Capability<memory::RetractClaim> {
    fn method(&self) -> &'static str {
        "DELETE"
    }
    fn path(&self) -> String {
        format!(
            "{}/{}/{}",
            self.subject(),
            &Space::of(self).space,
            &Cell::of(self).cell
        )
    }
    fn precondition(&self) -> Precondition {
        Precondition::IfMatch(memory::RetractClaim::of(self).when.clone())
    }
}
