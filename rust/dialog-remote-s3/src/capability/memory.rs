//! Memory S3 request implementations.
//!
//! This module provides `Access` implementations for memory (CAS cell)
//! capabilities, enabling them to be translated into presigned S3 URLs.

use super::{Access, Precondition};
use crate::Checksum;
use dialog_capability::{Capability, Policy};
use dialog_effects::memory::{self, Cell, Space};

impl Access for Capability<memory::Resolve> {
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

impl Access for Capability<memory::Publish> {
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

impl Access for Capability<memory::PublishClaim> {
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

impl Access for Capability<memory::Retract> {
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

impl Access for Capability<memory::RetractClaim> {
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
