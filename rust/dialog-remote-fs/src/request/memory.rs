//! Memory request translations.
//!
//! Path layout: `memory/{space}/{cell}` — mirrors
//! `dialog_storage::storage::provider::fs::memory` so the on-disk format
//! is byte-compatible with that provider's layout.
//!
//! Memory cells carry CAS preconditions: `Publish` and `Retract` require
//! the current edition to match a captured version (or the cell to be
//! absent on initial publish).

use super::{FsOp, FsRequest, Precondition};
use dialog_capability::{Capability, Policy};
use dialog_effects::memory::prelude::{PublishExt, ResolveExt, RetractExt};
use dialog_effects::memory::{Cell, Publish, PublishAttenuation, Resolve, Retract, Space, Version};

const MEMORY: &str = "memory";

impl From<Option<&Version>> for Precondition {
    fn from(version: Option<&Version>) -> Self {
        match version {
            Some(v) => Precondition::IfMatch(v.to_string()),
            None => Precondition::IfNoneMatch,
        }
    }
}

impl From<&Version> for Precondition {
    fn from(version: &Version) -> Self {
        Precondition::IfMatch(version.to_string())
    }
}

impl From<&Capability<Resolve>> for FsRequest {
    fn from(capability: &Capability<Resolve>) -> Self {
        FsRequest::new(
            FsOp::Read,
            vec![
                MEMORY.to_string(),
                capability.space().to_string(),
                capability.cell().to_string(),
            ],
        )
    }
}

impl From<&Capability<Publish>> for FsRequest {
    fn from(capability: &Capability<Publish>) -> Self {
        FsRequest::new(
            FsOp::Write,
            vec![
                MEMORY.to_string(),
                capability.space().to_string(),
                capability.cell().to_string(),
            ],
        )
        .with_precondition(capability.when().into())
    }
}

impl From<&Capability<PublishAttenuation>> for FsRequest {
    fn from(capability: &Capability<PublishAttenuation>) -> Self {
        let publish = PublishAttenuation::of(capability);
        FsRequest::new(
            FsOp::Write,
            vec![
                MEMORY.to_string(),
                Space::of(capability).space.to_string(),
                Cell::of(capability).cell.to_string(),
            ],
        )
        .with_precondition(publish.when.as_ref().into())
    }
}

impl From<&Capability<Retract>> for FsRequest {
    fn from(capability: &Capability<Retract>) -> Self {
        FsRequest::new(
            FsOp::Delete,
            vec![
                MEMORY.to_string(),
                capability.space().to_string(),
                capability.cell().to_string(),
            ],
        )
        .with_precondition(capability.when().into())
    }
}
