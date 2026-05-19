//! Memory request translations.
//!
//! Path layout: `memory/{space}/{cell}` — mirrors
//! `dialog_storage::storage::provider::fs::memory` so the on-disk format
//! is byte-compatible with that provider's layout.
//!
//! `space` and `cell` are slash-separated paths in the general case
//! (e.g. `cell = "branch/main"` for a branch head). The native FS
//! provider treats them as URL fragments and resolves intermediate
//! `/` boundaries as nested directories; this translation does the
//! same by splitting on `/` so each `FsRequest::path` entry is a
//! single directory or file name. Without the split the handle's
//! containment check rejects the slash as an invalid segment.
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
            build_memory_path(capability.space(), capability.cell()),
        )
    }
}

impl From<&Capability<Publish>> for FsRequest {
    fn from(capability: &Capability<Publish>) -> Self {
        FsRequest::new(
            FsOp::Write,
            build_memory_path(capability.space(), capability.cell()),
        )
        .with_precondition(capability.when().into())
    }
}

impl From<&Capability<PublishAttenuation>> for FsRequest {
    fn from(capability: &Capability<PublishAttenuation>) -> Self {
        let publish = PublishAttenuation::of(capability);
        FsRequest::new(
            FsOp::Write,
            build_memory_path(&Space::of(capability).space, &Cell::of(capability).cell),
        )
        .with_precondition(publish.when.as_ref().into())
    }
}

impl From<&Capability<Retract>> for FsRequest {
    fn from(capability: &Capability<Retract>) -> Self {
        FsRequest::new(
            FsOp::Delete,
            build_memory_path(capability.space(), capability.cell()),
        )
        .with_precondition(capability.when().into())
    }
}

/// Build a memory path as `[MEMORY, …space_segments, …cell_segments]`,
/// splitting `space` and `cell` on `/` so the result is a flat list of
/// single-name segments suitable for the handle layer.
fn build_memory_path(space: impl std::fmt::Display, cell: impl std::fmt::Display) -> Vec<String> {
    let mut path = vec![MEMORY.to_string()];
    push_segments(&mut path, space);
    push_segments(&mut path, cell);
    path
}

fn push_segments(path: &mut Vec<String>, value: impl std::fmt::Display) {
    for segment in value.to_string().split('/').filter(|s| !s.is_empty()) {
        path.push(segment.to_string());
    }
}
