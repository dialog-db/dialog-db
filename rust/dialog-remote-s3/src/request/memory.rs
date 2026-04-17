//! Memory S3 request implementations.
//!
//! This module provides `From<&Capability<Fx>> for S3Request` impls for
//! memory (CAS cell) capabilities, enabling them to be translated into
//! concrete S3 request descriptions.

use super::{Precondition, S3Request};
use dialog_capability::{Capability, Policy};
use dialog_common::Hasher;
use dialog_effects::memory::{
    Cell, Publish, PublishCapability, PublishClaim, Resolve, ResolveCapability, Retract,
    RetractCapability, Space, Version,
};

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

impl From<&Capability<Resolve>> for S3Request {
    fn from(capability: &Capability<Resolve>) -> Self {
        S3Request {
            method: "GET".to_string(),
            path: format!(
                "{}/{}/{}",
                capability.subject(),
                capability.space(),
                capability.cell()
            ),
            ..S3Request::default()
        }
    }
}

impl From<&Capability<Publish>> for S3Request {
    fn from(capability: &Capability<Publish>) -> Self {
        S3Request {
            method: "PUT".to_string(),
            path: format!(
                "{}/{}/{}",
                capability.subject(),
                capability.space(),
                capability.cell()
            ),
            checksum: Some(Hasher::Sha256.checksum(capability.content())),
            precondition: capability.when().into(),
            ..S3Request::default()
        }
    }
}

impl From<&Capability<PublishClaim>> for S3Request {
    fn from(capability: &Capability<PublishClaim>) -> Self {
        let publish = PublishClaim::of(capability);
        S3Request {
            method: "PUT".to_string(),
            path: format!(
                "{}/{}/{}",
                capability.subject(),
                &Space::of(capability).space,
                &Cell::of(capability).cell
            ),
            checksum: Some(publish.checksum.clone()),
            precondition: publish.when.as_ref().into(),
            ..S3Request::default()
        }
    }
}

impl From<&Capability<Retract>> for S3Request {
    fn from(capability: &Capability<Retract>) -> Self {
        S3Request {
            method: "DELETE".to_string(),
            path: format!(
                "{}/{}/{}",
                capability.subject(),
                capability.space(),
                capability.cell()
            ),
            precondition: capability.when().into(),
            ..S3Request::default()
        }
    }
}
