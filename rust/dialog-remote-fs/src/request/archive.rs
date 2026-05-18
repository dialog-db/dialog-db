//! Archive request translations.
//!
//! Path layout: `archive/{catalog}/{base58(digest)}` — mirrors
//! `dialog_storage::storage::provider::fs::archive` so the on-disk
//! format is byte-compatible with that provider's layout.

use super::{FsOp, FsRequest};
use base58::ToBase58;
use dialog_capability::{Capability, Policy};
use dialog_effects::archive::prelude::{GetExt, PutExt};
use dialog_effects::archive::{Catalog, Get, Put, PutAttenuation};

const ARCHIVE: &str = "archive";

impl From<&Capability<Get>> for FsRequest {
    fn from(capability: &Capability<Get>) -> Self {
        FsRequest::new(
            FsOp::Read,
            vec![
                ARCHIVE.to_string(),
                capability.catalog().to_string(),
                capability.digest().as_bytes().to_base58(),
            ],
        )
    }
}

impl From<&Capability<Put>> for FsRequest {
    fn from(capability: &Capability<Put>) -> Self {
        FsRequest::new(
            FsOp::Write,
            vec![
                ARCHIVE.to_string(),
                capability.catalog().to_string(),
                capability.digest().as_bytes().to_base58(),
            ],
        )
    }
}

impl From<&Capability<PutAttenuation>> for FsRequest {
    fn from(capability: &Capability<PutAttenuation>) -> Self {
        let put = PutAttenuation::of(capability);
        FsRequest::new(
            FsOp::Write,
            vec![
                ARCHIVE.to_string(),
                Catalog::of(capability).catalog.to_string(),
                put.digest.as_bytes().to_base58(),
            ],
        )
    }
}
