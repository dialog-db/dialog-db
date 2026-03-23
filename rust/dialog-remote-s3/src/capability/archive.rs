//! Archive S3 request implementations.
//!
//! This module provides `S3Request` implementations for archive
//! (content-addressed) capabilities, enabling them to be translated into
//! presigned S3 URLs.

use super::S3Request;
use crate::Checksum;
use base58::ToBase58;
use dialog_capability::{Capability, Policy};
use dialog_effects::archive::{self, Catalog};

impl S3Request for Capability<archive::Get> {
    fn method(&self) -> &'static str {
        "GET"
    }
    fn path(&self) -> String {
        format!(
            "{}/{}/{}",
            self.subject(),
            Catalog::of(self).catalog,
            archive::Get::of(self).digest.as_bytes().to_base58()
        )
    }
}

impl S3Request for Capability<archive::Put> {
    fn method(&self) -> &'static str {
        "PUT"
    }
    fn path(&self) -> String {
        format!(
            "{}/{}/{}",
            self.subject(),
            Catalog::of(self).catalog,
            archive::Put::of(self).digest.as_bytes().to_base58()
        )
    }
    fn checksum(&self) -> Option<Checksum> {
        Some(crate::Hasher::Sha256.checksum(&archive::Put::of(self).content))
    }
}

impl S3Request for Capability<archive::PutClaim> {
    fn method(&self) -> &'static str {
        "PUT"
    }
    fn path(&self) -> String {
        format!(
            "{}/{}/{}",
            self.subject(),
            Catalog::of(self).catalog,
            archive::PutClaim::of(self).digest.as_bytes().to_base58()
        )
    }
    fn checksum(&self) -> Option<Checksum> {
        Some(archive::PutClaim::of(self).checksum.clone())
    }
}
