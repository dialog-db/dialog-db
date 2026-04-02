//! Archive S3 request implementations.
//!
//! This module provides `Access` implementations for archive
//! (content-addressed) capabilities, enabling them to be translated into
//! presigned S3 URLs.

use super::Access;
use crate::Checksum;
use base58::ToBase58;
use dialog_capability::{Capability, Policy};
use dialog_effects::archive::{Catalog, Get, Put, PutClaim};

impl Access for Capability<Get> {
    fn method(&self) -> &'static str {
        "GET"
    }
    fn path(&self) -> String {
        format!(
            "{}/{}/{}",
            self.subject(),
            Catalog::of(self).catalog,
            Get::of(self).digest.as_bytes().to_base58()
        )
    }
}

impl Access for Capability<Put> {
    fn method(&self) -> &'static str {
        "PUT"
    }
    fn path(&self) -> String {
        format!(
            "{}/{}/{}",
            self.subject(),
            Catalog::of(self).catalog,
            Put::of(self).digest.as_bytes().to_base58()
        )
    }
    fn checksum(&self) -> Option<Checksum> {
        Some(crate::Hasher::Sha256.checksum(&Put::of(self).content))
    }
}

impl Access for Capability<PutClaim> {
    fn method(&self) -> &'static str {
        "PUT"
    }
    fn path(&self) -> String {
        format!(
            "{}/{}/{}",
            self.subject(),
            Catalog::of(self).catalog,
            PutClaim::of(self).digest.as_bytes().to_base58()
        )
    }
    fn checksum(&self) -> Option<Checksum> {
        Some(PutClaim::of(self).checksum.clone())
    }
}
