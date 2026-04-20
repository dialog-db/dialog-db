//! Archive S3 request implementations.
//!
//! This module provides `From<&Capability<Fx>> for S3Request` impls for
//! archive (content-addressed) capabilities, enabling them to be
//! translated into concrete S3 request descriptions.

use super::S3Request;
use base58::ToBase58;
use dialog_capability::{Capability, Policy};
use dialog_common::Hasher;
use dialog_effects::archive::prelude::{GetExt, PutExt};
use dialog_effects::archive::{Catalog, Get, Put, PutAttenuation};

impl From<&Capability<Get>> for S3Request {
    fn from(capability: &Capability<Get>) -> Self {
        S3Request {
            method: "GET".to_string(),
            path: format!(
                "{}/{}/{}",
                capability.subject(),
                capability.catalog(),
                capability.digest().as_bytes().to_base58()
            ),
            ..S3Request::default()
        }
    }
}

impl From<&Capability<Put>> for S3Request {
    fn from(capability: &Capability<Put>) -> Self {
        S3Request {
            method: "PUT".to_string(),
            path: format!(
                "{}/{}/{}",
                capability.subject(),
                capability.catalog(),
                capability.digest().as_bytes().to_base58()
            ),
            checksum: Some(Hasher::Sha256.checksum(capability.content())),
            ..S3Request::default()
        }
    }
}

impl From<&Capability<PutAttenuation>> for S3Request {
    fn from(capability: &Capability<PutAttenuation>) -> Self {
        let put = PutAttenuation::of(capability);
        S3Request {
            method: "PUT".to_string(),
            path: format!(
                "{}/{}/{}",
                capability.subject(),
                Catalog::of(capability).catalog,
                put.digest.as_bytes().to_base58()
            ),
            checksum: Some(put.checksum.clone()),
            ..S3Request::default()
        }
    }
}
