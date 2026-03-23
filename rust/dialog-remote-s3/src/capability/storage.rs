//! Storage S3 request implementations.
//!
//! This module provides `S3Request` implementations for storage capabilities,
//! enabling them to be translated into presigned S3 URLs.

use super::S3Request;
use crate::Checksum;
use base58::ToBase58;
use dialog_capability::{Capability, Policy};
use dialog_effects::storage::{self, Store};

impl S3Request for Capability<storage::Get> {
    fn method(&self) -> &'static str {
        "GET"
    }
    fn path(&self) -> String {
        format!(
            "{}/{}/{}",
            self.subject(),
            Store::of(self).store,
            storage::Get::of(self).key.to_base58()
        )
    }
}

impl S3Request for Capability<storage::Set> {
    fn method(&self) -> &'static str {
        "PUT"
    }
    fn path(&self) -> String {
        format!(
            "{}/{}/{}",
            self.subject(),
            Store::of(self).store,
            storage::Set::of(self).key.to_base58()
        )
    }
    fn checksum(&self) -> Option<Checksum> {
        Some(crate::Hasher::Sha256.checksum(&storage::Set::of(self).value))
    }
}

impl S3Request for Capability<storage::SetClaim> {
    fn method(&self) -> &'static str {
        "PUT"
    }
    fn path(&self) -> String {
        format!(
            "{}/{}/{}",
            self.subject(),
            Store::of(self).store,
            storage::SetClaim::of(self).key.to_base58()
        )
    }
    fn checksum(&self) -> Option<Checksum> {
        Some(storage::SetClaim::of(self).checksum.clone())
    }
}

impl S3Request for Capability<storage::Delete> {
    fn method(&self) -> &'static str {
        "DELETE"
    }
    fn path(&self) -> String {
        format!(
            "{}/{}/{}",
            self.subject(),
            Store::of(self).store,
            storage::Delete::of(self).key.to_base58()
        )
    }
}

impl S3Request for Capability<storage::List> {
    fn method(&self) -> &'static str {
        "GET"
    }
    fn path(&self) -> String {
        String::new()
    }
    fn params(&self) -> Option<Vec<(String, String)>> {
        let mut params = vec![
            ("list-type".to_owned(), "2".to_owned()),
            (
                "prefix".to_owned(),
                format!("{}/{}", self.subject(), &Store::of(self).store),
            ),
        ];

        if let Some(token) = &storage::List::of(self).continuation_token {
            params.push(("continuation-token".to_owned(), token.clone()));
        }

        Some(params)
    }
}
