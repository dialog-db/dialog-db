//! Structured errors returned by remote HTTP services.

use std::error::Error as StdError;

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// A non-success response returned by a remote HTTP service.
///
/// Transport implementations should bound response bodies before constructing
/// this value. `code` is the service's stable machine-readable classification
/// when its error envelope supplied one; `message` is safe, bounded detail for
/// the immediate caller.
#[derive(Clone, Debug, Deserialize, Error, PartialEq, Eq, Serialize)]
#[error("Service returned HTTP {status}: {message}")]
pub struct ServiceResponseError {
    /// HTTP response status.
    pub status: u16,
    /// Stable service error code, when supplied.
    pub code: Option<String>,
    /// Bounded service error detail.
    pub message: String,
}

impl ServiceResponseError {
    /// Construct a structured service response error.
    pub fn new(status: u16, code: Option<String>, message: impl Into<String>) -> Self {
        Self {
            status,
            code,
            message: message.into(),
        }
    }
}

/// Find a structured service response in an error's source chain.
///
/// Higher-level operations such as repository pull and push wrap several
/// command-specific errors. This helper lets callers classify the original
/// HTTP response without matching every intermediate wrapper.
pub fn find_service_response<'a>(
    mut error: &'a (dyn StdError + 'static),
) -> Option<&'a ServiceResponseError> {
    loop {
        if let Some(service) = error.downcast_ref::<ServiceResponseError>() {
            return Some(service);
        }
        error = error.source()?;
    }
}
