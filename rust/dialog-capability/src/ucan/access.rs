//! UCAN access support — implements [`Scope`](crate::access::Scope) for UCAN scope.
//!
//! The full [`Protocol`](crate::access::Protocol) implementation for [`Ucan`](super::Ucan)
//! lives in the `dialog-capability-ucan` crate, which has access to credential
//! and delegation types.

use crate::Did;
use crate::access;

use super::scope;

impl access::Scope for scope::Scope {
    fn subject(&self) -> &Did {
        use dialog_ucan::subject::Subject as UcanSubject;
        match &self.subject {
            UcanSubject::Specific(did) => did,
            UcanSubject::Any => {
                static ANY: std::sync::LazyLock<Did> =
                    std::sync::LazyLock::new(|| crate::ANY_SUBJECT.parse().expect("valid DID"));
                &ANY
            }
        }
    }
}
