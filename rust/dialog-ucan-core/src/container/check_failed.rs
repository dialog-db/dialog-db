//! Conversion from [`CheckFailed`] (defined in `crate::invocation`) to
//! [`ContainerError`]. A free function rather than a `From` impl because
//! both types live outside this module's defining crate (orphan rule).

use super::ContainerError;
use crate::invocation::CheckFailed;

/// Convert a `CheckFailed` error to a `ContainerError`.
pub fn check_failed_to_container_error(err: CheckFailed) -> ContainerError {
    match err {
        CheckFailed::DelegationAudienceMismatch {
            claimed,
            authorized,
        } => ContainerError::Invocation(format!(
            "invalid proof issuer chain: claimed {} authorized {}",
            claimed, authorized
        )),
        CheckFailed::UnauthorizedSubject {
            claimed,
            authorized,
        } => ContainerError::Invocation(format!(
            "subject not allowed by proof: claimed {} authorized {}",
            claimed, authorized
        )),
        CheckFailed::UnprovenSubject { subject, issuer } => ContainerError::Invocation(format!(
            "root proof issuer is not the subject: subject {} issuer {}",
            subject, issuer
        )),
        CheckFailed::CommandEscalation {
            claimed,
            authorized,
        } => ContainerError::Invocation(format!(
            "command mismatch: expected {:?}, found {:?}",
            authorized, claimed
        )),
        CheckFailed::PolicyViolation(predicate) => {
            ContainerError::Invocation(format!("predicate failed: {:?}", predicate))
        }
        CheckFailed::PolicyIncompatibility(run_err) => {
            ContainerError::Invocation(format!("predicate run error: {}", run_err))
        }
        CheckFailed::WaitingOnPromise(waiting) => {
            ContainerError::Invocation(format!("waiting on promise: {:?}", waiting))
        }
        CheckFailed::InvalidTimeWindow { range } => {
            ContainerError::Invocation(format!("invalid time window: {:?}", range))
        }
        CheckFailed::TimeBound(err) => {
            ContainerError::Invocation(format!("time bound error: {:?}", err))
        }
    }
}
