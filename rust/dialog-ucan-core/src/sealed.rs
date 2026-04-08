use crate::{command::Command, issuer::Issuer, subject::Subject, unset::Unset};
use dialog_varsig::{Did, Signature};
use ipld_core::cid::Cid;

#[doc(hidden)]
pub trait IssuerOrUnset<S: Signature> {}
impl<S: Signature> IssuerOrUnset<S> for Unset {}
impl<S: Signature, I: Issuer<S>> IssuerOrUnset<S> for I {}

#[doc(hidden)]
pub trait DidOrUnset {}
impl DidOrUnset for Unset {}
impl DidOrUnset for Did {}

#[doc(hidden)]
pub trait SubjectOrUnset {}
impl SubjectOrUnset for Unset {}
impl SubjectOrUnset for Subject {}

#[doc(hidden)]
pub trait CommandOrUnset {}
impl CommandOrUnset for Unset {}
impl CommandOrUnset for Command {}

#[doc(hidden)]
pub trait ProofsOrUnset {}
impl ProofsOrUnset for Unset {}
impl ProofsOrUnset for Vec<Cid> {}
