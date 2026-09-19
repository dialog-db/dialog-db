//! Performing an invocation a peer sent.
//!
//! The mirror of `dialog-remote-ucan-s3`'s authorizer, diverging at the
//! last step: where that one redeems a verified invocation for a
//! presigned S3 permit, this one performs the effect against a local
//! store and answers with its output. Everything before that step is the
//! same job and is done the same way.
//!
//! # Three questions, asked in order
//!
//! 1. **Is the invocation real?** [`InvocationChain::verify`] walks the
//!    proof chain, checks every signature and every revocation, and
//!    bounds it in time. Nothing else here re-derives any of that.
//! 2. **What does it invoke?** The command path selects the effect and
//!    the arguments rebuild its capability. Both come out of the
//!    *verified* invocation, never from anything sent beside it.
//! 3. **Is the payload the one it named?** For effects that carry bytes,
//!    [`crate::resolve`] finds them in the container and proves they are
//!    what was committed to.
//!
//! Only then does anything run.
//!
//! # What is not served, and why
//!
//! The blob effects are absent. `blob::Read` returns a `BlobReader` and
//! `blob::Write`/`blob::Import` return a `BlobWriter` — streaming
//! handles, not values — so there is nothing for a request/response
//! exchange to encode. Blobs want a stream of their own, which QUIC
//! gives cheaply and which this module deliberately does not fake by
//! buffering a whole blob into a response. Until that exists they are
//! refused by name rather than silently missing.

use dialog_capability::{Capability, Provider, Subject};
use dialog_common::ConditionalSync;
use dialog_did_web::{PerformingResolver, Resolve};
use dialog_effects::{Use, archive, memory};
use dialog_ucan_core::container::bundle::InvocationBundle;
use dialog_ucan_core::{
    Environment, InvocationChain, VerificationContext, revocation::RevocationChecker,
};
use dialog_varsig::did::Did;
use ipld_core::ipld::Ipld;
use serde::de::DeserializeOwned;
use std::collections::BTreeMap;
use std::sync::Arc;

use crate::wire::{Refusal, Response, encode};
use dialog_ucan::Args;

/// What a peer performs invocations against.
///
/// Every effect this module serves, and nothing else — so a type that
/// satisfies it can answer any request that gets past verification, and
/// one that cannot is rejected at compile time rather than by a refusal
/// at run time.
pub trait Store:
    Provider<archive::Get>
    + Provider<archive::Put>
    + Provider<archive::Import>
    + Provider<memory::Resolve>
    + Provider<memory::Publish>
    + Provider<memory::Retract>
    + ConditionalSync
{
}

impl<T> Store for T where
    T: Provider<archive::Get>
        + Provider<archive::Put>
        + Provider<archive::Import>
        + Provider<memory::Resolve>
        + Provider<memory::Publish>
        + Provider<memory::Retract>
        + ConditionalSync
{
}

/// Answers invocations against a local store.
///
/// Holds the two policies verification needs and the store performing
/// is done against. Resolution and revocation are the embedder's to
/// choose, exactly as they are for the S3 authorizer: a peer with no
/// revocation index establishes nothing about revocation status, and
/// [`UnverifiedRevocations`] is named so that is not a surprise.
///
/// [`UnverifiedRevocations`]: dialog_ucan_core::UnverifiedRevocations
pub struct Responder<S, Resolver, Revocations = dialog_ucan_core::UnverifiedRevocations> {
    store: S,
    resolver: Arc<Resolver>,
    revocations: Arc<Revocations>,
}

impl<S, Resolver> Responder<S, Resolver> {
    /// Serve `store`, resolving issuer DIDs through `resolver`.
    pub fn new(store: S, resolver: Resolver) -> Self {
        Self {
            store,
            resolver: Arc::new(resolver),
            revocations: Arc::new(dialog_ucan_core::UnverifiedRevocations),
        }
    }
}

impl<S, Resolver, Revocations> Responder<S, Resolver, Revocations> {
    /// What invocations are performed against.
    pub fn store(&self) -> &S {
        &self.store
    }

    /// Check every proof against `revocations` while verifying.
    pub fn with_revocations<Checked>(
        self,
        revocations: Checked,
    ) -> Responder<S, Resolver, Checked> {
        Responder {
            store: self.store,
            resolver: self.resolver,
            revocations: Arc::new(revocations),
        }
    }
}

impl<S, Resolver, Revocations> Responder<S, Resolver, Revocations>
where
    S: Store,
    Resolver: Provider<Resolve> + ConditionalSync,
    Revocations: RevocationChecker + ConditionalSync,
{
    /// Verify a `ctn-v1` container and perform what it invokes.
    ///
    /// Never returns an error: a peer answers a stranger, so every
    /// outcome is a [`Response`] it chooses to send. An effect that
    /// failed is [`Response::Performed`] carrying that failure, because
    /// the peer did run it; everything that stopped short is a
    /// [`Refusal`].
    pub async fn answer(&self, container: &[u8]) -> Response {
        match self.perform(container).await {
            Ok(response) => response,
            Err(refusal) => Response::Refused(refusal),
        }
    }

    async fn perform(&self, container: &[u8]) -> Result<Response, Refusal> {
        let bundle = InvocationBundle::try_from(container)
            .map_err(|error| Refusal::Malformed(format!("not an invocation container: {error}")))?;
        let chain = bundle
            .chain()
            .map_err(|error| Refusal::Malformed(format!("incomplete proofs: {error}")))?;

        self.verify(&chain).await?;

        let subject = chain.subject().clone();
        let args = chain.arguments();
        let command: Vec<&str> = chain.command().0.iter().map(String::as_str).collect();

        match command.as_slice() {
            ["use", "get", "archive", "block"] => {
                let capability = archive_claim::<archive::Get>(&subject, args)?;
                Ok(performed(
                    Provider::<archive::Get>::execute(&self.store, capability).await,
                ))
            }
            ["use", "put", "archive", "block"] => self.put(&bundle, &subject, args).await,
            ["use", "get", "memory", "cell"] => {
                // A unit effect is constructed, never read: there is
                // nothing in the arguments to read it from, and asking
                // serde for one fails rather than yielding the only
                // value it could have had.
                let capability = memory_leaf(&subject, args, memory::Resolve)?;
                Ok(performed(
                    Provider::<memory::Resolve>::execute(&self.store, capability).await,
                ))
            }
            ["use", "put", "memory", "cell"] => {
                let capability = self.publish(&bundle, &subject, args)?;
                Ok(performed(
                    Provider::<memory::Publish>::execute(&self.store, capability).await,
                ))
            }
            ["use", "delete", "memory", "cell"] => {
                let capability = memory_claim::<memory::Retract>(&subject, args)?;
                Ok(performed(
                    Provider::<memory::Retract>::execute(&self.store, capability).await,
                ))
            }
            ["use", _, "archive", "blob"] => Err(Refusal::UnknownCommand(format!(
                "/{} answers with a stream, which this protocol does not carry yet",
                command.join("/")
            ))),
            _ => Err(Refusal::UnknownCommand(format!("/{}", command.join("/")))),
        }
    }

    async fn verify(
        &self,
        chain: &InvocationChain<dialog_varsig::AnySignature>,
    ) -> Result<(), Refusal> {
        let resolver = PerformingResolver::new(self.resolver.as_ref());
        let environment = Environment::new(chain.proof_store(), resolver, &*self.revocations);
        chain
            .verify(&VerificationContext::new(&environment))
            .await
            .map(|_window| ())
            .map_err(|error| match error {
                // Our own inability to check says nothing about their
                // request, so it must not read as a denial.
                dialog_ucan_core::ContainerError::Configuration(detail) => {
                    Refusal::Internal(format!("could not verify the chain: {detail}"))
                }
                other => Refusal::Unauthorized(other.to_string()),
            })
    }

    /// `archive::Put` and `archive::Import` share the command
    /// `put/archive/block`, so the arguments are what tell them apart: a
    /// put commits to one `digest` and `checksum`, an import to a list
    /// of `checksums`. Reading the list first means a one-block import
    /// stays an import rather than being silently reinterpreted.
    async fn put(
        &self,
        bundle: &InvocationBundle,
        subject: &Did,
        args: &Args,
    ) -> Result<Response, Refusal> {
        if args.contains_key("checksums") {
            let committed: archive::ImportAttenuation = from_args(args)?;
            // The two lists are read as pairs, so a sender that signed a
            // different number of each has not described anything and is
            // refused before a single block is looked up.
            if committed.digests.len() != committed.checksums.len() {
                return Err(Refusal::Malformed(format!(
                    "import signed {} digests and {} checksums",
                    committed.digests.len(),
                    committed.checksums.len()
                )));
            }
            let mut blocks = Vec::with_capacity(committed.checksums.len());
            for (digest, checksum) in committed.digests.iter().zip(&committed.checksums) {
                blocks.push(crate::resolve::block(bundle, digest, checksum).map_err(unresolved)?);
            }
            let capability = archive_leaf(subject, args, archive::Import { blocks })?;
            return Ok(performed(
                Provider::<archive::Import>::execute(&self.store, capability).await,
            ));
        }

        let committed: archive::PutAttenuation = from_args(args)?;
        let block = crate::resolve::block(bundle, &committed.digest, &committed.checksum)
            .map_err(unresolved)?;
        let capability = archive_leaf(subject, args, archive::Put { block })?;
        Ok(performed(
            Provider::<archive::Put>::execute(&self.store, capability).await,
        ))
    }

    fn publish(
        &self,
        bundle: &InvocationBundle,
        subject: &Did,
        args: &Args,
    ) -> Result<Capability<memory::Publish>, Refusal> {
        let committed: memory::PublishAttenuation = from_args(args)?;
        let content = crate::resolve::at(bundle, &committed.checksum)
            .map_err(unresolved)?
            .to_vec();
        memory_leaf(
            subject,
            args,
            memory::Publish {
                content,
                when: committed.when,
            },
        )
    }
}

/// Encode an effect's output, which is itself a `Result` — a failure of
/// the effect belongs inside [`Response::Performed`], not beside it.
fn performed<T: serde::Serialize>(output: T) -> Response {
    match encode("output", &output) {
        Ok(bytes) => Response::Performed(bytes),
        Err(error) => Response::Refused(Refusal::Internal(error.to_string())),
    }
}

fn unresolved(error: crate::resolve::ResolveError) -> Refusal {
    match error {
        crate::resolve::ResolveError::Impostor { .. } => Refusal::Unauthorized(error.to_string()),
        other => Refusal::Malformed(other.to_string()),
    }
}

/// Read one value out of the invocation's arguments.
fn from_args<T: DeserializeOwned>(args: &Args) -> Result<T, Refusal> {
    let map: BTreeMap<String, Ipld> = args
        .iter()
        .map(|(key, value)| {
            Ipld::try_from(value)
                .map(|ipld| (key.clone(), ipld))
                .map_err(|error| Refusal::Malformed(format!("unresolved promise '{key}': {error}")))
        })
        .collect::<Result<_, _>>()?;
    ipld_core::serde::from_ipld(Ipld::Map(map))
        .map_err(|error| Refusal::Malformed(format!("arguments do not fit the command: {error}")))
}

/// `Subject -> Use -> Archive -> Catalog -> leaf`, with the leaf given
/// rather than read from the arguments — which is how an effect that
/// carries bytes is rebuilt around the bytes that were resolved.
fn archive_leaf<Fx>(subject: &Did, args: &Args, leaf: Fx) -> Result<Capability<Fx>, Refusal>
where
    Fx: dialog_capability::Policy<Of = archive::Catalog>,
    <Fx as dialog_capability::Constraint>::Capability: dialog_capability::Ability,
{
    let catalog: archive::Catalog = from_args(args)?;
    Ok(Subject::from(subject.clone())
        .attenuate(Use)
        .attenuate(archive::Archive)
        .attenuate(catalog)
        .attenuate(leaf))
}

fn archive_claim<Fx>(subject: &Did, args: &Args) -> Result<Capability<Fx>, Refusal>
where
    Fx: dialog_capability::Policy<Of = archive::Catalog> + DeserializeOwned,
    <Fx as dialog_capability::Constraint>::Capability: dialog_capability::Ability,
{
    let leaf: Fx = from_args(args)?;
    archive_leaf(subject, args, leaf)
}

fn memory_leaf<Fx>(subject: &Did, args: &Args, leaf: Fx) -> Result<Capability<Fx>, Refusal>
where
    Fx: dialog_capability::Policy<Of = memory::Cell>,
    <Fx as dialog_capability::Constraint>::Capability: dialog_capability::Ability,
{
    let space: memory::Space = from_args(args)?;
    let cell: memory::Cell = from_args(args)?;
    Ok(Subject::from(subject.clone())
        .attenuate(Use)
        .attenuate(memory::Memory)
        .attenuate(space)
        .attenuate(cell)
        .attenuate(leaf))
}

fn memory_claim<Fx>(subject: &Did, args: &Args) -> Result<Capability<Fx>, Refusal>
where
    Fx: dialog_capability::Policy<Of = memory::Cell> + DeserializeOwned,
    <Fx as dialog_capability::Constraint>::Capability: dialog_capability::Ability,
{
    let leaf: Fx = from_args(args)?;
    memory_leaf(subject, args, leaf)
}

#[cfg(test)]
mod tests;
