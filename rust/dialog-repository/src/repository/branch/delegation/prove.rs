//! Chain search over retained delegations.
//!
//! The tree-backed counterpart of the certificate store's
//! [`prove`](dialog_capability::access::CertificateStore::prove), searching
//! the `dialog.ucan/*` facts instead of listing and decoding stored
//! certificate files. The walk considers **slim candidates** read from the
//! facts (issuer, subject, command, validity bounds) and only fetches and
//! decodes an envelope at admission time:
//!
//! - a **direct** candidate (issuer = subject) is admitted the moment it is
//!   seen, so the common case reads one envelope no matter how many
//!   delegations are retained;
//! - an **indirect** candidate is deferred until every direct candidate of
//!   the hop has been tried, preserving the direct-first preference of the
//!   certificate-store walk;
//! - a candidate whose subject, command cover, or validity window already
//!   fails on the facts is skipped without touching its envelope.
//!
//! Each queue entry carries the principals already on its path, so a cyclic
//! delegation graph cannot loop the walk; `MAX_DEPTH` still bounds honest
//! depth exactly like the certificate-store walk.

use dialog_artifacts::{Artifact, ArtifactSelector, Entity, Value};
use dialog_capability::access::{AuthorizeError, Certificate as _, Proof as _, TimeRange};
use dialog_capability::{ANY_SUBJECT, Did, Fork, Provider};
use dialog_common::ConditionalSync;
use dialog_effects::archive::{Get, Put};
use dialog_effects::blob::{Import as BlobImport, Read as BlobRead};
use dialog_effects::memory::Resolve;
use dialog_ucan::{Scope, UcanCertificate, UcanProof};
use dialog_ucan_core::command::Command;
use dialog_ucan_core::subject::Subject as UcanSubject;
use futures_util::StreamExt as _;
use std::collections::HashSet;
use std::fmt::Display;

use super::{
    DELEGATION_AUDIENCE, DELEGATION_COMMAND, DELEGATION_EXPIRATION, DELEGATION_ISSUER,
    DELEGATION_NOT_BEFORE, DELEGATION_SUBJECT, Delegations,
};
use crate::repository::branch::blob::index_store;
use crate::{Blob, Branch, RemoteSite, Select};

/// Maximum chain depth, matching
/// [`CertificateStore::MAX_DEPTH`](dialog_capability::access::CertificateStore::MAX_DEPTH).
const MAX_DEPTH: usize = 10;

impl<'a> Delegations<'a> {
    /// Search the retained delegations for a chain proving `principal` may
    /// access `access`, mirroring the certificate store's
    /// [`prove`](dialog_capability::access::CertificateStore::prove)
    /// semantics over the branch's `dialog.ucan/*` facts.
    pub fn prove(self, principal: Did, access: Scope) -> ProveDelegation<'a> {
        ProveDelegation {
            branch: self.branch,
            principal,
            access,
            duration: TimeRange::unbounded(),
        }
    }
}

/// A chain search over retained delegations. Created by
/// [`Delegations::prove`].
pub struct ProveDelegation<'a> {
    branch: &'a Branch,
    principal: Did,
    access: Scope,
    duration: TimeRange,
}

/// A delegation considered by the walk, read entirely from its facts; the
/// envelope is untouched until admission.
struct Candidate {
    entity: Entity,
    issuer: Did,
    /// The certificate's own validity window, from the fact bounds.
    range: TimeRange,
}

/// One pending hop of the walk.
struct Hop {
    audience: Did,
    chain: Vec<(UcanCertificate, TimeRange)>,
    /// Every principal already on this path, for cycle prevention.
    path: HashSet<Did>,
    depth: usize,
}

fn malformed(context: &str, error: impl Display) -> AuthorizeError {
    AuthorizeError::Malformed {
        detail: format!("{context}: {error}"),
    }
}

impl ProveDelegation<'_> {
    /// Constrain the proof to a time range the chain must cover.
    pub fn during(mut self, duration: TimeRange) -> Self {
        self.duration = duration;
        self
    }

    /// Execute the search, returning the proof chain.
    pub async fn perform<Env>(self, env: &Env) -> Result<UcanProof, AuthorizeError>
    where
        Env: Provider<Get>
            + Provider<Put>
            + Provider<Resolve>
            + Provider<BlobRead>
            + Provider<BlobImport>
            + Provider<Fork<RemoteSite, Get>>
            + Provider<Fork<RemoteSite, Resolve>>
            + Provider<Fork<RemoteSite, BlobRead>>
            + ConditionalSync
            + 'static,
    {
        let branch = self.branch;
        let access = &self.access;
        let duration = &self.duration;

        let subject = match &access.subject {
            UcanSubject::Specific(did) => did.clone(),
            // An `Any` subject needs no proof, exactly as the
            // certificate-store walk decides it.
            UcanSubject::Any => return Ok(UcanProof::new(access.clone())),
        };
        if self.principal == subject {
            return Ok(UcanProof::new(access.clone()));
        }

        let store = index_store(branch, env).await;

        let mut queue: Vec<Hop> = vec![Hop {
            audience: self.principal.clone(),
            chain: Vec::new(),
            path: HashSet::from([self.principal.clone()]),
            depth: 0,
        }];

        while let Some(hop) = queue.pop() {
            if hop.depth >= MAX_DEPTH {
                continue;
            }

            // One audience-scoped scan yields the hop's candidate entities;
            // their facts filter and order them without a single decode.
            let candidates = Select::new(
                branch,
                ArtifactSelector::new()
                    .the(DELEGATION_AUDIENCE.parse().expect("valid attribute"))
                    .is(Value::String(hop.audience.to_string())),
            )
            .execute(store.clone())
            .await
            .map_err(|error| malformed("candidate scan failed", error))?;
            futures_util::pin_mut!(candidates);

            let mut deferred: Vec<Candidate> = Vec::new();
            let mut admitted_direct = None;

            while let Some(item) = candidates.next().await {
                let fact = item.map_err(|error| malformed("candidate fact undecodable", error))?;
                let Some(candidate) = self.candidate(branch, &store, fact.of, &subject).await?
                else {
                    continue;
                };

                if candidate.issuer == subject {
                    // Direct grant: admit immediately. The first one whose
                    // envelope verifies completes the chain.
                    if let Some(admitted) = self
                        .admit(branch, env, &candidate, access, duration)
                        .await?
                    {
                        admitted_direct = Some(admitted);
                        break;
                    }
                } else if !hop.path.contains(&candidate.issuer) {
                    deferred.push(candidate);
                }
            }

            if let Some((certificate, range)) = admitted_direct {
                let mut chain = hop.chain;
                chain.insert(0, (certificate, range));
                let effective = chain
                    .iter()
                    .fold(TimeRange::unbounded(), |acc, (_, r)| acc.intersect(r));
                let mut proof = UcanProof::new(access.clone());
                for (certificate, _) in chain {
                    proof.push(certificate);
                }
                proof.set_duration(effective);
                return Ok(proof);
            }

            // No direct grant at this hop: admit the deferred indirect
            // candidates and queue the next hops.
            for candidate in deferred {
                let Some((certificate, range)) = self
                    .admit(branch, env, &candidate, access, duration)
                    .await?
                else {
                    continue;
                };
                let issuer = candidate.issuer.clone();
                let mut chain = hop.chain.clone();
                chain.insert(0, (certificate, range));
                let mut path = hop.path.clone();
                path.insert(issuer.clone());
                queue.push(Hop {
                    audience: issuer,
                    chain,
                    path,
                    depth: hop.depth + 1,
                });
            }
        }

        Err(AuthorizeError::UnprovenSubject {
            claimed: self.principal.clone(),
            authorized: subject,
        })
    }

    /// Read a candidate's slim facts and filter on them: subject match,
    /// command cover, validity window. `None` means the candidate cannot
    /// serve this claim and its envelope is never touched.
    async fn candidate<S>(
        &self,
        branch: &Branch,
        store: &S,
        entity: Entity,
        subject: &Did,
    ) -> Result<Option<Candidate>, AuthorizeError>
    where
        S: dialog_storage::StorageBackend<
                Key = dialog_storage::Blake3Hash,
                Value = Vec<u8>,
                Error = dialog_storage::DialogStorageError,
            > + Clone
            + ConditionalSync,
    {
        let facts = Select::new(branch, ArtifactSelector::new().of(entity.clone()))
            .execute(store.clone())
            .await
            .map_err(|error| malformed("candidate read failed", error))?;
        futures_util::pin_mut!(facts);

        let mut issuer = None;
        let mut candidate_subject = None;
        let mut command = None;
        let mut not_before = None;
        let mut expiration = None;
        while let Some(item) = facts.next().await {
            let fact: Artifact =
                item.map_err(|error| malformed("candidate fact undecodable", error))?;
            match (fact.the.as_str(), fact.is) {
                (DELEGATION_ISSUER, Value::String(did)) => issuer = Some(did),
                (DELEGATION_SUBJECT, Value::String(did)) => candidate_subject = Some(did),
                (DELEGATION_COMMAND, Value::String(path)) => command = Some(path),
                (DELEGATION_NOT_BEFORE, Value::UnsignedInt(seconds)) => {
                    not_before = Some(seconds as u64)
                }
                (DELEGATION_EXPIRATION, Value::UnsignedInt(seconds)) => {
                    expiration = Some(seconds as u64)
                }
                _ => {}
            }
        }
        let (Some(issuer), Some(candidate_subject), Some(command)) =
            (issuer, candidate_subject, command)
        else {
            // Not a complete delegation record (foreign facts on a blob
            // entity, or a partially visible one): not a candidate.
            return Ok(None);
        };

        // Subject: specific match or powerline.
        if candidate_subject != subject.to_string() && candidate_subject != ANY_SUBJECT {
            return Ok(None);
        }

        // Command cover: the requested command must fall under the
        // delegated one. The envelope's authoritative verify re-checks
        // this at admission; failing here just skips the fetch.
        match Command::parse(&command) {
            Ok(delegated) if self.access.command.starts_with(&delegated) => {}
            _ => return Ok(None),
        }

        // Validity window, from the fact bounds.
        let range = TimeRange {
            not_before,
            expiration,
        };
        if !range.covers(&self.duration) {
            return Ok(None);
        }

        let issuer: Did = issuer
            .parse()
            .map_err(|_| malformed("candidate issuer is not a DID", issuer.clone()))?;

        Ok(Some(Candidate {
            entity,
            issuer,
            range,
        }))
    }

    /// Fetch and decode a candidate's envelope and run the authoritative
    /// verification (command cover and policy). `None` means the envelope
    /// rejected a claim its facts admitted; the walk moves on.
    async fn admit<Env>(
        &self,
        branch: &Branch,
        env: &Env,
        candidate: &Candidate,
        access: &Scope,
        duration: &TimeRange,
    ) -> Result<Option<(UcanCertificate, TimeRange)>, AuthorizeError>
    where
        Env: Provider<Get>
            + Provider<Put>
            + Provider<Resolve>
            + Provider<BlobRead>
            + Provider<BlobImport>
            + Provider<Fork<RemoteSite, Get>>
            + Provider<Fork<RemoteSite, Resolve>>
            + Provider<Fork<RemoteSite, BlobRead>>
            + ConditionalSync
            + 'static,
    {
        let mut reader = Blob::from(candidate.entity.clone())
            .read(branch.into())
            .perform(env)
            .await
            .map_err(|error| malformed("envelope unavailable", error))?;
        let mut bytes = Vec::new();
        while let Some(chunk) = reader
            .next()
            .await
            .map_err(|error| malformed("envelope read failed", error))?
        {
            bytes.extend(chunk);
        }

        let certificate = UcanCertificate::decode(&bytes)?;
        let Ok(range) = certificate.verify(access) else {
            return Ok(None);
        };
        if !range.covers(duration) {
            return Ok(None);
        }
        debug_assert_eq!(
            (range.not_before, range.expiration),
            (candidate.range.not_before, candidate.range.expiration),
            "fact bounds must mirror the envelope's"
        );
        Ok(Some((certificate, range)))
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use crate::RepositoryExt as _;
    use anyhow::Result;
    use dialog_capability::Subject;
    use dialog_capability::access::{CertificateStore, Prove};
    use dialog_credentials::Ed25519Signer;
    use dialog_network::Network;
    use dialog_operator::helpers::unique_name;
    use dialog_operator::{DeriveOperator as _, Operator, Profile};
    use dialog_storage::provider::Volatile;
    use dialog_storage::provider::storage::{Storage, VolatileSpace};
    use dialog_ucan::{Parameters, Ucan, UcanDelegation};
    use dialog_ucan_core::{DelegationBuilder, DelegationChain};
    use dialog_varsig::Principal as _;

    /// Both stores, populated identically: every scenario proves against
    /// the legacy certificate store AND the tree-backed walk, and the two
    /// must agree.
    struct Harness {
        branch: crate::Branch,
        operator: Operator<VolatileSpace>,
        legacy: Volatile,
    }

    impl Harness {
        async fn new(name: &str) -> Result<Self> {
            let storage = Storage::volatile();
            let profile = Profile::open(unique_name(name)).perform(&storage).await?;
            let operator = profile
                .derive(b"test")
                .allow(Subject::any())
                .network(Network::default())
                .build(storage)
                .await?;
            let repo = profile
                .repository(unique_name("repo"))
                .open()
                .perform(&operator)
                .await?;
            let branch = repo.branch("main").open().perform(&operator).await?;
            Ok(Self {
                branch,
                operator,
                legacy: Volatile::new(),
            })
        }

        async fn retain(&self, chain: UcanDelegation) -> Result<()> {
            CertificateStore::<Ucan>::save(&self.legacy, &chain)
                .await
                .unwrap();
            self.branch
                .delegations()
                .retain(chain)
                .perform(&self.operator)
                .await?;
            Ok(())
        }

        /// Prove against both stores and assert they agree on success and
        /// chain length; return the tree walk's verdict.
        async fn parity(
            &self,
            principal: &Did,
            scope: Scope,
            duration: TimeRange,
        ) -> Result<UcanProof, AuthorizeError> {
            let mut legacy_claim = Prove::<Ucan>::new(principal.clone(), scope.clone());
            legacy_claim.duration = duration;
            let legacy = CertificateStore::<Ucan>::prove(&self.legacy, legacy_claim).await;

            let tree = self
                .branch
                .delegations()
                .prove(principal.clone(), scope)
                .during(duration)
                .perform(&self.operator)
                .await;

            match (&legacy, &tree) {
                (Ok(expected), Ok(actual)) => assert_eq!(
                    expected.proofs().len(),
                    actual.proofs().len(),
                    "both walks must find chains of the same length"
                ),
                (Err(_), Err(_)) => {}
                (expected, actual) => panic!(
                    "walks disagree: legacy ok={} tree ok={}",
                    expected.is_ok(),
                    actual.is_ok()
                ),
            }
            tree
        }
    }

    async fn signer() -> Ed25519Signer {
        Ed25519Signer::generate().await.unwrap()
    }

    async fn delegate(
        issuer: &Ed25519Signer,
        audience: &Ed25519Signer,
        subject: UcanSubject,
    ) -> UcanDelegation {
        let delegation = DelegationBuilder::new()
            .issuer(issuer.clone())
            .audience(audience)
            .subject(subject)
            .command(vec!["storage".to_string()])
            .try_build()
            .await
            .unwrap();
        UcanDelegation::new(DelegationChain::new(delegation))
    }

    fn scope(subject: &Ed25519Signer, command: &[&str]) -> Scope {
        Scope {
            subject: UcanSubject::Specific(subject.did()),
            command: Command(command.iter().map(|s| s.to_string()).collect()),
            parameters: Parameters::default(),
        }
    }

    #[dialog_common::test]
    async fn it_proves_with_direct_delegation() -> Result<()> {
        let harness = Harness::new("prove-direct").await?;
        let space = signer().await;
        let holder = signer().await;
        harness
            .retain(delegate(&space, &holder, UcanSubject::Specific(space.did())).await)
            .await?;

        let proof = harness
            .parity(
                &holder.did(),
                scope(&space, &["storage"]),
                TimeRange::unbounded(),
            )
            .await;
        assert_eq!(proof.expect("direct grant proves").proofs().len(), 1);
        Ok(())
    }

    #[dialog_common::test]
    async fn it_proves_with_powerline_delegation() -> Result<()> {
        let harness = Harness::new("prove-powerline").await?;
        let space = signer().await;
        let holder = signer().await;
        harness
            .retain(delegate(&space, &holder, UcanSubject::Any).await)
            .await?;

        let proof = harness
            .parity(
                &holder.did(),
                scope(&space, &["storage"]),
                TimeRange::unbounded(),
            )
            .await;
        assert!(proof.is_ok(), "powerline proves: {:?}", proof.err());
        Ok(())
    }

    #[dialog_common::test]
    async fn it_fails_without_delegation() -> Result<()> {
        let harness = Harness::new("prove-none").await?;
        let space = signer().await;
        let holder = signer().await;

        let proof = harness
            .parity(
                &holder.did(),
                scope(&space, &["storage"]),
                TimeRange::unbounded(),
            )
            .await;
        assert!(matches!(proof, Err(AuthorizeError::UnprovenSubject { .. })));
        Ok(())
    }

    #[dialog_common::test]
    async fn it_fails_for_wrong_audience() -> Result<()> {
        let harness = Harness::new("prove-wrong-aud").await?;
        let space = signer().await;
        let holder = signer().await;
        let stranger = signer().await;
        harness
            .retain(delegate(&space, &holder, UcanSubject::Specific(space.did())).await)
            .await?;

        let proof = harness
            .parity(
                &stranger.did(),
                scope(&space, &["storage"]),
                TimeRange::unbounded(),
            )
            .await;
        assert!(proof.is_err());
        Ok(())
    }

    #[dialog_common::test]
    async fn it_fails_for_wrong_subject() -> Result<()> {
        let harness = Harness::new("prove-wrong-subj").await?;
        let space = signer().await;
        let other_space = signer().await;
        let holder = signer().await;
        harness
            .retain(delegate(&space, &holder, UcanSubject::Specific(space.did())).await)
            .await?;

        let proof = harness
            .parity(
                &holder.did(),
                scope(&other_space, &["storage"]),
                TimeRange::unbounded(),
            )
            .await;
        assert!(proof.is_err());
        Ok(())
    }

    #[dialog_common::test]
    async fn it_fails_for_uncovered_command() -> Result<()> {
        let harness = Harness::new("prove-command").await?;
        let space = signer().await;
        let holder = signer().await;
        harness
            .retain(delegate(&space, &holder, UcanSubject::Specific(space.did())).await)
            .await?;

        // The delegation grants /storage; /archive is not beneath it.
        let proof = harness
            .parity(
                &holder.did(),
                scope(&space, &["archive"]),
                TimeRange::unbounded(),
            )
            .await;
        assert!(proof.is_err());
        Ok(())
    }

    #[dialog_common::test]
    async fn it_rejects_an_expired_delegation() -> Result<()> {
        use dialog_common::time;
        use dialog_ucan_core::time::timestamp::Timestamp;

        let harness = Harness::new("prove-expired").await?;
        let space = signer().await;
        let holder = signer().await;

        let now = time::now()
            .duration_since(time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let past = Timestamp::try_from((now - 3600) as i128).unwrap();
        let delegation = DelegationBuilder::new()
            .issuer(space.clone())
            .audience(&holder)
            .subject(UcanSubject::Specific(space.did()))
            .command(vec!["storage".to_string()])
            .expiration(past)
            .try_build()
            .await
            .unwrap();
        harness
            .retain(UcanDelegation::new(DelegationChain::new(delegation)))
            .await?;

        let proof = harness
            .parity(
                &holder.did(),
                scope(&space, &["storage"]),
                TimeRange {
                    not_before: Some(now),
                    expiration: Some(now + 60),
                },
            )
            .await;
        assert!(proof.is_err(), "an expired delegation must not prove");
        Ok(())
    }

    #[dialog_common::test]
    async fn it_proves_via_powerline_chain() -> Result<()> {
        let harness = Harness::new("prove-powerline-chain").await?;
        let space = signer().await;
        let intermediary = signer().await;
        let holder = signer().await;

        harness
            .retain(delegate(&space, &intermediary, UcanSubject::Any).await)
            .await?;
        harness
            .retain(delegate(&intermediary, &holder, UcanSubject::Specific(space.did())).await)
            .await?;

        let proof = harness
            .parity(
                &holder.did(),
                scope(&space, &["storage"]),
                TimeRange::unbounded(),
            )
            .await;
        assert_eq!(proof.expect("chain proves").proofs().len(), 2);
        Ok(())
    }

    #[dialog_common::test]
    async fn it_proves_through_powerline_middle_link() -> Result<()> {
        let harness = Harness::new("prove-mid-powerline").await?;
        let space = signer().await;
        let intermediary = signer().await;
        let holder = signer().await;

        harness
            .retain(delegate(&space, &intermediary, UcanSubject::Specific(space.did())).await)
            .await?;
        harness
            .retain(delegate(&intermediary, &holder, UcanSubject::Any).await)
            .await?;

        let proof = harness
            .parity(
                &holder.did(),
                scope(&space, &["storage"]),
                TimeRange::unbounded(),
            )
            .await;
        assert_eq!(proof.expect("chain proves").proofs().len(), 2);
        Ok(())
    }

    #[dialog_common::test]
    async fn it_proves_through_powerline_powerline_chain() -> Result<()> {
        let harness = Harness::new("prove-powerline-powerline").await?;
        let space = signer().await;
        let intermediary = signer().await;
        let holder = signer().await;

        harness
            .retain(delegate(&space, &intermediary, UcanSubject::Any).await)
            .await?;
        harness
            .retain(delegate(&intermediary, &holder, UcanSubject::Any).await)
            .await?;

        let proof = harness
            .parity(
                &holder.did(),
                scope(&space, &["storage"]),
                TimeRange::unbounded(),
            )
            .await;
        assert_eq!(proof.expect("chain proves").proofs().len(), 2);
        Ok(())
    }

    #[dialog_common::test]
    async fn it_proves_self_authorization_without_certificates() -> Result<()> {
        let harness = Harness::new("prove-self").await?;
        let space = signer().await;

        let proof = harness
            .parity(
                &space.did(),
                scope(&space, &["storage"]),
                TimeRange::unbounded(),
            )
            .await;
        assert!(
            proof
                .expect("self-authorization proves")
                .proofs()
                .is_empty(),
            "self-authorization needs no chain"
        );
        Ok(())
    }

    /// A cyclic delegation graph (A grants B, B grants A, neither reaching
    /// the subject) must terminate with a denial rather than loop. The
    /// certificate-store walk survives this only through its depth bound;
    /// the tree walk's per-path visited set prunes it outright, and both
    /// must agree on the verdict.
    #[dialog_common::test]
    async fn it_terminates_on_a_cyclic_delegation_graph() -> Result<()> {
        let harness = Harness::new("prove-cycle").await?;
        let space = signer().await;
        let alpha = signer().await;
        let beta = signer().await;

        harness
            .retain(delegate(&alpha, &beta, UcanSubject::Specific(space.did())).await)
            .await?;
        harness
            .retain(delegate(&beta, &alpha, UcanSubject::Specific(space.did())).await)
            .await?;

        let proof = harness
            .parity(
                &beta.did(),
                scope(&space, &["storage"]),
                TimeRange::unbounded(),
            )
            .await;
        assert!(matches!(proof, Err(AuthorizeError::UnprovenSubject { .. })));
        Ok(())
    }
}
