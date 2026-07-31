//! Memoized proof chains.
//!
//! Proving access walks the certificate store breadth-first, reading and
//! decoding certificates at every hop. The chain it finds only changes
//! when a certificate is retained or the operator's key rotates, while
//! the invocation built on top of it is fresh every time. The memo keeps
//! the chain so repeated authorizations pay for the walk once.

use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use dialog_capability::Did;
use dialog_capability::access::{Certificate, Proof, Protocol, Prove, Scope as _, TimeRange};
use dialog_common::time::{UNIX_EPOCH, now};
use dialog_common::{ConditionalSend, ConditionalSync};

/// How long a memoized chain may be reused before it is proven again.
///
/// Bounds how long a client can keep presenting a chain that the access
/// service has already learned to refuse.
const RETENTION_SECONDS: u64 = 60;

/// How much validity a memoized chain must have left over to be reused.
///
/// Keeps the client from building an invocation on a chain that expires
/// while the request is in flight.
const EXPIRY_MARGIN_SECONDS: u64 = 60;

/// The current time in whole seconds since the Unix epoch.
pub fn unix_now() -> u64 {
    now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// What a memoized chain answers for: a principal claiming a command
/// over a subject, proven against one certificate store.
///
/// The principal is the operator DID, so a rotated operator misses.
/// Parameters are deliberately absent — they change on every request,
/// and the certificates are re-checked against them on recall.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Key {
    /// The capability subject the request was addressed to, which is
    /// what routes it to a certificate store.
    store: Did,
    /// The principal claiming access.
    principal: Did,
    /// The subject the claimed access is over.
    subject: Did,
    /// The command being claimed.
    command: Vec<String>,
}

impl Key {
    fn of<P: Protocol>(store: &Did, input: &Prove<P>) -> Self {
        Self {
            store: store.clone(),
            principal: input.principal.clone(),
            subject: input.access.subject().clone(),
            command: input.access.command().to_vec(),
        }
    }
}

/// A proven chain, type-erased so that one memo serves every protocol.
trait Entry: ConditionalSend + ConditionalSync {
    fn as_any(&self) -> &dyn Any;
}

struct Chain<P: Protocol> {
    certificates: Vec<P::Certificate>,
    proven_at: u64,
}

impl<P> Entry for Chain<P>
where
    P: Protocol,
    P::Certificate: 'static,
{
    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Chains proven earlier, shared by every handle onto the memo.
#[derive(Clone, Default)]
pub struct ProofMemo {
    entries: Arc<Mutex<HashMap<Key, Box<dyn Entry>>>>,
}

impl ProofMemo {
    /// Recall a chain proven earlier for the same principal, subject and
    /// command, re-checked against this request.
    ///
    /// Returns `None` — leaving the caller to prove again — when nothing
    /// was memoized, when the memo has aged past its retention, when the
    /// certificates do not cover this request's parameters or duration,
    /// or when too little of the chain's validity is left.
    pub fn recall<P>(&self, store: &Did, input: &Prove<P>, now: u64) -> Option<P::Proof>
    where
        P: Protocol,
        P::Certificate: 'static,
    {
        let entries = self.entries.lock().ok()?;
        let chain = entries
            .get(&Key::of(store, input))?
            .as_any()
            .downcast_ref::<Chain<P>>()?;

        if now.saturating_sub(chain.proven_at) >= RETENTION_SECONDS {
            return None;
        }

        // The walk that found this chain checked every certificate
        // against the access it was proven for. This request carries its
        // own parameters, so check them again — in memory, without
        // touching the store.
        let mut duration = TimeRange::unbounded();
        for certificate in &chain.certificates {
            let range = certificate.verify(&input.access).ok()?;
            if !range.covers(&input.duration) {
                return None;
            }
            duration = duration.intersect(&range);
        }

        if !duration.contains(now.saturating_add(EXPIRY_MARGIN_SECONDS)) {
            return None;
        }

        let mut proof = P::Proof::new(input.access.clone());
        for certificate in &chain.certificates {
            proof.push(certificate.clone());
        }
        proof.set_duration(duration);

        Some(proof)
    }

    /// Memoize the chain behind a freshly proven proof.
    pub fn remember<P>(&self, store: &Did, input: &Prove<P>, proof: &P::Proof, now: u64)
    where
        P: Protocol,
        P::Certificate: 'static,
    {
        let chain = Chain::<P> {
            certificates: proof.proofs().to_vec(),
            proven_at: now,
        };

        if let Ok(mut entries) = self.entries.lock() {
            entries.insert(Key::of(store, input), Box::new(chain));
        }
    }

    /// Forget every memoized chain.
    ///
    /// A newly retained certificate can complete or shorten a chain for
    /// any subject — a powerline certificate for all of them at once —
    /// so the memo is dropped whole rather than guessing which entries
    /// the new certificate reaches.
    pub fn forget(&self) {
        if let Ok(mut entries) = self.entries.lock() {
            entries.clear();
        }
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use std::sync::atomic::{AtomicUsize, Ordering};

    use dialog_capability::access::{
        AuthorizeError, Certificate as _, CertificateStore, Delegation as _,
    };
    use dialog_credentials::Ed25519Signer;
    use dialog_ucan::{Parameters, Scope, Ucan, UcanCertificate, UcanDelegation};
    use dialog_ucan_core::command::Command;
    use dialog_ucan_core::subject::Subject as UcanSubject;
    use dialog_ucan_core::time::timestamp::Timestamp;
    use dialog_ucan_core::{DelegationBuilder, DelegationChain};
    use dialog_varsig::Principal;

    use super::*;

    /// A certificate store that counts the lookups made against it, so a
    /// test can tell a proof that walked the store from one the memo
    /// answered.
    #[derive(Default)]
    struct CountingStore {
        certificates: Mutex<Vec<UcanCertificate>>,
        lookups: AtomicUsize,
    }

    impl CountingStore {
        fn lookups(&self) -> usize {
            self.lookups.load(Ordering::Relaxed)
        }
    }

    #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
    impl CertificateStore<Ucan> for CountingStore {
        async fn list(
            &self,
            audience: &Did,
            subject: Option<&Did>,
        ) -> Result<Vec<UcanCertificate>, AuthorizeError> {
            self.lookups.fetch_add(1, Ordering::Relaxed);

            let certificates = self.certificates.lock().unwrap();
            Ok(certificates
                .iter()
                .filter(|certificate| {
                    certificate.audience() == audience && certificate.subject() == subject
                })
                .cloned()
                .collect())
        }

        async fn save(&self, delegation: &UcanDelegation) -> Result<(), AuthorizeError> {
            let mut certificates = self.certificates.lock().unwrap();
            certificates.extend(delegation.certificates());
            Ok(())
        }
    }

    async fn signer() -> Ed25519Signer {
        Ed25519Signer::generate().await.unwrap()
    }

    async fn delegate(
        issuer: &Ed25519Signer,
        audience: &Ed25519Signer,
        subject: UcanSubject,
        command: &[&str],
        expiration: Option<u64>,
    ) -> UcanDelegation {
        let mut builder = DelegationBuilder::new()
            .issuer(issuer.clone())
            .audience(audience)
            .subject(subject)
            .command(command.iter().map(|segment| segment.to_string()).collect());

        if let Some(expiration) = expiration {
            builder = builder.expiration(Timestamp::try_from(expiration as i128).unwrap());
        }

        UcanDelegation::new(DelegationChain::new(builder.try_build().await.unwrap()))
    }

    fn scope(subject: &Ed25519Signer, command: &[&str]) -> Scope {
        Scope {
            subject: UcanSubject::Specific(subject.did()),
            command: Command(command.iter().map(|segment| segment.to_string()).collect()),
            parameters: Parameters::default(),
        }
    }

    /// Prove through the certificates and memoize the result, the way
    /// the operator's `Authorize` provider does on a miss.
    async fn prove(
        certificates: &CountingStore,
        memo: &ProofMemo,
        addressed_to: &Did,
        input: &Prove<Ucan>,
        now: u64,
    ) -> Result<(), AuthorizeError> {
        let proof = CertificateStore::<Ucan>::prove(certificates, input.clone()).await?;
        memo.remember::<Ucan>(addressed_to, input, &proof, now);
        Ok(())
    }

    #[dialog_common::test]
    async fn it_proves_once_across_repeated_authorizations() {
        let subject = signer().await;
        let operator = signer().await;
        let addressed_to = signer().await.did();
        let certificates = CountingStore::default();
        let memo = ProofMemo::default();
        let now = 1_000;

        CertificateStore::<Ucan>::save(
            &certificates,
            &delegate(
                &subject,
                &operator,
                UcanSubject::Specific(subject.did()),
                &["storage"],
                None,
            )
            .await,
        )
        .await
        .unwrap();

        let input = Prove::<Ucan>::new(operator.did(), scope(&subject, &["storage"]));

        assert!(memo.recall::<Ucan>(&addressed_to, &input, now).is_none());
        prove(&certificates, &memo, &addressed_to, &input, now)
            .await
            .unwrap();

        let lookups = certificates.lookups();
        assert!(lookups > 0, "the first proof walks the store");

        for _ in 0..5 {
            let recalled = memo
                .recall::<Ucan>(&addressed_to, &input, now)
                .expect("the memoized chain answers");
            assert_eq!(recalled.proofs().len(), 1);
        }

        assert_eq!(
            certificates.lookups(),
            lookups,
            "later authorizations do not walk the store"
        );
    }

    #[dialog_common::test]
    async fn it_reproves_after_retaining_a_new_certificate() {
        let subject = signer().await;
        let operator = signer().await;
        let addressed_to = signer().await.did();
        let certificates = CountingStore::default();
        let memo = ProofMemo::default();
        let now = 1_000;

        CertificateStore::<Ucan>::save(
            &certificates,
            &delegate(
                &subject,
                &operator,
                UcanSubject::Specific(subject.did()),
                &["storage"],
                None,
            )
            .await,
        )
        .await
        .unwrap();

        let input = Prove::<Ucan>::new(operator.did(), scope(&subject, &["storage"]));
        prove(&certificates, &memo, &addressed_to, &input, now)
            .await
            .unwrap();
        assert!(memo.recall::<Ucan>(&addressed_to, &input, now).is_some());

        // Retaining is a save followed by dropping the memo, which is
        // what the operator's `Retain` provider does.
        let other = signer().await;
        CertificateStore::<Ucan>::save(
            &certificates,
            &delegate(&subject, &other, UcanSubject::Any, &["storage"], None).await,
        )
        .await
        .unwrap();
        memo.forget();

        assert!(
            memo.recall::<Ucan>(&addressed_to, &input, now).is_none(),
            "a retained certificate invalidates what was memoized"
        );

        let lookups = certificates.lookups();
        prove(&certificates, &memo, &addressed_to, &input, now)
            .await
            .unwrap();
        assert!(
            certificates.lookups() > lookups,
            "the chain is proven again"
        );
    }

    #[dialog_common::test]
    async fn it_reproves_when_the_cached_chain_nears_expiry() {
        let subject = signer().await;
        let operator = signer().await;
        let addressed_to = signer().await.did();
        let certificates = CountingStore::default();
        let memo = ProofMemo::default();
        let now = 1_000;

        // Valid for another half hour when proven, but only 30 seconds
        // when recalled — less than the margin a request needs.
        let expiration = now + 1_800;
        CertificateStore::<Ucan>::save(
            &certificates,
            &delegate(
                &subject,
                &operator,
                UcanSubject::Specific(subject.did()),
                &["storage"],
                Some(expiration),
            )
            .await,
        )
        .await
        .unwrap();

        let input = Prove::<Ucan>::new(operator.did(), scope(&subject, &["storage"]));
        prove(&certificates, &memo, &addressed_to, &input, now)
            .await
            .unwrap();
        assert!(memo.recall::<Ucan>(&addressed_to, &input, now).is_some());

        let nearly_expired = expiration - 30;
        prove(&certificates, &memo, &addressed_to, &input, nearly_expired)
            .await
            .unwrap();

        assert!(
            memo.recall::<Ucan>(&addressed_to, &input, nearly_expired)
                .is_none(),
            "a chain that expires within the margin is proven again"
        );
    }

    #[dialog_common::test]
    async fn it_reproves_after_the_memo_ages_out() {
        let subject = signer().await;
        let operator = signer().await;
        let addressed_to = signer().await.did();
        let certificates = CountingStore::default();
        let memo = ProofMemo::default();
        let now = 1_000;

        CertificateStore::<Ucan>::save(
            &certificates,
            &delegate(
                &subject,
                &operator,
                UcanSubject::Specific(subject.did()),
                &["storage"],
                None,
            )
            .await,
        )
        .await
        .unwrap();

        let input = Prove::<Ucan>::new(operator.did(), scope(&subject, &["storage"]));
        prove(&certificates, &memo, &addressed_to, &input, now)
            .await
            .unwrap();

        assert!(
            memo.recall::<Ucan>(&addressed_to, &input, now + RETENTION_SECONDS - 1)
                .is_some()
        );
        assert!(
            memo.recall::<Ucan>(&addressed_to, &input, now + RETENTION_SECONDS)
                .is_none(),
            "a chain the service may have stopped honouring is proven again"
        );
    }

    #[dialog_common::test]
    async fn it_scopes_the_memo_by_subject_and_command() {
        let subject = signer().await;
        let other_subject = signer().await;
        let operator = signer().await;
        let addressed_to = signer().await.did();
        let certificates = CountingStore::default();
        let memo = ProofMemo::default();
        let now = 1_000;

        // A powerline grant over the root command: it verifies against
        // any subject and any command, so only the memo's key keeps one
        // scope's chain out of another's.
        CertificateStore::<Ucan>::save(
            &certificates,
            &delegate(&subject, &operator, UcanSubject::Any, &[], None).await,
        )
        .await
        .unwrap();

        let input = Prove::<Ucan>::new(operator.did(), scope(&subject, &["storage"]));
        prove(&certificates, &memo, &addressed_to, &input, now)
            .await
            .unwrap();
        assert!(memo.recall::<Ucan>(&addressed_to, &input, now).is_some());

        let other_command = Prove::<Ucan>::new(operator.did(), scope(&subject, &["archive"]));
        assert!(
            memo.recall::<Ucan>(&addressed_to, &other_command, now)
                .is_none(),
            "a chain proven for one command does not answer for another"
        );

        let other_subject = Prove::<Ucan>::new(operator.did(), scope(&other_subject, &["storage"]));
        assert!(
            memo.recall::<Ucan>(&addressed_to, &other_subject, now)
                .is_none(),
            "a chain proven for one subject does not answer for another"
        );
    }

    #[dialog_common::test]
    async fn it_scopes_the_memo_by_principal_and_certificate_store() {
        let subject = signer().await;
        let operator = signer().await;
        let rotated = signer().await;
        let addressed_to = signer().await.did();
        let elsewhere = signer().await.did();
        let certificates = CountingStore::default();
        let memo = ProofMemo::default();
        let now = 1_000;

        CertificateStore::<Ucan>::save(
            &certificates,
            &delegate(&subject, &operator, UcanSubject::Any, &[], None).await,
        )
        .await
        .unwrap();

        let input = Prove::<Ucan>::new(operator.did(), scope(&subject, &["storage"]));
        prove(&certificates, &memo, &addressed_to, &input, now)
            .await
            .unwrap();

        let rotated = Prove::<Ucan>::new(rotated.did(), scope(&subject, &["storage"]));
        assert!(
            memo.recall::<Ucan>(&addressed_to, &rotated, now).is_none(),
            "a rotated operator does not inherit the previous one's chain"
        );

        assert!(
            memo.recall::<Ucan>(&elsewhere, &input, now).is_none(),
            "a chain proven against one certificate store does not answer for another"
        );
    }
}
