#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use dialog_capability::access::{Certificate, CertificateStore};
    use dialog_credentials::Ed25519Signer;
    use dialog_effects::storage::{Directory, Location};
    use dialog_storage::resource::Resource;
    use dialog_ucan_core::subject::Subject;
    use dialog_ucan_core::{DelegationBuilder, DelegationChain};
    use dialog_varsig::Principal;

    use crate::{Ucan, UcanDelegation};

    async fn generate_signer() -> Ed25519Signer {
        Ed25519Signer::generate().await.unwrap()
    }

    async fn delegate(
        issuer: &Ed25519Signer,
        audience: &Ed25519Signer,
        subject: Subject,
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

    fn unique_name(prefix: &str) -> String {
        use dialog_common::time;
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let ts = time::now()
            .duration_since(time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
        format!("{prefix}-{ts}-{seq}")
    }

    #[cfg(not(target_arch = "wasm32"))]
    type Store = dialog_storage::provider::FileSystem;

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    type Store = dialog_storage::provider::IndexedDb;

    async fn open_store(name: &str) -> Store {
        let location = Location::new(Directory::Temp, name);
        Store::open(&location).await.unwrap()
    }

    #[dialog_common::test]
    async fn it_saves_and_lists_certificates() {
        let store = open_store(&unique_name("cert-save")).await;
        let space = generate_signer().await;
        let operator = generate_signer().await;

        let delegation = delegate(&space, &operator, Subject::Specific(space.did())).await;

        CertificateStore::<Ucan>::save(&store, &delegation)
            .await
            .unwrap();

        let certs = CertificateStore::<Ucan>::list(&store, &operator.did(), Some(&space.did()))
            .await
            .unwrap();

        assert_eq!(certs.len(), 1);
        assert_eq!(certs[0].issuer(), &space.did());
        assert_eq!(certs[0].audience(), &operator.did());
        assert_eq!(certs[0].subject(), Some(&space.did()));
    }

    #[dialog_common::test]
    async fn it_stores_powerline_certificates() {
        let store = open_store(&unique_name("cert-powerline")).await;
        let space = generate_signer().await;
        let operator = generate_signer().await;

        // Powerline: subject=None means it applies to any subject
        let delegation = delegate(&space, &operator, Subject::Any).await;
        CertificateStore::<Ucan>::save(&store, &delegation)
            .await
            .unwrap();

        // Listed under subject=None
        let certs = CertificateStore::<Ucan>::list(&store, &operator.did(), None)
            .await
            .unwrap();

        assert_eq!(certs.len(), 1);
        assert!(certs[0].subject().is_none());
    }

    #[dialog_common::test]
    async fn it_separates_powerline_from_specific() {
        let store = open_store(&unique_name("cert-separate")).await;
        let space = generate_signer().await;
        let operator = generate_signer().await;

        // Store both a specific and a powerline delegation
        let specific = delegate(&space, &operator, Subject::Specific(space.did())).await;
        let powerline = delegate(&space, &operator, Subject::Any).await;

        CertificateStore::<Ucan>::save(&store, &specific)
            .await
            .unwrap();
        CertificateStore::<Ucan>::save(&store, &powerline)
            .await
            .unwrap();

        // Specific query returns only the specific cert
        let specific_certs =
            CertificateStore::<Ucan>::list(&store, &operator.did(), Some(&space.did()))
                .await
                .unwrap();
        assert_eq!(specific_certs.len(), 1);
        assert_eq!(specific_certs[0].subject(), Some(&space.did()));

        // Powerline query returns only the powerline cert
        let powerline_certs = CertificateStore::<Ucan>::list(&store, &operator.did(), None)
            .await
            .unwrap();
        assert_eq!(powerline_certs.len(), 1);
        assert!(powerline_certs[0].subject().is_none());
    }

    #[dialog_common::test]
    async fn it_returns_empty_for_unknown_audience() {
        let store = open_store(&unique_name("cert-unknown")).await;
        let unknown = generate_signer().await.did();

        let certs = CertificateStore::<Ucan>::list(&store, &unknown, None)
            .await
            .unwrap();

        assert!(certs.is_empty());
    }

    #[dialog_common::test]
    async fn it_isolates_by_audience() {
        let store = open_store(&unique_name("cert-isolate")).await;
        let space = generate_signer().await;
        let operator1 = generate_signer().await;
        let operator2 = generate_signer().await;

        let delegation = delegate(&space, &operator1, Subject::Specific(space.did())).await;
        CertificateStore::<Ucan>::save(&store, &delegation)
            .await
            .unwrap();

        // operator2 should not see operator1's certificates
        let certs = CertificateStore::<Ucan>::list(&store, &operator2.did(), Some(&space.did()))
            .await
            .unwrap();

        assert!(certs.is_empty());
    }

    #[dialog_common::test]
    async fn it_stores_multiple_delegations_for_same_audience() {
        let store = open_store(&unique_name("cert-multi")).await;
        let space1 = generate_signer().await;
        let space2 = generate_signer().await;
        let operator = generate_signer().await;

        let d1 = delegate(&space1, &operator, Subject::Specific(space1.did())).await;
        let d2 = delegate(&space2, &operator, Subject::Specific(space2.did())).await;

        CertificateStore::<Ucan>::save(&store, &d1).await.unwrap();
        CertificateStore::<Ucan>::save(&store, &d2).await.unwrap();

        let certs1 = CertificateStore::<Ucan>::list(&store, &operator.did(), Some(&space1.did()))
            .await
            .unwrap();
        assert_eq!(certs1.len(), 1);

        let certs2 = CertificateStore::<Ucan>::list(&store, &operator.did(), Some(&space2.did()))
            .await
            .unwrap();
        assert_eq!(certs2.len(), 1);
    }

    #[dialog_common::test]
    async fn it_persists_certificates_across_opens() {
        let name = unique_name("cert-persist");
        let space = generate_signer().await;
        let operator = generate_signer().await;

        {
            let store = open_store(&name).await;
            let delegation = delegate(&space, &operator, Subject::Specific(space.did())).await;
            CertificateStore::<Ucan>::save(&store, &delegation)
                .await
                .unwrap();
        }

        let store = open_store(&name).await;
        let certs = CertificateStore::<Ucan>::list(&store, &operator.did(), Some(&space.did()))
            .await
            .unwrap();

        assert_eq!(certs.len(), 1);
    }

    mod volatile {
        use super::*;
        use dialog_storage::provider::Volatile;

        #[dialog_common::test]
        async fn it_saves_and_lists_certificates() {
            let store = Volatile::new();
            let space = generate_signer().await;
            let operator = generate_signer().await;

            let delegation = delegate(&space, &operator, Subject::Specific(space.did())).await;

            CertificateStore::<Ucan>::save(&store, &delegation)
                .await
                .unwrap();

            let certs = CertificateStore::<Ucan>::list(&store, &operator.did(), Some(&space.did()))
                .await
                .unwrap();

            assert_eq!(certs.len(), 1);
        }

        #[dialog_common::test]
        async fn it_finds_powerline_certificates() {
            let store = Volatile::new();
            let space = generate_signer().await;
            let operator = generate_signer().await;

            let delegation = delegate(&space, &operator, Subject::Any).await;
            CertificateStore::<Ucan>::save(&store, &delegation)
                .await
                .unwrap();

            let certs = CertificateStore::<Ucan>::list(&store, &operator.did(), None)
                .await
                .unwrap();

            assert_eq!(certs.len(), 1);
            assert!(certs[0].subject().is_none());
        }
    }

    mod prove {
        use super::*;
        use crate::scope::{Parameters, Scope};
        use dialog_capability::access::{Proof, Prove};
        use dialog_ucan_core::command::Command;
        use dialog_ucan_core::subject::Subject as UcanSubject;

        fn scope(subject: &Ed25519Signer, command: &[&str]) -> Scope {
            Scope {
                subject: UcanSubject::Specific(subject.did()),
                command: Command(command.iter().map(|s| s.to_string()).collect()),
                parameters: Parameters::default(),
            }
        }

        #[dialog_common::test]
        async fn it_proves_with_direct_delegation() {
            let store = open_store(&unique_name("prove-direct")).await;
            let space = generate_signer().await;
            let operator = generate_signer().await;

            let delegation = delegate(&space, &operator, Subject::Specific(space.did())).await;
            CertificateStore::<Ucan>::save(&store, &delegation)
                .await
                .unwrap();

            let access = scope(&space, &["storage"]);
            let prove = Prove::<Ucan>::new(operator.did(), access);
            let proof = CertificateStore::<Ucan>::prove(&store, prove).await;

            assert!(
                proof.is_ok(),
                "direct delegation should prove: {:?}",
                proof.err()
            );
            assert!(!proof.unwrap().proofs().is_empty());
        }

        #[dialog_common::test]
        async fn it_proves_with_powerline_delegation() {
            let store = open_store(&unique_name("prove-powerline")).await;
            let space = generate_signer().await;
            let operator = generate_signer().await;

            let delegation = delegate(&space, &operator, Subject::Any).await;
            CertificateStore::<Ucan>::save(&store, &delegation)
                .await
                .unwrap();

            let access = scope(&space, &["storage"]);
            let prove = Prove::<Ucan>::new(operator.did(), access);
            let proof = CertificateStore::<Ucan>::prove(&store, prove).await;

            assert!(proof.is_ok(), "powerline should prove: {:?}", proof.err());
        }

        #[dialog_common::test]
        async fn it_fails_prove_without_delegation() {
            let store = open_store(&unique_name("prove-none")).await;
            let space = generate_signer().await;
            let operator = generate_signer().await;

            let access = scope(&space, &["storage"]);
            let prove = Prove::<Ucan>::new(operator.did(), access);
            let result = CertificateStore::<Ucan>::prove(&store, prove).await;

            assert!(result.is_err());
        }

        #[dialog_common::test]
        async fn it_fails_prove_for_wrong_audience() {
            let store = open_store(&unique_name("prove-wrong-aud")).await;
            let space = generate_signer().await;
            let operator1 = generate_signer().await;
            let operator2 = generate_signer().await;

            let delegation = delegate(&space, &operator1, Subject::Specific(space.did())).await;
            CertificateStore::<Ucan>::save(&store, &delegation)
                .await
                .unwrap();

            // operator2 has no delegation
            let access = scope(&space, &["storage"]);
            let prove = Prove::<Ucan>::new(operator2.did(), access);
            let result = CertificateStore::<Ucan>::prove(&store, prove).await;

            assert!(result.is_err());
        }

        #[dialog_common::test]
        async fn it_proves_self_authorization() {
            // When principal == subject, prove succeeds without any stored certs
            let store = open_store(&unique_name("prove-self")).await;
            let space = generate_signer().await;

            let access = scope(&space, &["storage"]);
            let prove = Prove::<Ucan>::new(space.did(), access);
            let proof = CertificateStore::<Ucan>::prove(&store, prove).await;

            assert!(proof.is_ok(), "self-authorization should succeed");
            // Self-auth proof has no delegation chain
            assert!(proof.unwrap().proofs().is_empty());
        }

        async fn delegate_with_expiration(
            issuer: &Ed25519Signer,
            audience: &Ed25519Signer,
            subject: Subject,
            expiration: dialog_ucan_core::time::timestamp::Timestamp,
        ) -> UcanDelegation {
            let delegation = DelegationBuilder::new()
                .issuer(issuer.clone())
                .audience(audience)
                .subject(subject)
                .command(vec!["storage".to_string()])
                .expiration(expiration)
                .try_build()
                .await
                .unwrap();

            UcanDelegation::new(DelegationChain::new(delegation))
        }

        #[dialog_common::test]
        async fn it_rejects_expired_delegation() {
            use dialog_capability::access::TimeRange;
            use dialog_ucan_core::time::timestamp::Timestamp;

            let store = open_store(&unique_name("prove-expired")).await;
            let space = generate_signer().await;
            let operator = generate_signer().await;

            // Create a delegation that expired 1 hour ago
            let now_secs = dialog_common::time::now()
                .duration_since(dialog_common::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();
            let past = Timestamp::try_from((now_secs - 3600) as i128).unwrap();

            let delegation =
                delegate_with_expiration(&space, &operator, Subject::Specific(space.did()), past)
                    .await;

            CertificateStore::<Ucan>::save(&store, &delegation)
                .await
                .unwrap();

            // Request validity at "now", which is after the expiration
            let now = dialog_common::time::now()
                .duration_since(dialog_common::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();
            let access = scope(&space, &["storage"]);
            let mut prove = Prove::<Ucan>::new(operator.did(), access);
            prove.duration = TimeRange {
                not_before: Some(now),
                expiration: Some(now + 60),
            };
            let result = CertificateStore::<Ucan>::prove(&store, prove).await;

            assert!(result.is_err(), "expired delegation should not prove");
        }

        #[dialog_common::test]
        async fn it_proves_via_powerline_chain() {
            // space delegates to intermediary via powerline (any subject),
            // intermediary re-delegates to operator for a specific subject.
            // prove should walk the chain: operator -> intermediary -> space.
            let store = open_store(&unique_name("prove-powerline-chain")).await;
            let space = generate_signer().await;
            let intermediary = generate_signer().await;
            let operator = generate_signer().await;

            // space -> intermediary (powerline)
            let d1 = delegate(&space, &intermediary, Subject::Any).await;
            CertificateStore::<Ucan>::save(&store, &d1).await.unwrap();

            // intermediary -> operator (specific subject = space)
            let d2 = delegate(&intermediary, &operator, Subject::Specific(space.did())).await;
            CertificateStore::<Ucan>::save(&store, &d2).await.unwrap();

            let access = scope(&space, &["storage"]);
            let prove = Prove::<Ucan>::new(operator.did(), access);
            let proof = CertificateStore::<Ucan>::prove(&store, prove).await;

            assert!(
                proof.is_ok(),
                "powerline chain should prove: {:?}",
                proof.err()
            );
            assert_eq!(proof.unwrap().proofs().len(), 2);
        }

        #[dialog_common::test]
        async fn it_fails_prove_for_wrong_subject() {
            let store = open_store(&unique_name("prove-wrong-subj")).await;
            let space1 = generate_signer().await;
            let space2 = generate_signer().await;
            let operator = generate_signer().await;

            // Delegated for space1 only
            let delegation = delegate(&space1, &operator, Subject::Specific(space1.did())).await;
            CertificateStore::<Ucan>::save(&store, &delegation)
                .await
                .unwrap();

            // Try to prove for space2
            let access = scope(&space2, &["storage"]);
            let prove = Prove::<Ucan>::new(operator.did(), access);
            let result = CertificateStore::<Ucan>::prove(&store, prove).await;

            assert!(result.is_err(), "wrong subject should not prove");
        }

        #[dialog_common::test]
        async fn it_proves_through_powerline_middle_link() {
            // space -> intermediary (specific) -> operator (powerline)
            // The intermediary has a powerline delegation to operator,
            // meaning operator can act on any subject the intermediary
            // has access to.
            let store = open_store(&unique_name("prove-mid-powerline")).await;
            let space = generate_signer().await;
            let intermediary = generate_signer().await;
            let operator = generate_signer().await;

            // space -> intermediary (specific for space)
            let d1 = delegate(&space, &intermediary, Subject::Specific(space.did())).await;
            CertificateStore::<Ucan>::save(&store, &d1).await.unwrap();

            // intermediary -> operator (powerline)
            let d2 = delegate(&intermediary, &operator, Subject::Any).await;
            CertificateStore::<Ucan>::save(&store, &d2).await.unwrap();

            let access = scope(&space, &["storage"]);
            let prove = Prove::<Ucan>::new(operator.did(), access);
            let proof = CertificateStore::<Ucan>::prove(&store, prove).await;

            assert!(
                proof.is_ok(),
                "chain through powerline middle should prove: {:?}",
                proof.err()
            );
            assert_eq!(proof.unwrap().proofs().len(), 2);
        }

        #[dialog_common::test]
        async fn it_proves_through_powerline_chain() {
            // space -> intermediary (powerline) -> operator (powerline)
            let store = open_store(&unique_name("prove-powerline-powerline")).await;
            let space = generate_signer().await;
            let intermediary = generate_signer().await;
            let operator = generate_signer().await;

            let d1 = delegate(&space, &intermediary, Subject::Any).await;
            CertificateStore::<Ucan>::save(&store, &d1).await.unwrap();

            let d2 = delegate(&intermediary, &operator, Subject::Any).await;
            CertificateStore::<Ucan>::save(&store, &d2).await.unwrap();

            let access = scope(&space, &["storage"]);
            let prove = Prove::<Ucan>::new(operator.did(), access);
            let proof = CertificateStore::<Ucan>::prove(&store, prove).await;

            assert!(
                proof.is_ok(),
                "powerline -> powerline chain should prove: {:?}",
                proof.err()
            );
            assert_eq!(proof.unwrap().proofs().len(), 2);
        }

        #[dialog_common::test]
        async fn it_proves_when_audience_is_subject() {
            // Space delegates to itself (audience == subject). This happens
            // when a space grants itself capabilities for internal operations.
            let store = open_store(&unique_name("prove-audience-is-subject")).await;
            let space = generate_signer().await;

            let delegation = delegate(&space, &space, Subject::Specific(space.did())).await;
            CertificateStore::<Ucan>::save(&store, &delegation)
                .await
                .unwrap();

            // A different operator tries to prove via the self-delegation
            let operator = generate_signer().await;
            let d2 = delegate(&space, &operator, Subject::Specific(space.did())).await;
            CertificateStore::<Ucan>::save(&store, &d2).await.unwrap();

            let access = scope(&space, &["storage"]);
            let prove = Prove::<Ucan>::new(operator.did(), access);
            let proof = CertificateStore::<Ucan>::prove(&store, prove).await;

            assert!(
                proof.is_ok(),
                "should prove via direct delegation: {:?}",
                proof.err()
            );
        }
    }
}
