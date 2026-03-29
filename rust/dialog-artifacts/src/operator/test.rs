#[cfg(test)]
mod tests {
    use crate::profile::Profile;
    use crate::remote::Remote;
    use crate::storage::Storage;

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

    #[dialog_common::test]
    async fn it_builds_operator_from_profile() {
        let storage = Storage::temp_storage();

        let profile = Profile::open(Storage::temp(&unique_name("build")))
            .perform(&storage)
            .await
            .unwrap();

        let operator = profile
            .operator(b"alice")
            .network(Remote)
            .build(storage)
            .await
            .unwrap();

        assert_ne!(operator.profile_did(), operator.did());
    }

    #[dialog_common::test]
    async fn operator_key_is_deterministic() {
        let storage = Storage::temp_storage();

        let profile = Profile::open(Storage::temp(&unique_name("det")))
            .perform(&storage)
            .await
            .unwrap();

        let op1 = profile
            .operator(b"alice")
            .network(Remote)
            .build(storage.clone())
            .await
            .unwrap();

        let op2 = profile
            .operator(b"alice")
            .network(Remote)
            .build(storage)
            .await
            .unwrap();

        assert_eq!(op1.did(), op2.did());
    }

    #[dialog_common::test]
    async fn different_contexts_produce_different_operators() {
        let storage = Storage::temp_storage();

        let profile = Profile::open(Storage::temp(&unique_name("ctx")))
            .perform(&storage)
            .await
            .unwrap();

        let alice = profile
            .operator(b"alice")
            .network(Remote)
            .build(storage.clone())
            .await
            .unwrap();

        let bob = profile
            .operator(b"bob")
            .network(Remote)
            .build(storage)
            .await
            .unwrap();

        assert_ne!(alice.did(), bob.did());
    }

    #[dialog_common::test]
    async fn end_to_end_profile_operator_repository() {
        use dialog_capability::Subject;
        use dialog_effects::archive::prelude::{ArchiveExt, SubjectExt as ArchiveSubjectExt};
        use dialog_effects::memory::prelude::{MemoryExt, SubjectExt as MemorySubjectExt};

        let storage = Storage::temp_storage();

        let profile = Profile::open(Storage::temp(&unique_name("e2e")))
            .perform(&storage)
            .await
            .unwrap();

        let operator = profile
            .operator(b"alice")
            .allow(Subject::any().archive().catalog("index"))
            .allow(Subject::any().archive().catalog("content"))
            .allow(Subject::any().memory().space("local"))
            .network(Remote)
            .build(storage)
            .await
            .unwrap();

        let home = crate::Repository::open(Storage::temp(&unique_name("home")))
            .perform(&operator)
            .await
            .unwrap();

        assert!(!home.did().to_string().is_empty());
        assert_ne!(profile.did(), home.did());
        assert!(operator.storage().stores().contains(&home.did()));
    }

    #[cfg(feature = "ucan")]
    mod delegation_tests {
        use super::*;
        use dialog_capability::Subject;
        use dialog_capability::ucan::Ucan;
        use dialog_credentials::Ed25519Signer;
        use dialog_effects::archive::prelude::{ArchiveExt, SubjectExt as ArchiveSubjectExt};
        use dialog_effects::storage as fx_storage;
        use dialog_varsig::Principal;

        /// 1. Self-grant: issuer == subject, delegation succeeds (no proof chain needed)
        #[dialog_common::test]
        async fn self_grant_produces_delegation() {
            let storage = Storage::temp_storage();

            let profile = Profile::open(Storage::temp(&unique_name("self")))
                .perform(&storage)
                .await
                .unwrap();

            let operator = profile
                .operator(b"alice")
                .network(Remote)
                .build(storage)
                .await
                .unwrap();

            let signer = Ed25519Signer::generate().await.unwrap();
            let did = signer.did();

            let result = Ucan::delegate(&Subject::from(did).archive().catalog("index"))
                .issuer(signer)
                .audience(operator.did())
                .perform(&operator)
                .await;

            assert!(
                result.is_ok(),
                "self-grant should succeed: {:?}",
                result.err()
            );
        }

        /// 2. Powerline self-grant: subject = Any, issuer set → delegation succeeds
        #[dialog_common::test]
        async fn powerline_self_grant_produces_delegation() {
            let storage = Storage::temp_storage();

            let profile = Profile::open(Storage::temp(&unique_name("psg")))
                .perform(&storage)
                .await
                .unwrap();

            let operator = profile
                .operator(b"alice")
                .network(Remote)
                .build(storage)
                .await
                .unwrap();

            let result = Ucan::delegate(&Subject::any())
                .issuer(Ed25519Signer::generate().await.unwrap())
                .audience(operator.did())
                .perform(&operator)
                .await;

            assert!(
                result.is_ok(),
                "powerline self-grant should succeed: {:?}",
                result.err()
            );
        }

        /// 3. Scoped delegation found: .allow(archive/index) → delegate for archive/index succeeds
        #[dialog_common::test]
        async fn scoped_delegation_found() {
            use dialog_effects::archive::prelude::{ArchiveExt, SubjectExt as ArchiveSubjectExt};

            let storage = Storage::temp_storage();

            let profile = Profile::open(Storage::temp(&unique_name("found")))
                .perform(&storage)
                .await
                .unwrap();

            let operator = profile
                .operator(b"alice")
                .allow(Subject::any().archive().catalog("index"))
                .network(Remote)
                .build(storage)
                .await
                .unwrap();

            let result = Ucan::delegate(&Subject::from(profile.did()).archive().catalog("index"))
                .audience(operator.did())
                .perform(&operator)
                .await;

            assert!(
                result.is_ok(),
                "should find delegation for archive/index: {:?}",
                result.err()
            );
        }

        /// 4. Scoped delegation denied: .allow(archive/index) → delegate for archive/secret fails
        #[dialog_common::test]
        async fn scoped_delegation_denied() {
            use dialog_effects::archive::prelude::{ArchiveExt, SubjectExt as ArchiveSubjectExt};

            let storage = Storage::temp_storage();

            let profile = Profile::open(Storage::temp(&unique_name("deny")))
                .perform(&storage)
                .await
                .unwrap();

            let operator = profile
                .operator(b"alice")
                .allow(Subject::any().archive().catalog("index"))
                .network(Remote)
                .build(storage)
                .await
                .unwrap();

            let result = Ucan::delegate(&Subject::from(profile.did()).archive().catalog("secret"))
                .audience(operator.did())
                .perform(&operator)
                .await;

            assert!(result.is_err(), "should deny non-delegated archive/secret");
        }

        /// 5. Powerline delegation: .allow(Subject::any()) → delegate for anything succeeds
        #[dialog_common::test]
        async fn powerline_delegation_allows_anything() {
            let storage = Storage::temp_storage();

            let profile = Profile::open(Storage::temp(&unique_name("power")))
                .perform(&storage)
                .await
                .unwrap();

            let operator = profile
                .operator(b"admin")
                .allow(Subject::any())
                .network(Remote)
                .build(storage)
                .await
                .unwrap();

            let result = Ucan::delegate(
                &Subject::from(profile.did())
                    .attenuate(fx_storage::Storage)
                    .attenuate(fx_storage::Store::new("anything")),
            )
            .audience(operator.did())
            .perform(&operator)
            .await;

            assert!(
                result.is_ok(),
                "powerline should allow any capability: {:?}",
                result.err()
            );
        }

        /// 6. No delegation: nothing allowed → delegate fails
        #[dialog_common::test]
        async fn no_delegation_fails() {
            use dialog_effects::archive::prelude::{ArchiveExt, SubjectExt as ArchiveSubjectExt};

            let storage = Storage::temp_storage();

            let profile = Profile::open(Storage::temp(&unique_name("none")))
                .perform(&storage)
                .await
                .unwrap();

            let operator = profile
                .operator(b"alice")
                .network(Remote)
                .build(storage)
                .await
                .unwrap();

            let result = Ucan::delegate(&Subject::from(profile.did()).archive().catalog("index"))
                .audience(operator.did())
                .perform(&operator)
                .await;

            assert!(result.is_err(), "should fail with no delegations");
        }

        /// 7. No issuer set: resolves via Identify/Sign, finds chain via profile DID
        #[dialog_common::test]
        async fn no_issuer_uses_profile_did() {
            use dialog_effects::archive::prelude::{ArchiveExt, SubjectExt as ArchiveSubjectExt};

            let storage = Storage::temp_storage();

            let profile = Profile::open(Storage::temp(&unique_name("noiss")))
                .perform(&storage)
                .await
                .unwrap();

            let operator = profile
                .operator(b"alice")
                .allow(Subject::any().archive().catalog("data"))
                .network(Remote)
                .build(storage)
                .await
                .unwrap();

            let result = Ucan::delegate(&Subject::from(profile.did()).archive().catalog("data"))
                .audience(operator.did())
                .perform(&operator)
                .await;

            assert!(
                result.is_ok(),
                "should find chain without explicit issuer: {:?}",
                result.err()
            );
        }
    }
}
