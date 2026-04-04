use crate::helpers::unique_name;
use crate::profile::Profile;
use crate::remote::Remote;
use crate::storage::Storage;
use dialog_capability::Provider;

#[cfg(test)]
mod tests {
    use super::*;

    #[dialog_common::test]
    async fn it_builds_operator_from_profile() {
        let storage = Storage::temp_storage();

        let profile = Profile::open(Storage::temp(&unique_name("test")))
            .perform(&storage)
            .await
            .unwrap();

        let operator = profile
            .derive(b"test")
            .network(Remote)
            .build(storage)
            .await
            .unwrap();

        assert_ne!(operator.did(), profile.did());
    }

    #[dialog_common::test]
    async fn operator_key_is_deterministic() {
        let storage = Storage::temp_storage();
        let profile_loc = Storage::temp(&unique_name("det"));

        let profile = Profile::open(profile_loc.clone())
            .perform(&storage)
            .await
            .unwrap();

        let op1 = profile
            .derive(b"same-context")
            .network(Remote)
            .build(storage.clone())
            .await
            .unwrap();

        let op2 = profile
            .derive(b"same-context")
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

        let op1 = profile
            .derive(b"context-a")
            .network(Remote)
            .build(storage.clone())
            .await
            .unwrap();

        let op2 = profile
            .derive(b"context-b")
            .network(Remote)
            .build(storage)
            .await
            .unwrap();

        assert_ne!(op1.did(), op2.did());
    }

    #[cfg(feature = "ucan")]
    mod delegation_tests {
        use super::*;
        use dialog_capability::access::{Permit, ProofChain as _};
        use dialog_capability::Subject;
        use dialog_capability_ucan::scope::Scope;
        use dialog_capability_ucan::Ucan;
        use dialog_credentials::Ed25519Signer;
        use dialog_effects::archive::prelude::{ArchiveExt, SubjectExt as ArchiveSubjectExt};
        use dialog_effects::storage as fx_storage;
        use dialog_varsig::Principal;

        /// Helper: claim authorization via the new Claim<Ucan> effect.
        ///
        /// Uses the profile DID as the capability subject (where the permit
        /// store is mounted), and operator DID as the claimant.
        async fn claim_access(
            operator: &crate::operator::Operator,
            capability: &impl dialog_capability::Ability,
        ) -> Result<dialog_capability_ucan::UcanPermit, dialog_capability::access::AuthorizeError>
        {
            let scope = Scope::from(capability);

            // Subject is the profile DID (where delegations are stored)
            Subject::from(operator.profile_did())
                .attenuate(Permit)
                .invoke(dialog_capability::access::Claim::<Ucan>::new(
                    operator.did(),
                    scope,
                ))
                .perform(operator)
                .await
        }

        /// 1. Self-grant: operator == subject, claim succeeds (no proof chain needed)
        #[dialog_common::test]
        async fn self_grant_produces_delegation() {
            let storage = Storage::temp_storage();

            let profile = Profile::open(Storage::temp(&unique_name("self")))
                .perform(&storage)
                .await
                .unwrap();

            let operator = profile
                .derive(b"alice")
                .network(Remote)
                .build(storage)
                .await
                .unwrap();

            // Claim on the operator's own DID should self-authorize
            let cap = Subject::from(operator.did()).archive().catalog("index");
            let result = claim_access(&operator, &cap).await;

            assert!(
                result.is_ok(),
                "self-grant should succeed: {:?}",
                result.err()
            );
        }

        /// 2. Powerline self-grant: operator == any subject
        #[dialog_common::test]
        async fn powerline_self_grant_produces_delegation() {
            let storage = Storage::temp_storage();

            let profile = Profile::open(Storage::temp(&unique_name("psg")))
                .perform(&storage)
                .await
                .unwrap();

            let operator = profile
                .derive(b"alice")
                .network(Remote)
                .build(storage)
                .await
                .unwrap();

            // Claim on operator's own DID - self-authorized
            let cap = Subject::from(operator.did());
            let result = claim_access(&operator, &cap).await;

            assert!(
                result.is_ok(),
                "powerline self-grant should succeed: {:?}",
                result.err()
            );
        }

        /// 3. Scoped delegation found: .allow(archive/index) → claim for archive/index succeeds
        #[dialog_common::test]
        async fn scoped_delegation_found() {
            let storage = Storage::temp_storage();

            let profile = Profile::open(Storage::temp(&unique_name("found")))
                .perform(&storage)
                .await
                .unwrap();

            let operator = profile
                .derive(b"alice")
                .allow(Subject::any().archive().catalog("index"))
                .network(Remote)
                .build(storage)
                .await
                .unwrap();

            let cap = Subject::from(profile.did()).archive().catalog("index");
            let result = claim_access(&operator, &cap).await;

            assert!(
                result.is_ok(),
                "should find delegation for archive/index: {:?}",
                result.err()
            );
        }

        /// 4. Scoped delegation denied: .allow(archive/index) → claim for archive/secret fails
        #[dialog_common::test]
        async fn scoped_delegation_denied() {
            let storage = Storage::temp_storage();

            let profile = Profile::open(Storage::temp(&unique_name("deny")))
                .perform(&storage)
                .await
                .unwrap();

            let operator = profile
                .derive(b"alice")
                .allow(Subject::any().archive().catalog("index"))
                .network(Remote)
                .build(storage)
                .await
                .unwrap();

            let cap = Subject::from(profile.did()).archive().catalog("secret");
            let result = claim_access(&operator, &cap).await;

            assert!(result.is_err(), "should deny non-delegated archive/secret");
        }

        /// 5. Powerline delegation: .allow(Subject::any()) → claim for anything succeeds
        #[dialog_common::test]
        async fn powerline_delegation_allows_anything() {
            let storage = Storage::temp_storage();

            let profile = Profile::open(Storage::temp(&unique_name("power")))
                .perform(&storage)
                .await
                .unwrap();

            let operator = profile
                .derive(b"admin")
                .allow(Subject::any())
                .network(Remote)
                .build(storage)
                .await
                .unwrap();

            let cap = Subject::from(profile.did())
                .attenuate(fx_storage::Storage)
                .attenuate(fx_storage::Store::new("anything"));
            let result = claim_access(&operator, &cap).await;

            assert!(
                result.is_ok(),
                "powerline should allow any capability: {:?}",
                result.err()
            );
        }

        /// 6. No delegation: nothing allowed → claim fails
        #[dialog_common::test]
        async fn no_delegation_fails() {
            let storage = Storage::temp_storage();

            let profile = Profile::open(Storage::temp(&unique_name("none")))
                .perform(&storage)
                .await
                .unwrap();

            let operator = profile
                .derive(b"alice")
                .network(Remote)
                .build(storage)
                .await
                .unwrap();

            let cap = Subject::from(profile.did()).archive().catalog("index");
            let result = claim_access(&operator, &cap).await;

            assert!(result.is_err(), "should fail with no delegations");
        }

        /// 7. No issuer set: resolves via profile DID
        #[dialog_common::test]
        async fn no_issuer_uses_profile_did() {
            let storage = Storage::temp_storage();

            let profile = Profile::open(Storage::temp(&unique_name("nois")))
                .perform(&storage)
                .await
                .unwrap();

            let operator = profile
                .derive(b"alice")
                .allow(Subject::any().archive().catalog("index"))
                .network(Remote)
                .build(storage)
                .await
                .unwrap();

            let cap = Subject::from(profile.did()).archive().catalog("index");
            let result = claim_access(&operator, &cap).await;

            assert!(
                result.is_ok(),
                "should find chain via profile DID: {:?}",
                result.err()
            );
        }
    }
}
