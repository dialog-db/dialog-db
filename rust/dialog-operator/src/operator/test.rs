use crate::helpers::unique_name;
use crate::network::Network;
use crate::profile::Profile;
use dialog_storage::provider::storage::{Storage, VolatileSpace};

#[cfg(test)]
mod tests {
    use super::*;

    #[dialog_common::test]
    async fn it_builds_operator_from_profile() {
        let storage = Storage::volatile();

        let profile = Profile::open(unique_name("test"))
            .perform(&storage)
            .await
            .unwrap();

        let operator = profile
            .derive(b"test")
            .network(Network)
            .build(storage)
            .await
            .unwrap();

        assert!(!operator.did().to_string().is_empty());
    }

    #[dialog_common::test]
    async fn it_derives_different_operators_from_different_contexts() {
        let storage1 = Storage::volatile();
        let profile1 = Profile::open(unique_name("ctx1"))
            .perform(&storage1)
            .await
            .unwrap();
        let op1 = profile1
            .derive(b"context-a")
            .network(Network)
            .build(storage1)
            .await
            .unwrap();

        let storage2 = Storage::volatile();
        let profile2 = Profile::open(unique_name("ctx2"))
            .perform(&storage2)
            .await
            .unwrap();
        let op2 = profile2
            .derive(b"context-b")
            .network(Network)
            .build(storage2)
            .await
            .unwrap();

        assert_ne!(op1.did(), op2.did());
    }

    mod delegation_tests {
        use super::*;
        use dialog_capability::Subject;
        use dialog_capability::access::{self as cap_access, Access, AuthorizeError};
        use dialog_effects::archive::prelude::{ArchiveExt, ArchiveSubjectExt};
        use dialog_ucan::scope::Scope;
        use dialog_ucan::{Ucan, UcanProof};

        async fn claim_access(
            operator: &super::super::super::Operator<VolatileSpace>,
            capability: &impl dialog_capability::Ability,
        ) -> Result<UcanProof, AuthorizeError> {
            let scope = Scope::from(capability);

            Subject::from(operator.profile_did())
                .attenuate(Access)
                .invoke(cap_access::Prove::<Ucan>::new(operator.did(), scope))
                .perform(operator)
                .await
        }

        #[dialog_common::test]
        async fn self_grant_produces_delegation() {
            let storage = Storage::volatile();

            let profile = Profile::open(unique_name("self"))
                .perform(&storage)
                .await
                .unwrap();

            let operator = profile
                .derive(b"alice")
                .network(Network)
                .build(storage)
                .await
                .unwrap();

            let cap = Subject::from(operator.did()).archive().catalog("index");
            let result = claim_access(&operator, &cap).await;

            assert!(
                result.is_ok(),
                "self-grant should succeed: {:?}",
                result.err()
            );
        }

        #[dialog_common::test]
        async fn powerline_self_grant_produces_delegation() {
            let storage = Storage::volatile();

            let profile = Profile::open(unique_name("psg"))
                .perform(&storage)
                .await
                .unwrap();

            let operator = profile
                .derive(b"alice")
                .network(Network)
                .build(storage)
                .await
                .unwrap();

            let cap = Subject::from(operator.did());
            let result = claim_access(&operator, &cap).await;

            assert!(
                result.is_ok(),
                "powerline self-grant should succeed: {:?}",
                result.err()
            );
        }

        #[dialog_common::test]
        async fn scoped_delegation_found() {
            let storage = Storage::volatile();

            let profile = Profile::open(unique_name("found"))
                .perform(&storage)
                .await
                .unwrap();

            let operator = profile
                .derive(b"alice")
                .allow(Subject::any().archive().catalog("index"))
                .network(Network)
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

        #[dialog_common::test]
        async fn scoped_delegation_denied() {
            let storage = Storage::volatile();

            let profile = Profile::open(unique_name("deny"))
                .perform(&storage)
                .await
                .unwrap();

            let operator = profile
                .derive(b"alice")
                .allow(Subject::any().archive().catalog("index"))
                .network(Network)
                .build(storage)
                .await
                .unwrap();

            let cap = Subject::from(profile.did()).archive().catalog("secret");
            let result = claim_access(&operator, &cap).await;

            assert!(result.is_err(), "should deny non-delegated archive/secret");
        }

        #[dialog_common::test]
        async fn powerline_delegation_allows_anything() {
            let storage = Storage::volatile();

            let profile = Profile::open(unique_name("power"))
                .perform(&storage)
                .await
                .unwrap();

            use dialog_effects::storage as fx_storage;
            let operator = profile
                .derive(b"admin")
                .allow(Subject::any())
                .network(Network)
                .build(storage)
                .await
                .unwrap();

            let cap = Subject::from(profile.did()).attenuate(fx_storage::Storage);
            let result = claim_access(&operator, &cap).await;

            assert!(
                result.is_ok(),
                "powerline should allow any capability: {:?}",
                result.err()
            );
        }

        #[dialog_common::test]
        async fn no_delegation_fails() {
            let storage = Storage::volatile();

            let profile = Profile::open(unique_name("none"))
                .perform(&storage)
                .await
                .unwrap();

            let operator = profile
                .derive(b"alice")
                .network(Network)
                .build(storage)
                .await
                .unwrap();

            let cap = Subject::from(profile.did()).archive().catalog("index");
            let result = claim_access(&operator, &cap).await;

            assert!(result.is_err(), "should fail with no delegations");
        }

        #[dialog_common::test]
        async fn no_issuer_uses_profile_did() {
            let storage = Storage::volatile();

            let profile = Profile::open(unique_name("nois"))
                .perform(&storage)
                .await
                .unwrap();

            let operator = profile
                .derive(b"alice")
                .allow(Subject::any().archive().catalog("index"))
                .network(Network)
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

    mod time_bound_tests {
        use super::*;
        use crate::profile::Profile;
        use dialog_capability::Subject;
        use dialog_capability::access::{
            self as cap_access, Access, Authorization as _, Proof as _, TimeRange,
        };
        use dialog_effects::archive::prelude::{ArchiveExt, ArchiveSubjectExt};
        use dialog_ucan::Ucan;
        use dialog_ucan::scope::Scope;
        use dialog_ucan_core::time::Timestamp;
        use dialog_ucan_core::time::timestamp::{Duration, UNIX_EPOCH};

        fn ts(secs: u64) -> Timestamp {
            Timestamp::new(UNIX_EPOCH + Duration::from_secs(secs)).unwrap()
        }

        async fn build_operator_with_profile()
        -> (super::super::super::Operator<VolatileSpace>, Profile) {
            let storage = Storage::volatile();
            let profile = Profile::open(unique_name("time"))
                .perform(&storage)
                .await
                .unwrap();
            let operator = profile
                .derive(b"test")
                .allow(Subject::any())
                .build(storage)
                .await
                .unwrap();
            (operator, profile)
        }

        /// Build an operator WITHOUT a powerline delegation.
        /// Only explicitly delegated capabilities will be available.
        async fn build_restricted_operator_with_profile()
        -> (super::super::super::Operator<VolatileSpace>, Profile) {
            let storage = Storage::volatile();
            let profile = Profile::open(unique_name("time-restricted"))
                .perform(&storage)
                .await
                .unwrap();
            let operator = profile.derive(b"test").build(storage).await.unwrap();
            (operator, profile)
        }

        #[dialog_common::test]
        async fn time_bounded_delegation_sets_proof_duration() {
            let (operator, profile) = build_operator_with_profile().await;

            // Delegate with time bounds: valid from 1000 to 5000
            let chain = profile
                .access()
                .claim(Subject::from(profile.did()).archive().catalog("index"))
                .not_before(ts(1000))
                .expires(ts(5000))
                .delegate(operator.did())
                .perform(&operator)
                .await
                .unwrap();

            profile
                .access()
                .save(chain)
                .perform(&operator)
                .await
                .unwrap();

            // Claim with unbounded duration (I don't care)
            let cap = Subject::from(profile.did()).archive().catalog("index");
            let scope = Scope::from(&cap);
            let proof = Subject::from(profile.did())
                .attenuate(Access)
                .invoke(cap_access::Prove::<Ucan>::new(operator.did(), scope))
                .perform(&operator)
                .await
                .unwrap();

            // Proof duration should reflect the certificate bounds
            let duration = proof.duration();
            assert_eq!(duration.not_before, Some(1000));
            assert_eq!(duration.expiration, Some(5000));
        }

        #[dialog_common::test]
        async fn prove_rejects_cert_that_expires_too_early() {
            let (operator, profile) = build_restricted_operator_with_profile().await;

            // Delegate with expiration at 1000
            let chain = profile
                .access()
                .claim(Subject::from(profile.did()).archive().catalog("data"))
                .expires(ts(1000))
                .delegate(operator.did())
                .perform(&operator)
                .await
                .unwrap();

            profile
                .access()
                .save(chain)
                .perform(&operator)
                .await
                .unwrap();

            // Request authorization valid until 5000 - should fail
            let cap = Subject::from(profile.did()).archive().catalog("data");
            let scope = Scope::from(&cap);
            let mut claim = cap_access::Prove::<Ucan>::new(operator.did(), scope);
            claim.duration = TimeRange {
                not_before: None,
                expiration: Some(5000),
            };

            let result = Subject::from(profile.did())
                .attenuate(Access)
                .invoke(claim)
                .perform(&operator)
                .await;

            assert!(
                result.is_err(),
                "should reject: cert expires at 1000 but requested until 5000"
            );
        }

        #[dialog_common::test]
        async fn prove_rejects_cert_starting_too_late() {
            let (operator, profile) = build_restricted_operator_with_profile().await;

            // Delegate with not_before at 5000
            let chain = profile
                .access()
                .claim(Subject::from(profile.did()).archive().catalog("data"))
                .not_before(ts(5000))
                .delegate(operator.did())
                .perform(&operator)
                .await
                .unwrap();

            profile
                .access()
                .save(chain)
                .perform(&operator)
                .await
                .unwrap();

            // Request authorization valid from 1000 - should fail
            let cap = Subject::from(profile.did()).archive().catalog("data");
            let scope = Scope::from(&cap);
            let mut claim = cap_access::Prove::<Ucan>::new(operator.did(), scope);
            claim.duration = TimeRange {
                not_before: Some(1000),
                expiration: None,
            };

            let result = Subject::from(profile.did())
                .attenuate(Access)
                .invoke(claim)
                .perform(&operator)
                .await;

            assert!(
                result.is_err(),
                "should reject: cert starts at 5000 but requested from 1000"
            );
        }

        #[dialog_common::test]
        async fn prove_accepts_cert_covering_requested_window() {
            let (operator, profile) = build_restricted_operator_with_profile().await;

            // Delegate valid from 100 to 10000
            let chain = profile
                .access()
                .claim(Subject::from(profile.did()).archive().catalog("data"))
                .not_before(ts(100))
                .expires(ts(10000))
                .delegate(operator.did())
                .perform(&operator)
                .await
                .unwrap();

            profile
                .access()
                .save(chain)
                .perform(&operator)
                .await
                .unwrap();

            // Request authorization valid from 500 to 5000 - cert covers this
            let cap = Subject::from(profile.did()).archive().catalog("data");
            let scope = Scope::from(&cap);
            let mut claim = cap_access::Prove::<Ucan>::new(operator.did(), scope);
            claim.duration = TimeRange {
                not_before: Some(500),
                expiration: Some(5000),
            };

            let result = Subject::from(profile.did())
                .attenuate(Access)
                .invoke(claim)
                .perform(&operator)
                .await;

            assert!(
                result.is_ok(),
                "should accept: cert covers requested window: {:?}",
                result.err()
            );
        }

        #[dialog_common::test]
        async fn prove_unbounded_request_accepts_any_cert() {
            let (operator, profile) = build_restricted_operator_with_profile().await;

            // Delegate with short window
            let chain = profile
                .access()
                .claim(Subject::from(profile.did()).archive().catalog("data"))
                .not_before(ts(100))
                .expires(ts(200))
                .delegate(operator.did())
                .perform(&operator)
                .await
                .unwrap();

            profile
                .access()
                .save(chain)
                .perform(&operator)
                .await
                .unwrap();

            // Request with no time constraints ("I don't care")
            let cap = Subject::from(profile.did()).archive().catalog("data");
            let scope = Scope::from(&cap);
            let claim = cap_access::Prove::<Ucan>::new(operator.did(), scope);

            let result = Subject::from(profile.did())
                .attenuate(Access)
                .invoke(claim)
                .perform(&operator)
                .await;

            assert!(
                result.is_ok(),
                "unbounded request should accept any cert: {:?}",
                result.err()
            );

            // But the proof should carry the cert's actual bounds
            let proof = result.unwrap();
            assert_eq!(proof.duration().not_before, Some(100));
            assert_eq!(proof.duration().expiration, Some(200));
        }

        #[dialog_common::test]
        async fn authorization_rejects_widening_expiration() {
            let (operator, profile) = build_restricted_operator_with_profile().await;

            // Delegate expiring at 1000
            let chain = profile
                .access()
                .claim(Subject::from(profile.did()).archive().catalog("data"))
                .expires(ts(1000))
                .delegate(operator.did())
                .perform(&operator)
                .await
                .unwrap();

            profile
                .access()
                .save(chain)
                .perform(&operator)
                .await
                .unwrap();

            // Get proof
            let cap = Subject::from(profile.did()).archive().catalog("data");
            let scope = Scope::from(&cap);
            let proof = Subject::from(profile.did())
                .attenuate(Access)
                .invoke(cap_access::Prove::<Ucan>::new(operator.did(), scope))
                .perform(&operator)
                .await
                .unwrap();

            let signer = dialog_credentials::Ed25519Signer::from(profile.credential().clone());
            let authorization = proof.claim(signer).unwrap();

            // Try to set expiration beyond proof bounds
            let result = authorization.expires(5000);
            assert!(
                result.is_err(),
                "should reject widening expiration beyond proof"
            );
        }

        #[dialog_common::test]
        async fn authorization_rejects_widening_not_before() {
            let (operator, profile) = build_restricted_operator_with_profile().await;

            // Delegate starting at 1000
            let chain = profile
                .access()
                .claim(Subject::from(profile.did()).archive().catalog("data"))
                .not_before(ts(1000))
                .delegate(operator.did())
                .perform(&operator)
                .await
                .unwrap();

            profile
                .access()
                .save(chain)
                .perform(&operator)
                .await
                .unwrap();

            // Get proof
            let cap = Subject::from(profile.did()).archive().catalog("data");
            let scope = Scope::from(&cap);
            let proof = Subject::from(profile.did())
                .attenuate(Access)
                .invoke(cap_access::Prove::<Ucan>::new(operator.did(), scope))
                .perform(&operator)
                .await
                .unwrap();

            let signer = dialog_credentials::Ed25519Signer::from(profile.credential().clone());
            let authorization = proof.claim(signer).unwrap();

            // Try to set not_before earlier than proof bounds
            let result = authorization.not_before(500);
            assert!(
                result.is_err(),
                "should reject widening not_before before proof"
            );
        }

        #[dialog_common::test]
        async fn authorization_accepts_narrowing() {
            let (operator, profile) = build_restricted_operator_with_profile().await;

            // Delegate valid from 100 to 10000
            let chain = profile
                .access()
                .claim(Subject::from(profile.did()).archive().catalog("data"))
                .not_before(ts(100))
                .expires(ts(10000))
                .delegate(operator.did())
                .perform(&operator)
                .await
                .unwrap();

            profile
                .access()
                .save(chain)
                .perform(&operator)
                .await
                .unwrap();

            // Get proof
            let cap = Subject::from(profile.did()).archive().catalog("data");
            let scope = Scope::from(&cap);
            let proof = Subject::from(profile.did())
                .attenuate(Access)
                .invoke(cap_access::Prove::<Ucan>::new(operator.did(), scope))
                .perform(&operator)
                .await
                .unwrap();

            let signer = dialog_credentials::Ed25519Signer::from(profile.credential().clone());
            let authorization = proof.claim(signer).unwrap();

            // Narrow the window - should succeed
            let result = authorization.not_before(500).unwrap().expires(5000);

            assert!(
                result.is_ok(),
                "narrowing should succeed: {:?}",
                result.err()
            );
        }
    }

    mod space_tests {
        use super::*;
        use dialog_capability::Subject;
        use dialog_effects::space::{self as space_fx, SpaceExt as _};

        #[dialog_common::test]
        async fn it_denies_space_load_for_wrong_subject() {
            let storage = Storage::volatile();
            let profile = Profile::open(unique_name("space-deny"))
                .perform(&storage)
                .await
                .unwrap();

            let operator = profile
                .derive(b"test")
                .allow(Subject::any())
                .network(Network)
                .build(storage)
                .await
                .unwrap();

            // Use a wrong DID as subject
            let wrong_did = dialog_capability::did!("key:z6MkWrongDid");
            let result: Result<_, _> = Subject::from(wrong_did)
                .attenuate(space_fx::Space::new("repo"))
                .load()
                .perform(&operator)
                .await;

            assert!(result.is_err(), "should deny space load for wrong subject");
        }

        #[dialog_common::test]
        async fn it_allows_space_for_profile_subject() {
            let storage = Storage::volatile();
            let profile = Profile::open(unique_name("space-allow"))
                .perform(&storage)
                .await
                .unwrap();

            let operator = profile
                .derive(b"test")
                .allow(Subject::any())
                .network(Network)
                .build(storage)
                .await
                .unwrap();

            // Use the correct profile DID as subject
            let result: Result<_, _> = Subject::from(operator.profile_did())
                .attenuate(space_fx::Space::new("repo"))
                .load()
                .perform(&operator)
                .await;

            // Will fail with NotFound (no space created), not with access denied
            let err = result.unwrap_err();
            assert!(
                err.to_string().contains("not found") || err.to_string().contains("Not found"),
                "should fail with not-found, not access denied: {err}"
            );
        }
    }
}
