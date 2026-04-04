use crate::helpers::unique_name;
use crate::profile::Profile;
use crate::remote::Remote;
use crate::storage::Storage;

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

    mod delegation_tests {
        use super::*;
        use crate::Operator;
        use dialog_capability::Subject;
        use dialog_capability::access::{self as cap_access, AuthorizeError, Permit};
        use dialog_capability_ucan::scope::Scope;
        use dialog_capability_ucan::{Ucan, UcanPermit};
        use dialog_effects::archive::prelude::{ArchiveExt, SubjectExt as ArchiveSubjectExt};
        use dialog_effects::storage as fx_storage;

        /// Helper: claim authorization via the new Claim<Ucan> effect.
        ///
        /// Uses the profile DID as the capability subject (where the permit
        /// store is mounted), and operator DID as the claimant.
        async fn claim_access(
            operator: &Operator,
            capability: &impl dialog_capability::Ability,
        ) -> Result<UcanPermit, AuthorizeError> {
            let scope = Scope::from(capability);

            // Subject is the profile DID (where delegations are stored)
            Subject::from(operator.profile_did())
                .attenuate(Permit)
                .invoke(cap_access::Claim::<Ucan>::new(operator.did(), scope))
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

        /// Helper: create and save a time-bounded delegation from profile to operator.
        use dialog_common::time::{Duration, UNIX_EPOCH};
        use dialog_ucan::time::Timestamp;

        async fn save_timed_delegation(
            operator: &Operator,
            profile: &Profile,
            scope: &impl dialog_capability::Ability,
            not_before: Option<Timestamp>,
            expiration: Option<Timestamp>,
        ) {
            use cap_access::Save;
            use dialog_ucan::DelegationChain;
            use dialog_ucan::delegation::builder::DelegationBuilder;

            let scope = Scope::from(scope);
            let mut builder = DelegationBuilder::new()
                .issuer(profile.credential().signer().clone())
                .audience(&operator.did())
                .subject(scope.subject.clone())
                .command(scope.command.segments().clone())
                .policy(scope.policy());

            if let Some(exp) = expiration {
                builder = builder.expiration(exp);
            }
            if let Some(nbf) = not_before {
                builder = builder.not_before(nbf);
            }

            let delegation = builder.try_build().await.unwrap();
            let chain = DelegationChain::new(delegation);

            Subject::from(profile.did())
                .attenuate(Permit)
                .invoke(Save::<Ucan>::new(chain))
                .perform(operator)
                .await
                .unwrap();
        }

        /// Helper: claim with a time duration constraint.
        async fn claim_access_during(
            operator: &Operator,
            capability: &impl dialog_capability::Ability,
            duration: cap_access::TimeRange,
        ) -> Result<UcanPermit, AuthorizeError> {
            let scope = Scope::from(capability);
            let mut claim = cap_access::Claim::<Ucan>::new(operator.did(), scope);
            claim.duration = duration;

            Subject::from(operator.profile_did())
                .attenuate(Permit)
                .invoke(claim)
                .perform(operator)
                .await
        }

        /// 7. Expired delegation is rejected when claiming during a later time.
        #[dialog_common::test]
        async fn expired_delegation_rejected() {
            let storage = Storage::temp_storage();
            let profile = Profile::open(Storage::temp(&unique_name("exp")))
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

            // Delegation expires at t=1000
            let exp = Timestamp::new(UNIX_EPOCH + Duration::from_secs(1000)).unwrap();
            save_timed_delegation(&operator, &profile, &cap, None, Some(exp)).await;

            // Claim requiring validity at t=2000 should fail (delegation expired)
            let duration = cap_access::TimeRange {
                not_before: Some(2000),
                expiration: None,
            };
            let result = claim_access_during(&operator, &cap, duration).await;
            assert!(
                result.is_err(),
                "should reject expired delegation: {:?}",
                result.ok().map(|_| "found")
            );
        }

        /// 8. Not-yet-valid delegation is rejected when claiming during an earlier time.
        #[dialog_common::test]
        async fn not_yet_valid_delegation_rejected() {
            let storage = Storage::temp_storage();
            let profile = Profile::open(Storage::temp(&unique_name("nbf")))
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

            // Delegation not valid before t=5000
            let nbf = Timestamp::new(UNIX_EPOCH + Duration::from_secs(5000)).unwrap();
            save_timed_delegation(&operator, &profile, &cap, Some(nbf), None).await;

            // Claim requiring validity before t=3000 should fail
            let duration = cap_access::TimeRange {
                not_before: None,
                expiration: Some(3000),
            };
            let result = claim_access_during(&operator, &cap, duration).await;
            assert!(
                result.is_err(),
                "should reject not-yet-valid delegation: {:?}",
                result.ok().map(|_| "found")
            );
        }

        /// 9. Delegation within the required time range succeeds.
        #[dialog_common::test]
        async fn valid_time_range_succeeds() {
            let storage = Storage::temp_storage();
            let profile = Profile::open(Storage::temp(&unique_name("valid")))
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

            // Delegation valid from t=1000 to t=5000
            let nbf = Timestamp::new(UNIX_EPOCH + Duration::from_secs(1000)).unwrap();
            let exp = Timestamp::new(UNIX_EPOCH + Duration::from_secs(5000)).unwrap();
            save_timed_delegation(&operator, &profile, &cap, Some(nbf), Some(exp)).await;

            // Claim requiring validity from t=2000 to t=4000 should succeed (within range)
            let duration = cap_access::TimeRange {
                not_before: Some(2000),
                expiration: Some(4000),
            };
            let result = claim_access_during(&operator, &cap, duration).await;
            assert!(
                result.is_ok(),
                "should accept delegation within time range: {:?}",
                result.err()
            );
        }

        /// 10. Unbounded claim accepts any time-bounded delegation.
        #[dialog_common::test]
        async fn unbounded_claim_accepts_bounded_delegation() {
            let storage = Storage::temp_storage();
            let profile = Profile::open(Storage::temp(&unique_name("unb")))
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

            // Delegation with bounds
            let nbf = Timestamp::new(UNIX_EPOCH + Duration::from_secs(1000)).unwrap();
            let exp = Timestamp::new(UNIX_EPOCH + Duration::from_secs(5000)).unwrap();
            save_timed_delegation(&operator, &profile, &cap, Some(nbf), Some(exp)).await;

            // Unbounded claim should accept any delegation
            let result = claim_access(&operator, &cap).await;
            assert!(
                result.is_ok(),
                "unbounded claim should accept bounded delegation: {:?}",
                result.err()
            );
        }

        /// 11. No issuer set: resolves via profile DID
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
