use crate::helpers::unique_name;
use crate::profile::Profile;
use crate::remote::Remote;
use dialog_storage::provider::environment::{Environment, VolatileSpace};

#[cfg(test)]
mod tests {
    use super::*;

    #[dialog_common::test]
    async fn it_builds_operator_from_profile() {
        let env = Environment::volatile();

        let profile = Profile::open(unique_name("test"))
            .perform(&env)
            .await
            .unwrap();

        let operator = profile
            .derive(b"test")
            .network(Remote)
            .build(env)
            .await
            .unwrap();

        assert!(!operator.did().to_string().is_empty());
    }

    #[dialog_common::test]
    async fn it_derives_different_operators_from_different_contexts() {
        let env1 = Environment::volatile();
        let profile1 = Profile::open(unique_name("ctx1"))
            .perform(&env1)
            .await
            .unwrap();
        let op1 = profile1
            .derive(b"context-a")
            .network(Remote)
            .build(env1)
            .await
            .unwrap();

        let env2 = Environment::volatile();
        let profile2 = Profile::open(unique_name("ctx2"))
            .perform(&env2)
            .await
            .unwrap();
        let op2 = profile2
            .derive(b"context-b")
            .network(Remote)
            .build(env2)
            .await
            .unwrap();

        assert_ne!(op1.did(), op2.did());
    }

    mod delegation_tests {
        use super::*;
        use dialog_capability::Subject;
        use dialog_capability::access::{self as cap_access, Access, AuthorizeError};
        use dialog_effects::archive::prelude::{ArchiveExt, SubjectExt as ArchiveSubjectExt};
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
            let env = Environment::volatile();

            let profile = Profile::open(unique_name("self"))
                .perform(&env)
                .await
                .unwrap();

            let operator = profile
                .derive(b"alice")
                .network(Remote)
                .build(env)
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
            let env = Environment::volatile();

            let profile = Profile::open(unique_name("psg"))
                .perform(&env)
                .await
                .unwrap();

            let operator = profile
                .derive(b"alice")
                .network(Remote)
                .build(env)
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
            let env = Environment::volatile();

            let profile = Profile::open(unique_name("found"))
                .perform(&env)
                .await
                .unwrap();

            let operator = profile
                .derive(b"alice")
                .allow(Subject::any().archive().catalog("index"))
                .network(Remote)
                .build(env)
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
            let env = Environment::volatile();

            let profile = Profile::open(unique_name("deny"))
                .perform(&env)
                .await
                .unwrap();

            let operator = profile
                .derive(b"alice")
                .allow(Subject::any().archive().catalog("index"))
                .network(Remote)
                .build(env)
                .await
                .unwrap();

            let cap = Subject::from(profile.did()).archive().catalog("secret");
            let result = claim_access(&operator, &cap).await;

            assert!(result.is_err(), "should deny non-delegated archive/secret");
        }

        #[dialog_common::test]
        async fn powerline_delegation_allows_anything() {
            let env = Environment::volatile();

            let profile = Profile::open(unique_name("power"))
                .perform(&env)
                .await
                .unwrap();

            use dialog_effects::storage as fx_storage;
            let operator = profile
                .derive(b"admin")
                .allow(Subject::any())
                .network(Remote)
                .build(env)
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
            let env = Environment::volatile();

            let profile = Profile::open(unique_name("none"))
                .perform(&env)
                .await
                .unwrap();

            let operator = profile
                .derive(b"alice")
                .network(Remote)
                .build(env)
                .await
                .unwrap();

            let cap = Subject::from(profile.did()).archive().catalog("index");
            let result = claim_access(&operator, &cap).await;

            assert!(result.is_err(), "should fail with no delegations");
        }

        #[dialog_common::test]
        async fn no_issuer_uses_profile_did() {
            let env = Environment::volatile();

            let profile = Profile::open(unique_name("nois"))
                .perform(&env)
                .await
                .unwrap();

            let operator = profile
                .derive(b"alice")
                .allow(Subject::any().archive().catalog("index"))
                .network(Remote)
                .build(env)
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

    mod space_tests {
        use super::*;
        use dialog_capability::Subject;
        use dialog_effects::space::{self as space_fx, SpaceExt as _};

        #[dialog_common::test]
        async fn it_denies_space_load_for_wrong_subject() {
            let env = Environment::volatile();
            let profile = Profile::open(unique_name("space-deny"))
                .perform(&env)
                .await
                .unwrap();

            let operator = profile
                .derive(b"test")
                .allow(Subject::any())
                .network(Remote)
                .build(env)
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
            let env = Environment::volatile();
            let profile = Profile::open(unique_name("space-allow"))
                .perform(&env)
                .await
                .unwrap();

            let operator = profile
                .derive(b"test")
                .allow(Subject::any())
                .network(Remote)
                .build(env)
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
