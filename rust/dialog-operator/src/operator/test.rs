use crate::helpers::unique_name;
use crate::profile::Profile;
use dialog_network::Network;
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
            .network(Network::default())
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
            .network(Network::default())
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
            .network(Network::default())
            .build(storage2)
            .await
            .unwrap();

        assert_ne!(op1.did(), op2.did());
    }

    mod delegation_tests {
        use super::*;
        use dialog_capability::Subject;
        use dialog_effects::archive::prelude::{ArchiveExt, ArchiveSubjectExt};

        #[dialog_common::test]
        async fn self_grant_produces_delegation() {
            let storage = Storage::volatile();

            let profile = Profile::open(unique_name("self"))
                .perform(&storage)
                .await
                .unwrap();

            let operator = profile
                .derive(b"alice")
                .network(Network::default())
                .build(storage)
                .await
                .unwrap();

            let result = profile
                .access()
                .prove(Subject::from(operator.did()).archive().catalog("index"))
                .audience(&operator)
                .perform(&operator)
                .await;

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
                .network(Network::default())
                .build(storage)
                .await
                .unwrap();

            let result = profile
                .access()
                .prove(Subject::from(operator.did()))
                .audience(&operator)
                .perform(&operator)
                .await;

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
                .network(Network::default())
                .build(storage)
                .await
                .unwrap();

            let result = profile
                .access()
                .prove(Subject::from(profile.did()).archive().catalog("index"))
                .audience(&operator)
                .perform(&operator)
                .await;

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
                .network(Network::default())
                .build(storage)
                .await
                .unwrap();

            let result = profile
                .access()
                .prove(Subject::from(profile.did()).archive().catalog("secret"))
                .audience(&operator)
                .perform(&operator)
                .await;

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
                .network(Network::default())
                .build(storage)
                .await
                .unwrap();

            let result = profile
                .access()
                .prove(Subject::from(profile.did()).attenuate(fx_storage::Storage))
                .audience(&operator)
                .perform(&operator)
                .await;

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
                .network(Network::default())
                .build(storage)
                .await
                .unwrap();

            let result = profile
                .access()
                .prove(Subject::from(profile.did()).archive().catalog("index"))
                .audience(&operator)
                .perform(&operator)
                .await;

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
                .network(Network::default())
                .build(storage)
                .await
                .unwrap();

            let result = profile
                .access()
                .prove(Subject::from(profile.did()).archive().catalog("index"))
                .audience(&operator)
                .perform(&operator)
                .await;

            assert!(
                result.is_ok(),
                "should find chain via profile DID: {:?}",
                result.err()
            );
        }
    }

    mod time_bound_tests {
        use super::*;
        use crate::Operator;
        use crate::profile::Profile;
        use dialog_capability::Subject;
        use dialog_capability::access::{Authorization as _, Proof as _};
        use dialog_credentials::Ed25519Signer;
        use dialog_effects::archive::prelude::{ArchiveExt, ArchiveSubjectExt};
        use dialog_ucan_core::time::Timestamp;
        use dialog_ucan_core::time::timestamp::{Duration, UNIX_EPOCH};

        fn ts(secs: u64) -> Timestamp {
            Timestamp::new(UNIX_EPOCH + Duration::from_secs(secs)).unwrap()
        }

        async fn build_operator_with_profile() -> (Operator<VolatileSpace>, Profile) {
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
        async fn build_restricted_operator_with_profile() -> (Operator<VolatileSpace>, Profile) {
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
            let proof = profile
                .access()
                .prove(Subject::from(profile.did()).archive().catalog("index"))
                .audience(&operator)
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
            let result = profile
                .access()
                .prove(Subject::from(profile.did()).archive().catalog("data"))
                .audience(&operator)
                .expires(ts(5000))
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
            let result = profile
                .access()
                .prove(Subject::from(profile.did()).archive().catalog("data"))
                .audience(&operator)
                .not_before(ts(1000))
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
            let result = profile
                .access()
                .prove(Subject::from(profile.did()).archive().catalog("data"))
                .audience(&operator)
                .not_before(ts(500))
                .expires(ts(5000))
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
            let proof = profile
                .access()
                .prove(Subject::from(profile.did()).archive().catalog("data"))
                .audience(&operator)
                .perform(&operator)
                .await
                .unwrap();

            // But the proof should carry the cert's actual bounds
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

            let proof = profile
                .access()
                .prove(Subject::from(profile.did()).archive().catalog("data"))
                .audience(&operator)
                .perform(&operator)
                .await
                .unwrap();

            let signer = Ed25519Signer::from(profile.signer().clone());
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

            let proof = profile
                .access()
                .prove(Subject::from(profile.did()).archive().catalog("data"))
                .audience(&operator)
                .perform(&operator)
                .await
                .unwrap();

            let signer = Ed25519Signer::from(profile.signer().clone());
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

            let proof = profile
                .access()
                .prove(Subject::from(profile.did()).archive().catalog("data"))
                .audience(&operator)
                .perform(&operator)
                .await
                .unwrap();

            let signer = Ed25519Signer::from(profile.signer().clone());
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

    mod s3_credential_tests {
        use super::*;
        use dialog_capability::Subject;
        use dialog_common::Blake3Hash;
        use dialog_effects::archive::prelude::*;
        use dialog_effects::credential::Secret;
        use dialog_effects::memory::prelude::*;
        use dialog_network::NetworkAddress as SiteAddress;
        use dialog_remote_s3::helpers::S3Address;
        use dialog_remote_s3::{Address, S3Credential};

        fn address_from(s3: &S3Address) -> SiteAddress {
            SiteAddress::S3(
                Address::builder(&s3.endpoint)
                    .region("us-east-1")
                    .bucket(&s3.bucket)
                    .build()
                    .unwrap(),
            )
        }

        #[dialog_common::test]
        fn credential_roundtrips_through_secret() {
            let cred = S3Credential::new("test-access-key", "test-secret-key");
            let secret: Secret = cred.clone().into();
            let restored: S3Credential = secret.try_into().unwrap();

            assert_eq!(restored.access_key_id(), cred.access_key_id());
            assert_eq!(restored.secret_access_key(), cred.secret_access_key());
        }

        #[dialog_common::test]
        async fn fork_fails_without_saved_credential(s3: S3Address) -> anyhow::Result<()> {
            let storage = Storage::volatile();
            let profile = Profile::open(unique_name("s3-no-cred"))
                .perform(&storage)
                .await
                .unwrap();
            let operator = profile
                .derive(b"test")
                .allow(Subject::any())
                .network(Network::default())
                .build(storage)
                .await
                .unwrap();

            let address = address_from(&s3);

            // Fork without saving credentials: should fail with credential not found
            let result = Subject::from(operator.profile_did())
                .archive()
                .catalog("data")
                .get(Blake3Hash::hash(b"test"))
                .fork(&address)
                .perform(&operator)
                .await;

            let err = result.unwrap_err();
            assert!(
                err.to_string().contains("not found")
                    || err.to_string().contains("Credential not found"),
                "should fail with credential not found, got: {err}"
            );
            Ok(())
        }

        #[dialog_common::test]
        async fn fork_loads_saved_credential_for_get(s3: S3Address) -> anyhow::Result<()> {
            let storage = Storage::volatile();
            let profile = Profile::open(unique_name("s3-get"))
                .perform(&storage)
                .await
                .unwrap();
            let operator = profile
                .derive(b"test")
                .allow(Subject::any())
                .network(Network::default())
                .build(storage)
                .await
                .unwrap();

            let address = address_from(&s3);
            let credential = S3Credential::new(&s3.access_key_id, &s3.secret_access_key);

            profile
                .credential()
                .site(&address)
                .save(credential)
                .perform(&operator)
                .await
                .unwrap();

            // Fork get: credential is loaded, request reaches the S3 server,
            // returns None because the content doesn't exist (not an auth error).
            let result = Subject::from(operator.profile_did())
                .archive()
                .catalog("cred-test")
                .get(Blake3Hash::hash(b"nonexistent"))
                .fork(&address)
                .perform(&operator)
                .await;

            let content = result?;
            assert!(content.is_none(), "content should not exist");
            Ok(())
        }

        #[dialog_common::test]
        async fn fork_loads_saved_credential_for_put_and_get(s3: S3Address) -> anyhow::Result<()> {
            let storage = Storage::volatile();
            let profile = Profile::open(unique_name("s3-put-get"))
                .perform(&storage)
                .await
                .unwrap();
            let operator = profile
                .derive(b"test")
                .allow(Subject::any())
                .network(Network::default())
                .build(storage)
                .await
                .unwrap();

            let address = address_from(&s3);
            let credential = S3Credential::new(&s3.access_key_id, &s3.secret_access_key);

            profile
                .credential()
                .site(&address)
                .save(credential)
                .perform(&operator)
                .await
                .unwrap();

            let content = b"hello from operator".to_vec();
            let digest = Blake3Hash::hash(&content);

            // Put content via fork
            Subject::from(operator.profile_did())
                .archive()
                .catalog("cred-roundtrip")
                .put(digest.clone(), content.clone())
                .fork(&address)
                .perform(&operator)
                .await
                .unwrap();

            // Get it back via fork
            let retrieved = Subject::from(operator.profile_did())
                .archive()
                .catalog("cred-roundtrip")
                .get(digest)
                .fork(&address)
                .perform(&operator)
                .await
                .unwrap();

            assert_eq!(retrieved, Some(content));
            Ok(())
        }

        #[dialog_common::test]
        async fn fork_memory_publish_and_resolve(s3: S3Address) -> anyhow::Result<()> {
            let storage = Storage::volatile();
            let profile = Profile::open(unique_name("s3-mem-pub"))
                .perform(&storage)
                .await?;
            let operator = profile
                .derive(b"test")
                .allow(Subject::any())
                .network(Network::default())
                .build(storage)
                .await?;

            let address = address_from(&s3);
            let credential = S3Credential::new(&s3.access_key_id, &s3.secret_access_key);
            profile
                .credential()
                .site(&address)
                .save(credential)
                .perform(&operator)
                .await?;

            let subject = operator.profile_did();
            let content = b"memory content".to_vec();

            let edition = Subject::from(subject.clone())
                .memory()
                .space("test-space")
                .cell("head")
                .publish(content.clone(), None)
                .fork(&address)
                .perform(&operator)
                .await?;

            let resolved = Subject::from(subject)
                .memory()
                .space("test-space")
                .cell("head")
                .resolve()
                .fork(&address)
                .perform(&operator)
                .await?;

            let publication = resolved.unwrap();
            assert_eq!(publication.content, content);
            assert_eq!(publication.version, edition);
            Ok(())
        }

        #[dialog_common::test]
        async fn fork_memory_update_existing(s3: S3Address) -> anyhow::Result<()> {
            let storage = Storage::volatile();
            let profile = Profile::open(unique_name("s3-mem-upd"))
                .perform(&storage)
                .await?;
            let operator = profile
                .derive(b"test")
                .allow(Subject::any())
                .network(Network::default())
                .build(storage)
                .await?;

            let address = address_from(&s3);
            let credential = S3Credential::new(&s3.access_key_id, &s3.secret_access_key);
            profile
                .credential()
                .site(&address)
                .save(credential)
                .perform(&operator)
                .await?;

            let subject = operator.profile_did();

            let edition1 = Subject::from(subject.clone())
                .memory()
                .space("upd-space")
                .cell("head")
                .publish(b"initial".to_vec(), None)
                .fork(&address)
                .perform(&operator)
                .await?;

            let edition2 = Subject::from(subject.clone())
                .memory()
                .space("upd-space")
                .cell("head")
                .publish(b"updated".to_vec(), Some(edition1))
                .fork(&address)
                .perform(&operator)
                .await?;

            let resolved = Subject::from(subject)
                .memory()
                .space("upd-space")
                .cell("head")
                .resolve()
                .fork(&address)
                .perform(&operator)
                .await?;

            let publication = resolved.unwrap();
            assert_eq!(publication.content, b"updated");
            assert_eq!(publication.version, edition2);
            Ok(())
        }

        #[dialog_common::test]
        async fn fork_memory_cas_conflict(s3: S3Address) -> anyhow::Result<()> {
            let storage = Storage::volatile();
            let profile = Profile::open(unique_name("s3-mem-cas"))
                .perform(&storage)
                .await?;
            let operator = profile
                .derive(b"test")
                .allow(Subject::any())
                .network(Network::default())
                .build(storage)
                .await?;

            let address = address_from(&s3);
            let credential = S3Credential::new(&s3.access_key_id, &s3.secret_access_key);
            profile
                .credential()
                .site(&address)
                .save(credential)
                .perform(&operator)
                .await?;

            let subject = operator.profile_did();

            let edition1 = Subject::from(subject.clone())
                .memory()
                .space("cas-space")
                .cell("head")
                .publish(b"initial".to_vec(), None)
                .fork(&address)
                .perform(&operator)
                .await?;

            Subject::from(subject.clone())
                .memory()
                .space("cas-space")
                .cell("head")
                .publish(b"by-writer-1".to_vec(), Some(edition1.clone()))
                .fork(&address)
                .perform(&operator)
                .await?;

            let result = Subject::from(subject.clone())
                .memory()
                .space("cas-space")
                .cell("head")
                .publish(b"by-writer-2".to_vec(), Some(edition1))
                .fork(&address)
                .perform(&operator)
                .await;

            assert!(result.is_err(), "CAS should fail due to edition mismatch");

            let resolved = Subject::from(subject)
                .memory()
                .space("cas-space")
                .cell("head")
                .resolve()
                .fork(&address)
                .perform(&operator)
                .await?;

            assert_eq!(resolved.unwrap().content, b"by-writer-1");
            Ok(())
        }

        #[dialog_common::test]
        async fn fork_memory_retract(s3: S3Address) -> anyhow::Result<()> {
            let storage = Storage::volatile();
            let profile = Profile::open(unique_name("s3-mem-ret"))
                .perform(&storage)
                .await?;
            let operator = profile
                .derive(b"test")
                .allow(Subject::any())
                .network(Network::default())
                .build(storage)
                .await?;

            let address = address_from(&s3);
            let credential = S3Credential::new(&s3.access_key_id, &s3.secret_access_key);
            profile
                .credential()
                .site(&address)
                .save(credential)
                .perform(&operator)
                .await?;

            let subject = operator.profile_did();

            let edition = Subject::from(subject.clone())
                .memory()
                .space("ret-space")
                .cell("head")
                .publish(b"to-be-retracted".to_vec(), None)
                .fork(&address)
                .perform(&operator)
                .await?;

            Subject::from(subject.clone())
                .memory()
                .space("ret-space")
                .cell("head")
                .retract(edition)
                .fork(&address)
                .perform(&operator)
                .await?;

            let resolved = Subject::from(subject)
                .memory()
                .space("ret-space")
                .cell("head")
                .resolve()
                .fork(&address)
                .perform(&operator)
                .await?;

            assert!(resolved.is_none(), "cell should be empty after retract");
            Ok(())
        }
    }

    mod ucan_fork_tests {
        use super::*;
        use dialog_capability::Subject;
        use dialog_common::Blake3Hash;
        use dialog_effects::archive::prelude::*;
        use dialog_effects::memory::prelude::*;
        use dialog_network::NetworkAddress as SiteAddress;
        use dialog_remote_ucan_s3::UcanAddress;
        use dialog_remote_ucan_s3::helpers::UcanS3Address;

        fn ucan_address(s3: &UcanS3Address) -> SiteAddress {
            SiteAddress::Ucan(UcanAddress::new(&s3.access_service_url))
        }

        #[dialog_common::test]
        async fn fork_archive_get_returns_none_for_missing(
            s3: UcanS3Address,
        ) -> anyhow::Result<()> {
            let storage = Storage::volatile();
            let profile = Profile::open(unique_name("ucan-get-miss"))
                .perform(&storage)
                .await?;
            let operator = profile
                .derive(b"test")
                .allow(Subject::any())
                .network(Network::default())
                .build(storage)
                .await?;

            let address = ucan_address(&s3);

            let result = Subject::from(operator.profile_did())
                .archive()
                .catalog("data")
                .get(Blake3Hash::hash(b"nonexistent"))
                .fork(&address)
                .perform(&operator)
                .await?;

            assert!(result.is_none(), "content should not exist");
            Ok(())
        }

        #[dialog_common::test]
        async fn fork_archive_put_and_get(s3: UcanS3Address) -> anyhow::Result<()> {
            let storage = Storage::volatile();
            let profile = Profile::open(unique_name("ucan-put-get"))
                .perform(&storage)
                .await?;
            let operator = profile
                .derive(b"test")
                .allow(Subject::any())
                .network(Network::default())
                .build(storage)
                .await?;

            let address = ucan_address(&s3);
            let content = b"hello from ucan".to_vec();
            let digest = Blake3Hash::hash(&content);

            Subject::from(operator.profile_did())
                .archive()
                .catalog("ucan-roundtrip")
                .put(digest.clone(), content.clone())
                .fork(&address)
                .perform(&operator)
                .await?;

            let retrieved = Subject::from(operator.profile_did())
                .archive()
                .catalog("ucan-roundtrip")
                .get(digest)
                .fork(&address)
                .perform(&operator)
                .await?;

            assert_eq!(retrieved, Some(content));
            Ok(())
        }

        #[dialog_common::test]
        async fn fork_memory_resolve_returns_none_for_missing(
            s3: UcanS3Address,
        ) -> anyhow::Result<()> {
            let storage = Storage::volatile();
            let profile = Profile::open(unique_name("ucan-mem-miss"))
                .perform(&storage)
                .await?;
            let operator = profile
                .derive(b"test")
                .allow(Subject::any())
                .network(Network::default())
                .build(storage)
                .await?;

            let address = ucan_address(&s3);

            let result = Subject::from(operator.profile_did())
                .memory()
                .space("test-space")
                .cell("test-cell")
                .resolve()
                .fork(&address)
                .perform(&operator)
                .await?;

            assert!(result.is_none(), "cell should not exist");
            Ok(())
        }

        #[dialog_common::test]
        async fn fork_memory_publish_and_resolve(s3: UcanS3Address) -> anyhow::Result<()> {
            let storage = Storage::volatile();
            let profile = Profile::open(unique_name("ucan-mem-pub"))
                .perform(&storage)
                .await?;
            let operator = profile
                .derive(b"test")
                .allow(Subject::any())
                .network(Network::default())
                .build(storage)
                .await?;

            let address = ucan_address(&s3);
            let subject = operator.profile_did();
            let content = b"memory content".to_vec();

            let edition = Subject::from(subject.clone())
                .memory()
                .space("test-space")
                .cell("head")
                .publish(content.clone(), None)
                .fork(&address)
                .perform(&operator)
                .await?;

            let resolved = Subject::from(subject)
                .memory()
                .space("test-space")
                .cell("head")
                .resolve()
                .fork(&address)
                .perform(&operator)
                .await?;

            let publication = resolved.unwrap();
            assert_eq!(publication.content, content);
            assert_eq!(publication.version, edition);
            Ok(())
        }

        #[dialog_common::test]
        async fn fork_memory_update_existing(s3: UcanS3Address) -> anyhow::Result<()> {
            let storage = Storage::volatile();
            let profile = Profile::open(unique_name("ucan-mem-upd"))
                .perform(&storage)
                .await?;
            let operator = profile
                .derive(b"test")
                .allow(Subject::any())
                .network(Network::default())
                .build(storage)
                .await?;

            let address = ucan_address(&s3);
            let subject = operator.profile_did();

            let edition1 = Subject::from(subject.clone())
                .memory()
                .space("upd-space")
                .cell("head")
                .publish(b"initial".to_vec(), None)
                .fork(&address)
                .perform(&operator)
                .await?;

            let edition2 = Subject::from(subject.clone())
                .memory()
                .space("upd-space")
                .cell("head")
                .publish(b"updated".to_vec(), Some(edition1))
                .fork(&address)
                .perform(&operator)
                .await?;

            let resolved = Subject::from(subject)
                .memory()
                .space("upd-space")
                .cell("head")
                .resolve()
                .fork(&address)
                .perform(&operator)
                .await?;

            let publication = resolved.unwrap();
            assert_eq!(publication.content, b"updated");
            assert_eq!(publication.version, edition2);
            Ok(())
        }

        #[dialog_common::test]
        async fn fork_memory_cas_conflict(s3: UcanS3Address) -> anyhow::Result<()> {
            let storage = Storage::volatile();
            let profile = Profile::open(unique_name("ucan-mem-cas"))
                .perform(&storage)
                .await?;
            let operator = profile
                .derive(b"test")
                .allow(Subject::any())
                .network(Network::default())
                .build(storage)
                .await?;

            let address = ucan_address(&s3);
            let subject = operator.profile_did();

            let edition1 = Subject::from(subject.clone())
                .memory()
                .space("cas-space")
                .cell("head")
                .publish(b"initial".to_vec(), None)
                .fork(&address)
                .perform(&operator)
                .await?;

            // Update with correct edition
            Subject::from(subject.clone())
                .memory()
                .space("cas-space")
                .cell("head")
                .publish(b"by-writer-1".to_vec(), Some(edition1.clone()))
                .fork(&address)
                .perform(&operator)
                .await?;

            // Try to update with stale edition
            let result = Subject::from(subject.clone())
                .memory()
                .space("cas-space")
                .cell("head")
                .publish(b"by-writer-2".to_vec(), Some(edition1))
                .fork(&address)
                .perform(&operator)
                .await;

            assert!(result.is_err(), "CAS should fail due to edition mismatch");

            // Verify value is still from writer-1
            let resolved = Subject::from(subject)
                .memory()
                .space("cas-space")
                .cell("head")
                .resolve()
                .fork(&address)
                .perform(&operator)
                .await?;

            assert_eq!(resolved.unwrap().content, b"by-writer-1");
            Ok(())
        }

        #[dialog_common::test]
        async fn fork_memory_retract(s3: UcanS3Address) -> anyhow::Result<()> {
            let storage = Storage::volatile();
            let profile = Profile::open(unique_name("ucan-mem-ret"))
                .perform(&storage)
                .await?;
            let operator = profile
                .derive(b"test")
                .allow(Subject::any())
                .network(Network::default())
                .build(storage)
                .await?;

            let address = ucan_address(&s3);
            let subject = operator.profile_did();

            let edition = Subject::from(subject.clone())
                .memory()
                .space("ret-space")
                .cell("head")
                .publish(b"to-be-retracted".to_vec(), None)
                .fork(&address)
                .perform(&operator)
                .await?;

            Subject::from(subject.clone())
                .memory()
                .space("ret-space")
                .cell("head")
                .retract(edition)
                .fork(&address)
                .perform(&operator)
                .await?;

            let resolved = Subject::from(subject)
                .memory()
                .space("ret-space")
                .cell("head")
                .resolve()
                .fork(&address)
                .perform(&operator)
                .await?;

            assert!(resolved.is_none(), "cell should be empty after retract");
            Ok(())
        }

        #[dialog_common::test]
        async fn fork_with_scoped_delegation(s3: UcanS3Address) -> anyhow::Result<()> {
            let storage = Storage::volatile();
            let profile = Profile::open(unique_name("ucan-scoped"))
                .perform(&storage)
                .await?;
            // Only delegate archive access, not memory
            let operator = profile
                .derive(b"test")
                .allow(Subject::any().archive().catalog("allowed"))
                .network(Network::default())
                .build(storage)
                .await?;

            let address = ucan_address(&s3);
            let content = b"scoped content".to_vec();
            let digest = Blake3Hash::hash(&content);

            // Put to allowed catalog should succeed
            Subject::from(operator.profile_did())
                .archive()
                .catalog("allowed")
                .put(digest.clone(), content.clone())
                .fork(&address)
                .perform(&operator)
                .await?;

            // Get from allowed catalog should succeed
            let retrieved = Subject::from(operator.profile_did())
                .archive()
                .catalog("allowed")
                .get(digest)
                .fork(&address)
                .perform(&operator)
                .await?;

            assert_eq!(retrieved, Some(content));
            Ok(())
        }
    }

    mod space_tests {
        use super::*;
        use dialog_capability::{Subject, did};
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
                .network(Network::default())
                .build(storage)
                .await
                .unwrap();

            // Use a wrong DID as subject
            let wrong_did = did!("key:z6MkWrongDid");
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
                .network(Network::default())
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
