//! Tests for delegation conformance to the UCAN specification.
mod delegation_conformance {
    use std::sync::OnceLock;

    use base64::prelude::*;
    use dialog_ucan::Delegation;
    use dialog_varsig::eddsa::Ed25519Signature;
    use testresult::TestResult;

    const DELEGATION_FIXTURE_STR: &str = include_str!("./fixtures/delegation.json");
    static DELEGATION_FIXTURE: OnceLock<serde_json::Value> = OnceLock::new();
    fn delegation_fixture() -> &'static serde_json::Value {
        DELEGATION_FIXTURE.get_or_init(|| {
            serde_json::from_str(DELEGATION_FIXTURE_STR).expect("fixture is invalid JSON")
        })
    }

    #[test]
    fn test_expected_version() -> TestResult {
        assert_eq!(
            delegation_fixture()
                .get("version")
                .expect("fixture has delegation key")
                .clone(),
            "1.0.0-rc.1".to_string()
        );
        Ok(())
    }

    #[test]
    fn test_top_level_parse() -> TestResult {
        let b64_txt: &str = delegation_fixture()["valid"][0]["token"]
            .as_str()
            .expect("valid delegation token is a string");

        let bytes: Vec<u8> = BASE64_STANDARD.decode(b64_txt)?;
        let delegation: Delegation<Ed25519Signature> = serde_ipld_dagcbor::from_slice(&bytes)?;
        assert_eq!(delegation.policy(), &vec![]);

        Ok(())
    }

    #[test]
    fn test_all_valid_delegations_roundtrip() -> TestResult {
        let valid = delegation_fixture()["valid"]
            .as_array()
            .expect("valid is an array");
        for (idx, entry) in valid.iter().enumerate() {
            let name = entry["name"].as_str().unwrap();
            let b64_txt = entry["token"].as_str().expect("token is a string");
            let original_bytes = BASE64_STANDARD.decode(b64_txt)?;
            let delegation: Delegation<Ed25519Signature> =
                serde_ipld_dagcbor::from_slice(&original_bytes)
                    .unwrap_or_else(|e| panic!("failed to decode '{name}': {e}"));
            let re_encoded = serde_ipld_dagcbor::to_vec(&delegation)
                .unwrap_or_else(|e| panic!("failed to re-encode '{name}': {e}"));
            assert_eq!(
                original_bytes, re_encoded,
                "roundtrip mismatch for delegation '{name}' (idx={idx})"
            );
        }
        Ok(())
    }
}
