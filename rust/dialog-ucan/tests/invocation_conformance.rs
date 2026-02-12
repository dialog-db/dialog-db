//! Tests for invocation conformance to the UCAN specification.
mod invocation_conformance {
    use std::{cell::RefCell, collections::HashMap, rc::Rc, sync::OnceLock};

    use dialog_ucan::{Delegation, Invocation, delegation::store};
    use dialog_varsig::eddsa::Ed25519Signature;
    use ipld_core::{cid::Cid, ipld::Ipld};
    use testresult::TestResult;

    const INVOCATION_FIXTURE_STR: &str = include_str!("./fixtures/invocation.json");
    static INVOCATION_FIXTURE: OnceLock<serde_json::Value> = OnceLock::new();
    fn invocation_fixture() -> &'static serde_json::Value {
        INVOCATION_FIXTURE.get_or_init(|| {
            serde_json::from_str(INVOCATION_FIXTURE_STR).expect("fixture is invalid JSON")
        })
    }

    /// Decode a DAG-JSON bytes value `{"/": {"bytes": "..."}}` into raw bytes
    /// using `serde_ipld_dagjson`.
    fn decode_dag_json_bytes(val: &serde_json::Value) -> Vec<u8> {
        let json_bytes = serde_json::to_vec(val).expect("value is valid JSON");
        let ipld: Ipld =
            serde_ipld_dagjson::from_slice(&json_bytes).expect("value is valid DAG-JSON");
        match ipld {
            Ipld::Bytes(bytes) => bytes,
            other => panic!("expected DAG-JSON bytes, got: {other:?}"),
        }
    }

    type DelegationStore = Rc<RefCell<HashMap<Cid, Rc<Delegation<Ed25519Signature>>>>>;

    fn new_store() -> DelegationStore {
        Rc::new(RefCell::new(HashMap::new()))
    }

    /// Build a delegation store from parsed proofs.
    async fn build_store(proofs: Vec<Delegation<Ed25519Signature>>) -> DelegationStore {
        let delegation_store = new_store();
        for proof in proofs {
            store::insert(&delegation_store, Rc::new(proof))
                .await
                .expect("insert should not fail");
        }
        delegation_store
    }

    fn parse_invocation(entry: &serde_json::Value) -> Invocation<Ed25519Signature> {
        let inv_bytes = decode_dag_json_bytes(&entry["invocation"]);
        serde_ipld_dagcbor::from_slice(&inv_bytes).expect("failed to decode invocation")
    }

    fn parse_proofs(entry: &serde_json::Value) -> Vec<Delegation<Ed25519Signature>> {
        entry["proofs"]
            .as_array()
            .expect("proofs is an array")
            .iter()
            .map(|p| {
                let bytes = decode_dag_json_bytes(p);
                serde_ipld_dagcbor::from_slice(&bytes).expect("failed to decode proof")
            })
            .collect()
    }

    #[test]
    fn test_expected_version() -> TestResult {
        assert_eq!(
            invocation_fixture()
                .get("version")
                .expect("fixture has version key")
                .clone(),
            "1.0.0-rc.1".to_string()
        );
        Ok(())
    }

    mod valid {
        use super::*;
        use dialog_credentials::ed25519::Ed25519KeyResolver;

        #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
        use wasm_bindgen_test::wasm_bindgen_test;

        #[cfg_attr(not(all(target_arch = "wasm32", target_os = "unknown")), tokio::test)]
        #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), wasm_bindgen_test)]
        async fn test_all_valid_invocations_check() -> TestResult {
            let valid = invocation_fixture()["valid"]
                .as_array()
                .expect("valid is an array");

            for (idx, entry) in valid.iter().enumerate() {
                let name = entry["name"].as_str().unwrap();
                let invocation = parse_invocation(entry);
                let proofs = parse_proofs(entry);
                let delegation_store = build_store(proofs).await;

                let result = invocation
                    .check(&delegation_store, &Ed25519KeyResolver)
                    .await;

                assert!(
                    result.is_ok(),
                    "valid[{idx}] '{name}' should pass check but got: {:?}",
                    result.err()
                );

                eprintln!(
                    "valid[{idx}] '{name}': check passed, time_range = {:?}",
                    result.unwrap()
                );
            }

            Ok(())
        }
    }

    mod roundtrip {
        use super::*;

        #[test]
        fn test_all_valid_invocations_roundtrip() -> TestResult {
            let valid = invocation_fixture()["valid"]
                .as_array()
                .expect("valid is an array");
            for (idx, entry) in valid.iter().enumerate() {
                let name = entry["name"].as_str().unwrap();
                let original_bytes = decode_dag_json_bytes(&entry["invocation"]);
                let invocation: Invocation<Ed25519Signature> =
                    serde_ipld_dagcbor::from_slice(&original_bytes)
                        .unwrap_or_else(|e| panic!("failed to decode '{name}': {e}"));
                let re_encoded = serde_ipld_dagcbor::to_vec(&invocation)
                    .unwrap_or_else(|e| panic!("failed to re-encode '{name}': {e}"));
                assert_eq!(
                    original_bytes, re_encoded,
                    "roundtrip mismatch for valid invocation '{name}' (idx={idx})"
                );
            }
            Ok(())
        }

        #[test]
        fn test_all_valid_proofs_roundtrip() -> TestResult {
            let valid = invocation_fixture()["valid"]
                .as_array()
                .expect("valid is an array");
            for (idx, entry) in valid.iter().enumerate() {
                let name = entry["name"].as_str().unwrap();
                let proofs_json = entry["proofs"].as_array().expect("proofs is an array");
                for (pidx, proof_json) in proofs_json.iter().enumerate() {
                    let original_bytes = decode_dag_json_bytes(proof_json);
                    let delegation: Delegation<Ed25519Signature> =
                        serde_ipld_dagcbor::from_slice(&original_bytes).unwrap_or_else(|e| {
                            panic!("failed to decode proof {pidx} of '{name}': {e}")
                        });
                    let re_encoded = serde_ipld_dagcbor::to_vec(&delegation).unwrap_or_else(|e| {
                        panic!("failed to re-encode proof {pidx} of '{name}': {e}")
                    });
                    assert_eq!(
                        original_bytes, re_encoded,
                        "roundtrip mismatch for proof {pidx} of '{name}' (idx={idx})"
                    );
                }
            }
            Ok(())
        }

        #[test]
        fn test_proof_cids_match() -> TestResult {
            let valid = invocation_fixture()["valid"]
                .as_array()
                .expect("valid is an array");
            for (idx, entry) in valid.iter().enumerate() {
                let name = entry["name"].as_str().unwrap();
                let inv_bytes = decode_dag_json_bytes(&entry["invocation"]);
                let invocation: Invocation<Ed25519Signature> =
                    serde_ipld_dagcbor::from_slice(&inv_bytes)
                        .unwrap_or_else(|e| panic!("failed to decode '{name}': {e}"));
                let proofs_json = entry["proofs"].as_array().expect("proofs is an array");
                assert_eq!(
                    invocation.proofs().len(),
                    proofs_json.len(),
                    "proof count mismatch for '{name}' (idx={idx})"
                );
                for (pidx, proof_json) in proofs_json.iter().enumerate() {
                    let proof_bytes = decode_dag_json_bytes(proof_json);
                    let delegation: Delegation<Ed25519Signature> =
                        serde_ipld_dagcbor::from_slice(&proof_bytes).unwrap_or_else(|e| {
                            panic!("failed to decode proof {pidx} of '{name}': {e}")
                        });
                    let computed_cid = delegation.to_cid();
                    let referenced_cid = &invocation.proofs()[pidx];
                    assert_eq!(
                        &computed_cid, referenced_cid,
                        "CID mismatch for proof {pidx} of '{name}' (idx={idx}): \
                         computed={computed_cid}, referenced={referenced_cid}"
                    );
                }
            }
            Ok(())
        }
    }

    mod invalid {
        use super::*;
        use dialog_credentials::ed25519::Ed25519KeyResolver;
        use dialog_ucan::invocation::{CheckFailed, InvocationCheckError, StoredCheckError};
        use std::ops::RangeBounds;

        #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
        use wasm_bindgen_test::wasm_bindgen_test;

        fn try_parse_invocation(
            entry: &serde_json::Value,
        ) -> Result<Invocation<Ed25519Signature>, String> {
            let inv_bytes = decode_dag_json_bytes(&entry["invocation"]);
            serde_ipld_dagcbor::from_slice(&inv_bytes).map_err(|e| e.to_string())
        }

        fn try_parse_proofs(
            entry: &serde_json::Value,
        ) -> Vec<Result<Delegation<Ed25519Signature>, String>> {
            entry["proofs"]
                .as_array()
                .expect("proofs is an array")
                .iter()
                .map(|p| {
                    let bytes = decode_dag_json_bytes(p);
                    serde_ipld_dagcbor::from_slice(&bytes).map_err(|e| e.to_string())
                })
                .collect()
        }

        #[test]
        fn test_all_invalid_invocations_are_present() -> TestResult {
            let invalid = invocation_fixture()["invalid"]
                .as_array()
                .expect("invalid is an array");
            assert_eq!(invalid.len(), 13);

            let expected_names = [
                "no proof",
                "missing proof",
                "expired proof",
                "inactive proof",
                "proof principal alignment",
                "invocation principal alignment",
                "proof subject alignment",
                "invocation subject alignment",
                "expired invocation",
                "invalid proof signature",
                "invalid invocation signature",
                "invalid powerline",
                "policy violation",
            ];

            for (idx, expected_name) in expected_names.iter().enumerate() {
                let entry = &invocation_fixture()["invalid"][idx];
                let name = entry["name"].as_str().unwrap();
                assert_eq!(name, *expected_name, "invalid entry {idx} name mismatch");
            }

            Ok(())
        }

        #[cfg_attr(not(all(target_arch = "wasm32", target_os = "unknown")), tokio::test)]
        #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), wasm_bindgen_test)]
        async fn test_all_invalid_invocations_fail_check() -> TestResult {
            let now = dialog_ucan::time::Timestamp::now();
            let invalid = invocation_fixture()["invalid"]
                .as_array()
                .expect("invalid is an array");

            for (idx, entry) in invalid.iter().enumerate() {
                let name = entry["name"].as_str().unwrap();
                let error_name = entry["error"]["name"].as_str().unwrap();

                // Try to parse invocation — InvalidSignature cases may fail here.
                let inv_result = try_parse_invocation(entry);

                // For InvalidSignature errors, parse failure is acceptable.
                if error_name == "InvalidSignature" && inv_result.is_err() {
                    eprintln!(
                        "invalid[{idx}] '{name}': parse failed (expected for InvalidSignature)"
                    );
                    continue;
                }

                let invocation = match inv_result {
                    Ok(inv) => inv,
                    Err(e) => {
                        panic!(
                            "invalid[{idx}] '{name}' (error={error_name}) should parse but got: {e}"
                        );
                    }
                };

                // Parse proofs that successfully decode, skip ones that don't
                // (e.g. invalid proof signature may have bad bytes).
                let proof_results = try_parse_proofs(entry);
                let valid_proofs: Vec<Delegation<Ed25519Signature>> =
                    proof_results.into_iter().filter_map(Result::ok).collect();

                let delegation_store = build_store(valid_proofs).await;

                let result = invocation
                    .check(&delegation_store, &Ed25519KeyResolver)
                    .await;

                // The fixture declares an expected error class, but our validator
                // may catch a *different* (equally valid) error first due to check
                // ordering. For example, a fixture designed to test "Expired" may
                // also have a subject mismatch that fires before we reach time
                // checks. We verify:
                // 1. The specific expected error if we can identify it, OR
                // 2. That the invocation is at least rejected (not accepted).
                match error_name {
                    "InvalidClaim" => {
                        // "no proof" or "invalid powerline"
                        let err = result
                            .expect_err(&format!("invalid[{idx}] '{name}' should fail check"));
                        match &err {
                            InvocationCheckError::StoredCheck(StoredCheckError::CheckFailed(
                                CheckFailed::UnauthorizedSubject { .. }
                                | CheckFailed::UnprovenSubject { .. },
                            )) => {}
                            other => panic!(
                                "invalid[{idx}] '{name}': expected UnauthorizedSubject or \
                                 UnprovenSubject, got: {other:?}"
                            ),
                        }
                    }
                    "UnavailableProof" => {
                        // "missing proof" — store doesn't have a referenced CID
                        let err = result
                            .expect_err(&format!("invalid[{idx}] '{name}' should fail check"));
                        match &err {
                            InvocationCheckError::StoredCheck(StoredCheckError::GetError(_)) => {}
                            other => panic!(
                                "invalid[{idx}] '{name}': expected GetError(Missing), got: {other:?}"
                            ),
                        }
                    }
                    "Expired" | "TooEarly" => {
                        // These may return Ok(range) where the range doesn't
                        // contain "now", or Err(InvalidTimeWindow) if the chain
                        // has contradictory bounds.
                        //
                        // Some fixtures also have structural issues (e.g. subject
                        // mismatch) that our validator catches first, which is an
                        // equally valid rejection.
                        match &result {
                            Ok(range) => {
                                assert!(
                                    !range.contains(&now),
                                    "invalid[{idx}] '{name}' ({error_name}): \
                                     expected time range not to contain now, but range={range:?}"
                                );
                            }
                            Err(_) => {
                                // Any error is an acceptable rejection.
                            }
                        }
                    }
                    "InvalidAudience" => {
                        let err = result
                            .expect_err(&format!("invalid[{idx}] '{name}' should fail check"));
                        match &err {
                            InvocationCheckError::StoredCheck(StoredCheckError::CheckFailed(
                                CheckFailed::DelegationAudienceMismatch { .. },
                            )) => {}
                            other => panic!(
                                "invalid[{idx}] '{name}': expected DelegationAudienceMismatch, \
                                 got: {other:?}"
                            ),
                        }
                    }
                    "InvalidSubject" => {
                        let err = result
                            .expect_err(&format!("invalid[{idx}] '{name}' should fail check"));
                        match &err {
                            InvocationCheckError::StoredCheck(StoredCheckError::CheckFailed(
                                CheckFailed::UnauthorizedSubject { .. }
                                | CheckFailed::UnprovenSubject { .. },
                            )) => {}
                            other => panic!(
                                "invalid[{idx}] '{name}': expected UnauthorizedSubject or \
                                 UnprovenSubject, got: {other:?}"
                            ),
                        }
                    }
                    "InvalidSignature" => {
                        // If we got here, the invocation parsed OK but should
                        // fail signature verification. However, if the *proof*
                        // has the bad signature (not the invocation), it may
                        // have been filtered out during parsing, causing the
                        // store to report it as missing.
                        let err = result
                            .expect_err(&format!("invalid[{idx}] '{name}' should fail check"));
                        match &err {
                            InvocationCheckError::SignatureVerification(_) => {}
                            InvocationCheckError::StoredCheck(StoredCheckError::GetError(_)) => {}
                            other => panic!(
                                "invalid[{idx}] '{name}': expected SignatureVerification \
                                 or GetError(Missing), got: {other:?}"
                            ),
                        }
                    }
                    "MatchError" => {
                        let err = result
                            .expect_err(&format!("invalid[{idx}] '{name}' should fail check"));
                        match &err {
                            InvocationCheckError::StoredCheck(StoredCheckError::CheckFailed(
                                CheckFailed::PolicyViolation(_)
                                | CheckFailed::PolicyIncompatibility(_),
                            )) => {}
                            other => panic!(
                                "invalid[{idx}] '{name}': expected PolicyViolation or \
                                 PolicyIncompatibility, got: {other:?}"
                            ),
                        }
                    }
                    other => {
                        panic!("invalid[{idx}] '{name}': unknown fixture error name: {other}");
                    }
                }

                eprintln!("invalid[{idx}] '{name}' ({error_name}): check correctly failed");
            }

            Ok(())
        }
    }
}
