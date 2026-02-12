//! Tests for policy predicate conformance using fixtures.
mod policy_conformance {
    use std::sync::OnceLock;

    use dialog_ucan::delegation::policy::predicate::Predicate;
    use ipld_core::ipld::Ipld;
    use testresult::TestResult;

    const POLICY_FIXTURE_STR: &str = include_str!("./fixtures/policy.json");
    static POLICY_FIXTURE: OnceLock<serde_json::Value> = OnceLock::new();
    fn policy_fixture() -> &'static serde_json::Value {
        POLICY_FIXTURE.get_or_init(|| {
            serde_json::from_str(POLICY_FIXTURE_STR).expect("fixture is invalid JSON")
        })
    }

    mod valid {
        use super::*;

        fn setup(idx: usize) -> (Ipld, Vec<Vec<Predicate>>) {
            let json: serde_json::Value =
                serde_json::from_str(POLICY_FIXTURE_STR).expect("fixture is invalid JSON");

            let args_bytes =
                serde_json::to_vec(&json["valid"][idx]["args"]).expect("fixture is valid JSON");
            let args_ipld: Ipld =
                serde_ipld_dagjson::from_slice(&args_bytes).expect("fixture is valid DAG-JSON");

            let policy_json: &serde_json::Value = &policy_fixture()["valid"][idx]["policies"];
            let policy_bytes = serde_json::to_vec(&policy_json).expect("policy is valid JSON");
            let policy_ipld: Ipld =
                serde_ipld_dagjson::from_slice(&policy_bytes).expect("policy is valid DAG-JSON");

            let mut policies = Vec::new();
            if let Ipld::List(ipld_policies) = policy_ipld {
                for ipld_policy in ipld_policies {
                    let mut policy: Vec<Predicate> = Vec::new();
                    if let Ipld::List(ipld_predicates) = ipld_policy {
                        for ipld_predicate in &ipld_predicates {
                            let predicate = Predicate::try_from(ipld_predicate.clone())
                                .expect("invalid nested predicate");
                            policy.push(predicate);
                        }
                    } else {
                        panic!("expected policy to be a list");
                    }
                    policies.push(policy);
                }
            } else {
                panic!("expected policy to be a list");
            }

            (args_ipld, policies)
        }

        mod scenario_zero {
            use super::*;

            static SCENARIO_ZERO_FIXTURE: OnceLock<(Ipld, Vec<Vec<Predicate>>)> = OnceLock::new();
            fn scenario_zero_fixture() -> &'static (Ipld, Vec<Vec<Predicate>>) {
                SCENARIO_ZERO_FIXTURE.get_or_init(|| setup(0))
            }

            #[test]
            fn test_zeroth_policy() -> TestResult {
                let (args, policies) = scenario_zero_fixture();
                for policy in &policies[0] {
                    assert!(policy.clone().run(args)?);
                }
                Ok(())
            }

            #[test]
            fn test_first_policy() -> TestResult {
                let (args, policies) = scenario_zero_fixture();
                for policy in &policies[1] {
                    assert!(policy.clone().run(args)?);
                }
                Ok(())
            }

            #[test]
            fn test_second_policy() -> TestResult {
                let (args, policies) = scenario_zero_fixture();
                for policy in &policies[2] {
                    assert!(policy.clone().run(args)?);
                }
                Ok(())
            }

            #[test]
            fn test_third_policy() -> TestResult {
                let (args, policies) = scenario_zero_fixture();
                for policy in &policies[3] {
                    assert!(policy.clone().run(args)?);
                }
                Ok(())
            }

            #[test]
            fn test_fourth_policy() -> TestResult {
                let (args, policies) = scenario_zero_fixture();
                for policy in &policies[4] {
                    assert!(policy.clone().run(args)?);
                }
                Ok(())
            }

            #[test]
            fn test_fifth_policy() -> TestResult {
                let (args, policies) = scenario_zero_fixture();
                for policy in &policies[5] {
                    assert!(policy.clone().run(args)?);
                }
                Ok(())
            }
        }

        mod scenario_one {
            use super::*;

            static SCENARIO_ONE_FIXTURE: OnceLock<(Ipld, Vec<Vec<Predicate>>)> = OnceLock::new();
            fn scenario_one_fixture() -> &'static (Ipld, Vec<Vec<Predicate>>) {
                SCENARIO_ONE_FIXTURE.get_or_init(|| setup(1))
            }

            #[test]
            fn test_the_lone_policy() -> TestResult {
                let (args, policies) = scenario_one_fixture();

                for policy in &policies[0] {
                    assert!(policy.clone().run(args)?);
                }

                Ok(())
            }
        }

        mod scenario_two {
            use super::*;

            static SCENARIO_TWO_FIXTURE: OnceLock<(Ipld, Vec<Vec<Predicate>>)> = OnceLock::new();
            fn scenario_two_fixture() -> &'static (Ipld, Vec<Vec<Predicate>>) {
                SCENARIO_TWO_FIXTURE.get_or_init(|| setup(2))
            }

            #[test]
            fn test_zeroth_policy() -> TestResult {
                let (args, policies) = scenario_two_fixture();
                for policy in &policies[0] {
                    assert!(policy.clone().run(args)?);
                }
                Ok(())
            }

            #[test]
            fn test_first_policy() -> TestResult {
                let (args, policies) = scenario_two_fixture();
                for policy in &policies[1] {
                    assert!(policy.clone().run(args)?);
                }
                Ok(())
            }

            #[test]
            fn test_second_policy() -> TestResult {
                let (args, policies) = scenario_two_fixture();
                for policy in &policies[2] {
                    assert!(policy.clone().run(args)?);
                }
                Ok(())
            }

            #[test]
            fn test_third_policy() -> TestResult {
                let (args, policies) = scenario_two_fixture();
                for policy in &policies[3] {
                    assert!(policy.clone().run(args)?);
                }
                Ok(())
            }

            #[test]
            fn test_fourth_policy() -> TestResult {
                let (args, policies) = scenario_two_fixture();
                for policy in &policies[4] {
                    assert!(policy.clone().run(args)?);
                }
                Ok(())
            }

            #[test]
            fn test_fifth_policy() -> TestResult {
                let (args, policies) = scenario_two_fixture();
                for policy in &policies[5] {
                    assert!(policy.clone().run(args)?);
                }
                Ok(())
            }

            #[test]
            fn test_sixth_policy() -> TestResult {
                let (args, policies) = scenario_two_fixture();
                for policy in &policies[6] {
                    assert!(policy.clone().run(args)?);
                }
                Ok(())
            }
        }

        mod scenario_three {
            use super::*;

            static SCENARIO_THREE_FIXTURE: OnceLock<(Ipld, Vec<Vec<Predicate>>)> = OnceLock::new();
            fn scenario_three_fixture() -> &'static (Ipld, Vec<Vec<Predicate>>) {
                SCENARIO_THREE_FIXTURE.get_or_init(|| setup(3))
            }

            #[test]
            fn test_zeroth_policy() -> TestResult {
                let (args, policies) = scenario_three_fixture();
                for policy in &policies[0] {
                    assert!(policy.clone().run(args)?);
                }
                Ok(())
            }
        }

        mod scenario_four {
            use super::*;

            static SCENARIO_FOUR_FIXTURE: OnceLock<(Ipld, Vec<Vec<Predicate>>)> = OnceLock::new();
            fn scenario_four_fixture() -> &'static (Ipld, Vec<Vec<Predicate>>) {
                SCENARIO_FOUR_FIXTURE.get_or_init(|| setup(4))
            }

            #[test]
            fn test_zeroth_policy() -> TestResult {
                let (args, policies) = scenario_four_fixture();
                for policy in &policies[0] {
                    assert!(policy.clone().run(args)?);
                }
                Ok(())
            }
        }

        mod scenario_five {
            use super::*;

            static SCENARIO_FIVE_FIXTURE: OnceLock<(Ipld, Vec<Vec<Predicate>>)> = OnceLock::new();
            fn scenario_five_fixture() -> &'static (Ipld, Vec<Vec<Predicate>>) {
                SCENARIO_FIVE_FIXTURE.get_or_init(|| setup(5))
            }

            #[test]
            fn test_zeroth_policy() -> TestResult {
                let (args, policies) = scenario_five_fixture();
                for policy in &policies[0] {
                    assert!(policy.clone().run(args)?);
                }
                Ok(())
            }
        }
    }

    mod invalid {
        use super::*;

        fn setup(idx: usize) -> (Ipld, Vec<Vec<Predicate>>) {
            let json: serde_json::Value =
                serde_json::from_str(POLICY_FIXTURE_STR).expect("fixture is invalid JSON");

            let args_bytes =
                serde_json::to_vec(&json["invalid"][idx]["args"]).expect("fixture is valid JSON");
            let args_ipld: Ipld =
                serde_ipld_dagjson::from_slice(&args_bytes).expect("fixture is valid DAG-JSON");

            let policy_json: &serde_json::Value = &policy_fixture()["invalid"][idx]["policies"];
            let policy_bytes = serde_json::to_vec(&policy_json).expect("policy is valid JSON");
            let policy_ipld: Ipld =
                serde_ipld_dagjson::from_slice(&policy_bytes).expect("policy is valid DAG-JSON");

            let mut policies = Vec::new();
            if let Ipld::List(ipld_policies) = policy_ipld {
                for ipld_policy in ipld_policies {
                    let mut policy: Vec<Predicate> = Vec::new();
                    if let Ipld::List(ipld_predicates) = ipld_policy {
                        for ipld_predicate in &ipld_predicates {
                            let predicate = Predicate::try_from(ipld_predicate.clone())
                                .expect("invalid nested predicate");
                            policy.push(predicate);
                        }
                    } else {
                        panic!("expected policy to be a list");
                    }
                    policies.push(policy);
                }
            } else {
                panic!("expected policy to be a list");
            }

            (args_ipld, policies)
        }

        mod scenario_zero {
            use super::*;

            static SCENARIO_ZERO_FIXTURE: OnceLock<(Ipld, Vec<Vec<Predicate>>)> = OnceLock::new();
            fn scenario_zero_fixture() -> &'static (Ipld, Vec<Vec<Predicate>>) {
                SCENARIO_ZERO_FIXTURE.get_or_init(|| setup(0))
            }

            #[test]
            fn test_zeroth_policy() -> TestResult {
                let (args, policies) = scenario_zero_fixture();
                for policy in &policies[0] {
                    assert!(!policy.clone().run(args)?);
                }
                Ok(())
            }

            #[test]
            fn test_first_policy() -> TestResult {
                let (args, policies) = scenario_zero_fixture();
                for policy in &policies[1] {
                    assert!(!policy.clone().run(args)?);
                }
                Ok(())
            }

            #[test]
            fn test_second_policy() -> TestResult {
                let (args, policies) = scenario_zero_fixture();
                for policy in &policies[2] {
                    assert!(!policy.clone().run(args)?);
                }
                Ok(())
            }

            #[test]
            fn test_third_policy() -> TestResult {
                let (args, policies) = scenario_zero_fixture();
                for policy in &policies[3] {
                    assert!(!policy.clone().run(args)?);
                }
                Ok(())
            }

            #[test]
            fn test_fourth_policy() -> TestResult {
                let (args, policies) = scenario_zero_fixture();
                for policy in &policies[4] {
                    assert!(!policy.clone().run(args)?);
                }
                Ok(())
            }
        }

        mod scenario_one {
            use super::*;

            static SCENARIO_ONE_FIXTURE: OnceLock<(Ipld, Vec<Vec<Predicate>>)> = OnceLock::new();
            fn scenario_one_fixture() -> &'static (Ipld, Vec<Vec<Predicate>>) {
                SCENARIO_ONE_FIXTURE.get_or_init(|| setup(1))
            }

            #[test]
            fn test_zeroth_policy() -> TestResult {
                let (args, policies) = scenario_one_fixture();
                for policy in &policies[0] {
                    assert!(!policy.clone().run(args)?);
                }
                Ok(())
            }
        }

        mod scenario_two {
            use super::*;

            static SCENARIO_TWO_FIXTURE: OnceLock<(Ipld, Vec<Vec<Predicate>>)> = OnceLock::new();
            fn scenario_two_fixture() -> &'static (Ipld, Vec<Vec<Predicate>>) {
                SCENARIO_TWO_FIXTURE.get_or_init(|| setup(2))
            }

            #[test]
            fn test_zeroth_policy() -> TestResult {
                let (args, policies) = scenario_two_fixture();
                // The conjunction must not hold: at least one predicate
                // must fail (return false or error).
                let all_pass = policies[0]
                    .iter()
                    .all(|p| p.clone().run(args).unwrap_or(false));
                assert!(!all_pass);
                Ok(())
            }
        }

        mod scenario_three {
            use super::*;

            static SCENARIO_THREE_FIXTURE: OnceLock<(Ipld, Vec<Vec<Predicate>>)> = OnceLock::new();
            fn scenario_three_fixture() -> &'static (Ipld, Vec<Vec<Predicate>>) {
                SCENARIO_THREE_FIXTURE.get_or_init(|| setup(3))
            }

            #[test]
            fn test_zeroth_policy() -> TestResult {
                let (args, policies) = scenario_three_fixture();
                // The conjunction must not hold: at least one predicate
                // must fail (return false or error).
                let all_pass = policies[0]
                    .iter()
                    .all(|p| p.clone().run(args).unwrap_or(false));
                assert!(!all_pass);
                Ok(())
            }
        }
    }
}
