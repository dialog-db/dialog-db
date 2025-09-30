use super::*;
use crate::{Term, Value};

#[test]
fn test_syntax_analysis_new() {
    let analysis = SyntaxAnalysis::new(100);

    match analysis {
        SyntaxAnalysis::Candidate { cost, desired, depends } => {
            assert_eq!(cost, 100);
            assert_eq!(desired.count(), 0);
            assert_eq!(depends.size(), 0);
        }
        _ => panic!("Expected Candidate variant"),
    }
}

#[test]
fn test_syntax_analysis_depends() {
    let mut analysis = SyntaxAnalysis::new(50);

    assert_eq!(analysis.depends().size(), 0);
    assert_eq!(*analysis.cost(), 50);
}

#[test]
fn test_syntax_analysis_provides() {
    let mut analysis = SyntaxAnalysis::new(10);

    // Add a desired variable
    let term = Term::<Value>::var("x");
    analysis.desire(&term, 5);

    let provides = analysis.provides();
    assert_eq!(provides.size(), 1);
    assert!(provides.contains(&Term::<Value>::var("x")));
}

#[test]
fn test_plan_context_from_candidate() {
    let mut analysis = SyntaxAnalysis::new(100);
    let term = Term::<Value>::var("y");
    analysis.desire(&term, 10);

    let context: PlanContext = analysis.try_into().expect("Should convert to PlanContext");

    assert_eq!(context.cost, 100);
    assert_eq!(context.desired.count(), 1);
    assert_eq!(context.depends.size(), 0);
}

#[test]
fn test_plan_context_from_incomplete_fails() {
    let mut analysis = SyntaxAnalysis::new(100);
    let term = Term::<Value>::var("z");

    // Make it incomplete by requiring something
    analysis.require(&term);

    let result: Result<PlanContext, _> = analysis.try_into();
    assert!(result.is_err());
}

#[test]
fn test_plan_context_provides() {
    let mut scope = VariableScope::new();
    let term_a = Term::<Value>::var("a");
    scope.add(&term_a);

    let mut desired = Desired::new();
    desired.insert(&Term::<Value>::var("b"), 5);

    let context = PlanContext {
        cost: 50,
        desired,
        depends: scope,
    };

    let provides = context.provides();
    assert_eq!(provides.size(), 1);
    assert!(provides.contains(&Term::<Value>::var("b")));
}

#[test]
fn test_plan_context_depends() {
    let mut scope = VariableScope::new();
    let term = Term::<Value>::var("x");
    scope.add(&term);

    let context = PlanContext {
        cost: 20,
        desired: Desired::new(),
        depends: scope.clone(),
    };

    assert_eq!(context.depends().size(), 1);
    assert!(context.depends().contains(&Term::<Value>::var("x")));
}

#[test]
fn test_plan_context_round_trip() {
    let mut analysis = SyntaxAnalysis::new(75);
    let term = Term::<Value>::var("roundtrip");
    analysis.desire(&term, 8);

    let context: PlanContext = analysis.clone().try_into().expect("Should convert");
    let back: SyntaxAnalysis = context.into();

    match back {
        SyntaxAnalysis::Candidate { cost, desired, depends } => {
            assert_eq!(cost, 75);
            assert_eq!(desired.count(), 1);
            assert_eq!(depends.size(), 0);
        }
        _ => panic!("Expected Candidate variant"),
    }
}

#[test]
fn test_syntax_analysis_require_transitions_to_incomplete() {
    let mut analysis = SyntaxAnalysis::new(30);
    let term = Term::<Value>::var("required_var");

    analysis.require(&term);

    match analysis {
        SyntaxAnalysis::Incomplete { cost, required, desired, depends } => {
            assert_eq!(cost, 30);
            assert_eq!(required.count(), 1);
            assert_eq!(desired.count(), 0);
            assert_eq!(depends.size(), 0);
        }
        _ => panic!("Expected Incomplete variant after require"),
    }
}

#[test]
fn test_syntax_analysis_desire_adds_to_desired() {
    let mut analysis = SyntaxAnalysis::new(40);
    let term1 = Term::<Value>::var("var1");
    let term2 = Term::<Value>::var("var2");

    analysis.desire(&term1, 10);
    analysis.desire(&term2, 20);

    assert_eq!(analysis.desired().count(), 2);
    assert_eq!(*analysis.cost(), 40); // Cost unchanged for named variables
}

#[test]
fn test_syntax_analysis_desire_blank_increases_cost() {
    let mut analysis = SyntaxAnalysis::new(40);
    let blank_term = Term::<Value>::Variable {
        name: None,
        _type: crate::Type::default(),
    };

    analysis.desire(&blank_term, 15);

    assert_eq!(*analysis.cost(), 55); // 40 + 15
}

#[test]
fn test_syntax_analysis_incomplete_to_candidate_transition() {
    let mut analysis = SyntaxAnalysis::new(60);
    let term1 = Term::<Value>::var("a");
    let term2 = Term::<Value>::var("b");

    // Make it incomplete
    analysis.require(&term1);

    match &analysis {
        SyntaxAnalysis::Incomplete { .. } => {}
        _ => panic!("Should be Incomplete"),
    }

    // Satisfy the requirement by marking it desired
    analysis.desire(&term1, 5);

    // Should now be Candidate (no required left)
    match analysis {
        SyntaxAnalysis::Candidate { cost, desired, depends } => {
            assert_eq!(cost, 60);
            assert_eq!(desired.count(), 1);
            assert_eq!(depends.size(), 0);
        }
        _ => panic!("Expected Candidate after satisfying requirements"),
    }
}

#[test]
fn test_desired_and_depends_mutual_exclusivity() {
    let mut analysis = SyntaxAnalysis::new(100);
    let term = Term::<Value>::var("exclusive");

    // First, desire it
    analysis.desire(&term, 10);
    assert_eq!(analysis.desired().count(), 1);
    assert_eq!(analysis.depends().size(), 0);

    // Note: We'll add depend() method in Phase 5
    // For now, this test documents the expected behavior
}