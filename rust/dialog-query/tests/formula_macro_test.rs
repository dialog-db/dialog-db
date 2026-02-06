use dialog_query::{dsl::Input, Match, Term};
use dialog_macros::Formula;

/// Test that the Formula macro can parse basic struct with #[formula] and #[derived] attributes
#[derive(Debug, Clone, Formula)]
pub struct TestSum {
    /// First number
    pub of: u32,
    /// Second number
    pub with: u32,
    /// Result of addition
    #[derived]
    pub is: u32,
}

impl TestSum {
    pub fn derive(input: Input<Self>) -> Vec<Self> {
        vec![TestSum {
            of: input.of,
            with: input.with,
            is: input.of + input.with,
        }]
    }
}

#[test]
fn test_formula_macro_compiles() {
    // This test just verifies that the macro compiles without errors
    // In the next steps we'll add tests for generated functionality
}

#[test]
fn test_input_struct_generated() {
    // Verify TestSumInput exists with only non-derived fields
    let input = Input::<TestSum> { of: 5, with: 3 };
    assert_eq!(input.of, 5);
    assert_eq!(input.with, 3);
}

#[test]
fn test_match_struct_generated() {
    // Verify TestSumMatch exists with all fields as Term<T>
    let match_pattern = Match::<TestSum> {
        of: Term::var("x"),
        with: Term::var("y"),
        is: Term::var("result"),
    };

    // Just verify the struct can be constructed
    // More detailed tests will come when we implement the full Formula trait
    assert!(matches!(match_pattern.of, Term::Variable { .. }));
}
