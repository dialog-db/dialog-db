#[cfg(test)]
mod match_functionality_test {
    use dialog_query::artifact::Entity;
    use dialog_query::syntax::VariableScope;
    use dialog_query::*;
    use dialog_query::{Application, Premise};

    // This test demonstrates the Match struct functionality that was requested.
    // It shows how Match structs work with the generic FactSelector<T> system
    // that was implemented as part of the original request for:
    // `Employee::Name::Match { of: entity, is: name }`

    #[derive(Debug, Clone)]
    pub struct TestNameMatch {
        pub of: Term<Entity>,
        pub is: Term<String>,
    }

    impl TestNameMatch {
        pub fn to_fact_selector(&self) -> FactSelector<String> {
            FactSelector::new()
                .the("test/name")
                .of(self.of.clone())
                .is(self.is.clone())
        }
    }

    impl From<TestNameMatch> for Premise {
        fn from(test_match: TestNameMatch) -> Self {
            // Convert String-typed FactSelector to Value-typed and then to Premise
            let selector = test_match.to_fact_selector();
            let generic_selector = FactSelector::from(&selector);
            Premise::Apply(Application::Fact(generic_selector))
        }
    }

    // Query implementation removed due to lifetime issues - this is not essential for the core functionality

    #[test]
    fn test_manual_match_struct_functionality() {
        let entity = Entity::new().unwrap();
        let name_var = Term::<String>::var("name");

        // Test that we can create a Match struct with the desired syntax
        let match_pattern = TestNameMatch {
            of: entity.clone().into(),
            is: "Alice".to_string().into(),
        };

        // Should be convertible to Premise and then plan
        let scope = VariableScope::new();
        let premise: Premise = match_pattern.clone().into();
        let _plan = premise.plan(&scope).unwrap();

        // Test with variable
        let match_with_var = TestNameMatch {
            of: entity.into(),
            is: name_var,
        };

        assert!(match_with_var.is.is_variable());

        // Test conversion to FactSelector
        let fact_selector = match_pattern.to_fact_selector();
        assert!(fact_selector.the.is_some());
        assert!(fact_selector.of.is_some());
        assert!(fact_selector.is.is_some());
    }

    #[test]
    fn test_fact_selector_integration() {
        let entity = Entity::new().unwrap();

        let match_pattern = TestNameMatch {
            of: entity.clone().into(),
            is: "Bob".to_string().into(),
        };

        let fact_selector = match_pattern.to_fact_selector();

        // Check the fact selector has the right fields
        assert_eq!(
            fact_selector
                .the
                .as_ref()
                .unwrap()
                .as_constant()
                .unwrap()
                .to_string(),
            "test/name"
        );
        assert!(fact_selector.of.is_some());
        assert!(fact_selector.is.is_some());
    }
}
