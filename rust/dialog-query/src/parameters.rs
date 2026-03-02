use crate::Parameter;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A name-to-parameter mapping that describes how a premise is applied.
///
/// Every premise type (relation, concept, formula, constraint) exposes its
/// inputs and outputs as named parameters. A `Parameters` instance binds
/// each parameter name to a [`Parameter`] — either a concrete constant
/// or a named variable that will be resolved during query evaluation.
///
/// During planning, the [`Schema`](crate::Schema) is consulted to determine
/// which parameters are required vs optional, and the planner uses this
/// information together with the current [`Environment`](crate::Environment)
/// to decide whether the premise is viable.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct Parameters(HashMap<String, Parameter>);
impl Parameters {
    /// Create a new empty parameter set
    pub fn new() -> Self {
        Self::default()
    }
    /// Returns the parameter associated with the given name, if has one.
    pub fn get(&self, name: &str) -> Option<&Parameter> {
        self.0.get(name)
    }

    /// Inserts a new parameter binding for the given name.
    /// If the parameter already exists, it will be overwritten.
    ///
    /// Accepts anything convertible to [`Parameter`], including `Term<T>`
    /// for any `T: Scalar`.
    pub fn insert(&mut self, name: String, param: impl Into<Parameter>) {
        self.0.insert(name, param.into());
    }

    /// Checks if a parameter binding exists for the given name.
    pub fn contains(&self, name: &str) -> bool {
        self.0.contains_key(name)
    }

    /// Returns an iterator over all name-parameter pairs in this binding set.
    pub fn iter(&self) -> impl Iterator<Item = (&String, &Parameter)> {
        self.0.iter()
    }

    /// Returns an iterator over the parameter names in this binding set.
    pub fn keys(&self) -> impl Iterator<Item = &String> {
        self.0.keys()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifact::{Type, Value};

    #[dialog_common::test]
    fn it_performs_basic_operations() {
        let mut terms = Parameters::new();

        let name_param = Parameter::Variable {
            name: Some("name".into()),
            typ: None,
        };
        terms.insert("name".to_string(), name_param.clone());

        assert_eq!(terms.get("name"), Some(&name_param));
        assert_eq!(terms.get("nonexistent"), None);
        assert!(terms.contains("name"));
        assert!(!terms.contains("nonexistent"));

        let collected: Vec<_> = terms.iter().collect();
        assert_eq!(collected.len(), 1);
        assert_eq!(collected[0].0, &"name".to_string());
        assert_eq!(collected[0].1, &name_param);
    }

    #[dialog_common::test]
    fn it_serializes_variables_to_json() {
        let mut params = Parameters::new();
        params.insert(
            "name".to_string(),
            Parameter::Variable {
                name: Some("x".into()),
                typ: None,
            },
        );
        params.insert(
            "age".to_string(),
            Parameter::Variable {
                name: Some("y".into()),
                typ: None,
            },
        );

        let json = serde_json::to_value(&params).unwrap();
        let obj = json.as_object().unwrap();
        assert_eq!(obj.len(), 2);
        assert_eq!(obj["name"], serde_json::json!({"?": {"name": "x"}}));
        assert_eq!(obj["age"], serde_json::json!({"?": {"name": "y"}}));
    }

    #[dialog_common::test]
    fn it_serializes_constants_to_json() {
        let mut params = Parameters::new();
        params.insert("name".to_string(), Parameter::from("Alice".to_string()));
        params.insert("age".to_string(), Parameter::from(42u32));

        let json = serde_json::to_value(&params).unwrap();
        let obj = json.as_object().unwrap();
        assert_eq!(obj["name"], serde_json::json!("Alice"));
        assert_eq!(obj["age"], serde_json::json!(42));
    }

    #[dialog_common::test]
    fn it_serializes_mixed_terms_to_json() {
        let mut params = Parameters::new();
        params.insert(
            "this".to_string(),
            Parameter::Variable {
                name: Some("person".into()),
                typ: None,
            },
        );
        params.insert("name".to_string(), Parameter::from("Alice".to_string()));

        let json = serde_json::to_value(&params).unwrap();
        let obj = json.as_object().unwrap();
        assert_eq!(obj["this"], serde_json::json!({"?": {"name": "person"}}));
        assert_eq!(obj["name"], serde_json::json!("Alice"));
    }

    #[dialog_common::test]
    fn it_deserializes_variables_from_json() {
        let json = serde_json::json!({
            "name": {"?": {"name": "x"}},
            "age": {"?": {"name": "y"}}
        });

        let params: Parameters = serde_json::from_value(json).unwrap();
        assert_eq!(
            params.get("name"),
            Some(&Parameter::Variable {
                name: Some("x".into()),
                typ: None
            })
        );
        assert_eq!(
            params.get("age"),
            Some(&Parameter::Variable {
                name: Some("y".into()),
                typ: None
            })
        );
    }

    #[dialog_common::test]
    fn it_deserializes_constants_from_json() {
        let json = serde_json::json!({
            "name": "Alice",
            "active": true
        });

        let params: Parameters = serde_json::from_value(json).unwrap();
        assert_eq!(
            params.get("name"),
            Some(&Parameter::Constant(Value::from("Alice".to_string())))
        );
        assert_eq!(
            params.get("active"),
            Some(&Parameter::Constant(Value::from(true)))
        );
    }

    #[dialog_common::test]
    fn it_deserializes_blank_variable_from_json() {
        let json = serde_json::json!({
            "name": {"?": {}}
        });

        let params: Parameters = serde_json::from_value(json).unwrap();
        assert!(params.contains("name"));
        let param = params.get("name").unwrap();
        assert_eq!(param, &Parameter::blank());
    }

    #[dialog_common::test]
    fn it_round_trips_through_json() {
        let mut original = Parameters::new();
        original.insert(
            "this".to_string(),
            Parameter::Variable {
                name: Some("entity".into()),
                typ: None,
            },
        );
        original.insert("name".to_string(), Parameter::from("Alice".to_string()));
        original.insert("active".to_string(), Parameter::from(true));

        let json = serde_json::to_value(&original).unwrap();
        let restored: Parameters = serde_json::from_value(json).unwrap();
        assert_eq!(original, restored);
    }

    #[dialog_common::test]
    fn it_preserves_json_integer_types() {
        let json = serde_json::json!({"count": 42, "offset": -5, "ratio": 3.14});
        let params: Parameters = serde_json::from_value(json).unwrap();

        // Integer types are now correctly preserved (the bug is fixed!)
        assert_eq!(
            params.get("count"),
            Some(&Parameter::Constant(Value::UnsignedInt(42)))
        );
        assert_eq!(
            params.get("offset"),
            Some(&Parameter::Constant(Value::SignedInt(-5)))
        );
        assert_eq!(
            params.get("ratio"),
            Some(&Parameter::Constant(Value::Float(3.14)))
        );
    }

    #[dialog_common::test]
    fn it_deserializes_empty_parameters() {
        let json = serde_json::json!({});
        let params: Parameters = serde_json::from_value(json).unwrap();
        assert_eq!(params, Parameters::new());
    }

    #[dialog_common::test]
    fn it_preserves_type_info_from_typed_terms() {
        use crate::Term;

        let mut params = Parameters::new();
        params.insert(
            "name".to_string(),
            Parameter::from(Term::<String>::var("x")),
        );

        let param = params.get("name").unwrap();
        assert_eq!(
            param,
            &Parameter::Variable {
                name: Some("x".into()),
                typ: Some(Type::String)
            }
        );
    }
}
