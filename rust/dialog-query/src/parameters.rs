use crate::pattern::{Pattern, Shape};
use crate::term::Term;
use crate::types::Any;
use serde::ser::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::BTreeMap;
use std::sync::Arc;

/// A name-to-parameter mapping that describes how a premise is applied.
///
/// Every premise type (relation, concept, formula, constraint) exposes its
/// inputs and outputs as named parameters. A `Parameters` instance binds
/// each parameter name to a [`Pattern`]: a leaf, which is a
/// [`Term<Any>`] (a concrete constant or a named variable that will be
/// resolved during query evaluation), or a map, a list or a tuple of
/// patterns.
///
/// A premise reads a term in each of its slots, and [`get`](Self::get)
/// and [`iter`](Self::iter) give it one whatever the parameter holds: a
/// leaf as it is, and for a map, a list or a tuple the variable that
/// stands for the whole value (its *whole*, see [`Shape`]). The premise
/// is run with that variable and the pattern is applied around it, so a
/// premise that knows nothing of patterns keeps working. Whatever asks
/// which variables a premise binds or needs reads
/// [`pattern`](Self::pattern) and looks inside.
///
/// During planning, the [`Schema`](crate::Schema) is consulted to determine
/// which parameters are required vs optional, and the planner uses this
/// information together with the current [`Environment`](crate::Environment)
/// to decide whether the premise is viable.
#[derive(Debug, Clone, PartialEq, Default)]
/// Kept in name order: everything that iterates a query's parameters,
/// the planner among them, then sees them in one order in every
/// process, and a plan does not depend on the hash seed the process
/// drew.
///
/// Shared on clone: a premise, a plan and every conclusion a concept
/// yields carry the same parameters, and the evaluator clones them per
/// row, so a clone is a reference count and the map is copied only when
/// a holder writes to it.
pub struct Parameters(Arc<BTreeMap<String, Parameter>>);

/// What one name is bound to: the term a premise reads there, and the
/// pattern it stands for when the parameter is a map, a list or a
/// tuple.
#[derive(Debug, Clone, PartialEq)]
struct Parameter {
    /// A leaf, or the whole of `shape`.
    term: Term<Any>,
    /// The map, list or tuple this parameter holds, when it holds one.
    shape: Option<Arc<Pattern>>,
}

impl Serialize for Parameters {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if let Some((name, pattern)) = self.shapes().next() {
            return Err(S::Error::custom(format!(
                "the parameter `{name}` holds the pattern {pattern}, which has no wire form: \
                 only a variable or a constant is written"
            )));
        }
        serializer.collect_map(self.iter())
    }
}

impl<'de> Deserialize<'de> for Parameters {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        BTreeMap::<String, Term<Any>>::deserialize(deserializer).map(|map| {
            Parameters(Arc::new(
                map.into_iter()
                    .map(|(name, term)| (name, Parameter { term, shape: None }))
                    .collect(),
            ))
        })
    }
}

impl Parameters {
    /// Create a new empty parameter set
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the term a premise reads under the given name, if it has
    /// one: the leaf the parameter holds, or the variable standing for
    /// the whole value where it holds a map, a list or a tuple.
    pub fn get(&self, name: &str) -> Option<&Term<Any>> {
        self.0.get(name).map(|parameter| &parameter.term)
    }

    /// Returns the pattern the given name is bound to, if it has one. A
    /// leaf is the pattern of its term.
    pub fn pattern(&self, name: &str) -> Option<Pattern> {
        self.0.get(name).map(|parameter| match &parameter.shape {
            Some(shape) => shape.as_ref().clone(),
            None => Pattern::Term(parameter.term.clone()),
        })
    }

    /// Returns what the given name is bound to in one lookup: the term a
    /// premise reads there, and the map, list or tuple pattern it stands
    /// for when the parameter holds one.
    pub fn slot(&self, name: &str) -> Option<(&Term<Any>, Option<&Pattern>)> {
        self.0
            .get(name)
            .map(|parameter| (&parameter.term, parameter.shape.as_deref()))
    }

    /// Returns the map, list or tuple pattern the given name is bound
    /// to, when it is bound to one.
    pub fn shape(&self, name: &str) -> Option<&Pattern> {
        self.0.get(name)?.shape.as_deref()
    }

    /// Inserts a new parameter binding for the given name.
    /// If the parameter already exists, it will be overwritten.
    ///
    /// Accepts anything convertible to [`Term<Any>`], including `Term<T>`
    /// for any `T: Scalar`.
    pub fn insert(&mut self, name: String, param: Term<Any>) {
        Arc::make_mut(&mut self.0).insert(
            name,
            Parameter {
                term: param,
                shape: None,
            },
        );
    }

    /// Binds the given name to a pattern, overwriting what it was bound
    /// to. A pattern that holds no variable is the value it builds, and
    /// is bound as that constant.
    pub fn set(&mut self, name: String, pattern: impl Into<Pattern>) {
        let parameter = match pattern.into() {
            Pattern::Term(term) => Parameter { term, shape: None },
            pattern => match pattern.as_value() {
                Some(value) => Parameter {
                    term: Term::Constant(value),
                    shape: None,
                },
                None => Parameter {
                    term: Shape::whole_of(&pattern),
                    shape: Some(Arc::new(pattern)),
                },
            },
        };
        Arc::make_mut(&mut self.0).insert(name, parameter);
    }

    /// Removes the parameter binding for the given name, returning the
    /// term a premise read there when there was one.
    pub fn remove(&mut self, name: &str) -> Option<Term<Any>> {
        Arc::make_mut(&mut self.0)
            .remove(name)
            .map(|parameter| parameter.term)
    }

    /// Checks if a parameter binding exists for the given name.
    pub fn contains(&self, name: &str) -> bool {
        self.0.contains_key(name)
    }

    /// Returns an iterator over all name-term pairs in this binding
    /// set: under each name the term a premise reads there (see
    /// [`get`](Self::get)).
    pub fn iter(&self) -> impl Iterator<Item = (&String, &Term<Any>)> {
        self.0
            .iter()
            .map(|(name, parameter)| (name, &parameter.term))
    }

    /// Returns an iterator over the parameter names in this binding set.
    pub fn keys(&self) -> impl Iterator<Item = &String> {
        self.0.keys()
    }

    /// Returns an iterator over the parameters that hold a map, a list
    /// or a tuple, with the pattern each holds.
    pub fn shapes(&self) -> impl Iterator<Item = (&String, &Pattern)> {
        self.0
            .iter()
            .filter_map(|(name, parameter)| Some((name, parameter.shape.as_deref()?)))
    }

    /// Whether any parameter holds a map, a list or a tuple.
    pub fn has_shapes(&self) -> bool {
        self.0.values().any(|parameter| parameter.shape.is_some())
    }

    /// Every leaf these parameters hold: the terms of the leaf
    /// parameters, and the terms inside each map, list and tuple. The
    /// wholes are not among them.
    pub fn leaves(&self) -> Vec<&Term<Any>> {
        let mut leaves = Vec::new();
        for parameter in self.0.values() {
            match &parameter.shape {
                Some(shape) => leaves.extend(shape.terms()),
                None => leaves.push(&parameter.term),
            }
        }
        leaves
    }

    /// Every leaf these parameters hold, each under an address: a leaf
    /// parameter under its name, and a leaf inside a map, a list or a
    /// tuple under the parameter's name and its place in the pattern as
    /// written. Renaming variables moves no address.
    pub fn addressed(&self) -> Vec<(String, &Term<Any>)> {
        let mut leaves = Vec::new();
        for (name, parameter) in self.0.iter() {
            match &parameter.shape {
                Some(shape) => leaves.extend(
                    shape
                        .terms()
                        .into_iter()
                        .enumerate()
                        .map(|(place, term)| (format!("{name}.{place}"), term)),
                ),
                None => leaves.push((name.clone(), &parameter.term)),
            }
        }
        leaves
    }

    /// These parameters with every leaf replaced by what `f` gives for
    /// it, inside a map, a list or a tuple too. A whole follows the
    /// pattern it stands for.
    pub fn map_terms(&self, f: impl Fn(&Term<Any>) -> Term<Any>) -> Parameters {
        let mut mapped = Parameters::new();
        for (name, parameter) in self.0.iter() {
            match &parameter.shape {
                Some(shape) => mapped.set(name.clone(), shape.map_terms(&f)),
                None => mapped.insert(name.clone(), f(&parameter.term)),
            }
        }
        mapped
    }

    /// These parameters as a premise is run with them, every one a
    /// leaf, and the shapes to apply around the premise: for each map,
    /// list or tuple, the variable now standing in its slot and the
    /// pattern. See [`Shape`].
    pub fn lowered(&self) -> (Parameters, Vec<Shape>) {
        self.lowered_with(Shape::of)
    }

    /// [`lowered`](Self::lowered), with the whole of a pattern that
    /// holds a blank told apart by what `number` gives: a conjunction
    /// numbers the patterns its premises hold, so its plan does not
    /// depend on what else the process ran.
    pub(crate) fn lowered_by(&self, mut number: impl FnMut() -> u64) -> (Parameters, Vec<Shape>) {
        self.lowered_with(|pattern| Shape::numbered(pattern, &mut number))
    }

    fn lowered_with(
        &self,
        mut shape: impl FnMut(Arc<Pattern>) -> Shape,
    ) -> (Parameters, Vec<Shape>) {
        let mut shapes = Vec::new();
        let mut lowered = Parameters::new();
        for (name, parameter) in self.0.iter() {
            match &parameter.shape {
                Some(pattern) => {
                    let shape = shape(pattern.clone());
                    lowered.insert(name.clone(), shape.whole.clone());
                    shapes.push(shape);
                }
                None => lowered.insert(name.clone(), parameter.term.clone()),
            }
        }
        (lowered, shapes)
    }

    /// These parameters under other names: each bound under what `key`
    /// gives for its name, holding what it held.
    pub fn rekeyed(&self, key: impl Fn(&str) -> String) -> Parameters {
        Parameters(Arc::new(
            self.0
                .iter()
                .map(|(name, parameter)| (key(name), parameter.clone()))
                .collect(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::corpus::{Type, Value};
    use crate::term::Term;
    use crate::types::Any;

    #[dialog_common::test]
    fn it_performs_basic_operations() {
        let mut terms = Parameters::new();

        let name_param: Term<Any> = Term::Variable {
            name: Some("name".into()),
            descriptor: Any(None),
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
        params.insert("name".to_string(), Term::var("x"));
        params.insert("age".to_string(), Term::var("y"));

        let json = serde_json::to_value(&params).unwrap();
        let obj = json.as_object().unwrap();
        assert_eq!(obj.len(), 2);
        assert_eq!(obj["name"], serde_json::json!({"?": {"name": "x"}}));
        assert_eq!(obj["age"], serde_json::json!({"?": {"name": "y"}}));
    }

    #[dialog_common::test]
    fn it_serializes_constants_to_json() {
        let mut params = Parameters::new();
        params.insert("name".to_string(), Term::constant("Alice".to_string()));
        params.insert("age".to_string(), Term::constant(42u32));

        let json = serde_json::to_value(&params).unwrap();
        let obj = json.as_object().unwrap();
        assert_eq!(obj["name"], serde_json::json!("Alice"));
        assert_eq!(obj["age"], serde_json::json!(42));
    }

    #[dialog_common::test]
    fn it_serializes_mixed_terms_to_json() {
        let mut params = Parameters::new();
        params.insert("this".to_string(), Term::var("person"));
        params.insert("name".to_string(), Term::constant("Alice".to_string()));

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
            Some(&Term::Variable {
                name: Some("x".into()),
                descriptor: Any(None)
            })
        );
        assert_eq!(
            params.get("age"),
            Some(&Term::Variable {
                name: Some("y".into()),
                descriptor: Any(None)
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
            Some(&Term::Constant(Value::from("Alice".to_string())))
        );
        assert_eq!(
            params.get("active"),
            Some(&Term::Constant(Value::from(true)))
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
        assert_eq!(param, &Term::blank());
    }

    #[dialog_common::test]
    fn it_round_trips_through_json() {
        let mut original = Parameters::new();
        original.insert("this".to_string(), Term::var("entity"));
        original.insert("name".to_string(), Term::constant("Alice".to_string()));
        original.insert("active".to_string(), Term::constant(true));

        let json = serde_json::to_value(&original).unwrap();
        let restored: Parameters = serde_json::from_value(json).unwrap();
        assert_eq!(original, restored);
    }

    #[dialog_common::test]
    fn it_preserves_json_integer_types() {
        let json = serde_json::json!({"count": 42, "offset": -5, "ratio": 3.15});
        let params: Parameters = serde_json::from_value(json).unwrap();

        // Integer types are now correctly preserved (the bug is fixed!)
        assert_eq!(
            params.get("count"),
            Some(&Term::Constant(Value::UnsignedInt(42)))
        );
        assert_eq!(
            params.get("offset"),
            Some(&Term::Constant(Value::SignedInt(-5)))
        );
        assert_eq!(
            params.get("ratio"),
            Some(&Term::Constant(Value::Float(3.15)))
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
        let mut params = Parameters::new();
        params.insert("name".to_string(), Term::<String>::var("x").into());

        let param = params.get("name").unwrap();
        assert_eq!(
            param,
            &Term::Variable {
                name: Some("x".into()),
                descriptor: Some(Type::String).into(),
            }
        );
    }
}
