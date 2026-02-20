use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Label {
    #[serde(untagged)]
    Dynamic(String),
    #[serde(untagged)]
    Static(&'static str),
}

impl Label {
    pub fn as_str(&self) -> &str {
        match self {
            Label::Static(s) => s,
            Label::Dynamic(s) => s.as_str(),
        }
    }
}

impl From<Label> for String {
    fn from(label: Label) -> String {
        match label {
            Label::Static(s) => s.into(),
            Label::Dynamic(s) => s,
        }
    }
}

#[test]
fn test_label_serialization() {
    let static_label = Label::Static("static");
    let dynamic_label = Label::Dynamic("dynamic".to_string());

    let serialized_static = serde_json::to_string(&static_label).unwrap();
    let serialized_dynamic = serde_json::to_string(&dynamic_label).unwrap();

    assert_eq!(serialized_static, r#""static""#);
    assert_eq!(serialized_dynamic, r#""dynamic""#);
}

#[test]
fn test_label_deserialization() {
    let label = Label::Dynamic("label".to_string());

    let deserialized: Label = serde_json::from_str(r#""label""#).unwrap();
    println!("{:?}", deserialized);

    assert_eq!(deserialized, label);
}
