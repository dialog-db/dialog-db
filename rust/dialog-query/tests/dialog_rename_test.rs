use dialog_query::rule::Match;
use dialog_query::{Attribute, Concept, Entity, Term};

// -- Attribute rename tests --

mod item {
    use dialog_query::Attribute;

    /// The type of item
    #[derive(Attribute, Clone, PartialEq)]
    #[dialog(rename = "type")]
    pub struct Kind(pub String);

    /// Name of the item
    #[derive(Attribute, Clone, PartialEq)]
    pub struct Name(pub String);
}

#[test]
fn test_attribute_rename_overrides_name() {
    // Kind struct should have NAME = "type" instead of "kind"
    assert_eq!(item::Kind::NAME, "type");
}

#[test]
fn test_attribute_rename_preserves_other_metadata() {
    assert_eq!(item::Kind::NAMESPACE, "item");
    assert_eq!(item::Kind::DESCRIPTION, "The type of item");
    assert_eq!(
        item::Kind::CARDINALITY,
        dialog_query::attribute::Cardinality::One
    );
}

#[test]
fn test_attribute_rename_selector() {
    // Selector should use the renamed name
    assert_eq!(item::Kind::selector().to_string(), "item/type");
}

#[test]
fn test_attribute_rename_schema() {
    let schema = &item::Kind::SCHEMA;
    assert_eq!(schema.name, "type");
    assert_eq!(schema.namespace, "item");
}

#[test]
fn test_attribute_without_rename_unchanged() {
    // Name without rename should behave normally
    assert_eq!(item::Name::NAME, "name");
    assert_eq!(item::Name::NAMESPACE, "item");
}

// -- Concept field rename tests --

#[derive(Concept, Debug, Clone)]
pub struct Item {
    pub this: Entity,

    /// Name of the item
    pub name: item::Name,

    /// Type of the item
    #[dialog(rename = "type")]
    pub kind: item::Kind,
}

#[test]
fn test_concept_field_rename_in_attributes() {
    let concept = Item::CONCEPT;
    let attrs = concept.attributes().iter().collect::<Vec<_>>();

    assert_eq!(attrs.len(), 2);

    // First attribute: name (no rename)
    assert_eq!(attrs[0].0, "name");
    assert_eq!(attrs[0].1.name, "name");

    // Second attribute: renamed field "kind" -> key is "type"
    assert_eq!(attrs[1].0, "type");
    assert_eq!(attrs[1].1.name, "type");
}

#[test]
fn test_concept_field_rename_static_constants() {
    // Static constants should use the renamed name
    assert_eq!(ITEM_NAME.name, "name");
    assert_eq!(ITEM_TYPE.name, "type");
    assert_eq!(ITEM_TYPE.namespace, "item");
}

#[test]
fn test_concept_field_rename_match_struct() {
    // Match struct should still use the Rust field name
    let query = Match::<Item> {
        this: Term::var("item"),
        name: Term::var("item_name"),
        kind: Term::var("item_kind"),
    };

    // But when converted to Parameters, the key should be the renamed value
    let params: dialog_query::Parameters = query.into();
    assert!(
        params.get("type").is_some(),
        "Parameters should have 'type' key from renamed field"
    );
    assert!(
        params.get("kind").is_none(),
        "Parameters should NOT have 'kind' key (the Rust field name)"
    );
    assert!(
        params.get("name").is_some(),
        "Parameters should have 'name' key for unrenamed field"
    );
}

#[test]
fn test_concept_field_rename_default_match() {
    // Default Match should create Term::var with the renamed string
    let query = Match::<Item>::default();

    // The 'kind' field should have a variable named "type"
    match &query.kind {
        Term::Variable { name, .. } => {
            assert_eq!(
                name.as_deref(),
                Some("type"),
                "Default variable name should use renamed value"
            );
        }
        _ => panic!("Expected variable term"),
    }

    // The 'name' field should have a variable named "name" (unrenamed)
    match &query.name {
        Term::Variable { name, .. } => {
            assert_eq!(name.as_deref(), Some("name"));
        }
        _ => panic!("Expected variable term"),
    }
}

#[test]
fn test_concept_field_rename_terms() {
    // Terms method should still use Rust field name but produce renamed variable
    let term = ItemTerms::kind();
    match &term {
        Term::Variable { name, .. } => {
            assert_eq!(
                name.as_deref(),
                Some("type"),
                "Terms method should produce variable with renamed name"
            );
        }
        _ => panic!("Expected variable term"),
    }
}

// -- Combined: renamed Attribute used in renamed Concept field --

mod task {
    use dialog_query::Attribute;

    /// The reference identifier
    #[derive(Attribute, Clone, PartialEq)]
    #[dialog(rename = "ref")]
    pub struct Reference(pub String);

    /// Task title
    #[derive(Attribute, Clone, PartialEq)]
    pub struct Title(pub String);
}

#[derive(Concept, Debug, Clone)]
pub struct Task {
    pub this: Entity,
    pub title: task::Title,
    #[dialog(rename = "ref")]
    pub reference: task::Reference,
}

#[test]
fn test_combined_attribute_and_concept_rename() {
    // Attribute NAME should be "ref"
    assert_eq!(task::Reference::NAME, "ref");
    assert_eq!(task::Reference::selector().to_string(), "task/ref");

    // Concept attribute tuple key should be "ref"
    let concept = Task::CONCEPT;
    let attrs = concept.attributes().iter().collect::<Vec<_>>();
    assert_eq!(attrs[1].0, "ref");
    assert_eq!(attrs[1].1.name, "ref");

    // Static constant should be TASK_REF
    assert_eq!(TASK_REF.name, "ref");
}
