use dialog_query::{Concept, Entity, Term};

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
    // Kind struct should have name "type" instead of "kind"
    assert_eq!(item::Kind::descriptor().name(), "type");
}

#[test]
fn test_attribute_rename_preserves_other_metadata() {
    assert_eq!(item::Kind::descriptor().domain(), "item");
    assert_eq!(item::Kind::descriptor().description(), "The type of item");
    assert_eq!(
        item::Kind::descriptor().cardinality(),
        dialog_query::Cardinality::One
    );
}

#[test]
fn test_attribute_rename_selector() {
    // Selector should use the renamed name
    assert_eq!(item::Kind::the().to_string(), "item/type");
}

#[test]
fn test_attribute_rename_descriptor() {
    let desc = item::Kind::descriptor();
    assert_eq!(desc.name(), "type");
    assert_eq!(desc.domain(), "item");
}

#[test]
fn test_attribute_without_rename_unchanged() {
    // Name without rename should behave normally
    assert_eq!(item::Name::descriptor().name(), "name");
    assert_eq!(item::Name::descriptor().domain(), "item");
}

// -- Concept field rename tests --

#[derive(Concept, Debug, Clone, PartialEq)]
pub struct Item {
    pub this: Entity,

    /// Name of the item
    pub name: item::Name,

    /// Type of the item
    #[dialog(rename = "type")]
    pub kind: item::Kind,
}

#[test]
fn test_concept_field_rename_in_descriptor() {
    let descriptor: dialog_query::ConceptDescriptor = ItemQuery::default().into();
    let attrs: Vec<_> = descriptor.with().iter().collect();

    assert_eq!(attrs.len(), 2);

    // Find the attributes by name — NamedAttributes order is based on the vec
    let name_attr = attrs.iter().find(|(k, _)| *k == "name");
    let type_attr = attrs.iter().find(|(k, _)| *k == "type");

    assert!(name_attr.is_some(), "Should have 'name' key in descriptor");
    assert!(
        type_attr.is_some(),
        "Should have 'type' key (from renamed field) in descriptor"
    );

    // The 'type' key should point to the item/type attribute
    assert_eq!(type_attr.unwrap().1.name(), "type");
    assert_eq!(type_attr.unwrap().1.domain(), "item");
}

#[test]
fn test_concept_field_rename_query_struct() {
    // Query struct should still use the Rust field name
    let query = ItemQuery {
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
fn test_concept_field_rename_default_query() {
    // Default Query should create Term::var with the renamed string
    let query = ItemQuery::default();

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

#[derive(Concept, Debug, Clone, PartialEq)]
pub struct Task {
    pub this: Entity,
    pub title: task::Title,
    #[dialog(rename = "ref")]
    pub reference: task::Reference,
}

#[test]
fn test_combined_attribute_and_concept_rename() {
    // Attribute name should be "ref"
    assert_eq!(task::Reference::descriptor().name(), "ref");
    assert_eq!(task::Reference::the().to_string(), "task/ref");

    // Concept descriptor should have "ref" key
    let descriptor: dialog_query::ConceptDescriptor = TaskQuery::default().into();
    let attrs: Vec<_> = descriptor.with().iter().collect();
    let ref_attr = attrs.iter().find(|(k, _)| *k == "ref");
    assert!(ref_attr.is_some(), "Should have 'ref' key in descriptor");
    assert_eq!(ref_attr.unwrap().1.name(), "ref");
}
