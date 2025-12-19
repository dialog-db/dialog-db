use dialog_query::attribute::AttributeSchema;
use dialog_query::predicate::Concept;
use dialog_query::Type;

fn main() {
    let concept = Concept::Dynamic {
        description: String::new(),
        attributes: [
            (
                "name",
                AttributeSchema::new("user", "name", "User's name", Type::String),
            ),
            (
                "age",
                AttributeSchema::new("user", "age", "User's age", Type::UnsignedInt),
            ),
        ]
        .into(),
    };

    // Test serialization to JSON
    let json = serde_json::to_string_pretty(&concept).expect("Should serialize");
    println!(
        "Serialized concept (proper structure with attributes object):\n{}",
        json
    );

    // Test deserialization from JSON
    let deserialized: Concept = serde_json::from_str(&json).expect("Should deserialize");
    println!(
        "\nSuccessfully deserialized concept with operator: {}",
        deserialized.operator()
    );
    println!(
        "Number of attributes: {}",
        deserialized.attributes().count()
    );

    // Show the attributes
    for (name, attr) in deserialized.attributes().iter() {
        let type_str = match attr.content_type {
            Some(ty) => format!("{}", ty),
            None => "Any".to_string(),
        };
        println!(
            "  {}: {}/{} ({})",
            name, attr.namespace, attr.name, type_str
        );
    }
}
