use dialog_artifacts::Value;
use dialog_query::{FactSelector, Term};

#[test]
fn test_current_json_serialization() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing current FactSelector JSON serialization...\n");

    // Test 1: FactSelector with constants
    let selector1 = FactSelector::new()
        .the("person/name")
        .of(Term::var("user"))
        .is(Value::String("Alice".to_string()));

    let json1 = serde_json::to_string_pretty(&selector1)?;
    println!("FactSelector with constants and variable:");
    println!("{}\n", json1);

    // Test 2: FactSelector with all variables
    let selector2: FactSelector<Value> = FactSelector::new()
        .the(Term::<dialog_artifacts::Attribute>::var("attr"))
        .of(Term::<dialog_artifacts::Entity>::var("entity"))
        .is(Term::<Value>::var("value"));

    let json2 = serde_json::to_string_pretty(&selector2)?;
    println!("FactSelector with all variables:");
    println!("{}\n", json2);

    // Test 3: Minimal FactSelector
    let selector3: FactSelector<Value> = FactSelector::new().the("user/email");

    let json3 = serde_json::to_string_pretty(&selector3)?;
    println!("Minimal FactSelector:");
    println!("{}\n", json3);

    // Test deserialization
    println!("Testing deserialization...");
    let _deserialized1: FactSelector<Value> = serde_json::from_str(&json1)?;
    println!("✓ Successfully deserialized first example");

    let _deserialized2: FactSelector<Value> = serde_json::from_str(&json2)?;
    println!("✓ Successfully deserialized second example");

    println!("\nCurrent serialization works!");
    Ok(())
}
