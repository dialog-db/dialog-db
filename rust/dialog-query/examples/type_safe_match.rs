//! Example demonstrating the new type-safe Match API

use dialog_query::{Term, selection::Match};
// use dialog_artifacts::{Value, Entity};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Type-Safe Match API Demo ===\n");

    // Create an empty match
    let mut match_frame = Match::new();

    // Type-safe variable operations
    let name_term = Term::<String>::var("name");
    let age_term = Term::<u32>::var("age");
    let active_term = Term::<bool>::var("active");

    // Set values with compile-time type safety
    match_frame = match_frame.set(name_term.clone(), "Alice".to_string())?;
    match_frame = match_frame.set(age_term.clone(), 30u32)?;
    match_frame = match_frame.set(active_term.clone(), true)?;

    println!("✅ Set variables with type safety:");
    println!("   name: String = \"Alice\"");
    println!("   age: u32 = 30");
    println!("   active: bool = true\n");

    // Get values with runtime type verification
    let name: String = match_frame.get(&name_term)?;
    let age: u32 = match_frame.get(&age_term)?;
    let active: bool = match_frame.get(&active_term)?;

    println!("✅ Retrieved values with type safety:");
    println!("   name = \"{}\" (type: {})", name, std::any::type_name::<String>());
    println!("   age = {} (type: {})", age, std::any::type_name::<u32>());
    println!("   active = {} (type: {})\n", active, std::any::type_name::<bool>());

    // Check existence
    if match_frame.has(&name_term) {
        println!("✅ Variable 'name' exists in match");
    }

    // Demonstrate type mismatch error
    println!("\n=== Type Mismatch Demo ===");
    let wrong_age_term = Term::<String>::var("age");  // age is u32, not String
    
    match match_frame.get(&wrong_age_term) {
        Ok(_) => println!("❌ This shouldn't happen!"),
        Err(e) => println!("✅ Type mismatch caught: {}", e),
    }

    // Demonstrate constant terms
    println!("\n=== Constant Terms Demo ===");
    let constant_term = Term::Constant("Hello World".to_string());
    let constant_value: String = match_frame.get(&constant_term)?;
    println!("✅ Constant term resolved: \"{}\"", constant_value);

    // Demonstrate clean Term-based API
    println!("\n=== Clean Type-Safe API ===");
    let final_check: String = match_frame.get(&name_term)?;
    println!("✅ Term-based API is clean and type-safe: \"{}\"", final_check);

    Ok(())
}