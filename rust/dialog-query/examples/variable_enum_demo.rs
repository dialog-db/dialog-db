use dialog_artifacts::Value;
use dialog_query::{Term, Untyped};

fn main() {
    println!("Variable Enum Demo");
    println!("==================");

    // Untyped variables
    let untyped_var = Term::<Untyped>::var("anything");
    println!("Untyped variable: {}", untyped_var);
    println!("Data type: {:?}", untyped_var.data_type());

    // Typed variables with turbofish syntax
    let string_var = Term::<String>::var("name");
    let age_var = Term::<u64>::var("age");
    let active_var = Term::<bool>::var("active");

    println!("\nTyped variables:");
    println!("String variable: {}", string_var);
    println!("Age variable: {}", age_var);
    println!("Active variable: {}", active_var);

    // Type checking
    println!("\nType checking:");
    println!(
        "String var can unify with string: {}",
        string_var.can_unify_with(&Value::String("Alice".to_string()))
    );
    println!(
        "String var can unify with bool: {}",
        string_var.can_unify_with(&Value::Boolean(true))
    );
    println!(
        "Untyped var can unify with string: {}",
        untyped_var.can_unify_with(&Value::String("Alice".to_string()))
    );
    println!(
        "Untyped var can unify with bool: {}",
        untyped_var.can_unify_with(&Value::Boolean(true))
    );

    // Struct field access (instead of enum pattern matching)
    println!("\nStruct field access:");
    if string_var.data_type().is_some() {
        println!("String var is typed with name: {}", string_var.name().unwrap());
    } else {
        println!("Unexpected untyped variant");
    }

    if untyped_var.data_type().is_none() {
        println!("Untyped var is untyped with name: {}", untyped_var.name().unwrap());
    } else {
        println!("Unexpected typed variant");
    }
}
