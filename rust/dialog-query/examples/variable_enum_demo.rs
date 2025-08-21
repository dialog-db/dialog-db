use dialog_artifacts::Value;
use dialog_query::Term;

fn main() {
    println!("Variable Enum Demo");
    println!("==================");

    // Flexible value variables
    let untyped_var = Term::<Value>::var("anything");
    println!("Flexible variable: {:?}", untyped_var.name());
    println!("Data type: {:?}", untyped_var.data_type());

    // Typed variables with turbofish syntax
    let string_var = Term::<String>::var("name");
    let age_var = Term::<u64>::var("age");
    let active_var = Term::<bool>::var("active");

    println!("\nTyped variables:");
    println!("String variable: {:?}", string_var.name());
    println!("Age variable: {:?}", age_var.name());
    println!("Active variable: {:?}", active_var.name());

    // Type checking is enforced through the type system
    println!("\nType checking:");
    println!("String terms work with string values");
    println!("Value terms work with any value type");

    // Term introspection
    println!("\nTerm introspection:");
    if string_var.data_type().is_some() {
        println!("String var is typed with name: {:?}", string_var.name().unwrap());
    } else {
        println!("Unexpected flexible variant");
    }

    if untyped_var.data_type().is_none() {
        println!("Flexible var is flexible with name: {:?}", untyped_var.name().unwrap());
    } else {
        println!("Unexpected typed variant");
    }
}
