// Test a simple working select macro

use dialog_query::{Select, Term, v};
use dialog_artifacts::Value;

fn main() {
    // Test the current working API
    let select1 = Select::by_attribute(Term::Constant(Value::String("user/name".to_string())));
    println!("Basic select: {:?}", select1);
    
    // Test with variables  
    let user_var = v!(?user);
    let name_var = v!(?name<String>);
    let select2 = Select::by_attribute(Term::Constant(Value::String("user/name".to_string())))
        .with_entity(Term::Variable(user_var))
        .with_value(Term::Variable(name_var));
    println!("Select with variables: {:?}", select2);
}