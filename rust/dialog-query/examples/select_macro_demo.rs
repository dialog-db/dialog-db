use dialog_query::{Fact, Variable, Term};

fn main() {
    println!("Dialog Query Fact Selector Demo");
    println!("===========================");
    
    // Create attributes using string literals
    let user_name_attr = "user/name";
    let person_age_attr = "person/age";
    println!("Created attributes: '{}', '{}'", user_name_attr, person_age_attr);
    
    // Create variables using Variable constructors
    let user_var = Variable::Any("user");
    let name_var = Variable::String("name");
    let age_var = Variable::Any("age");
    println!("Created variables: ?{}, ?{}<String>, ?{}", user_var.name, name_var.name, age_var.name);
    
    // Create Fact selectors using the clean builder pattern
    println!("\nFact Selector Examples:");
    
    // 1. Simple attribute-only fact selector
    let fact_selector1 = Fact::select().the("user/name");
    println!("1. Fact selector by attribute: {:?}", fact_selector1);
    
    // 2. Fact selector with entity and value variables  
    let fact_selector2 = Fact::select()
        .the("user/name")
        .of(Term::Variable(Variable::Any("user")))
        .is(Term::Variable(Variable::String("name")));
    println!("2. Fact selector with variables: {:?}", fact_selector2);
    
    // 3. Fact selector with constant value
    let fact_selector3 = Fact::select()
        .the("user/status")
        .is(Term::Constant(dialog_artifacts::Value::String("active".to_string())));
    println!("3. Fact selector with constant: {:?}", fact_selector3);
    
    println!("\nThis achieves the goal of convenient Fact selector creation!");
    println!("Clean builder pattern:");
    println!("Fact::select().the(\"user/name\").of(Variable::Any(\"user\")).is(Variable::String(\"name\"))");
}