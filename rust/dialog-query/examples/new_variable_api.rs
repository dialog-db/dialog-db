use dialog_query::{Fact, Variable};

fn main() {
    println!("New Variable API Demo");
    println!("====================\n");
    
    // Basic untyped variable
    let user = Variable::new("user", None);
    println!("1. Basic variable: {:?}", user);
    
    // Any shortcut for untyped variables
    let user_any = Variable::Any("user_any");
    println!("2. Any shortcut: {:?}", user_any);
    
    // Explicit typed variable
    let name = Variable::new("name", Some(dialog_query::ValueDataType::String));
    println!("3. Explicit typed: {:?}", name);
    
    // Type-specific constructors
    let email = Variable::String("email");
    let age = Variable::UnsignedInt("age");
    let active = Variable::Boolean("active");
    let score = Variable::Float("score");
    let entity = Variable::Entity("profile");
    
    println!("4. Type constructors:");
    println!("   String: {:?}", email);
    println!("   UnsignedInt: {:?}", age);
    println!("   Boolean: {:?}", active);
    println!("   Float: {:?}", score);
    println!("   Entity: {:?}", entity);
    
    // Usage in Fact selector
    let fact_selector = Fact::select()
        .the("user/profile")
        .of(Variable::Entity("user"))
        .is(Variable::String("profile_id"));
    
    println!("\n5. In Fact selector:");
    println!("   {:?}", fact_selector);
    
    println!("\n✅ New Variable API provides:");
    println!("   - Variable::new(name, type) for explicit control");
    println!("   - Variable::Any(name) shortcut for untyped variables");
    println!("   - Convenient Variable::String(), Variable::Entity(), etc. constructors");
    println!("   - No complex macros to learn");
}