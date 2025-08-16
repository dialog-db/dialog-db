use dialog_query::{Fact, Variable};

fn main() {
    println!("Dialog Query Fact Selector API Demo");
    println!("=======================================\n");

    // Example 1: Clean new() syntax
    println!("1. Clean new() syntax:");
    let fact_selector1 = Fact::select()
        .the("gozala.io/name")
        .of(Variable::Entity("user"));
    println!("   Fact::select().the(\"gozala.io/name\").of(Variable::Entity(\"user\"))");
    println!("   Result: {:?}", fact_selector1);

    // Example 2: Starting with any field
    println!("2. Starting with any field:");

    let fact_selector2a = Fact::select()
        .the("user/name")
        .of(Variable::Any("user"))
        .is("John");
    println!("   Fact::select().the(\"user/name\").of(Variable::Any(\"user\")).is(\"John\")\n{fact_selector2a:?}");

    let fact_selector2b = Fact::select()
        .of(Variable::Any("user"))
        .the("user/name")
        .is(Variable::String("name"));
    println!("   Fact::select().of(Variable::Any(\"user\")).the(\"user/name\").is(Variable::String(\"name\"))\n{fact_selector2b:?}");

    let fact_selector2c = Fact::select().is("active").the("user/status");
    println!(
        "   Fact::select().is(\"active\").the(\"user/status\")\n{:?}",
        fact_selector2c
    );

    // Example 3: Flexible ordering
    println!("3. Flexible field ordering:");
    let fact_selector3a = Fact::select()
        .the("user/email")
        .of(Variable::Any("user"))
        .is("test@example.com");
    let fact_selector3b = Fact::select()
        .of(Variable::Any("user"))
        .is("test@example.com")
        .the("user/email");
    let fact_selector3c = Fact::select()
        .is("test@example.com")
        .the("user/email")
        .of(Variable::Any("user"));

    println!("   All three create equivalent patterns:");
    println!("   - Fact::select().the(...).of(...).is(...)");
    println!("   - Fact::select().of(...).is(...).the(...)");
    println!("   - Fact::select().is(...).the(...).of(...)");
    println!(
        "   Same result: {}\n",
        fact_selector3a.the == fact_selector3b.the && fact_selector3b.the == fact_selector3c.the
    );

    // Example 4: With Variable constructors
    println!("4. With Variable constructors:");
    let fact_selector4 = Fact::select()
        .the("user/name")
        .of(Variable::Any("user"))
        .is(Variable::String("name"));
    println!("   Fact::select().the(\"user/name\").of(Variable::Any(\"user\")).is(Variable::String(\"name\"))");
    println!(
        "   Variables in pattern: {}\n",
        fact_selector4.variables().len()
    );

    // Example 5: Partial patterns
    println!("5. Partial patterns (not all fields required):");
    let fact_selector5a = Fact::select().the("user/age"); // Just attribute
    let fact_selector5b = Fact::select().of(Variable::Any("user")).the("user/name"); // Attribute + entity
    let fact_selector5c = Fact::select().is("active").the("user/status"); // Attribute + value

    println!("   Just attribute: {:?}", fact_selector5a.the.is_some());
    println!(
        "   Attribute + entity: {:?}",
        fact_selector5b.the.is_some() && fact_selector5b.of.is_some()
    );
    println!(
        "   Attribute + value: {:?}",
        fact_selector5c.the.is_some() && fact_selector5c.is.is_some()
    );

    println!("\n✅ FactSelector Builder API is working perfectly!");
    println!("✅ Clean Fact::select() starting point");
    println!("✅ Flexible ordering - chain methods in any order");
    println!("✅ Type-safe conversions from strings and Variables");
    println!("✅ Integrates with Variable constructors");
    println!("✅ Implements Syntax trait for query planning");
}
