//! Example demonstrating the #[relation] attribute macro
//!
//! This example shows how to use the `#[relation]` attribute to generate
//! attribute structs and modules from an enum definition.

use dialog_query::{relation, Attribute};

// Define a relation using the #[relation] attribute
#[relation]
enum Employee {
    Name(String),
    Job(String),
    Salary(u32),
    #[many]
    Address(String),
}

// Define another relation
#[relation]
enum Product {
    Title(String),
    Price(u32),
    #[many]
    Category(String),
    InStock(bool),
}

fn main() {
    println!("=== #[relation] Attribute Demo ===\n");

    // Employee relation
    println!("Employee attributes:");

    let name = Employee::Name::new("Alice Johnson");
    let job = Employee::Job::new("Software Engineer");
    let salary = Employee::Salary::new(120000u32);
    let address = Employee::Address::new("123 Tech Street");

    println!("✅ Employee::Name: {:?}", name.value());
    println!("   - Attribute name: {}", Employee::Name::name());
    println!("   - Cardinality: {:?}", Employee::Name::cardinality());
    println!("   - Value type: {:?}", Employee::Name::value_type());

    println!("✅ Employee::Job: {:?}", job.value());
    println!("   - Attribute name: {}", Employee::Job::name());

    println!("✅ Employee::Salary: {:?}", salary.value());
    println!("   - Attribute name: {}", Employee::Salary::name());
    println!("   - Value type: {:?}", Employee::Salary::value_type());

    println!("✅ Employee::Address: {:?}", address.value());
    println!("   - Attribute name: {}", Employee::Address::name());
    println!(
        "   - Cardinality: {:?} (because of #[many])",
        Employee::Address::cardinality()
    );

    println!("\n{}\n", "=".repeat(50));

    // Product relation
    println!("Product attributes:");

    let title = Product::Title::new("Rust Programming Book");
    let price = Product::Price::new(4999u32); // in cents
    let category = Product::Category::new("Programming");
    let in_stock = Product::InStock::new(true);

    println!("✅ Product::Title: {:?}", title.value());
    println!("   - Attribute name: {}", Product::Title::name());

    println!("✅ Product::Price: {:?}", price.value());
    println!("   - Attribute name: {}", Product::Price::name());

    println!("✅ Product::Category: {:?}", category.value());
    println!("   - Attribute name: {}", Product::Category::name());
    println!(
        "   - Cardinality: {:?} (because of #[many])",
        Product::Category::cardinality()
    );

    println!("✅ Product::InStock: {:?}", in_stock.value());
    println!("   - Attribute name: {}", Product::InStock::name());
    println!("   - Value type: {:?}", Product::InStock::value_type());

    println!("\n🎉 All #[relation] attributes work perfectly!");

    // Show that you can consume values too
    println!("\nValue consumption:");
    let consumed_name = name.into_value();
    println!("Consumed Employee::Name value: {}", consumed_name);
}
