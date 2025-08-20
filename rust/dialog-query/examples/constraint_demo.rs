//! This example demonstrates what happens when trying to use unsupported types
//! Uncomment the lines below to see the compiler errors

use dialog_query::{Term, Untyped};
// use std::collections::HashMap;

#[allow(dead_code)]
struct MyCustomType {
    value: String,
}

fn main() {
    // These work fine - supported types
    let _string_var = Term::<String>::var("name");
    let _untyped_var = Term::<Untyped>::var("anything");

    println!("Supported types work fine!");

    // Uncomment these lines to see compiler errors:

    // This will fail: HashMap doesn't implement IntoValueDataType
    // let _map_var = Variable::<std::collections::HashMap<String, String>>::new("map");

    // This will fail: MyCustomType doesn't implement IntoValueDataType
    // let _custom_var = Variable::<MyCustomType>::new("custom");

    // This will fail: Option<String> doesn't implement IntoValueDataType
    // let _option_var = Variable::<Option<String>>::new("maybe");

    println!("The type system prevents invalid Variable types at compile time!");
}
