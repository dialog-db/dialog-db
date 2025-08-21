//! This example demonstrates what happens when trying to use unsupported types
//! Uncomment the lines below to see the compiler errors

use dialog_query::Term;
use dialog_artifacts::Value;
// use std::collections::HashMap;

#[allow(dead_code)]
struct MyCustomType {
    value: String,
}

fn main() {
    // These work fine - supported types
    let _string_var = Term::<String>::var("name");
    let _flexible_var = Term::<Value>::var("anything");

    println!("Supported types work fine!");

    // Uncomment these lines to see compiler errors:

    // This will fail: HashMap doesn't implement IntoValueDataType
    // let _map_var = Term::<std::collections::HashMap<String, String>>::var("map");

    // This will fail: MyCustomType doesn't implement IntoValueDataType
    // let _custom_var = Term::<MyCustomType>::var("custom");

    // This will fail: Option<String> doesn't implement IntoValueDataType
    // let _option_var = Term::<Option<String>>::var("maybe");

    println!("The type system prevents invalid Term types at compile time!");
}
