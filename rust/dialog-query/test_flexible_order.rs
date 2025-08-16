// Test flexible field ordering in curly brace macros

#[macro_export]
macro_rules! select_flexible {
    // All three fields - any order
    { the: $namespace:ident :: $name:ident, of: ?$entity:ident, is: ?$value:ident } => {
        format!("the-of-is: {}/{}, ?{}, ?{}", stringify!($namespace), stringify!($name), stringify!($entity), stringify!($value))
    };
    
    { of: ?$entity:ident, the: $namespace:ident :: $name:ident, is: ?$value:ident } => {
        format!("of-the-is: {}/{}, ?{}, ?{}", stringify!($namespace), stringify!($name), stringify!($entity), stringify!($value))
    };
    
    { is: ?$value:ident, the: $namespace:ident :: $name:ident, of: ?$entity:ident } => {
        format!("is-the-of: {}/{}, ?{}, ?{}", stringify!($namespace), stringify!($name), stringify!($entity), stringify!($value))
    };
    
    // Two fields - any order
    { the: $namespace:ident :: $name:ident, of: ?$entity:ident } => {
        format!("the-of: {}/{}, ?{}", stringify!($namespace), stringify!($name), stringify!($entity))
    };
    
    { of: ?$entity:ident, the: $namespace:ident :: $name:ident } => {
        format!("of-the: {}/{}, ?{}", stringify!($namespace), stringify!($name), stringify!($entity))
    };
}

fn main() {
    // Test different orderings
    println!("{}", select_flexible!{ the: user::name, of: ?user, is: ?name });
    println!("{}", select_flexible!{ of: ?user, the: user::name, is: ?name });
    println!("{}", select_flexible!{ is: ?name, the: user::name, of: ?user });
    println!("{}", select_flexible!{ the: user::name, of: ?user });
    println!("{}", select_flexible!{ of: ?user, the: user::name });
}