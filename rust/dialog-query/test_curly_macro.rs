// Test if we can use curly braces with alternative delimiters

#[macro_export]
macro_rules! select_curly {
    // Test with double colon
    { the: $namespace:ident :: $name:ident } => {
        concat!("Works: ", stringify!($namespace), "/", stringify!($name))
    };
    
    // Test with multiple fields
    { the: $namespace:ident :: $name:ident, of: ?$entity:ident, is: ?$value:ident } => {
        format!("Multi works: {}/{}, ?{}, ?{}", 
            stringify!($namespace), 
            stringify!($name),
            stringify!($entity),
            stringify!($value)
        )
    };
    
    // Test with @ prefix
    { the: @ $namespace:ident / $name:ident } => {
        concat!("@ Works: ", stringify!($namespace), "/", stringify!($name))
    };
}

fn main() {
    // Test curly brace syntax with double colon
    let test1 = select_curly!{ the: user::name };
    println!("{}", test1);
    
    // Test with multiple fields
    let test2 = select_curly!{ the: user::name, of: ?user, is: ?name };
    println!("{}", test2);
    
    // Test with @ prefix
    let test3 = select_curly!{ the: @user/name };
    println!("{}", test3);
}