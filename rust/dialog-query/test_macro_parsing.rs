// Test file to explore macro parsing options

#[macro_export]
macro_rules! test_slash {
    // Try to match user/name specifically
    (the: $namespace:ident / $name:ident) => {
        concat!(stringify!($namespace), "/", stringify!($name))
    };
}

#[macro_export] 
macro_rules! test_alternatives {
    // Double colon
    (the: $namespace:ident :: $name:ident) => {
        concat!(stringify!($namespace), "/", stringify!($name))
    };
    
    // Underscore  
    (the: $namespace:ident _ $name:ident) => {
        concat!(stringify!($namespace), "/", stringify!($name))
    };
    
    // Dot
    (the: $namespace:ident . $name:ident) => {
        concat!(stringify!($namespace), "/", stringify!($name))
    };
    
    // @ prefix
    (the: @ $namespace:ident / $name:ident) => {
        concat!(stringify!($namespace), "/", stringify!($name))
    };
}

fn main() {
    // Test which ones compile
    let test1 = test_alternatives!(the: user :: name);
    let test2 = test_alternatives!(the: user _ name);
    let test3 = test_alternatives!(the: user . name);
    let test4 = test_alternatives!(the: @ user / name);
    
    println!("user::name -> {}", test1);
    println!("user_name -> {}", test2);
    println!("user.name -> {}", test3);
    println!("@user/name -> {}", test4);
}