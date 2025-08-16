// Test alternative approaches for dotted namespaces

#[macro_export]
macro_rules! select_dotted {
    // Match dotted namespace with repetition
    { the: $($namespace:ident).+ :: $name:ident } => {
        {
            let parts: Vec<&str> = vec![$(stringify!($namespace)),+];
            format!("{}/{}", parts.join("."), stringify!($name))
        }
    };
}

#[macro_export]
macro_rules! select_hybrid {
    // Use slashes throughout: io/gozala/user/name
    { the: $($part:ident)/+ } => {
        {
            let parts: Vec<&str> = vec![$(stringify!($part)),+];
            parts.join("/")
        }
    };
}

#[macro_export]
macro_rules! select_at_prefix {
    // Use @ prefix with full path: @io.gozala.user/name
    { the: @ $($part:tt)+ } => {
        {
            // Would need custom parsing but shows the syntax works
            concat!("@", stringify!($($part)+))
        }
    };
}

fn main() {
    // Test dotted with double colon
    let test1 = select_dotted!{ the: io.gozala.user :: name };
    println!("Dotted with :: -> {}", test1);
    
    // Test all slashes
    let test2 = select_hybrid!{ the: io/gozala/user/name };
    println!("All slashes -> {}", test2);
    
    // Test @ prefix
    let test3 = select_at_prefix!{ the: @io.gozala.user/name };
    println!("@ prefix -> {}", test3);
}