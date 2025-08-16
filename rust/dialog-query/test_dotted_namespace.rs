// Test if we can handle dotted namespaces like io.gozala.user::name

#[macro_export]
macro_rules! select_dotted {
    // Try to match dotted namespace with double colon
    { the: $($namespace:ident).+ :: $name:ident } => {
        {
            let parts: Vec<&str> = vec![$(stringify!($namespace)),+];
            format!("{}/{}", parts.join("."), stringify!($name))
        }
    };
}

#[macro_export]
macro_rules! select_dotted_alt {
    // Alternative: capture the whole thing as token trees
    { the: $($tokens:tt)+ } => {
        {
            // This would need custom parsing
            stringify!($($tokens)+)
        }
    };
}

#[macro_export]
macro_rules! select_path_style {
    // Try path-like syntax
    { the: $namespace:path :: $name:ident } => {
        {
            format!("{:?}/{}", stringify!($namespace), stringify!($name))
        }
    };
}

fn main() {
    // Test different approaches
    
    // Approach 1: Repetition with dots
    let test1 = select_dotted!{ the: io.gozala.user :: name };
    println!("Dotted approach: {}", test1);
    
    // Approach 2: Token trees (would need parsing)
    let test2 = select_dotted_alt!{ the: io.gozala.user::name };
    println!("Token tree approach: {}", test2);
    
    // Approach 3: Path type
    // let test3 = select_path_style!{ the: io::gozala::user :: name };
    // println!("Path approach: {}", test3);
}