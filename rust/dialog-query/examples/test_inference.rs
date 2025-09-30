use dialog_query::artifact::{Type, Value};

pub enum Term<T: Into<Type>> {
    Variable { name: String, value_type: T },

    // With phantom types I had Constant(T)
    // Do I basically spread Value into here now
    Constant(Value),
}

impl<T: Into<Type>> Term<T> {
    pub fn var<Name: Into<String>>(name: Name) -> Self {
        #[allow(unreachable_code)]
        Term::Variable {
            name: name.into(),
            value_type: todo!("not sure how to do this"),
        }
    }
}

pub fn select<T: Into<Type>>(_term: Term<T>) -> Vec<T> {
    vec![]
}

// fn to_label(term: Term<Type::String>) -> String {
//     "String".to_string()
// }

#[test]
fn test_to_label() {
    let _term = Term::Constant(Value::String("Hello".to_string()));
    // assert_eq!(to_label(term), "String");
}
fn main() {
    println!("Hello, world!");
}
