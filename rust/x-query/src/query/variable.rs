use crate::PrimaryKey;

#[derive(Debug, Clone)]
pub enum VariableAssignment {
    Entity(PrimaryKey),
    Value(PrimaryKey),
    Attribute(PrimaryKey),
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Debug)]
pub struct Variable(String);

impl From<&'static str> for Variable {
    fn from(value: &'static str) -> Self {
        Self(value.to_owned())
    }
}

impl From<&String> for Variable {
    fn from(value: &String) -> Self {
        Self(value.clone())
    }
}

impl From<String> for Variable {
    fn from(value: String) -> Self {
        Self(value)
    }
}
