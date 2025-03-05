use crate::PrimaryKey;

#[derive(Debug, Clone)]
pub enum VariableAssignment {
    Entity(PrimaryKey),
    Value(PrimaryKey),
    Attribute(PrimaryKey),
}

impl From<VariableAssignment> for PrimaryKey {
    fn from(value: VariableAssignment) -> Self {
        match value {
            VariableAssignment::Entity(eav_key) => eav_key,
            VariableAssignment::Value(eav_key) => eav_key,
            VariableAssignment::Attribute(eav_key) => eav_key,
        }
    }
}

impl From<&VariableAssignment> for PrimaryKey {
    fn from(value: &VariableAssignment) -> Self {
        value.clone().into()
    }
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
