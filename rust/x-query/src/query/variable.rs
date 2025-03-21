use crate::{DataType, PrimaryKey};

use super::Scope;

#[derive(Debug, Clone)]
pub enum VariableAssignment {
    Entity(PrimaryKey),
    Attribute(PrimaryKey),
    Value(DataType, PrimaryKey),
}

impl From<VariableAssignment> for PrimaryKey {
    fn from(value: VariableAssignment) -> Self {
        match value {
            VariableAssignment::Entity(eav_key) => eav_key,
            VariableAssignment::Attribute(eav_key) => eav_key,
            VariableAssignment::Value(_, eav_key) => eav_key,
        }
    }
}

impl From<&VariableAssignment> for PrimaryKey {
    fn from(value: &VariableAssignment) -> Self {
        value.clone().into()
    }
}

#[derive(Default, PartialEq, Eq, PartialOrd, Ord, Clone, Debug)]
pub struct Variable(String);

impl Variable {
    pub fn scope(&self, scope: &Scope) -> Self {
        Self(format!("{}:{}", self.0, scope))
    }
}

impl From<&'static str> for Variable {
    fn from(value: &'static str) -> Self {
        Self(value.to_owned())
    }
}

impl From<&String> for Variable {
    fn from(value: &String) -> Self {
        Self(value.to_owned())
    }
}

impl From<String> for Variable {
    fn from(value: String) -> Self {
        Self(value)
    }
}
