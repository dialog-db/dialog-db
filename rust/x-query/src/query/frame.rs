use std::{collections::BTreeMap, sync::Arc};

use crate::XQueryError;

use super::{Variable, VariableAssignment};

#[derive(Debug, Clone, Default)]
pub struct Frame {
    variables: Arc<BTreeMap<Variable, VariableAssignment>>,
}

impl Frame {
    pub fn has(&self, variable: &Variable) -> bool {
        self.variables.contains_key(variable)
    }

    pub fn read(&self, variable: &Variable) -> Option<&VariableAssignment> {
        self.variables.get(variable)
    }

    pub fn assign(
        &self,
        variable: Variable,
        assignment: VariableAssignment,
    ) -> Result<Self, XQueryError> {
        if self.has(&variable) {
            return Err(XQueryError::InvalidAssignment(format!(
                "{:?} already assigned in this frame.",
                variable
            )));
        }

        let mut variables = (*self.variables).clone();
        variables.insert(variable, assignment);
        Ok(Self {
            variables: Arc::new(variables),
        })
    }
}
