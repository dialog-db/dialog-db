pub use super::{Analysis, Application};
pub use crate::error::AnalyzerError;
pub use crate::fact_selector::{ATTRIBUTE_COST, BASE_COST, ENTITY_COST, UNBOUND_COST, VALUE_COST};
pub use crate::FactSelector as FactApplication;
pub use crate::{Dependencies, Fact, Term};

impl FactApplication {
    pub fn analyze(&self) -> Result<Analysis, AnalyzerError> {
        let mut dependencies = Dependencies::new();

        if let Some(Term::Variable {
            name: Some(name), ..
        }) = &self.the
        {
            dependencies.desire(name.clone(), 200)
        }

        if let Some(Term::Variable {
            name: Some(name), ..
        }) = &self.of
        {
            dependencies.desire(name.clone(), 500)
        }

        if let Some(Term::Variable {
            name: Some(name), ..
        }) = &self.is
        {
            dependencies.desire(name.clone(), 300)
        }

        Ok(Analysis {
            dependencies,
            cost: 100,
        })
    }
}

impl From<FactApplication> for Application {
    fn from(selector: FactApplication) -> Self {
        Application::Select(selector)
    }
}
