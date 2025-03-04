use thiserror::Error;

#[derive(Error, Debug)]
pub enum XQueryError {
    #[error("Cannot parse value as attribute: {0}")]
    InvalidAttribute(String),
}
