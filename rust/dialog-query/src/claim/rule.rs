use crate::predicate::deductive_rule::DeductiveRule;

/// Represents a rule declaration, which registers it into
/// a session.
#[derive(Debug, Clone, PartialEq)]
pub struct Claim(DeductiveRule);
