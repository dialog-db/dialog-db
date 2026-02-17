//! Abstract syntax tree types for natural language Datalog.
//!
//! These types represent the parsed structure of prose-style Datalog statements.
//! A clause like `"Homer is Bart's father"` is represented as a [`Clause`] with
//! a name template `"@ is @'s father"` and arguments `[Value("Homer"), Value("Bart")]`.

use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};

static WILDCARD_COUNTER: AtomicUsize = AtomicUsize::new(0);

/// An expression that can appear as an argument in a clause.
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    /// A concrete value: a string identifier (e.g. `Homer`), an integer, or a list.
    Value(Val),
    /// A named variable (single uppercase letter: `X`, `Y`, `Z`).
    Variable(String),
    /// A wildcard `_` that matches anything. Each wildcard is internally unique.
    Wildcard(String),
    /// An aggregate applied to a variable (e.g. `Y.count`).
    Aggregate { name: String, term: Box<Expr> },
}

impl Expr {
    /// Create a new wildcard with a unique internal name.
    pub fn wildcard() -> Self {
        let id = WILDCARD_COUNTER.fetch_add(1, Ordering::Relaxed);
        Expr::Wildcard(format!("_{id}"))
    }

    /// Returns `true` if this expression is a variable or wildcard.
    pub fn is_variable(&self) -> bool {
        matches!(self, Expr::Variable(_) | Expr::Wildcard(_))
    }

    /// Returns `true` if this expression is a concrete value.
    pub fn is_value(&self) -> bool {
        matches!(self, Expr::Value(_))
    }
}

impl fmt::Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Expr::Value(v) => write!(f, "{v}"),
            Expr::Variable(name) => write!(f, "{name}"),
            Expr::Wildcard(_) => write!(f, "_"),
            Expr::Aggregate { name, term } => write!(f, "{term}.{name}"),
        }
    }
}

/// A concrete value.
#[derive(Debug, Clone, PartialEq)]
pub enum Val {
    /// A string identifier like `Homer`, `Bart`, `Alice`.
    Identifier(String),
    /// An integer like `42`.
    Integer(i64),
    /// A list of values like `[1, 2, 3]`.
    List(Vec<Val>),
}

impl fmt::Display for Val {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Val::Identifier(s) => write!(f, "{s}"),
            Val::Integer(n) => write!(f, "{n}"),
            Val::List(items) => {
                write!(f, "[")?;
                for (i, item) in items.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{item}")?;
                }
                write!(f, "]")
            }
        }
    }
}

/// A clause is a predicate template with argument slots.
///
/// The `name` uses `@` as placeholders for argument positions.
/// For example, `"Homer is Bart's father"` becomes:
/// - name: `"@ is @'s father"`
/// - args: `[Value("Homer"), Value("Bart")]`
#[derive(Debug, Clone, PartialEq)]
pub struct Clause {
    /// The name template with `@` placeholders for arguments.
    pub name: String,
    /// The arguments that fill the `@` slots.
    pub args: Vec<Expr>,
}

impl Clause {
    pub fn new(name: String, args: Vec<Expr>) -> Self {
        Self { name, args }
    }

    /// Compute the canonical name by substituting literal values into the template.
    ///
    /// Variables stay as `@`, but concrete values are inlined as `(value)`.
    /// E.g. with name `"@ is @'s father"` and args `[Value("Homer"), Variable("X")]`,
    /// the canonical name is `"(Homer) is @'s father"`.
    pub fn canonical_name(&self) -> String {
        let mut result = self.name.clone();
        for arg in &self.args {
            let replacement = match arg {
                Expr::Value(v) => format!("({v})"),
                _ => "*".to_string(),
            };
            if let Some(pos) = result.find('@') {
                result.replace_range(pos..pos + 1, &replacement);
            }
        }
        result.replace('*', "@")
    }
}

impl fmt::Display for Clause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut result = self.name.clone();
        for arg in &self.args {
            if let Some(pos) = result.find('@') {
                let replacement = format!("{arg}");
                result.replace_range(pos..pos + 1, &replacement);
            }
        }
        write!(f, "{result}")
    }
}

/// A body term in a rule condition — either a positive clause or a negated one.
#[derive(Debug, Clone, PartialEq)]
pub enum BodyTerm {
    /// A positive clause.
    Positive(Clause),
    /// A negated clause (`~clause` or `not clause`).
    Negated(Clause),
}

impl fmt::Display for BodyTerm {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BodyTerm::Positive(c) => write!(f, "{c}"),
            BodyTerm::Negated(c) => write!(f, "~{c}"),
        }
    }
}

/// A rule: a head clause optionally derived from body conditions.
///
/// A rule with no conditions is a **fact** (assertion).
/// A rule with conditions is a **deductive rule**.
#[derive(Debug, Clone, PartialEq)]
pub struct Rule {
    /// The head (conclusion) clause.
    pub head: Clause,
    /// The body (conditions). Empty for facts.
    pub body: Vec<BodyTerm>,
}

impl Rule {
    /// Create a fact (rule with no conditions).
    pub fn fact(head: Clause) -> Self {
        Self {
            head,
            body: vec![],
        }
    }

    /// Create a rule with conditions.
    pub fn new(head: Clause, body: Vec<BodyTerm>) -> Self {
        Self { head, body }
    }

    /// Returns `true` if this is a fact (no conditions).
    pub fn is_fact(&self) -> bool {
        self.body.is_empty()
    }

    /// Returns `true` if this is a deductive rule (has conditions).
    pub fn is_rule(&self) -> bool {
        !self.body.is_empty()
    }
}

impl fmt::Display for Rule {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.head)?;
        if !self.body.is_empty() {
            write!(f, " if ")?;
            for (i, term) in self.body.iter().enumerate() {
                if i > 0 {
                    write!(f, " and ")?;
                }
                write!(f, "{term}")?;
            }
        }
        Ok(())
    }
}

/// A parsed statement that the user can express in prose.
#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    /// Assert a fact or rule.
    Assert(Rule),
    /// Retract a fact (prefixed with `~`).
    Retract(Rule),
    /// Query for matching facts (suffixed with `?`).
    Query(Rule),
}

impl fmt::Display for Statement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Statement::Assert(rule) => write!(f, "{rule}"),
            Statement::Retract(rule) => write!(f, "~{rule}"),
            Statement::Query(rule) => write!(f, "{rule}?"),
        }
    }
}

/// A document is a sequence of statements parsed from a prose input.
#[derive(Debug, Clone, PartialEq)]
pub struct Document {
    pub statements: Vec<Statement>,
}

impl Document {
    pub fn new(statements: Vec<Statement>) -> Self {
        Self { statements }
    }
}

impl fmt::Display for Document {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, stmt) in self.statements.iter().enumerate() {
            if i > 0 {
                writeln!(f)?;
            }
            write!(f, "{stmt}")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_clause_display() {
        let clause = Clause::new(
            "@ is @'s father".to_string(),
            vec![
                Expr::Value(Val::Identifier("Homer".into())),
                Expr::Value(Val::Identifier("Bart".into())),
            ],
        );
        assert_eq!(clause.to_string(), "Homer is Bart's father");
    }

    #[test]
    fn test_clause_canonical_name() {
        let clause = Clause::new(
            "@ is @'s father".to_string(),
            vec![
                Expr::Value(Val::Identifier("Homer".into())),
                Expr::Variable("X".into()),
            ],
        );
        assert_eq!(clause.canonical_name(), "(Homer) is @'s father");
    }

    #[test]
    fn test_rule_display() {
        let rule = Rule::new(
            Clause::new(
                "@ is @'s parent".to_string(),
                vec![Expr::Variable("X".into()), Expr::Variable("Y".into())],
            ),
            vec![BodyTerm::Positive(Clause::new(
                "@ is @'s father".to_string(),
                vec![Expr::Variable("X".into()), Expr::Variable("Y".into())],
            ))],
        );
        assert_eq!(
            rule.to_string(),
            "X is Y's parent if X is Y's father"
        );
    }

    #[test]
    fn test_statement_display() {
        let fact = Statement::Assert(Rule::fact(Clause::new(
            "@ is @'s father".to_string(),
            vec![
                Expr::Value(Val::Identifier("Homer".into())),
                Expr::Value(Val::Identifier("Bart".into())),
            ],
        )));
        assert_eq!(fact.to_string(), "Homer is Bart's father");

        let query = Statement::Query(Rule::fact(Clause::new(
            "@ is @'s father".to_string(),
            vec![
                Expr::Variable("X".into()),
                Expr::Value(Val::Identifier("Bart".into())),
            ],
        )));
        assert_eq!(query.to_string(), "X is Bart's father?");
    }
}
