//! Parser for natural language Datalog prose.
//!
//! Converts a token stream (from [`crate::tokenizer`]) into an AST ([`crate::ast`]).
//!
//! The grammar follows the nl-datalog two-pass design:
//!
//! ```text
//! document    = statement (newline statement)*
//! statement   = '~' rule        → Retract
//!             | rule '?'        → Query
//!             | rule            → Assert
//! rule        = head_clause 'if' body
//!             | head_clause
//! body        = body_term ('and' body_term)*
//! body_term   = '~' body_clause   → Negated
//!             | body_clause        → Positive
//! head_clause = clause(head_expr)
//! body_clause = clause(body_expr)
//! head_expr   = body_expr aggregate?
//! body_expr   = variable | value
//! clause(e)   = (name_part | e)+   → build clause from interleaved parts
//! variable    = Var | Wildcard
//! value       = Value | '[' value* ']'
//! aggregate   = Aggregate
//! ```

use crate::ast::*;
use crate::error::ParseError;
use crate::tokenizer::{Token, ValueToken};

/// A token-stream parser.
struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    fn new(tokens: Vec<Token>) -> Self {
        Self { tokens, pos: 0 }
    }

    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.pos)
    }

    fn advance(&mut self) -> Option<&Token> {
        let tok = self.tokens.get(self.pos);
        if tok.is_some() {
            self.pos += 1;
        }
        tok
    }

    fn at_end_of_line(&self) -> bool {
        matches!(self.peek(), None | Some(Token::Newline))
    }

    // --- Value parsing ---

    fn parse_value(&mut self) -> Result<Val, ParseError> {
        match self.peek() {
            Some(Token::Value(ValueToken::Identifier(_))) => {
                if let Some(Token::Value(ValueToken::Identifier(s))) = self.advance().cloned() {
                    Ok(Val::Identifier(s))
                } else {
                    unreachable!()
                }
            }
            Some(Token::Value(ValueToken::Integer(_))) => {
                if let Some(Token::Value(ValueToken::Integer(n))) = self.advance().cloned() {
                    Ok(Val::Integer(n))
                } else {
                    unreachable!()
                }
            }
            Some(Token::BeginList) => {
                self.advance(); // consume `[`
                let mut items = Vec::new();
                while self.peek() != Some(&Token::EndList) {
                    if self.peek().is_none() {
                        return Err(ParseError::UnterminatedList);
                    }
                    items.push(self.parse_value()?);
                }
                self.advance(); // consume `]`
                Ok(Val::List(items))
            }
            other => Err(ParseError::Expected {
                expected: "value".into(),
                found: other.map(|t| format!("{t:?}")).unwrap_or("end of input".into()),
            }),
        }
    }

    // --- Expression parsing ---

    fn parse_body_expr(&mut self) -> Result<Expr, ParseError> {
        match self.peek() {
            Some(Token::Var(_)) => {
                if let Some(Token::Var(c)) = self.advance().cloned() {
                    Ok(Expr::Variable(c.to_string()))
                } else {
                    unreachable!()
                }
            }
            Some(Token::Wildcard) => {
                self.advance();
                Ok(Expr::wildcard())
            }
            Some(Token::Value(_) | Token::BeginList) => {
                Ok(Expr::Value(self.parse_value()?))
            }
            other => Err(ParseError::Expected {
                expected: "variable or value".into(),
                found: other.map(|t| format!("{t:?}")).unwrap_or("end of input".into()),
            }),
        }
    }

    fn parse_head_expr(&mut self) -> Result<Expr, ParseError> {
        let expr = self.parse_body_expr()?;
        // Check for aggregate suffix
        if let Some(Token::Aggregate(_)) = self.peek() {
            if let Some(Token::Aggregate(name)) = self.advance().cloned() {
                Ok(Expr::Aggregate {
                    name,
                    term: Box::new(expr),
                })
            } else {
                unreachable!()
            }
        } else {
            Ok(expr)
        }
    }

    // --- Clause parsing ---

    /// Parse a clause, using the given expression parser for arguments.
    ///
    /// A clause is an interleaving of name parts and argument expressions.
    /// The result is a `Clause` with a name template (using `@` for arg slots)
    /// and a list of argument expressions.
    fn parse_clause<F>(&mut self, mut parse_expr: F) -> Result<Clause, ParseError>
    where
        F: FnMut(&mut Self) -> Result<Expr, ParseError>,
    {
        let mut name_parts: Vec<String> = Vec::new();
        let mut args: Vec<Expr> = Vec::new();
        let mut last_was_name = false;

        loop {
            match self.peek() {
                // Stop at keywords, end of line, query marker, negation (in body context)
                None
                | Some(Token::Keyword(_))
                | Some(Token::Newline)
                | Some(Token::Query) => break,

                // A name part
                Some(Token::Name(_)) => {
                    if let Some(Token::Name(n)) = self.advance().cloned() {
                        if last_was_name {
                            // Merge with previous name part (space-separated)
                            let last = name_parts.last_mut().unwrap();
                            last.push(' ');
                            last.push_str(&n);
                        } else {
                            name_parts.push(n);
                        }
                        last_was_name = true;
                    }
                }

                // An expression (variable, value, list, aggregate)
                Some(
                    Token::Var(_) | Token::Wildcard | Token::Value(_) | Token::BeginList,
                ) => {
                    let expr = parse_expr(self)?;
                    name_parts.push("@".to_string());
                    args.push(expr);
                    last_was_name = false;
                }

                // Not token — stop (used for negated body terms)
                Some(Token::Not) => break,

                // Aggregate without a preceding expression — shouldn't happen in well-formed input
                Some(Token::Aggregate(_)) => break,

                // List end — stop
                Some(Token::EndList) => break,
            }
        }

        if name_parts.is_empty() {
            return Err(ParseError::EmptyClause);
        }

        // Join name parts, but don't insert a space before parts that start
        // with an apostrophe (e.g. "'s") so we get `@'s father` not `@ 's father`.
        let mut name = String::new();
        for part in &name_parts {
            if !name.is_empty() && !part.starts_with('\'') {
                name.push(' ');
            }
            name.push_str(part);
        }

        Ok(Clause::new(name, args))
    }

    fn parse_head_clause(&mut self) -> Result<Clause, ParseError> {
        self.parse_clause(Self::parse_head_expr)
    }

    fn parse_body_clause(&mut self) -> Result<Clause, ParseError> {
        self.parse_clause(Self::parse_body_expr)
    }

    // --- Body term parsing ---

    fn parse_body_term(&mut self) -> Result<BodyTerm, ParseError> {
        if self.peek() == Some(&Token::Not) {
            self.advance();
            let clause = self.parse_body_clause()?;
            Ok(BodyTerm::Negated(clause))
        } else {
            let clause = self.parse_body_clause()?;
            Ok(BodyTerm::Positive(clause))
        }
    }

    fn parse_body(&mut self) -> Result<Vec<BodyTerm>, ParseError> {
        let mut terms = vec![self.parse_body_term()?];
        while self.peek() == Some(&Token::Keyword("and".into())) {
            self.advance(); // consume `and`
            terms.push(self.parse_body_term()?);
        }
        Ok(terms)
    }

    // --- Rule parsing ---

    fn parse_rule(&mut self) -> Result<Rule, ParseError> {
        let head = self.parse_head_clause()?;

        if self.peek() == Some(&Token::Keyword("if".into())) {
            self.advance(); // consume `if`
            let body = self.parse_body()?;
            Ok(Rule::new(head, body))
        } else {
            Ok(Rule::fact(head))
        }
    }

    // --- Statement parsing ---

    fn parse_statement(&mut self) -> Result<Statement, ParseError> {
        // Check for retraction prefix
        if self.peek() == Some(&Token::Not) {
            // Peek ahead to see if this is a retraction (~ at start of line)
            // vs a negated body term. At statement level, ~ means retraction.
            self.advance();
            let rule = self.parse_rule()?;
            return Ok(Statement::Retract(rule));
        }

        let rule = self.parse_rule()?;

        // Check for query suffix
        if self.peek() == Some(&Token::Query) {
            self.advance();
            return Ok(Statement::Query(rule));
        }

        Ok(Statement::Assert(rule))
    }

    // --- Document parsing ---

    fn parse_document(&mut self) -> Result<Document, ParseError> {
        let mut statements = Vec::new();

        // Skip leading newlines
        while self.peek() == Some(&Token::Newline) {
            self.advance();
        }

        if !self.at_end_of_line() {
            statements.push(self.parse_statement()?);
        }

        while self.peek() == Some(&Token::Newline) {
            self.advance();
            // Skip consecutive newlines
            while self.peek() == Some(&Token::Newline) {
                self.advance();
            }
            if self.peek().is_some() {
                statements.push(self.parse_statement()?);
            }
        }

        Ok(Document::new(statements))
    }
}

/// Parse a prose string into a [`Document`] of statements.
///
/// Each line is treated as a separate statement. Lines can be:
/// - Assertions: `Homer is Bart's father`
/// - Rules: `X is Y's parent if X is Y's father`
/// - Queries: `X is Bart's father?`
/// - Retractions: `~Homer is Bart's father`
///
/// # Example
///
/// ```
/// use dialog_prose::parse;
///
/// let doc = parse("Homer is Bart's father\nX is Y's parent if X is Y's father").unwrap();
/// assert_eq!(doc.statements.len(), 2);
/// ```
pub fn parse(input: &str) -> Result<Document, ParseError> {
    let tokens = crate::tokenizer::tokenize(input);
    let mut parser = Parser::new(tokens);
    parser.parse_document()
}

/// Parse a single statement from a prose string.
///
/// # Example
///
/// ```
/// use dialog_prose::parse_statement;
/// use dialog_prose::ast::Statement;
///
/// let stmt = parse_statement("Homer is Bart's father").unwrap();
/// assert!(matches!(stmt, Statement::Assert(_)));
/// ```
pub fn parse_statement(input: &str) -> Result<Statement, ParseError> {
    let tokens = crate::tokenizer::tokenize(input);
    let mut parser = Parser::new(tokens);
    parser.parse_statement()
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn test_parse_simple_fact() {
        let doc = parse("Homer is Bart's father").unwrap();
        assert_eq!(doc.statements.len(), 1);

        let stmt = &doc.statements[0];
        match stmt {
            Statement::Assert(rule) => {
                assert!(rule.is_fact());
                assert_eq!(rule.head.name, "@ is @'s father");
                assert_eq!(rule.head.args.len(), 2);
                assert_eq!(rule.head.args[0], Expr::Value(Val::Identifier("Homer".into())));
                assert_eq!(rule.head.args[1], Expr::Value(Val::Identifier("Bart".into())));
            }
            _ => panic!("expected Assert"),
        }
    }

    #[test]
    fn test_parse_rule() {
        let doc = parse("X is Y's parent if X is Y's father").unwrap();
        let stmt = &doc.statements[0];
        match stmt {
            Statement::Assert(rule) => {
                assert!(rule.is_rule());
                assert_eq!(rule.head.name, "@ is @'s parent");
                assert_eq!(rule.body.len(), 1);
                match &rule.body[0] {
                    BodyTerm::Positive(c) => {
                        assert_eq!(c.name, "@ is @'s father");
                    }
                    _ => panic!("expected positive body term"),
                }
            }
            _ => panic!("expected Assert"),
        }
    }

    #[test]
    fn test_parse_rule_with_and() {
        let doc =
            parse("X is Y's grandfather if X is Z's father and Z is Y's parent")
                .unwrap();
        let stmt = &doc.statements[0];
        match stmt {
            Statement::Assert(rule) => {
                assert_eq!(rule.head.name, "@ is @'s grandfather");
                assert_eq!(rule.body.len(), 2);
            }
            _ => panic!("expected Assert"),
        }
    }

    #[test]
    fn test_parse_query() {
        let doc = parse("X is Bart's father?").unwrap();
        let stmt = &doc.statements[0];
        assert!(matches!(stmt, Statement::Query(_)));
    }

    #[test]
    fn test_parse_retraction() {
        let doc = parse("~Homer is Bart's father").unwrap();
        let stmt = &doc.statements[0];
        assert!(matches!(stmt, Statement::Retract(_)));
    }

    #[test]
    fn test_parse_negated_body() {
        let doc = parse("X is lonely if ~X has a friend").unwrap();
        let stmt = &doc.statements[0];
        match stmt {
            Statement::Assert(rule) => {
                assert_eq!(rule.body.len(), 1);
                assert!(matches!(rule.body[0], BodyTerm::Negated(_)));
            }
            _ => panic!("expected Assert"),
        }
    }

    #[test]
    fn test_parse_aggregate() {
        let doc =
            parse("X has Y.count grandchildren if X is Y's grandfather")
                .unwrap();
        let stmt = &doc.statements[0];
        match stmt {
            Statement::Assert(rule) => {
                assert_eq!(rule.head.name, "@ has @ grandchildren");
                assert_eq!(rule.head.args.len(), 2);
                match &rule.head.args[1] {
                    Expr::Aggregate { name, term } => {
                        assert_eq!(name, "count");
                        assert_eq!(**term, Expr::Variable("Y".into()));
                    }
                    _ => panic!("expected Aggregate"),
                }
            }
            _ => panic!("expected Assert"),
        }
    }

    #[test]
    fn test_parse_multiple_lines() {
        let input = "\
Homer is Bart's father
Homer is Lisa's father
X is Y's parent if X is Y's father";

        let doc = parse(input).unwrap();
        assert_eq!(doc.statements.len(), 3);
    }

    #[test]
    fn test_roundtrip_fact() {
        let input = "Homer is Bart's father";
        let doc = parse(input).unwrap();
        assert_eq!(doc.to_string(), input);
    }

    #[test]
    fn test_roundtrip_rule() {
        let input = "X is Y's parent if X is Y's father";
        let doc = parse(input).unwrap();
        assert_eq!(doc.to_string(), input);
    }

    #[test]
    fn test_roundtrip_query() {
        let input = "X is Bart's father?";
        let doc = parse(input).unwrap();
        assert_eq!(doc.to_string(), input);
    }

    #[test]
    fn test_roundtrip_complex() {
        let input = "\
Homer is Bart's father
Homer is Lisa's father
Abe is Homer's father
X is Y's parent if X is Y's father
X is Y's grandfather if X is Z's father and Z is Y's parent";

        let doc = parse(input).unwrap();
        assert_eq!(doc.to_string(), input);
    }

    #[test]
    fn test_parse_wildcard() {
        let doc = parse("_ is Bart's father?").unwrap();
        let stmt = &doc.statements[0];
        match stmt {
            Statement::Query(rule) => {
                assert!(matches!(rule.head.args[0], Expr::Wildcard(_)));
            }
            _ => panic!("expected Query"),
        }
    }

    #[test]
    fn test_parse_list_value() {
        let doc = parse("X has [1 2 3] items").unwrap();
        let stmt = &doc.statements[0];
        match stmt {
            Statement::Assert(rule) => {
                match &rule.head.args[1] {
                    Expr::Value(Val::List(items)) => {
                        assert_eq!(items.len(), 3);
                    }
                    _ => panic!("expected list value"),
                }
            }
            _ => panic!("expected Assert"),
        }
    }

    #[test]
    fn test_parse_integer_value() {
        let doc = parse("Abe has 2 grandchildren").unwrap();
        let stmt = &doc.statements[0];
        match stmt {
            Statement::Assert(rule) => {
                assert_eq!(rule.head.args[1], Expr::Value(Val::Integer(2)));
            }
            _ => panic!("expected Assert"),
        }
    }
}
