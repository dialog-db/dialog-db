//! Type conversion formulas for the query system
//!
//! This module provides formulas for converting between different types,
//! including string conversion and number parsing operations.

use crate::{Formula, Value, formula::Input};

/// ToString formula that converts any supported type to string
#[derive(Debug, Clone, Formula)]
pub struct ToString {
    /// Value to convert
    pub value: Value,
    /// Resulting string representation
    #[output]
    pub is: String,
}

impl ToString {
    /// Convert the input value to its string representation
    pub fn compute(input: ToStringInput) -> Vec<Self> {
        let string_repr = match &input.value {
            Value::String(s) => s.clone(),
            Value::UnsignedInt(n) => n.to_string(),
            Value::SignedInt(n) => n.to_string(),
            Value::Float(f) => f.to_string(),
            Value::Boolean(b) => b.to_string(),
            Value::Entity(e) => e.to_string(),
            Value::Symbol(s) => s.to_string(),
            Value::Bytes(bytes) => format!("Bytes({} bytes)", bytes.len()),
            Value::Record(record) => format!("Record({} bytes)", record.len()),
        };

        vec![ToString {
            value: input.value,
            is: string_repr,
        }]
    }
}

/// Parse a string into an unsigned integer (u128)
#[derive(Debug, Clone, Formula)]
pub struct ParseUnsignedInteger {
    /// String to parse
    pub text: String,
    /// Parsed unsigned integer
    #[output(cost = 2)]
    pub is: u128,
}

impl ParseUnsignedInteger {
    /// Parse the text as u128, returning empty on failure
    pub fn compute(input: Input<Self>) -> Vec<Self> {
        match input.text.trim().parse::<u128>() {
            Ok(number) => vec![ParseUnsignedInteger {
                text: input.text,
                is: number,
            }],
            Err(_) => vec![],
        }
    }
}

/// Parse a string into a signed integer (i128)
#[derive(Debug, Clone, Formula)]
pub struct ParseSignedInteger {
    /// String to parse
    pub text: String,
    /// Parsed signed integer
    #[output(cost = 2)]
    pub is: i128,
}

impl ParseSignedInteger {
    /// Parse the text as i128, returning empty on failure
    pub fn compute(input: Input<Self>) -> Vec<Self> {
        match input.text.trim().parse::<i128>() {
            Ok(number) => vec![ParseSignedInteger {
                text: input.text,
                is: number,
            }],
            Err(_) => vec![],
        }
    }
}

/// Parse a string into a floating point number (f64)
#[derive(Debug, Clone, Formula)]
pub struct ParseFloat {
    /// String to parse
    pub text: String,
    /// Parsed float
    #[output(cost = 2)]
    pub is: f64,
}

impl ParseFloat {
    /// Parse the text as f64, returning empty on failure
    pub fn compute(input: Input<Self>) -> Vec<Self> {
        match input.text.trim().parse::<f64>() {
            Ok(number) => vec![ParseFloat {
                text: input.text,
                is: number,
            }],
            Err(_) => vec![],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::{Application, Output};
    use crate::selection::Match;
    use crate::session::RuleRegistry;
    use crate::source::Source;
    use crate::{Entity, Query, Term};
    use dialog_artifacts::helpers::{test_operator, test_repo};
    use futures_util::TryStreamExt;

    #[dialog_common::test]
    async fn it_converts_number_to_string() -> anyhow::Result<()> {
        let operator = test_operator().await;
        let repo = test_repo(&operator).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let source = Source::new(&branch, &operator, RuleRegistry::new());

        let query = Query::<ToString> {
            value: Term::from(42u32).into(),
            is: Term::var("result"),
        };
        let results = query.perform(&source).try_vec().await?;

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].is, "42");
        Ok(())
    }

    #[dialog_common::test]
    async fn it_converts_boolean_to_string() -> anyhow::Result<()> {
        let operator = test_operator().await;
        let repo = test_repo(&operator).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let source = Source::new(&branch, &operator, RuleRegistry::new());

        let query = Query::<ToString> {
            value: Term::from(true).into(),
            is: Term::var("result"),
        };
        let results = query.perform(&source).try_vec().await?;

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].is, "true");
        Ok(())
    }

    #[dialog_common::test]
    async fn it_passes_string_through() -> anyhow::Result<()> {
        let operator = test_operator().await;
        let repo = test_repo(&operator).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let source = Source::new(&branch, &operator, RuleRegistry::new());

        let query = Query::<ToString> {
            value: Term::from("hello".to_string()).into(),
            is: Term::var("result"),
        };
        let results = query.perform(&source).try_vec().await?;

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].is, "hello");
        Ok(())
    }

    #[dialog_common::test]
    async fn it_converts_entity_to_string() -> anyhow::Result<()> {
        let operator = test_operator().await;
        let repo = test_repo(&operator).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let source = Source::new(&branch, &operator, RuleRegistry::new());

        let entity = Entity::new()?;
        let query = Query::<ToString> {
            value: Term::from(entity.clone()).into(),
            is: Term::var("result"),
        };
        let results = query.perform(&source).try_vec().await?;

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].is, entity.to_string());
        Ok(())
    }

    #[dialog_common::test]
    async fn it_converts_float_to_string() -> anyhow::Result<()> {
        let operator = test_operator().await;
        let repo = test_repo(&operator).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let source = Source::new(&branch, &operator, RuleRegistry::new());

        let query = Query::<ToString> {
            value: Term::from(3.15f64).into(),
            is: Term::var("result"),
        };
        let results = query.perform(&source).try_vec().await?;

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].is, "3.15");
        Ok(())
    }

    #[dialog_common::test]
    async fn it_converts_variable_input_to_string() -> anyhow::Result<()> {
        let operator = test_operator().await;
        let repo = test_repo(&operator).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let source = Source::new(&branch, &operator, RuleRegistry::new());

        let query = Query::<ToString> {
            value: Term::var("input"),
            is: Term::var("result"),
        };
        let mut candidate = Match::new();
        candidate.bind(&Term::var("input"), 42u32.into())?;
        let query_copy = query.clone();
        let matches: Vec<Match> = query
            .evaluate(candidate.seed(), &source)
            .try_collect()
            .await?;

        assert_eq!(matches.len(), 1);
        let proof = query_copy.realize(matches[0].clone())?;
        assert_eq!(proof.is, "42");
        Ok(())
    }

    #[dialog_common::test]
    async fn it_parses_unsigned_integer() -> anyhow::Result<()> {
        let operator = test_operator().await;
        let repo = test_repo(&operator).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let source = Source::new(&branch, &operator, RuleRegistry::new());

        let query = Query::<ParseUnsignedInteger> {
            text: Term::from("123".to_string()),
            is: Term::var("num"),
        };
        let results = query.perform(&source).try_vec().await?;

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].is, 123);
        Ok(())
    }

    #[dialog_common::test]
    async fn it_parses_unsigned_integer_with_whitespace() -> anyhow::Result<()> {
        let operator = test_operator().await;
        let repo = test_repo(&operator).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let source = Source::new(&branch, &operator, RuleRegistry::new());

        let query = Query::<ParseUnsignedInteger> {
            text: Term::from("  456  ".to_string()),
            is: Term::var("num"),
        };
        let results = query.perform(&source).try_vec().await?;

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].is, 456);
        Ok(())
    }

    #[dialog_common::test]
    async fn it_rejects_negative_as_unsigned() -> anyhow::Result<()> {
        let operator = test_operator().await;
        let repo = test_repo(&operator).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let source = Source::new(&branch, &operator, RuleRegistry::new());

        let query = Query::<ParseUnsignedInteger> {
            text: Term::from("-123".to_string()),
            is: Term::var("num"),
        };
        let results = query.perform(&source).try_vec().await?;

        assert_eq!(results.len(), 0);
        Ok(())
    }

    #[dialog_common::test]
    async fn it_parses_signed_integer() -> anyhow::Result<()> {
        let operator = test_operator().await;
        let repo = test_repo(&operator).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let source = Source::new(&branch, &operator, RuleRegistry::new());

        let query = Query::<ParseSignedInteger> {
            text: Term::from("-123".to_string()),
            is: Term::var("num"),
        };
        let results = query.perform(&source).try_vec().await?;

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].is, -123);
        Ok(())
    }

    #[dialog_common::test]
    async fn it_parses_positive_as_signed() -> anyhow::Result<()> {
        let operator = test_operator().await;
        let repo = test_repo(&operator).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let source = Source::new(&branch, &operator, RuleRegistry::new());

        let query = Query::<ParseSignedInteger> {
            text: Term::from("42".to_string()),
            is: Term::var("num"),
        };
        let results = query.perform(&source).try_vec().await?;

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].is, 42);
        Ok(())
    }

    #[dialog_common::test]
    async fn it_parses_float() -> anyhow::Result<()> {
        let operator = test_operator().await;
        let repo = test_repo(&operator).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let source = Source::new(&branch, &operator, RuleRegistry::new());

        let query = Query::<ParseFloat> {
            text: Term::from("3.15".to_string()),
            is: Term::var("num"),
        };
        let results = query.perform(&source).try_vec().await?;

        assert_eq!(results.len(), 1);
        assert!((results[0].is - 3.15).abs() < f64::EPSILON);
        Ok(())
    }

    #[dialog_common::test]
    async fn it_parses_negative_float() -> anyhow::Result<()> {
        let operator = test_operator().await;
        let repo = test_repo(&operator).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let source = Source::new(&branch, &operator, RuleRegistry::new());

        let query = Query::<ParseFloat> {
            text: Term::from("-2.5".to_string()),
            is: Term::var("num"),
        };
        let results = query.perform(&source).try_vec().await?;

        assert_eq!(results.len(), 1);
        assert!((results[0].is - -2.5).abs() < f64::EPSILON);
        Ok(())
    }

    #[dialog_common::test]
    async fn it_parses_integer_as_float() -> anyhow::Result<()> {
        let operator = test_operator().await;
        let repo = test_repo(&operator).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let source = Source::new(&branch, &operator, RuleRegistry::new());

        let query = Query::<ParseFloat> {
            text: Term::from("42".to_string()),
            is: Term::var("num"),
        };
        let results = query.perform(&source).try_vec().await?;

        assert_eq!(results.len(), 1);
        assert!((results[0].is - 42.0).abs() < f64::EPSILON);
        Ok(())
    }

    #[dialog_common::test]
    async fn it_rejects_invalid_text() -> anyhow::Result<()> {
        let operator = test_operator().await;
        let repo = test_repo(&operator).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let source = Source::new(&branch, &operator, RuleRegistry::new());

        for text in ["not a number", ""] {
            let query = Query::<ParseUnsignedInteger> {
                text: Term::from(text.to_string()),
                is: Term::var("num"),
            };
            assert_eq!(query.perform(&source).try_vec().await?.len(), 0);

            let query = Query::<ParseSignedInteger> {
                text: Term::from(text.to_string()),
                is: Term::var("num"),
            };
            assert_eq!(query.perform(&source).try_vec().await?.len(), 0);

            let query = Query::<ParseFloat> {
                text: Term::from(text.to_string()),
                is: Term::var("num"),
            };
            assert_eq!(query.perform(&source).try_vec().await?.len(), 0);
        }
        Ok(())
    }
}
