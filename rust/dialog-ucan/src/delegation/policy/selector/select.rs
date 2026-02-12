//! Typesafe selection on [`Ipld`] values.

use super::{Selectable, Selector, SelectorError, error::SelectorErrorReason, filter::Filter};
use ipld_core::ipld::Ipld;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::{cmp::Ordering, fmt, str::FromStr};
use thiserror::Error;

#[cfg(any(test, feature = "test_utils"))]
use arbitrary::{self, Arbitrary, Unstructured};

/// Typesafe selection via [`Ipld`] selectors.
#[derive(Clone)]
pub struct Select<T> {
    filters: Vec<Filter>,
    _marker: std::marker::PhantomData<T>,
}

impl<T> Serialize for Select<T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        Selector(self.filters.clone()).serialize(serializer)
    }
}

impl<'de, T> Deserialize<'de> for Select<T>
where
    Ipld: From<T>,
{
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let selector = Selector::deserialize(deserializer).map_err(serde::de::Error::custom)?;
        Ok(Select {
            filters: selector.0,
            _marker: std::marker::PhantomData,
        })
    }
}

impl<T> fmt::Debug for Select<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Select({:?})", self.filters)
    }
}

impl<T> PartialEq for Select<T> {
    fn eq(&self, other: &Self) -> bool {
        Selector(self.filters.clone()) == Selector(other.filters.clone())
    }
}

impl<T> Select<T> {
    /// Creates a new `Select` instance with the given filters.
    #[must_use]
    pub const fn new(filters: Vec<Filter>) -> Self {
        Self {
            filters,
            _marker: std::marker::PhantomData,
        }
    }

    /// Checks if two selectors are related.
    #[must_use]
    pub fn is_related<U: Clone>(&self, other: &Select<U>) -> bool
    where
        Ipld: From<T> + From<U>,
    {
        Selector(self.filters.clone()).is_related(&Selector(other.filters.clone()))
    }
}

impl<T: Selectable> Select<T> {
    /// Tries to retrieve the value from the given [`Ipld`] using the selector.
    ///
    /// # Errors
    ///
    /// Returns a [`SelectorError`] if the data shape does not conform to the requested path.
    #[allow(clippy::too_many_lines)]
    pub fn get(self, ctx: &Ipld) -> Result<T, SelectorError> {
        let got = self.filters.iter().try_fold(
            (ctx.clone(), vec![], false),
            |(ipld, mut seen_ops, is_try), op| {
                seen_ops.push(op);

                match op {
                    Filter::Try(inner) => {
                        let op: Filter = *inner.clone();
                        let ipld: Ipld =
                            Select::<Ipld>::new(vec![op]).get(ctx).unwrap_or(Ipld::Null);
                        Ok((ipld, seen_ops.clone(), true))
                    }
                    Filter::ArrayIndex(i) => {
                        let result = {
                            match ipld {
                                Ipld::List(xs) => {
                                    if xs.len() > (i32::MAX as usize) {
                                        return Err((
                                            is_try,
                                            SelectorError::from_refs(
                                                &seen_ops,
                                                SelectorErrorReason::IndexOutOfBounds,
                                            ),
                                        ));
                                    }

                                    if i.unsigned_abs() as usize > xs.len() {
                                        return Err((
                                            is_try,
                                            SelectorError::from_refs(
                                                &seen_ops,
                                                SelectorErrorReason::IndexOutOfBounds,
                                            ),
                                        ));
                                    }

                                    let idx: usize = if *i <= 0 {
                                        i.unsigned_abs() as usize
                                    } else {
                                        xs.len() - i.unsigned_abs() as usize
                                    };

                                    xs.get(idx)
                                        .ok_or((
                                            is_try,
                                            SelectorError::from_refs(
                                                &seen_ops,
                                                SelectorErrorReason::IndexOutOfBounds,
                                            ),
                                        ))
                                        .cloned()
                                }
                                Ipld::Null
                                | Ipld::Bool(_)
                                | Ipld::Integer(_)
                                | Ipld::Float(_)
                                | Ipld::String(_)
                                | Ipld::Bytes(_)
                                | Ipld::Map(_)
                                | Ipld::Link(_) => Err((
                                    is_try,
                                    SelectorError::from_refs(
                                        &seen_ops,
                                        SelectorErrorReason::NotAList,
                                    ),
                                )),
                            }
                        };

                        Ok((result?, seen_ops.clone(), is_try))
                    }
                    Filter::Field(k) => {
                        let result = match ipld {
                            Ipld::Map(xs) => xs
                                .get(k)
                                .ok_or((
                                    is_try,
                                    SelectorError::from_refs(
                                        &seen_ops,
                                        SelectorErrorReason::KeyNotFound,
                                    ),
                                ))
                                .cloned(),
                            Ipld::Null
                            | Ipld::Bool(_)
                            | Ipld::Integer(_)
                            | Ipld::Float(_)
                            | Ipld::String(_)
                            | Ipld::Bytes(_)
                            | Ipld::List(_)
                            | Ipld::Link(_) => Err((
                                is_try,
                                SelectorError::from_refs(&seen_ops, SelectorErrorReason::NotAMap),
                            )),
                        };

                        Ok((result?, seen_ops.clone(), is_try))
                    }
                    Filter::Values => {
                        let result = match ipld {
                            Ipld::List(xs) => Ok(Ipld::List(xs)),
                            Ipld::Map(xs) => Ok(Ipld::List(xs.values().cloned().collect())),
                            Ipld::Null
                            | Ipld::Bool(_)
                            | Ipld::Integer(_)
                            | Ipld::Float(_)
                            | Ipld::String(_)
                            | Ipld::Bytes(_)
                            | Ipld::Link(_) => Err((
                                is_try,
                                SelectorError::from_refs(
                                    &seen_ops,
                                    SelectorErrorReason::NotACollection,
                                ),
                            )),
                        };

                        Ok((result?, seen_ops.clone(), is_try))
                    }
                }
            },
        );

        let (ipld, path) = match got {
            Ok((ipld, seen_ops, _)) => Ok((ipld, seen_ops)),
            Err((is_try, ref e @ SelectorError { ref selector, .. })) => {
                if is_try {
                    Ok((Ipld::Null, selector.0.iter().collect::<Vec<_>>()))
                } else {
                    Err(e.clone())
                }
            }
        }?;

        T::try_select(ipld).map_err(|e| SelectorError::from_refs(&path, e))
    }
}

impl<T> From<Select<T>> for Ipld
where
    Ipld: From<T>,
{
    fn from(s: Select<T>) -> Self {
        Selector(s.filters).to_string().into()
    }
}

impl<T> FromStr for Select<T> {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let selector = Selector::from_str(s).map_err(ParseError)?;
        Ok(Select {
            filters: selector.0,
            _marker: std::marker::PhantomData,
        })
    }
}

/// Error type for parsing a selector.
#[derive(Debug, PartialEq, Error)]
#[error("Failed to parse selector: {0}")]
pub struct ParseError(#[from] nom::Err<super::error::ParseError>);

impl<T> PartialOrd for Select<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Selector(self.filters.clone()).partial_cmp(&Selector(other.filters.clone()))
    }
}

#[cfg(any(test, feature = "test_utils"))]
impl<'a, T> Arbitrary<'a> for Select<T> {
    fn arbitrary(u: &mut Unstructured<'a>) -> Result<Self, arbitrary::Error> {
        u.arbitrary::<Vec<Filter>>()
            .map(|filters| Select::new(filters))
    }
}

#[cfg(all(test, feature = "property_test", not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use crate::ipld::InternalIpld;

    mod get {
        use super::*;
        use crate::ipld::eq_with_float_nans_and_infinities;
        use proptest::prelude::*;
        use proptest_arbitrary_interop::arb;

        proptest! {
            #[test_log::test]
            fn test_identity(data in arb::<InternalIpld>()) {
                let selector = Select::<InternalIpld>::from_str(".")?;
                prop_assert!(eq_with_float_nans_and_infinities(&selector.get(&data.clone().into())?.into(), &data));
            }
        }

        proptest! {
            #[test_log::test]
            fn test_try_missing_is_null(data in arb::<InternalIpld>()) {
                let selector = Select::<Ipld>::from_str(".foo?")?;

                let mut cleaned_data = Ipld::from(data);
                if let Ipld::Map(ref mut m) = cleaned_data {
                    m.remove("foo");
                } else if let Ipld::List(_) = cleaned_data {
                    cleaned_data = Ipld::Null;
                }

                prop_assert_eq!(selector.get(&cleaned_data)?, Ipld::Null);
            }
        }

        proptest! {
            #[test_log::test]
            fn test_try_missing_plus_trailing_is_null(data in arb::<InternalIpld>(), more in arb::<Vec<Filter>>()) {
                let mut filters = vec![Filter::Try(Box::new(Filter::Field("foo".into())))];

                for f in &more {
                    if let Filter::Try(_inner) = f {
                        // Noop
                    } else {
                        filters.push(f.clone());
                    }
                }

                if filters.contains(&Filter::Values) || filters.contains(&Filter::Try(Box::new(Filter::Values))) {
                    prop_assume!(false);
                }

                let selector: Select<Ipld> = Select::new(filters);

                let mut cleaned_data = Ipld::from(data);
                if let Ipld::Map(ref mut m) = cleaned_data {
                    m.remove("foo");
                }

                prop_assert_eq!(selector.get(&cleaned_data)?, Ipld::Null);
            }
        }
    }
}
