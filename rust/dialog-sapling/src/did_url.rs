//! A persona DID that carries a suggested name, as
//! `did:key:z6MkpTHR…vktH?name=Ben`. See PDD006, "Suggested Names".
//!
//! A DID URL can carry a query, after a `?`. Sapling reads one parameter of
//! it, `name`: the name that the persona proposes for itself. Its value is
//! percent-encoded, as each part of a URL is, so `?name=Ben%20B.` suggests
//! `Ben B.`. A `+` is a `+`, not a space: the query of a URL is not a form. The
//! first `name` counts, and each other parameter is ignored.
//!
//! Both the client and the persona domain read it: the client to refuse
//! `contact add` with no name at all before it connects, and the persona
//! domain to take the name.

use percent_encoding::percent_decode_str;

/// The parameter of the query that holds the suggested name.
const NAME: &str = "name";

/// A DID with the suggested name of its query, if any.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DidUrl<'a> {
    /// The DID, with no query.
    pub did: &'a str,
    /// The suggested name, percent-decoded, or `None` if the query has no
    /// `name`.
    pub name: Option<SuggestedName>,
}

/// The value of `name` in the query of a DID URL.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SuggestedName {
    /// The name, percent-decoded.
    Text(String),
    /// A value that is not UTF-8 once decoded.
    NotUtf8,
}

/// The DID of `text`, and the suggested name of its query.
pub fn parse(text: &str) -> DidUrl<'_> {
    let Some((did, query)) = text.split_once('?') else {
        return DidUrl {
            did: text,
            name: None,
        };
    };
    // A fragment is not part of the query.
    let query = query.split_once('#').map_or(query, |(query, _)| query);
    let name = query
        .split('&')
        .find_map(|parameter| match parameter.split_once('=') {
            Some((NAME, value)) => Some(value),
            _ => None,
        })
        .map(|value| match percent_decode_str(value).decode_utf8() {
            Ok(name) => SuggestedName::Text(name.into_owned()),
            Err(_) => SuggestedName::NotUtf8,
        });
    DidUrl { did, name }
}

#[cfg(test)]
mod tests {
    use super::{DidUrl, SuggestedName, parse};

    const DID: &str = "did:key:z6MkpTHR8VNsBxYAAWHut2Geadd9jSwuBV8xRoAnwWsdvktH";

    fn text(name: &str) -> Option<SuggestedName> {
        Some(SuggestedName::Text(name.to_owned()))
    }

    #[test]
    fn it_reads_a_did_with_no_query() {
        assert_eq!(
            parse(DID),
            DidUrl {
                did: DID,
                name: None
            }
        );
    }

    #[test]
    fn it_reads_the_suggested_name_of_the_query() {
        assert_eq!(parse(&format!("{DID}?name=Ben")).name, text("Ben"));
        assert_eq!(parse(&format!("{DID}?name=Ben")).did, DID);
    }

    #[test]
    fn it_percent_decodes_the_name_and_keeps_a_plus() {
        assert_eq!(parse(&format!("{DID}?name=Ben%20B.")).name, text("Ben B."));
        assert_eq!(parse(&format!("{DID}?name=A+B")).name, text("A+B"));
        assert_eq!(parse(&format!("{DID}?name=%C3%89lise")).name, text("Élise"));
    }

    #[test]
    fn it_takes_the_first_name_and_ignores_each_other_parameter() {
        let url = format!("{DID}?route=ip:192.0.2.7:4433&name=Ben&name=Mum#frag");

        assert_eq!(parse(&url).name, text("Ben"));
        assert_eq!(parse(&format!("{DID}?names=Ben")).name, None);
        assert_eq!(parse(&format!("{DID}?")).name, None);
        assert_eq!(parse(&format!("{DID}?name=")).name, text(""));
    }

    #[test]
    fn it_tells_a_name_that_is_not_utf8() {
        assert_eq!(
            parse(&format!("{DID}?name=%FF")).name,
            Some(SuggestedName::NotUtf8)
        );
    }
}
