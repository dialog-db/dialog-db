//! The suffix that tells apart the things of one name: personas, and spaces.
//! See PDD002, "Suffixes", and PDD003, "Changes to PDD002", "Listing Spaces".
//!
//! When two or more things on the device have the same name, each gets a
//! suffix: the last N characters of its DID, where N is the smallest number,
//! [`MIN`] or more, that makes the suffixes of that name all different. A name
//! that only one thing has gets no suffix. The suffix of a thing depends on
//! every thing on the device, so a caller computes it over all of them, and
//! not over a filtered few.
//!
//! A space shows two names, its petname and its space name, and has one
//! suffix. It gets one when another space has the same text as either of its
//! names, and the suffix tells it apart from each space that shares a text
//! with it. A name of it whose text no other space has is shown with no
//! suffix. See [`across_names`], and PDD005, "Suffixes".

use std::collections::{HashMap, HashSet};

/// The fewest characters of a DID that a suffix has.
pub const MIN: usize = 4;

/// The suffix of each of `entries`, a name and a DID, in their order: the end
/// of the DID if another entry has the name, and `None` if not. The DIDs must
/// all be different.
pub fn suffixes<'a>(entries: impl IntoIterator<Item = (&'a str, &'a str)>) -> Vec<Option<String>> {
    let entries: Vec<(&str, &str)> = entries.into_iter().collect();
    let mut groups: HashMap<&str, Vec<&str>> = HashMap::new();
    for &(name, did) in &entries {
        groups.entry(name).or_default().push(did);
    }
    let lengths: HashMap<&str, usize> = groups
        .into_iter()
        .filter(|(_, dids)| dids.len() > 1)
        .map(|(name, dids)| (name, length(&dids)))
        .collect();
    entries
        .into_iter()
        .map(|(name, did)| lengths.get(name).map(|&length| end(did, length).to_owned()))
        .collect()
}

/// The suffix of an entry that shows several names, and whether each of its
/// names needs the suffix: whether another entry shows the same text.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AcrossNames {
    pub suffix: Option<String>,
    /// For each name of the entry, in its order, whether another entry has
    /// its text.
    pub shared: Vec<bool>,
}

/// The suffix of each of `entries`, the names that it shows and its DID, in
/// their order. An entry whose names all have texts that no other entry has
/// gets none. Any other gets the end of its DID, of the smallest length,
/// [`MIN`] or more, at which it differs from the end of each entry that
/// shares a text with it: the longest length that each such text needs by
/// [`suffixes`]. The DIDs must all be different. Two names of one entry with
/// one text count as one.
pub fn across_names(entries: &[(Vec<&str>, &str)]) -> Vec<AcrossNames> {
    let mut groups: HashMap<&str, Vec<&str>> = HashMap::new();
    for (names, did) in entries {
        let mut seen = HashSet::new();
        for name in names {
            if seen.insert(*name) {
                groups.entry(name).or_default().push(did);
            }
        }
    }
    let lengths: HashMap<&str, usize> = groups
        .iter()
        .filter(|(_, dids)| dids.len() > 1)
        .map(|(name, dids)| (*name, length(dids)))
        .collect();
    entries
        .iter()
        .map(|(names, did)| {
            let shared: Vec<bool> = names
                .iter()
                .map(|name| lengths.contains_key(name))
                .collect();
            let suffix = names
                .iter()
                .filter_map(|name| lengths.get(name))
                .max()
                .map(|&length| end(did, length).to_owned());
            AcrossNames { suffix, shared }
        })
        .collect()
}

/// The smallest length, [`MIN`] or more, at which the ends of `dids` are all
/// different. The DIDs are all different, so the length of the longest DID
/// always does.
fn length(dids: &[&str]) -> usize {
    let longest = dids
        .iter()
        .map(|did| did.chars().count())
        .max()
        .unwrap_or(0);
    (MIN..longest)
        .find(|&length| {
            let mut ends = HashSet::new();
            dids.iter().all(|did| ends.insert(end(did, length)))
        })
        .unwrap_or(longest.max(MIN))
}

/// The last `length` characters of `did`, or all of it if it is shorter.
fn end(did: &str, length: usize) -> &str {
    let count = did.chars().count();
    let skip = count.saturating_sub(length);
    let start = did.char_indices().nth(skip).map_or(did.len(), |(at, _)| at);
    &did[start..]
}

#[cfg(test)]
mod tests {
    use super::{AcrossNames, across_names, end, suffixes};

    /// The DID that ends with `end`.
    fn did(end: &str) -> String {
        format!("did:key:z6Mk{}{end}", "a".repeat(44 - end.len()))
    }

    /// The suffixes of `entries`, each a name and the end of its DID.
    fn of(entries: &[(&str, &str)]) -> Vec<Option<String>> {
        let dids: Vec<(&str, String)> = entries
            .iter()
            .map(|&(name, end)| (name, did(end)))
            .collect();
        suffixes(dids.iter().map(|(name, did)| (*name, did.as_str())))
    }

    #[test]
    fn it_gives_no_suffix_to_a_name_that_one_entry_has() {
        assert_eq!(of(&[("ana", "2doK"), ("work", "vktH")]), [None, None]);
    }

    #[test]
    fn it_gives_the_last_four_characters_when_they_tell_the_entries_apart() {
        assert_eq!(
            of(&[("ana", "2doK"), ("work", "VJGx"), ("ana", "vktH")]),
            [Some("2doK".to_owned()), None, Some("vktH".to_owned())]
        );
    }

    #[test]
    fn it_lengthens_every_suffix_of_a_name_until_they_all_differ() {
        assert_eq!(
            of(&[
                ("ana", "x2doK"),
                ("ana", "y2doK"),
                ("ana", "zzvkt"),
                ("work", "q2doK"),
            ]),
            [
                Some("x2doK".to_owned()),
                Some("y2doK".to_owned()),
                Some("zzvkt".to_owned()),
                None
            ]
        );
    }

    #[test]
    fn it_tells_suffixes_apart_by_case() {
        assert_eq!(
            of(&[("ana", "vktH"), ("ana", "VKTh")]),
            [Some("vktH".to_owned()), Some("VKTh".to_owned())]
        );
    }

    #[test]
    fn it_gives_nothing_for_no_entry() {
        assert_eq!(suffixes([]), Vec::<Option<String>>::new());
    }

    /// The suffixes of `entries`, each the names of an entry and the end of
    /// its DID, across the names.
    fn across(entries: &[(&[&str], &str)]) -> Vec<AcrossNames> {
        let dids: Vec<(Vec<&str>, String)> = entries
            .iter()
            .map(|&(names, end)| (names.to_vec(), did(end)))
            .collect();
        let entries: Vec<(Vec<&str>, &str)> = dids
            .iter()
            .map(|(names, did)| (names.clone(), did.as_str()))
            .collect();
        across_names(&entries)
    }

    fn named(suffix: Option<&str>, shared: &[bool]) -> AcrossNames {
        AcrossNames {
            suffix: suffix.map(str::to_owned),
            shared: shared.to_vec(),
        }
    }

    /// PDD005, "Suffixes across names": two spaces named `Book club` each get
    /// a suffix, and `Garden`, whose petname is `Patch`, gets none.
    #[test]
    fn it_gives_a_suffix_to_each_space_that_shares_the_text_of_a_name() {
        assert_eq!(
            across(&[
                (&["Book club"], "bTQ2"),
                (&["Book club"], "9nRa"),
                (&["Patch", "Garden"], "eUo9"),
            ]),
            [
                named(Some("bTQ2"), &[true]),
                named(Some("9nRa"), &[true]),
                named(None, &[false, false]),
            ]
        );
    }

    /// PDD005, "Suffixes": a petname that is the space name of another space
    /// needs a suffix, and the space name beside it does not.
    #[test]
    fn it_counts_a_petname_and_a_space_name_of_one_text_as_one_text() {
        assert_eq!(
            across(&[(&["Book club"], "bTQ2"), (&["Book club", "Garden"], "9nRa")]),
            [
                named(Some("bTQ2"), &[true]),
                named(Some("9nRa"), &[true, false])
            ]
        );
        assert_eq!(
            across(&[(&["Book club", "Book club"], "bTQ2")]),
            [named(None, &[false, false])]
        );
    }

    /// A space that shares two texts takes the longer length of the two, so
    /// that its suffix tells it apart from each space that shares a text.
    #[test]
    fn it_takes_the_longest_length_of_the_texts_that_an_entry_shares() {
        assert_eq!(
            across(&[
                (&["Mum", "Book club"], "x2doK"),
                (&["Book club"], "y2doK"),
                (&["Mum"], "zzvkt"),
            ]),
            [
                named(Some("x2doK"), &[true, true]),
                named(Some("y2doK"), &[true]),
                named(Some("zvkt"), &[true]),
            ]
        );
    }

    #[test]
    fn it_takes_the_last_characters_of_a_did() {
        assert_eq!(end("did:key:z6Mkabc", 4), "kabc");
        assert_eq!(end("abc", 4), "abc");
    }
}
