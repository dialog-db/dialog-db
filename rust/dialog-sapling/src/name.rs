//! Persona names and space names. See PDD003, "Changes to PDD002", "Names".
//!
//! One set of rules covers both. [`check`] applies Unicode NFC normalization
//! first, then the rules, and returns the normalized name. The daemon stores
//! that form and compares names byte for byte in it, so two names that look
//! the same compare the same.
//!
//! The general categories and `Default_Ignorable_Code_Point` come from the
//! Unicode tables of `icu_properties`, so the daemon follows the Unicode
//! version that it is built with. No table is frozen here.

use std::fmt;

use icu_properties::props::{DefaultIgnorableCodePoint, GeneralCategory};
use icu_properties::{CodePointMapData, CodePointSetData};
use unicode_normalization::UnicodeNormalization;

/// The limit of rule 2, in bytes of the normalized name.
pub const NAME_MAX: usize = 64;

/// `name` in Unicode NFC, the form that the daemon stores and compares.
pub fn normalize(name: &str) -> String {
    name.nfc().collect()
}

/// Normalizes a name, and checks it against rules 1 to 8, in order. The error
/// is the first rule that the name breaks.
pub fn check(name: &str) -> Result<String, NameError> {
    let name = normalize(name);
    // 1.
    if name.is_empty() {
        return Err(NameError::Empty);
    }
    // 2.
    if name.len() > NAME_MAX {
        return Err(NameError::TooLong { len: name.len() });
    }
    // 3.
    let categories = CodePointMapData::<GeneralCategory>::new();
    for character in name.chars() {
        if let Some(category) = forbidden_category(categories.get(character)) {
            return Err(NameError::Category {
                character,
                category,
            });
        }
    }
    // 4.
    let ignorable = CodePointSetData::new::<DefaultIgnorableCodePoint>();
    if let Some(character) = name.chars().find(|&c| ignorable.contains(c)) {
        return Err(NameError::DefaultIgnorable { character });
    }
    // 5.
    if let Some(character) = name
        .chars()
        .find(|&c| c != ' ' && categories.get(c) == GeneralCategory::SpaceSeparator)
    {
        return Err(NameError::Space { character });
    }
    // 6.
    if name.starts_with(' ') || name.ends_with(' ') {
        return Err(NameError::OuterSpace);
    }
    // 7.
    if name.contains('#') {
        return Err(NameError::Hash);
    }
    // 8.
    if name.starts_with("did:") {
        return Err(NameError::DidPrefix);
    }
    Ok(name)
}

/// The short name of `category`, when rule 3 refuses it: one of `Cc`, `Cf`,
/// `Cn`, `Co`, `Cs`, `Zl`, and `Zp`.
fn forbidden_category(category: GeneralCategory) -> Option<&'static str> {
    match category {
        GeneralCategory::Control => Some("Cc"),
        GeneralCategory::Format => Some("Cf"),
        GeneralCategory::Unassigned => Some("Cn"),
        GeneralCategory::PrivateUse => Some("Co"),
        GeneralCategory::Surrogate => Some("Cs"),
        GeneralCategory::LineSeparator => Some("Zl"),
        GeneralCategory::ParagraphSeparator => Some("Zp"),
        _ => None,
    }
}

/// A name that breaks a rule. The message names the rule, so that a route can
/// put it in the message of a `400`.
#[derive(Debug, PartialEq, Eq)]
pub enum NameError {
    /// Rule 1: the name is empty.
    Empty,
    /// Rule 2: the normalized name has `len` bytes, over [`NAME_MAX`].
    TooLong { len: usize },
    /// Rule 3: the name has a character of the category `category`, one of
    /// `Cc`, `Cf`, `Cn`, `Co`, `Cs`, `Zl`, and `Zp`.
    Category {
        character: char,
        category: &'static str,
    },
    /// Rule 4: the name has a character with the property
    /// `Default_Ignorable_Code_Point`.
    DefaultIgnorable { character: char },
    /// Rule 5: the name has a `Zs` other than U+0020.
    Space { character: char },
    /// Rule 6: the name starts or ends with U+0020.
    OuterSpace,
    /// Rule 7: the name has a `#`.
    Hash,
    /// Rule 8: the name starts with `did:`.
    DidPrefix,
}

impl NameError {
    /// The number of the rule that the name breaks, from 1 to 8.
    pub fn rule(&self) -> u8 {
        match self {
            Self::Empty => 1,
            Self::TooLong { .. } => 2,
            Self::Category { .. } => 3,
            Self::DefaultIgnorable { .. } => 4,
            Self::Space { .. } => 5,
            Self::OuterSpace => 6,
            Self::Hash => 7,
            Self::DidPrefix => 8,
        }
    }

    /// The `code` of this error in the Tonk error envelope, for
    /// `sapling persona create` and `sapling space create`.
    pub const CODE: &str = "INVALID_NAME";
}

impl fmt::Display for NameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => write!(f, "the name must not be empty"),
            Self::TooLong { len } => write!(
                f,
                "the name must be {NAME_MAX} bytes or shorter after normalization, \
                 and it is {len} bytes"
            ),
            Self::Category {
                character,
                category,
            } => write!(
                f,
                "the name must not contain a character of the category Cc, Cf, \
                 Cn, Co, Cs, Zl, or Zp, and it contains U+{:04X}, of the \
                 category {category}",
                u32::from(*character)
            ),
            Self::DefaultIgnorable { character } => write!(
                f,
                "the name must not contain a default ignorable code point, and \
                 it contains U+{:04X}",
                u32::from(*character)
            ),
            Self::Space { character } => write!(
                f,
                "the name must not contain a space separator other than U+0020, \
                 and it contains U+{:04X}",
                u32::from(*character)
            ),
            Self::OuterSpace => write!(f, "the name must not start or end with a space"),
            Self::Hash => write!(f, "the name must not contain `#`"),
            Self::DidPrefix => write!(f, "the name must not start with `did:`"),
        }
    }
}

impl std::error::Error for NameError {}

#[cfg(test)]
mod tests {
    use super::{NameError, check};

    #[test]
    fn it_refuses_an_empty_name() {
        let error = check("").unwrap_err();

        assert_eq!(error, NameError::Empty);
        assert_eq!(error.to_string(), "the name must not be empty");
    }

    #[test]
    fn it_refuses_a_name_of_65_bytes() {
        let error = check(&"a".repeat(65)).unwrap_err();

        assert_eq!(error, NameError::TooLong { len: 65 });
        assert_eq!(
            error.to_string(),
            "the name must be 64 bytes or shorter after normalization, \
             and it is 65 bytes"
        );
    }

    #[test]
    fn it_refuses_a_name_of_65_bytes_in_characters_of_more_than_one_byte() {
        // `€` is 3 bytes, and `é` is 2, so the name is 65 bytes in 22
        // characters.
        let name = format!("{}\u{E9}", "\u{20AC}".repeat(21));

        assert_eq!(check(&name), Err(NameError::TooLong { len: 65 }));
    }

    #[test]
    fn it_accepts_a_name_of_64_bytes() {
        let name = "a".repeat(64);

        assert_eq!(check(&name), Ok(name));
    }

    #[test]
    fn it_accepts_a_name_of_64_bytes_in_22_characters() {
        let name = format!("{}a", "\u{20AC}".repeat(21));

        assert_eq!(check(&name), Ok(name));
    }

    #[test]
    fn it_reports_the_length_of_the_normalized_name() {
        // U+0958 is 3 bytes as typed, and it normalizes to U+0915 U+093C, 6
        // bytes. The name is 63 bytes as typed, and 126 after normalization.
        let error = check(&"\u{958}".repeat(21)).unwrap_err();

        assert_eq!(error, NameError::TooLong { len: 126 });
        assert_eq!(
            error.to_string(),
            "the name must be 64 bytes or shorter after normalization, \
             and it is 126 bytes"
        );
    }

    #[test]
    fn it_applies_the_limit_to_the_normalized_name() {
        // 32 decomposed `é` are 96 bytes, and 64 bytes after normalization.
        let decomposed = "e\u{301}".repeat(32);

        assert_eq!(check(&decomposed), Ok("\u{E9}".repeat(32)));
    }

    #[test]
    fn it_refuses_a_name_with_a_character_of_each_refused_category() {
        let cases = [
            ('\t', "Cc"),
            ('\n', "Cc"),
            ('\u{200B}', "Cf"),
            ('\u{200D}', "Cf"),
            ('\u{378}', "Cn"),
            ('\u{E000}', "Co"),
            ('\u{2028}', "Zl"),
            ('\u{2029}', "Zp"),
        ];

        for (character, category) in cases {
            assert_eq!(
                check(&format!("a{character}b")),
                Err(NameError::Category {
                    character,
                    category
                }),
                "U+{:04X}",
                u32::from(character)
            );
        }
    }

    #[test]
    fn it_names_the_category_rule_and_the_character() {
        let error = check("a\u{200B}b").unwrap_err();

        assert_eq!(
            error.to_string(),
            "the name must not contain a character of the category Cc, Cf, Cn, \
             Co, Cs, Zl, or Zp, and it contains U+200B, of the category Cf"
        );
    }

    #[test]
    fn it_refuses_a_name_with_a_default_ignorable_code_point() {
        // U+3164 is a letter (`Lo`), and U+FE0F a mark (`Mn`). Only rule 4
        // refuses them.
        for character in ['\u{3164}', '\u{FE0F}', '\u{115F}', '\u{180B}'] {
            assert_eq!(
                check(&format!("a{character}b")),
                Err(NameError::DefaultIgnorable { character }),
                "U+{:04X}",
                u32::from(character)
            );
        }
        assert_eq!(
            check("a\u{3164}b").unwrap_err().to_string(),
            "the name must not contain a default ignorable code point, and it \
             contains U+3164"
        );
    }

    #[test]
    fn it_refuses_a_name_with_a_space_separator_other_than_u0020() {
        for character in ['\u{A0}', '\u{2003}', '\u{3000}', '\u{202F}'] {
            assert_eq!(
                check(&format!("a{character}b")),
                Err(NameError::Space { character }),
                "U+{:04X}",
                u32::from(character)
            );
        }
        assert_eq!(
            check("a\u{A0}b").unwrap_err().to_string(),
            "the name must not contain a space separator other than U+0020, and \
             it contains U+00A0"
        );
    }

    #[test]
    fn it_refuses_a_name_that_starts_or_ends_with_a_space() {
        for name in [" ", "  ", " ana", "ana ", " ana "] {
            assert_eq!(check(name), Err(NameError::OuterSpace), "{name:?}");
        }
        assert_eq!(
            NameError::OuterSpace.to_string(),
            "the name must not start or end with a space"
        );
    }

    #[test]
    fn it_accepts_a_space_inside_a_name() {
        assert_eq!(check("Book club"), Ok("Book club".to_owned()));
        assert_eq!(check("a  b"), Ok("a  b".to_owned()));
    }

    #[test]
    fn it_refuses_a_name_with_a_hash() {
        for name in ["a#b", "#1 did:fun"] {
            let error = check(name).unwrap_err();

            assert_eq!(error, NameError::Hash, "{name:?}");
            assert_eq!(error.to_string(), "the name must not contain `#`");
        }
    }

    #[test]
    fn it_refuses_a_name_that_starts_with_did() {
        let error = check("did:x").unwrap_err();

        assert_eq!(error, NameError::DidPrefix);
        assert_eq!(error.to_string(), "the name must not start with `did:`");
    }

    #[test]
    fn it_accepts_a_name_with_did_after_the_start() {
        assert_eq!(check("ana did:x"), Ok("ana did:x".to_owned()));
    }

    #[test]
    fn it_names_the_first_rule_that_the_name_breaks() {
        // Over the limit, and with a control character.
        assert_eq!(
            check(&format!("{}\t", "a".repeat(64))),
            Err(NameError::TooLong { len: 65 })
        );
        // U+200B is `Cf`, and also a default ignorable code point.
        assert!(matches!(
            check("a\u{200B}b"),
            Err(NameError::Category { .. })
        ));
        // U+00A0 at the start: rule 5 before rule 6.
        assert_eq!(
            check("\u{A0}ana"),
            Err(NameError::Space {
                character: '\u{A0}'
            })
        );
        // A space at the start, and a `#`: rule 6 before rule 7.
        assert_eq!(check(" a#b"), Err(NameError::OuterSpace));
        // A `#`, and `did:` at the start: rule 7 before rule 8.
        assert_eq!(check("did:#"), Err(NameError::Hash));
    }

    #[test]
    fn it_accepts_letters_of_other_scripts() {
        for name in ["\u{E9}", "Caf\u{E9}", "\u{65E5}\u{672C}", "\u{1F333}"] {
            assert_eq!(check(name), Ok(name.to_owned()), "{name:?}");
        }
    }

    #[test]
    fn it_normalizes_both_forms_of_an_e_with_an_acute_accent_to_the_same_bytes() {
        let composed = check("\u{E9}").unwrap();
        let decomposed = check("e\u{301}").unwrap();

        assert_eq!(composed, "\u{E9}");
        assert_eq!(composed.as_bytes(), decomposed.as_bytes());
    }

    #[test]
    fn it_maps_a_name_error_to_the_code_invalid_name() {
        assert_eq!(NameError::CODE, "INVALID_NAME");
    }
}
