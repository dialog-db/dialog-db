//! The formulas of Sapling: pure functions that the rules of its guest
//! kernel call, for the rules of Sapling that plain rules cannot express.
//! Each reads only its inputs, and the rules of `dialog-sapling` compute it.
//! A formula projects nothing on an input that it does not accept, so each
//! formula that can fail has a second formula for the problem, which a rule
//! that refuses a request reads.
//!
//! | Formula                    | Input        | Output                                         |
//! | -------------------------- | ------------ | ---------------------------------------------- |
//! | `sapling/name`             | `of`         | `is`: the name in its normal form              |
//! | `sapling/name-problem`     | `of`         | `rule`, `message`: the first rule it breaks    |
//! | `sapling/cause-key`        | `of`         | `is`: the cause key                            |
//! | `sapling/selector`         | `of`         | `kind`, `name`, `end`                          |
//! | `sapling/selector-problem` | `of`, `thing`| `rule`, `message`                              |
//! | `sapling/did-url`          | `of`         | `did`, `name`                                  |
//! | `sapling/did-url-problem`  | `of`         | `kind`, `rule`, `message`                      |
//! | `sapling/invite`           | `of`         | `space`, `sharer`, `peer`, `name`              |

use dialog_sapling::did_url::{self, SuggestedName};
use dialog_sapling::selector::Selector as Parsed;
use dialog_sapling::{cause, did, invite, name};

use crate::formula::Input;
use crate::{Entity, Formula};

/// A name in its normal form, if it passes the rules for names, 1 to 8.
/// Registered as `sapling/name`.
#[derive(Debug, Clone, Formula)]
pub struct Name {
    /// The text of the name, as given
    pub of: String,
    /// The name in Unicode NFC
    #[output]
    pub is: String,
}

impl Name {
    /// The normal form of `of`, or nothing if it breaks a rule
    pub fn compute(input: Input<Self>) -> Vec<Self> {
        match name::check(&input.of) {
            Ok(is) => vec![Name {
                of: input.of.clone(),
                is,
            }],
            Err(_) => Vec::new(),
        }
    }
}

/// The first rule for names that a text breaks: its number, from 1 to 8, and
/// its message. Registered as `sapling/name-problem`.
#[derive(Debug, Clone, Formula)]
pub struct NameProblem {
    /// The text of the name, as given
    pub of: String,
    /// The number of the rule
    #[output]
    pub rule: u32,
    /// The message of the rule
    #[output]
    pub message: String,
}

impl NameProblem {
    /// The rule that `of` breaks, or nothing if it is a valid name
    pub fn compute(input: Input<Self>) -> Vec<Self> {
        match name::check(&input.of) {
            Ok(_) => Vec::new(),
            Err(broken) => vec![NameProblem {
                of: input.of.clone(),
                rule: u32::from(broken.rule()),
                message: broken.to_string(),
            }],
        }
    }
}

/// The cause key of a create, if the text is one. Registered as
/// `sapling/cause-key`.
#[derive(Debug, Clone, Formula)]
pub struct CauseKey {
    /// The text of the key
    pub of: String,
    /// The key
    #[output]
    pub is: String,
}

impl CauseKey {
    /// `of`, if it is a cause key
    pub fn compute(input: Input<Self>) -> Vec<Self> {
        if cause::is_key(&input.of) {
            vec![CauseKey {
                of: input.of.clone(),
                is: input.of.clone(),
            }]
        } else {
            Vec::new()
        }
    }
}

/// A selector, parsed: its `kind` (`did`, `suffixed`, or `name`), its name
/// in its normal form, and its end: the suffix, or the DID. A part that the
/// kind has not is empty. Registered as `sapling/selector`.
#[derive(Debug, Clone, Formula)]
pub struct Selector {
    /// The text of the selector
    pub of: String,
    /// `did`, `suffixed`, or `name`
    #[output]
    pub kind: String,
    /// The name, or empty for a DID
    #[output]
    pub name: String,
    /// The suffix, the DID, or empty for a name
    #[output]
    pub end: String,
}

impl Selector {
    /// The parts of `of`, or nothing if it is not a valid selector
    pub fn compute(input: Input<Self>) -> Vec<Self> {
        let (kind, name, end) = match Parsed::parse(&input.of) {
            Ok(Parsed::Did(did)) => ("did", String::new(), did),
            Ok(Parsed::Suffixed { name, suffix }) => ("suffixed", name, suffix),
            Ok(Parsed::Name(name)) => ("name", name, String::new()),
            Err(_) => return Vec::new(),
        };
        vec![Selector {
            of: input.of.clone(),
            kind: kind.to_string(),
            name,
            end,
        }]
    }
}

/// Why a text is not a valid selector of a `thing`, such as `persona` or
/// `space`: the number of the rule of selectors that it breaks, 1, 3, or 4,
/// and its message. Registered as `sapling/selector-problem`.
#[derive(Debug, Clone, Formula)]
pub struct SelectorProblem {
    /// The text of the selector
    pub of: String,
    /// What the selector names, for the message
    pub thing: String,
    /// The number of the rule
    #[output]
    pub rule: u32,
    /// The message of the rule
    #[output]
    pub message: String,
}

impl SelectorProblem {
    /// The rule that `of` breaks, or nothing if it is a valid selector
    pub fn compute(input: Input<Self>) -> Vec<Self> {
        match Parsed::parse(&input.of) {
            Ok(_) => Vec::new(),
            Err(invalid) => vec![SelectorProblem {
                of: input.of.clone(),
                thing: input.thing.clone(),
                rule: u32::from(invalid.rule()),
                message: invalid.message(&input.thing),
            }],
        }
    }
}

/// A DID URL: the `did:key` of a key, and the name that it suggests in its
/// normal form, or empty when it suggests no valid name. Registered as
/// `sapling/did-url`.
#[derive(Debug, Clone, Formula)]
pub struct DidUrl {
    /// The text of the DID URL
    pub of: String,
    /// The DID, with no query
    #[output]
    pub did: Entity,
    /// The suggested name, or empty
    #[output]
    pub name: String,
}

impl DidUrl {
    /// The DID and the suggested name of `of`, or nothing if its DID is not
    /// the `did:key` of a key
    pub fn compute(input: Input<Self>) -> Vec<Self> {
        let url = did_url::parse(&input.of);
        let Some(did) = did::parse_key(url.did).and_then(|did| did.as_str().parse().ok()) else {
            return Vec::new();
        };
        let name = match url.name {
            Some(SuggestedName::Text(suggested)) => name::check(&suggested).unwrap_or_default(),
            Some(SuggestedName::NotUtf8) | None => String::new(),
        };
        vec![DidUrl {
            of: input.of.clone(),
            did,
            name,
        }]
    }
}

/// Why a text is not a DID URL with a valid suggested name. The `kind` is
/// one of:
///
/// - `not-did`: the text does not start with `did:`.
/// - `did`: its DID is not the `did:key` of a key, or its suggested name is
///   not UTF-8 once decoded.
/// - `no-name`: it suggests no name.
/// - `name`: its suggested name breaks the rule `rule` for names.
///
/// The `rule` is 0 for each kind but `name`. Registered as
/// `sapling/did-url-problem`.
#[derive(Debug, Clone, Formula)]
pub struct DidUrlProblem {
    /// The text of the DID URL
    pub of: String,
    /// `not-did`, `did`, `no-name`, or `name`
    #[output]
    pub kind: String,
    /// The rule for names that the suggested name breaks, or 0
    #[output]
    pub rule: u32,
    /// The message
    #[output]
    pub message: String,
}

impl DidUrlProblem {
    /// The problem of `of`, or nothing if it has none
    pub fn compute(input: Input<Self>) -> Vec<Self> {
        let problem = |kind: &str, rule: u32, message: String| {
            vec![DidUrlProblem {
                of: input.of.clone(),
                kind: kind.to_string(),
                rule,
                message,
            }]
        };
        let text = &input.of;
        if !text.starts_with("did:") {
            return problem("not-did", 0, format!("{text:?} is not a DID"));
        }
        let invalid = || {
            format!(
                "{text:?} is not the did:key of a persona, such as did:key:z6Mk…, with an \
                 optional suggested name, as did:key:z6Mk…?name=Ben"
            )
        };
        let url = did_url::parse(text);
        if did::parse_key(url.did).is_none() {
            return problem("did", 0, invalid());
        }
        match url.name {
            None => problem("no-name", 0, format!("{text:?} carries no suggested name")),
            Some(SuggestedName::NotUtf8) => problem("did", 0, invalid()),
            Some(SuggestedName::Text(suggested)) => match name::check(&suggested) {
                Ok(_) => Vec::new(),
                Err(broken) => problem("name", u32::from(broken.rule()), broken.to_string()),
            },
        }
    }
}

/// An invite: its space, its sharer (the issuer of the last link of its
/// chain), the device peer of the sharer, and the name that it suggests, if
/// that is a name in its normal form, or empty. Registered as
/// `sapling/invite`.
#[derive(Debug, Clone, Formula)]
pub struct Invite {
    /// The text of the invite
    pub of: String,
    /// The DID of the space
    #[output]
    pub space: Entity,
    /// The DID of the sharer
    #[output]
    pub sharer: Entity,
    /// The DID of the device peer of the sharer
    #[output]
    pub peer: Entity,
    /// The suggested name, or empty
    #[output]
    pub name: String,
}

impl Invite {
    /// The fields of the invite `of`, or nothing if it does not decode
    pub fn compute(input: Input<Self>) -> Vec<Self> {
        let Ok(decoded) = invite::Invite::decode(&input.of) else {
            return Vec::new();
        };
        let entity = |did: &did::Did| did.as_str().parse::<Entity>().ok();
        let (Some(space), Some(sharer), Some(peer)) = (
            entity(&decoded.space),
            decoded.sharer().and_then(entity),
            entity(&decoded.peer),
        ) else {
            return Vec::new();
        };
        let name = decoded
            .name
            .filter(|suggested| name::check(suggested).is_ok_and(|checked| checked == *suggested))
            .unwrap_or_default();
        vec![Invite {
            of: input.of.clone(),
            space,
            sharer,
            peer,
            name,
        }]
    }
}

#[cfg(test)]
mod tests {
    use dialog_artifacts::Value;
    use dialog_peer::helpers::{test_repo, test_session_with_peer};
    use dialog_sapling::invite::helpers::invite as test_invite;
    use dialog_sapling::suffix::{AcrossNames, across_names};

    use super::*;
    use crate::concept::descriptor::ConceptDescriptor;
    use crate::concept::query::ConceptQuery;
    use crate::formula::query::FormulaQuery;
    use crate::query::Application as _;
    use crate::rule::DeductiveRuleDescriptor;
    use crate::selection::{Match, Selection as _};
    use crate::session::RuleRegistry;
    use crate::source::test::TestEnv;
    use crate::{EvaluationError, Parameters, Term};

    /// The outputs of the formula `name` on the inputs `inputs`, as a query
    /// runs it: one map of output to value for each row that it projects.
    fn run(name: &str, inputs: &[(&str, Value)], outputs: &[&str]) -> Vec<Vec<Value>> {
        let mut terms = serde_json::Map::new();
        for (input, _) in inputs {
            terms.insert(
                (*input).to_string(),
                serde_json::json!({ "?": { "name": format!("in-{input}") } }),
            );
        }
        for output in outputs {
            terms.insert(
                (*output).to_string(),
                serde_json::json!({ "?": { "name": format!("out-{output}") } }),
            );
        }
        let formula: FormulaQuery =
            serde_json::from_value(serde_json::json!({ "assert": name, "where": terms }))
                .expect("a formula of Sapling");
        let mut row = Match::new();
        for (input, value) in inputs {
            row.bind(&Term::var(format!("in-{input}")), value.clone())
                .expect("an input binds");
        }
        formula
            .compute(row)
            .expect("the formula runs")
            .iter()
            .map(|row| {
                outputs
                    .iter()
                    .map(|output| {
                        row.lookup(&Term::var(format!("out-{output}")))
                            .and_then(|binding| binding.content())
                            .expect("each output is bound")
                    })
                    .collect()
            })
            .collect()
    }

    fn text(value: &str) -> Value {
        Value::String(value.to_string())
    }

    fn entity(value: &str) -> Value {
        Value::Entity(value.parse().expect("an entity"))
    }

    const DID: &str = "did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK";

    /// The cases of the rules for names: a text, and the rule that it
    /// breaks, or none.
    const NAMES: &[(&str, Option<u32>)] = &[
        ("ana", None),
        ("Book club", None),
        ("", Some(1)),
        (
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            Some(2),
        ),
        ("a\u{7}b", Some(3)),
        ("a\u{AD}b", Some(3)),
        ("a\u{200B}b", Some(3)),
        ("a\u{115F}b", Some(4)),
        ("a\u{A0}b", Some(5)),
        (" ana", Some(6)),
        ("ana ", Some(6)),
        ("a#b", Some(7)),
        ("did:ana", Some(8)),
    ];

    /// `sapling/name` gives the normal form of each name that passes the
    /// rules, and `sapling/name-problem` the first rule that each other
    /// text breaks, with its message: as `name::check` says.
    #[dialog_common::test]
    fn it_agrees_with_the_rules_for_names() {
        for (case, rule) in NAMES {
            let named = run("sapling/name", &[("of", text(case))], &["is"]);
            let problem = run(
                "sapling/name-problem",
                &[("of", text(case))],
                &["rule", "message"],
            );
            match name::check(case) {
                Ok(normal) => {
                    assert_eq!(*rule, None, "{case:?}");
                    assert_eq!(named, vec![vec![text(&normal)]], "{case:?}");
                    assert!(problem.is_empty(), "{case:?}");
                }
                Err(broken) => {
                    assert_eq!(*rule, Some(u32::from(broken.rule())), "{case:?}");
                    assert!(named.is_empty(), "{case:?}");
                    assert_eq!(
                        problem,
                        vec![vec![
                            Value::UnsignedInt(u128::from(broken.rule())),
                            text(&broken.to_string()),
                        ]],
                        "{case:?}"
                    );
                }
            }
        }
        // NFC: the decomposed `é` is the composed one.
        assert_eq!(
            run("sapling/name", &[("of", text("e\u{301}"))], &["is"]),
            vec![vec![text("\u{E9}")]]
        );
    }

    #[dialog_common::test]
    fn it_takes_a_cause_key_alone() {
        assert_eq!(
            run("sapling/cause-key", &[("of", text("0f3a-b_C"))], &["is"]),
            vec![vec![text("0f3a-b_C")]]
        );
        assert!(run("sapling/cause-key", &[("of", text("has space"))], &["is"]).is_empty());
    }

    /// `sapling/selector` parses as `Selector::parse` does, and
    /// `sapling/selector-problem` gives the rule that `Selector::parse`
    /// refuses.
    #[dialog_common::test]
    fn it_agrees_with_the_grammar_of_a_selector() {
        let cases: &[(&str, &[&str])] = &[
            ("did:x#ab", &["did", "", "did:x#ab"]),
            ("did:", &["did", "", "did:"]),
            ("ana#2doK#x", &["suffixed", "ana", "2doK#x"]),
            ("ana did:x", &["name", "ana did:x", ""]),
            (
                "ana#\u{E9}\u{E9}\u{E9}\u{E9}",
                &["suffixed", "ana", "\u{E9}\u{E9}\u{E9}\u{E9}"],
            ),
            ("", &[]),
            ("#abcd", &[]),
            ("ana#\u{E9}\u{E9}\u{E9}", &[]),
            (" ana", &[]),
        ];
        for (case, parts) in cases {
            let parsed = run(
                "sapling/selector",
                &[("of", text(case))],
                &["kind", "name", "end"],
            );
            let problem = run(
                "sapling/selector-problem",
                &[("of", text(case)), ("thing", text("space"))],
                &["rule", "message"],
            );
            match Parsed::parse(case) {
                Ok(_) => {
                    let expected: Vec<Value> = parts.iter().map(|part| text(part)).collect();
                    assert_eq!(parsed, vec![expected], "{case:?}");
                    assert!(problem.is_empty(), "{case:?}");
                }
                Err(invalid) => {
                    assert!(parts.is_empty(), "{case:?}");
                    assert!(parsed.is_empty(), "{case:?}");
                    assert_eq!(
                        problem,
                        vec![vec![
                            Value::UnsignedInt(u128::from(invalid.rule())),
                            text(&invalid.message("space")),
                        ]],
                        "{case:?}"
                    );
                }
            }
        }
    }

    #[dialog_common::test]
    fn it_reads_a_did_url_and_its_problem() {
        let url = |query: &str| format!("{DID}{query}");
        assert_eq!(
            run(
                "sapling/did-url",
                &[("of", text(&url("?name=Ben%20B.")))],
                &["did", "name"]
            ),
            vec![vec![entity(DID), text("Ben B.")]]
        );
        assert_eq!(
            run("sapling/did-url", &[("of", text(DID))], &["did", "name"]),
            vec![vec![entity(DID), text("")]],
            "a DID that suggests no name"
        );
        assert!(
            run(
                "sapling/did-url",
                &[("of", text("did:key:z6Mk"))],
                &["did", "name"]
            )
            .is_empty(),
            "a DID that is no key"
        );
        assert!(
            run(
                "sapling/did-url-problem",
                &[("of", text(&url("?name=Ben")))],
                &["kind", "rule", "message"]
            )
            .is_empty()
        );
        for (case, kind, rule) in [
            ("ana".to_string(), "not-did", 0),
            ("did:key:z6Mk".to_string(), "did", 0),
            (url("?name=%FF"), "did", 0),
            (DID.to_string(), "no-name", 0),
            (url("?name=%20Ben"), "name", 6),
        ] {
            let problem = run(
                "sapling/did-url-problem",
                &[("of", text(&case))],
                &["kind", "rule", "message"],
            );
            assert_eq!(problem.len(), 1, "{case:?}");
            assert_eq!(
                problem[0][..2],
                [text(kind), Value::UnsignedInt(rule)],
                "{case:?}"
            );
        }
    }

    /// `sapling/invite` decodes an invite that `Invite::encode` makes, and
    /// gives its space, its sharer, its peer, and its name.
    #[dialog_common::test]
    async fn it_decodes_an_invite() {
        let (encoded, sharer) = test_invite(Some("Ana")).await;
        let decoded = run(
            "sapling/invite",
            &[("of", text(&encoded.encode().expect("the invite encodes")))],
            &["space", "sharer", "peer", "name"],
        );
        assert_eq!(
            decoded,
            vec![vec![
                entity(encoded.space.as_str()),
                entity(sharer.as_str()),
                entity(encoded.peer.as_str()),
                text("Ana"),
            ]]
        );
        assert!(
            run(
                "sapling/invite",
                &[("of", text("sapling-invite-2.oA"))],
                &["space", "sharer", "peer", "name"]
            )
            .is_empty()
        );
    }

    #[dialog_common::test]
    fn it_cuts_a_suffix_and_counts_a_common_one() {
        assert_eq!(
            run(
                "text/suffix",
                &[
                    ("of", text("did:key:z6Mkabc\u{E9}")),
                    ("length", Value::UnsignedInt(4))
                ],
                &["is"]
            ),
            vec![vec![text("abc\u{E9}")]]
        );
        assert_eq!(
            run(
                "text/suffix",
                &[("of", text("abc")), ("length", Value::UnsignedInt(4))],
                &["is"]
            ),
            vec![vec![text("abc")]]
        );
        assert_eq!(
            run(
                "text/common-suffix-length",
                &[("of", text("x2doK")), ("with", text("y2doK"))],
                &["is"]
            ),
            vec![vec![Value::UnsignedInt(4)]]
        );
        assert_eq!(
            run(
                "text/common-suffix-length",
                &[("of", text("abc")), ("with", text("abd"))],
                &["is"]
            ),
            vec![vec![Value::UnsignedInt(0)]]
        );
    }

    /// The rules of a library: a rule of the field `field` of
    /// `test.out/<field>` for each output of `formula`, over the text that
    /// `test.in/text` holds.
    fn rule_of(
        formula: &str,
        outputs: &[(&str, &str)],
        extra: serde_json::Value,
    ) -> serde_json::Value {
        let mut with = serde_json::Map::new();
        let mut terms = serde_json::Map::new();
        terms.insert("of".into(), serde_json::json!({ "?": { "name": "text" } }));
        for (output, kind) in outputs {
            with.insert(
                (*output).to_string(),
                serde_json::json!({ "the": format!("test.out/{output}"), "as": kind }),
            );
            terms.insert(
                (*output).to_string(),
                serde_json::json!({ "?": { "name": output } }),
            );
        }
        if let serde_json::Value::Object(extra) = extra {
            terms.extend(extra);
        }
        serde_json::json!({
            "deduce": { "with": with },
            "when": [
                {
                    "assert": { "with": { "text": { "the": "test.in/text", "as": "text:" } } },
                    "where": {
                        "this": { "?": { "name": "this" } },
                        "text": { "?": { "name": "text" } }
                    }
                },
                { "assert": formula, "where": terms }
            ]
        })
    }

    /// A formula, the outputs that a rule derives with their types, the
    /// inputs past `of`, a text that it accepts, one that it does not, and
    /// the outputs of the one that it accepts.
    type Case = (
        &'static str,
        Vec<(&'static str, &'static str)>,
        serde_json::Value,
        String,
        String,
        Vec<Value>,
    );

    /// Each formula of Sapling runs in a rule: a rule from JSON derives its
    /// outputs from a stored text that it accepts, and nothing from one
    /// that it does not.
    #[dialog_common::test]
    async fn it_runs_each_formula_in_a_rule() -> anyhow::Result<()> {
        let (invite, sharer) = test_invite(Some("Ana")).await;
        let cases: Vec<Case> = vec![
            (
                "sapling/name",
                vec![("is", "text:")],
                serde_json::json!({}),
                "e\u{301}".into(),
                " ana".into(),
                vec![text("\u{E9}")],
            ),
            (
                "sapling/name-problem",
                vec![("rule", "natural:"), ("message", "text:")],
                serde_json::json!({}),
                "a#b".into(),
                "ana".into(),
                vec![Value::UnsignedInt(7), text("the name must not contain `#`")],
            ),
            (
                "sapling/cause-key",
                vec![("is", "text:")],
                serde_json::json!({}),
                "k-1".into(),
                "k 1".into(),
                vec![text("k-1")],
            ),
            (
                "sapling/selector",
                vec![("kind", "text:"), ("name", "text:"), ("end", "text:")],
                serde_json::json!({}),
                "ana#2doK".into(),
                "".into(),
                vec![text("suffixed"), text("ana"), text("2doK")],
            ),
            (
                "sapling/selector-problem",
                vec![("rule", "natural:"), ("message", "text:")],
                serde_json::json!({ "thing": "persona" }),
                "".into(),
                "ana".into(),
                vec![Value::UnsignedInt(1), text("a selector must not be empty")],
            ),
            (
                "sapling/did-url",
                vec![("did", "entity:"), ("name", "text:")],
                serde_json::json!({}),
                format!("{DID}?name=Ben"),
                "ana".into(),
                vec![entity(DID), text("Ben")],
            ),
            (
                "sapling/did-url-problem",
                vec![
                    ("kind", "text:"),
                    ("rule", "natural:"),
                    ("message", "text:"),
                ],
                serde_json::json!({}),
                DID.into(),
                format!("{DID}?name=Ben"),
                vec![
                    text("no-name"),
                    Value::UnsignedInt(0),
                    text(&format!("{DID:?} carries no suggested name")),
                ],
            ),
            (
                "sapling/invite",
                vec![
                    ("space", "entity:"),
                    ("sharer", "entity:"),
                    ("peer", "entity:"),
                    ("name", "text:"),
                ],
                serde_json::json!({}),
                invite.encode()?,
                "sapling-invite-2.oA".into(),
                vec![
                    entity(invite.space.as_str()),
                    entity(sharer.as_str()),
                    entity(invite.peer.as_str()),
                    text("Ana"),
                ],
            ),
        ];
        for (formula, outputs, extra, accepted, refused, expected) in cases {
            let (operator, profile) = test_session_with_peer().await;
            let repo = test_repo(&operator, &profile).await;
            let branch = repo.branch("main").open().perform(&operator).await?;
            let (yes, no): (Entity, Entity) = ("in:yes".parse()?, "in:no".parse()?);
            branch
                .transaction()
                .assert(crate::the!("test.in/text").of(yes.clone()).is(accepted))
                .assert(crate::the!("test.in/text").of(no.clone()).is(refused))
                .commit()
                .publish()
                .perform(&operator)
                .await?;
            let descriptor: DeductiveRuleDescriptor =
                serde_json::from_value(rule_of(formula, &outputs, extra))?;
            let mut registry = RuleRegistry::new();
            registry.register(descriptor.compile()?)?;
            let source = TestEnv::new(&branch, &operator, registry);
            let mut with = serde_json::Map::new();
            let mut terms = Parameters::new();
            terms.insert("this".into(), Term::var("this"));
            for (output, kind) in &outputs {
                with.insert(
                    (*output).to_string(),
                    serde_json::json!({ "the": format!("test.out/{output}"), "as": kind }),
                );
                terms.insert((*output).to_string(), Term::var(*output));
            }
            let predicate: ConceptDescriptor =
                serde_json::from_value(serde_json::json!({ "with": with }))?;
            let rows = ConceptQuery { terms, predicate }
                .evaluate(Match::new().seed(), &source)
                .try_vec()
                .await?;
            let found = rows
                .iter()
                .map(|row| {
                    let this = row.lookup(&Term::var("this"))?.content()?;
                    let values = outputs
                        .iter()
                        .map(|(output, _)| row.lookup(&Term::var(*output))?.content())
                        .collect::<Result<Vec<_>, EvaluationError>>()?;
                    Ok((this, values))
                })
                .collect::<Result<Vec<_>, EvaluationError>>()?;
            assert_eq!(
                found,
                vec![(Value::Entity(yes), expected)],
                "{formula}: the accepted text alone"
            );
        }
        Ok(())
    }

    /// The rules of the suffixes of a library: each name of a row is an
    /// entity of `test.name/`, with its row and its text, and each row has a
    /// `test.row/did`. A row gets a candidate of `test.row/shared` for each
    /// pair of rows with a text of its own, and 3 if it shares one; the
    /// pick `max` reads the greatest, and `text/suffix` cuts its DID to one
    /// more. A text of a row that another row has is `test.row/suffixed`.
    fn suffix_rules() -> anyhow::Result<Vec<crate::DeductiveRule>> {
        let pair = serde_json::json!([
            { "assert": { "with": {
                "row": { "the": "test.name/row", "as": "entity:" },
                "text": { "the": "test.name/text", "as": "text:" } } },
              "where": { "this": { "?": { "name": "own" } }, "row": { "?": { "name": "this" } }, "text": { "?": { "name": "text" } } } },
            { "assert": { "with": {
                "row": { "the": "test.name/row", "as": "entity:" },
                "text": { "the": "test.name/text", "as": "text:" } } },
              "where": { "this": { "?": { "name": "first" } }, "row": { "?": { "name": "a" } }, "text": { "?": { "name": "text" } } } },
            { "assert": { "with": {
                "row": { "the": "test.name/row", "as": "entity:" },
                "text": { "the": "test.name/text", "as": "text:" } } },
              "where": { "this": { "?": { "name": "second" } }, "row": { "?": { "name": "b" } }, "text": { "?": { "name": "text" } } } },
            { "assert": { "with": { "did": { "the": "test.row/did", "as": "text:" } } },
              "where": { "this": { "?": { "name": "a" } }, "did": { "?": { "name": "first-did" } } } },
            { "assert": { "with": { "did": { "the": "test.row/did", "as": "text:" } } },
              "where": { "this": { "?": { "name": "b" } }, "did": { "?": { "name": "second-did" } } } },
            { "assert": "<", "where": { "of": { "?": { "name": "first-did" } }, "with": { "?": { "name": "second-did" } } } }
        ]);
        let with = |extra: serde_json::Value| -> serde_json::Value {
            let mut when = pair.as_array().expect("a list").clone();
            when.push(extra);
            serde_json::Value::Array(when)
        };
        let rules = serde_json::json!([
            {
                "deduce": { "with": { "shared": { "the": "test.row/shared", "as": "natural:", "pick": "all:" } } },
                "when": with(serde_json::json!({ "assert": "text/common-suffix-length", "where": {
                    "of": { "?": { "name": "first-did" } },
                    "with": { "?": { "name": "second-did" } },
                    "is": { "?": { "name": "shared" } } } }))
            },
            {
                "deduce": { "with": { "shared": { "the": "test.row/shared", "as": "natural:", "pick": "all:" } } },
                "when": with(serde_json::json!({ "assert": "==", "where": { "this": { "?": { "name": "shared" } }, "is": 3 } }))
            },
            {
                "deduce": { "with": { "suffixed": { "the": "test.row/suffixed", "as": "text:", "pick": "all:" } } },
                "when": with(serde_json::json!({ "assert": "==", "where": { "this": { "?": { "name": "suffixed" } }, "is": { "?": { "name": "text" } } } }))
            },
            {
                "deduce": { "with": { "suffix": { "the": "test.row/suffix", "as": "text:" } } },
                "when": [
                    { "assert": { "with": { "shared": { "the": "test.row/shared", "as": "natural:", "pick": "max:" } } },
                      "where": { "this": { "?": { "name": "this" } }, "shared": { "?": { "name": "shared" } } } },
                    { "assert": { "with": { "did": { "the": "test.row/did", "as": "text:" } } },
                      "where": { "this": { "?": { "name": "this" } }, "did": { "?": { "name": "did" } } } },
                    { "assert": "math/sum", "where": { "of": { "?": { "name": "shared" } }, "with": 1, "is": { "?": { "name": "length" } } } },
                    { "assert": "text/suffix", "where": { "of": { "?": { "name": "did" } }, "length": { "?": { "name": "length" } }, "is": { "?": { "name": "suffix" } } } }
                ]
            }
        ]);
        let descriptors: Vec<DeductiveRuleDescriptor> = serde_json::from_value(rules)?;
        Ok(descriptors
            .into_iter()
            .map(|descriptor| descriptor.compile())
            .collect::<Result<_, _>>()?)
    }

    /// The DID that ends with `end`, as the tests of `suffix` make it.
    fn did_ending(end: &str) -> String {
        format!("did:key:z6Mk{}{end}", "a".repeat(44 - end.len()))
    }

    /// The suffix and the suffixed texts of each row of `entries`, each
    /// the names of a row and the end of its DID, as the rules derive them.
    async fn suffixes_by_rules(entries: &[(&[&str], &str)]) -> anyhow::Result<Vec<AcrossNames>> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let mut transaction = branch.transaction();
        for (index, (names, end)) in entries.iter().enumerate() {
            let row: Entity = format!("row:{index}").parse()?;
            transaction = transaction.assert(
                crate::the!("test.row/did")
                    .of(row.clone())
                    .is(did_ending(end)),
            );
            for (position, name) in names.iter().enumerate() {
                let own: Entity = format!("name:{index}-{position}").parse()?;
                transaction = transaction
                    .assert(crate::the!("test.name/row").of(own.clone()).is(row.clone()))
                    .assert(crate::the!("test.name/text").of(own).is(name.to_string()));
            }
        }
        transaction.commit().publish().perform(&operator).await?;
        let mut registry = RuleRegistry::new();
        for rule in suffix_rules()? {
            registry.register(rule)?;
        }
        let source = TestEnv::new(&branch, &operator, registry);
        let read = |the: &str, pick: &str| -> anyhow::Result<ConceptQuery> {
            let predicate: ConceptDescriptor =
                serde_json::from_value(serde_json::json!({ "with": {
                    "value": { "the": the, "as": "text:", "pick": pick }
                }}))?;
            let mut terms = Parameters::new();
            terms.insert("this".into(), Term::var("this"));
            terms.insert("value".into(), Term::var("value"));
            Ok(ConceptQuery { terms, predicate })
        };
        let mut found = Vec::new();
        for (the, pick) in [("test.row/suffix", "last:"), ("test.row/suffixed", "all:")] {
            let rows = read(the, pick)?
                .evaluate(Match::new().seed(), &source)
                .try_vec()
                .await?;
            let mut pairs = Vec::new();
            for row in rows {
                let this = row.lookup(&Term::var("this"))?.content()?;
                let value = row.lookup(&Term::var("value"))?.content()?;
                pairs.push((this, value));
            }
            found.push(pairs);
        }
        Ok(entries
            .iter()
            .enumerate()
            .map(|(index, (names, _))| {
                let row = entity(&format!("row:{index}"));
                let suffix = found[0]
                    .iter()
                    .find(|(this, _)| *this == row)
                    .map(|(_, value)| match value {
                        Value::String(suffix) => suffix.clone(),
                        other => panic!("a suffix is text: {other:?}"),
                    });
                let shared = names
                    .iter()
                    .map(|name| found[1].contains(&(row.clone(), text(name))))
                    .collect();
                AcrossNames { suffix, shared }
            })
            .collect())
    }

    /// The rules of the suffixes give what `across_names` gives, on each
    /// of its tests and on the tests of `suffixes`.
    #[dialog_common::test]
    async fn it_gives_the_suffixes_of_across_names_by_rules() -> anyhow::Result<()> {
        let cases: &[&[(&[&str], &str)]] = &[
            &[(&["ana"], "2doK"), (&["work"], "vktH")],
            &[(&["ana"], "2doK"), (&["work"], "VJGx"), (&["ana"], "vktH")],
            &[
                (&["ana"], "x2doK"),
                (&["ana"], "y2doK"),
                (&["ana"], "zzvkt"),
                (&["work"], "q2doK"),
            ],
            &[(&["ana"], "vktH"), (&["ana"], "VKTh")],
            &[
                (&["Book club"], "bTQ2"),
                (&["Book club"], "9nRa"),
                (&["Patch", "Garden"], "eUo9"),
            ],
            &[(&["Book club"], "bTQ2"), (&["Book club", "Garden"], "9nRa")],
            &[(&["Book club", "Book club"], "bTQ2")],
            &[
                (&["Mum", "Book club"], "x2doK"),
                (&["Book club"], "y2doK"),
                (&["Mum"], "zzvkt"),
            ],
        ];
        for entries in cases {
            let dids: Vec<(Vec<&str>, String)> = entries
                .iter()
                .map(|(names, end)| (names.to_vec(), did_ending(end)))
                .collect();
            let borrowed: Vec<(Vec<&str>, &str)> = dids
                .iter()
                .map(|(names, did)| (names.clone(), did.as_str()))
                .collect();
            assert_eq!(
                suffixes_by_rules(entries).await?,
                across_names(&borrowed),
                "{entries:?}"
            );
        }
        Ok(())
    }

    /// A build of `dialog-query` without the feature has no formula under
    /// `sapling/`; this build has each of them.
    #[dialog_common::test]
    fn it_registers_the_formulas_of_sapling_with_the_feature() {
        let parsed: Result<FormulaQuery, _> = serde_json::from_value(serde_json::json!({
            "assert": "sapling/name",
            "where": { "of": "ana", "is": { "?": { "name": "is" } } }
        }));
        assert_eq!(
            parsed.map(|formula| formula.name()).ok(),
            Some("sapling/name")
        );
    }
}
