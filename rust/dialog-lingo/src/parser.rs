//! The parse pipeline, ported from Ubiquity Parser 2
//! (`modules/parser/new/parser.js` at `mozilla/ubiquity@11dc94e`).
//!
//! Parsing generates and scores rather than matching: every place a
//! verb could be, every way the connecting words could split the
//! arguments, every role a bare argument could move into, and every
//! reading of every argument becomes a candidate parse. Noun and verb
//! confidence then rank them. The steps keep Ubiquity's order and
//! numbers:
//!
//! 1. break words (languages without spaces)
//! 2. find verbs at the start or end of the input
//! 3. (clitics: never implemented in Ubiquity either)
//! 4. group the rest into arguments by delimiter, and try an explicit
//!    selection as the object
//! 5. substitute the selection for anaphora
//! 6. strip articles
//! 7. try bare objects in other roles ("google" → "with google")
//! 8. suggest verbs for parses without one (noun-first)
//! 9. read every argument with its noun
//! 10. fill empty arguments with defaults
//! 11. score: `m + Σ argument score · m`, where `m` carries every penalty
//!
//! Deviations from Ubiquity, each fixing a defect in its code:
//! - a verb-only input multiplied the verb score in twice (steps 4 and 8);
//!   here it is multiplied once.
//! - a default's score was halved on the shared cached object, so it
//!   decayed with every parse that used it; here it is halved once.
//! - identical parses reached through different paths were all listed;
//!   here only the best-scoring one is kept.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::grammar::{Branching, Grammar, OBJECT};
use crate::noun::{self, Suggestion, Value};
use crate::registry::{Context, Memory, Registry, Selection, Verb};

/// A ranked reading of the input.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Parse {
    /// The verb's id: the command to assert.
    pub verb: String,
    /// The verb name shown.
    pub name: String,
    /// The part of the input that matched the verb, if any. `None` when
    /// the verb was suggested from the arguments (noun-first).
    pub input: Option<String>,
    /// The score; higher is better.
    pub score: f64,
    /// One entry per argument of the verb, in the verb's order.
    pub arguments: Vec<Filled>,
    /// The parse as the user would read it.
    pub display: Vec<Segment>,
}

impl Parse {
    /// The parse as plain text: arguments in brackets.
    pub fn display_text(&self) -> String {
        self.display
            .iter()
            .map(|segment| match segment.kind {
                SegmentKind::Argument => format!("[{}]", segment.text),
                SegmentKind::Missing => format!("({})", segment.text),
                _ => segment.text.clone(),
            })
            .collect::<Vec<_>>()
            .join(" ")
    }

    /// Whether every argument has a value.
    pub fn is_complete(&self) -> bool {
        self.arguments.iter().all(|filled| filled.value.is_some())
    }
}

/// How one argument of a parse was filled.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Filled {
    /// The argument's role.
    pub role: String,
    /// The command field it fills.
    pub field: String,
    /// The reading shown.
    pub text: String,
    /// The value for the field; `None` while the argument is empty.
    pub value: Option<Value>,
    /// Whether the value came from the input or selection rather than
    /// a default.
    pub given: bool,
}

/// A piece of a displayed parse.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Segment {
    /// What the piece is.
    pub kind: SegmentKind,
    /// Its text.
    pub text: String,
    /// The role, for delimiters, arguments and missing arguments.
    pub role: Option<String>,
}

/// The kinds of display segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SegmentKind {
    /// The verb.
    Verb,
    /// A word introducing a role.
    Delimiter,
    /// A filled argument.
    Argument,
    /// An empty argument, shown by its label.
    Missing,
}

/// Parse `input` and return the best `max` parses, best first.
pub fn parse(
    grammar: &Grammar,
    registry: &Registry,
    memory: &Memory,
    context: &Context,
    input: &str,
    max: usize,
) -> Vec<Parse> {
    Query::new(grammar, registry, memory, context).run(input, max)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Order {
    Initial,
    Final,
}

#[derive(Debug, Clone)]
struct VerbRef {
    index: usize,
    name: String,
    input: Option<String>,
    order: Order,
    score: f64,
}

#[derive(Debug, Clone)]
struct Arg {
    /// Left-to-right display position; `None` for a default.
    order: Option<i32>,
    input: String,
    modifier: String,
    from_selection: bool,
    /// The selected entity, when the argument's whole text stood for it.
    entity: Option<String>,
    inactive_prefix: String,
    suggestion: Option<Suggestion>,
}

impl Arg {
    fn typed(order: i32, input: String, modifier: &str) -> Self {
        Self {
            order: Some(order),
            input,
            modifier: modifier.to_owned(),
            from_selection: false,
            entity: None,
            inactive_prefix: String::new(),
            suggestion: None,
        }
    }
}

#[derive(Debug, Clone)]
struct Draft {
    verb: Option<VerbRef>,
    args: BTreeMap<String, Vec<Arg>>,
    multiplier: f64,
    score: f64,
}

impl Draft {
    fn new(verb: Option<VerbRef>) -> Self {
        // A parse whose verb must be suggested from its arguments starts
        // at 0.3, below any parse where the verb was typed.
        let multiplier = if verb.is_some() { 1.0 } else { 0.3 };
        Self {
            verb,
            args: BTreeMap::new(),
            multiplier,
            score: 0.0,
        }
    }

    fn push(&mut self, role: &str, arg: Arg) {
        self.args.entry(role.to_owned()).or_default().push(arg);
    }
}

struct Split {
    words: Vec<String>,
    all: Vec<String>,
}

fn is_separator(ch: char) -> bool {
    ch.is_whitespace() || ch == '\u{200b}'
}

/// Ubiquity's `splitWords`: words and the separators between them, so an
/// argument spanning several words keeps its original spacing.
fn split_words(input: &str) -> Split {
    let mut words = Vec::new();
    let mut all = Vec::new();
    let mut current = String::new();
    let mut gap = String::new();
    for ch in input.chars() {
        if is_separator(ch) {
            if !current.is_empty() {
                words.push(std::mem::take(&mut current));
                all.push(words.last().cloned().unwrap_or_default());
            }
            if !words.is_empty() {
                gap.push(ch);
            }
        } else {
            if !gap.is_empty() {
                all.push(std::mem::take(&mut gap));
            }
            current.push(ch);
        }
    }
    if !current.is_empty() {
        words.push(current.clone());
        all.push(current);
    }
    Split { words, all }
}

fn join(parts: &[String]) -> String {
    parts.concat()
}

fn power_set(items: &[usize]) -> Vec<Vec<usize>> {
    let mut sets = vec![Vec::new()];
    for item in items {
        let extended: Vec<Vec<usize>> = sets
            .iter()
            .map(|set| {
                let mut set = set.clone();
                set.push(*item);
                set
            })
            .collect();
        sets.extend(extended);
    }
    sets
}

/// Every name, and every suffix after a word separator, lowercased, in
/// Ubiquity's order: whole names first. Each carries the name it came from.
fn subnames(verb: &Verb) -> Vec<(String, String)> {
    let mut by_offset: Vec<(usize, usize, String, String)> = Vec::new();
    for (position, name) in verb.names.iter().enumerate() {
        let mut offsets = vec![0];
        let chars: Vec<(usize, char)> = name.char_indices().collect();
        for (i, (_, ch)) in chars.iter().enumerate() {
            if (matches!(ch, '-' | '_') || ch.is_whitespace())
                && let Some((next, _)) = chars.get(i + 1)
            {
                offsets.push(*next);
            }
        }
        for offset in offsets {
            by_offset.push((
                offset,
                position,
                name[offset..].to_lowercase(),
                name.clone(),
            ));
        }
    }
    by_offset.sort_by_key(|(offset, position, _, _)| (*offset, *position));
    by_offset
        .into_iter()
        .map(|(_, _, subname, name)| (subname, name))
        .collect()
}

struct Query<'a> {
    grammar: &'a Grammar,
    registry: &'a Registry,
    memory: &'a Memory,
    context: &'a Context,
    subnames: Vec<Vec<(String, String)>>,
    signatures: BTreeSet<String>,
}

impl<'a> Query<'a> {
    fn new(
        grammar: &'a Grammar,
        registry: &'a Registry,
        memory: &'a Memory,
        context: &'a Context,
    ) -> Self {
        let subnames = registry.verbs.iter().map(subnames).collect();
        // Every set of roles some verb could take, to drop parses no verb
        // could accept after roles are reassigned.
        let mut signatures = BTreeSet::new();
        for verb in &registry.verbs {
            let roles: Vec<usize> = (0..verb.arguments.len()).collect();
            for set in power_set(&roles) {
                if !set.is_empty() {
                    let mut names: Vec<&str> = set
                        .iter()
                        .map(|i| verb.arguments[*i].role.as_str())
                        .collect();
                    names.sort_unstable();
                    signatures.insert(names.join(","));
                }
            }
        }
        Self {
            grammar,
            registry,
            memory,
            context,
            subnames,
            signatures,
        }
    }

    fn verb(&self, verb: &VerbRef) -> &'a Verb {
        &self.registry.verbs[verb.index]
    }

    fn run(&self, input: &str, max: usize) -> Vec<Parse> {
        // Step 1.
        let input = self.grammar.word_break(input.trim());

        // Step 2.
        let pairs = self.find_verbs(&input);

        // Step 4.
        let mut drafts: Vec<Draft> = pairs
            .iter()
            .flat_map(|(verb, rest)| self.find_arguments(rest, verb.clone()))
            .collect();
        if let Some(selection) = &self.context.selection
            && !selection.text.is_empty()
        {
            let interpolated: Vec<Draft> = drafts
                .iter()
                .map(|draft| self.interpolate_selection(draft, selection))
                .collect();
            drafts.extend(interpolated);
        }

        // Step 5.
        if let Some(target) = self
            .context
            .selection
            .as_ref()
            .or(self.context.this.as_ref())
            && self.find_anaphor(&input).is_some()
        {
            let substituted: Vec<Draft> = drafts
                .iter()
                .flat_map(|draft| self.substitute_anaphora(draft, target))
                .collect();
            drafts.extend(substituted);
        }

        // Step 6.
        let normalized: Vec<Draft> = drafts
            .iter()
            .flat_map(|draft| self.normalize(draft))
            .collect();
        drafts.extend(normalized);

        // Step 7.
        let moved: Vec<Draft> = drafts
            .iter()
            .flat_map(|draft| self.objects_to_other_roles(draft))
            .collect();
        drafts.extend(moved);

        // Step 8.
        let verbed: Vec<Draft> = drafts
            .iter()
            .flat_map(|draft| self.suggest_verbs(draft))
            .map(|draft| self.apply_penalties(draft))
            .collect();

        // Steps 9–11.
        let mut parses: Vec<Parse> = verbed
            .iter()
            .flat_map(|draft| self.read_arguments(draft))
            .map(|draft| self.finish(&draft))
            .collect();
        parses.sort_by(|a, b| b.score.total_cmp(&a.score));

        let mut seen = BTreeSet::new();
        parses.retain(|parse| {
            let values: Vec<_> = parse
                .arguments
                .iter()
                .map(|filled| (filled.field.clone(), filled.value.clone()))
                .collect();
            seen.insert(format!("{}|{values:?}", parse.verb))
        });
        parses.truncate(max);
        parses
    }

    /// Whether `piece` (lowercased) starts some verb name or name suffix.
    fn starts_a_name(&self, piece: &str) -> bool {
        let piece = piece.to_lowercase();
        self.subnames
            .iter()
            .flatten()
            .any(|(subname, _)| subname.starts_with(&piece))
    }

    /// Step 2 (`verbFinder`): the trivial "no verb" pair, plus a pair for
    /// every verb whose name the start or the end of the input begins.
    fn find_verbs(&self, input: &str) -> Vec<(Option<VerbRef>, String)> {
        let mut pairs = vec![(None, input.trim().to_owned())];

        // Word ends (or every character boundary, without spaces).
        let ends: Vec<usize> = if self.grammar.spaces {
            let mut ends: Vec<usize> = input
                .char_indices()
                .filter(|(i, ch)| {
                    !is_separator(*ch)
                        && input[i + ch.len_utf8()..]
                            .chars()
                            .next()
                            .is_none_or(is_separator)
                })
                .map(|(i, ch)| i + ch.len_utf8())
                .collect();
            ends.reverse();
            ends
        } else {
            let mut ends: Vec<usize> = input
                .char_indices()
                .map(|(i, ch)| i + ch.len_utf8())
                .collect();
            ends.reverse();
            ends
        };

        let mut verb_only = None;
        if let Some(end) = ends.iter().find(|end| self.starts_a_name(&input[..**end])) {
            let piece = input[..*end].to_owned();
            let rest = input[*end..].trim().to_owned();
            let mut order = Order::Initial;
            if rest.is_empty() {
                verb_only = Some(piece.clone());
                if self.grammar.verb_final > self.grammar.verb_initial {
                    order = Order::Final;
                }
            }
            self.add_verbs(&mut pairs, &piece, rest, order);
        }

        // Starts: the beginning, or after a separator (every boundary,
        // without spaces). The earliest start gives the longest piece.
        let starts: Vec<usize> = if self.grammar.spaces {
            std::iter::once(0)
                .chain(
                    input
                        .char_indices()
                        .filter(|(_, ch)| is_separator(*ch))
                        .map(|(i, ch)| i + ch.len_utf8()),
                )
                .filter(|start| {
                    input[*start..]
                        .chars()
                        .next()
                        .is_some_and(|ch| !is_separator(ch))
                })
                .collect()
        } else {
            input.char_indices().map(|(i, _)| i).collect()
        };
        if let Some(start) = starts
            .iter()
            .find(|start| self.starts_a_name(input[**start..].trim_end()))
        {
            let piece = input[*start..].trim_end().to_owned();
            let rest = input[..*start].trim().to_owned();
            if !rest.is_empty() || verb_only.as_deref() != Some(piece.as_str()) {
                self.add_verbs(&mut pairs, &piece, rest, Order::Final);
            }
        }
        pairs
    }

    fn add_verbs(
        &self,
        pairs: &mut Vec<(Option<VerbRef>, String)>,
        piece: &str,
        rest: String,
        order: Order,
    ) {
        let lower = piece.to_lowercase();
        for (index, verb) in self.registry.verbs.iter().enumerate() {
            let Some((_, name)) = self.subnames[index]
                .iter()
                .find(|(subname, _)| subname.starts_with(&lower))
            else {
                continue;
            };
            // Initial letters say more than later ones; the 0.4 floor keeps
            // a one-letter prefix above noun-first suggestions (0.3).
            let ratio = piece.chars().count() as f64 / name.chars().count() as f64;
            let multiplier = match order {
                Order::Initial => self.grammar.verb_initial,
                Order::Final => self.grammar.verb_final,
            };
            let score = (0.4 + 0.6 * ratio.sqrt()) * multiplier;
            // Each past choice of this verb for this prefix pulls the
            // score toward 1: the n-th root of a score in [0, 1].
            let reinforcement = f64::from(self.memory.score(piece, &verb.id)) + 1.0;
            let score = score.powf(1.0 / reinforcement);
            pairs.push((
                Some(VerbRef {
                    index,
                    name: name.clone(),
                    input: Some(piece.to_owned()),
                    order,
                    score,
                }),
                rest.clone(),
            ));
        }
    }

    /// The markers a verb's arguments can use; all markers without a verb.
    fn markers_for(&self, verb: Option<&VerbRef>) -> Vec<(&'a str, &'a str)> {
        let roles: Option<BTreeSet<&str>> = verb.map(|verb| {
            self.verb(verb)
                .arguments
                .iter()
                .map(|argument| argument.role.as_str())
                .collect()
        });
        self.grammar
            .markers
            .iter()
            .filter(|marker| {
                roles
                    .as_ref()
                    .is_none_or(|roles| roles.contains(marker.role.as_str()))
            })
            .map(|marker| (marker.role.as_str(), marker.delimiter.as_str()))
            .collect()
    }

    /// Step 4 (`argFinder`): every way the delimiters could split the
    /// argument string. Words outside any delimited argument are objects.
    fn find_arguments(&self, rest: &str, verb: Option<VerbRef>) -> Vec<Draft> {
        if rest.is_empty() {
            return vec![Draft::new(verb)];
        }
        if let Some(verb) = &verb
            && self.verb(verb).arguments.is_empty()
        {
            return Vec::new();
        }

        let Split { words, all } = split_words(rest);
        let markers = self.markers_for(verb.as_ref());
        let roles_of = |word: &str| -> Vec<&'a str> {
            let word = word.to_lowercase();
            markers
                .iter()
                .filter(|(_, delimiter)| delimiter.to_lowercase() == word)
                .map(|(role, _)| *role)
                .collect()
        };
        let candidates: Vec<usize> = (0..words.len())
            .filter(|i| !roles_of(&words[*i]).is_empty())
            .collect();

        let mut drafts = Vec::new();
        'sets: for mut delimiters in power_set(&candidates) {
            delimiters.sort_unstable();
            if delimiters.windows(2).any(|pair| pair[0] + 1 == pair[1]) {
                continue 'sets;
            }
            let mut seed = Draft::new(verb.clone());
            if delimiters.is_empty() {
                seed.push(OBJECT, Arg::typed(1, join(&all), ""));
                drafts.push(seed);
                continue;
            }
            let count = delimiters.len() as i32;
            let last = delimiters[delimiters.len() - 1];
            match self.grammar.branching {
                Branching::Left => {
                    if last < words.len() - 1 {
                        seed.push(
                            OBJECT,
                            Arg::typed(2 * count + 2, join(&all[2 * last + 2..]), ""),
                        );
                    }
                }
                Branching::Right => {
                    let first = delimiters[0];
                    if first > 0 {
                        seed.push(OBJECT, Arg::typed(1, join(&all[..2 * first - 1]), ""));
                    }
                }
            }

            let mut drafts_so_far = vec![seed];
            if self.grammar.branching == Branching::Left {
                delimiters.reverse();
            }
            for (i, at) in delimiters.iter().enumerate() {
                // The words this delimiter can reach: up to the next
                // delimiter on its branching side. A delimiter with nothing
                // on that side (trailing in English, leading in Japanese)
                // yields no parse for this combination.
                let span = match self.grammar.branching {
                    Branching::Left => at
                        .checked_sub(1)
                        .map(|max| (delimiters.get(i + 1).map_or(0, |next| next + 1), max)),
                    Branching::Right => {
                        let max = delimiters
                            .get(i + 1)
                            .map_or(words.len() - 1, |next| next - 1);
                        (*at < max).then_some((at + 1, max))
                    }
                };
                let Some((min, max)) = span.filter(|(min, max)| min <= max) else {
                    drafts_so_far = Vec::new();
                    break;
                };
                let modifier = &words[*at];
                let mut next = Vec::new();
                for j in min..=max {
                    for role in roles_of(modifier) {
                        for base in &drafts_so_far {
                            let mut draft = base.clone();
                            let i = i as i32;
                            match self.grammar.branching {
                                Branching::Left => {
                                    draft.push(
                                        role,
                                        Arg::typed(
                                            1 + 2 * (count - i),
                                            join(&all[2 * j..=2 * max]),
                                            modifier,
                                        ),
                                    );
                                    if j != min {
                                        draft.push(
                                            OBJECT,
                                            Arg::typed(
                                                2 * (count - i),
                                                join(&all[2 * min..=2 * (j - 1)]),
                                                "",
                                            ),
                                        );
                                    }
                                }
                                Branching::Right => {
                                    draft.push(
                                        role,
                                        Arg::typed(
                                            2 * i + 2,
                                            join(&all[2 * min..=2 * j]),
                                            modifier,
                                        ),
                                    );
                                    if j != max {
                                        draft.push(
                                            OBJECT,
                                            Arg::typed(
                                                2 * i + 3,
                                                join(&all[2 * (j + 1)..=2 * max]),
                                                "",
                                            ),
                                        );
                                    }
                                }
                            }
                            next.push(draft);
                        }
                    }
                }
                drafts_so_far = next;
            }
            // Two arguments in one role (other than the object) make the
            // parse count double; Ubiquity turned these off for speed.
            drafts_so_far.retain(|draft| {
                draft
                    .args
                    .iter()
                    .all(|(role, args)| role == OBJECT || args.len() <= 1)
            });
            drafts.extend(drafts_so_far);
        }
        drafts
    }

    /// Step 4, with a selection: the whole selection as an object.
    fn interpolate_selection(&self, draft: &Draft, selection: &Selection) -> Draft {
        let mut copy = draft.clone();
        copy.push(
            OBJECT,
            Arg {
                order: Some(-1),
                input: selection.text.clone(),
                modifier: self.grammar.object_delimiter().unwrap_or("").to_owned(),
                from_selection: true,
                entity: selection.entity.clone(),
                inactive_prefix: String::new(),
                suggestion: None,
            },
        );
        copy.multiplier *= 1.2;
        copy
    }

    /// The first anaphor in `text`, as a byte range.
    fn find_anaphor(&self, text: &str) -> Option<(usize, usize, bool)> {
        let lower = text.to_lowercase();
        if lower.len() != text.len() {
            return None;
        }
        let mut best: Option<(usize, usize, bool)> = None;
        for anaphor in &self.grammar.anaphora {
            let anaphor = anaphor.to_lowercase();
            let mut from = 0;
            while let Some(found) = lower[from..].find(&anaphor) {
                let start = from + found;
                let end = start + anaphor.len();
                let bounded = !self.grammar.spaces
                    || (lower[..start]
                        .chars()
                        .next_back()
                        .is_none_or(|ch| !ch.is_alphanumeric())
                        && lower[end..]
                            .chars()
                            .next()
                            .is_none_or(|ch| !ch.is_alphanumeric()));
                if bounded {
                    let whole = start == 0 && end == text.len();
                    if best.is_none_or(|(at, _, _)| start < at) {
                        best = Some((start, end, whole));
                    }
                    break;
                }
                from = start + 1;
            }
        }
        best
    }

    /// Step 5: a copy for every argument containing an anaphor, with the
    /// selection in its place.
    fn substitute_anaphora(&self, draft: &Draft, target: &Selection) -> Vec<Draft> {
        let mut copies = Vec::new();
        for (role, args) in &draft.args {
            for (i, arg) in args.iter().enumerate() {
                let Some((start, end, whole)) = self.find_anaphor(&arg.input) else {
                    continue;
                };
                let mut copy = draft.clone();
                let replaced = &mut copy.args.get_mut(role).expect("role exists")[i];
                replaced.input = format!(
                    "{}{}{}",
                    &arg.input[..start],
                    target.text,
                    &arg.input[end..]
                );
                if whole {
                    replaced.entity = target.entity.clone();
                }
                copy.multiplier *= 1.2;
                copies.push(copy);
            }
        }
        copies
    }

    /// Step 6: a copy with a leading article stripped, kept for display.
    fn normalize(&self, draft: &Draft) -> Vec<Draft> {
        let mut copies = Vec::new();
        for (role, args) in &draft.args {
            for (i, arg) in args.iter().enumerate() {
                for article in &self.grammar.articles {
                    let lower = arg.input.to_lowercase();
                    let article = article.to_lowercase();
                    if lower.starts_with(&article)
                        && lower[article.len()..].starts_with(char::is_whitespace)
                    {
                        let rest = arg.input[article.len()..].trim_start();
                        let mut copy = draft.clone();
                        let stripped = &mut copy.args.get_mut(role).expect("role exists")[i];
                        stripped.inactive_prefix = arg.input[..arg.input.len() - rest.len()].into();
                        stripped.input = rest.to_owned();
                        copies.push(copy);
                    }
                }
            }
        }
        copies
    }

    /// Step 7 (`applyObjectsToOtherRoles`): "calendar" can mean "to
    /// calendar", "google" can mean "with google". Not for a typed object
    /// of a known verb that takes one: "twitter hello" is not "twitter as
    /// hello".
    fn objects_to_other_roles(&self, draft: &Draft) -> Vec<Draft> {
        let Some(objects) = draft.args.get(OBJECT) else {
            return Vec::new();
        };
        let verb = draft.verb.as_ref().map(|verb| self.verb(verb));
        let roles: Vec<(&str, &str)> = match verb {
            Some(verb) => verb
                .arguments
                .iter()
                .filter(|argument| argument.role != OBJECT)
                .filter_map(|argument| {
                    self.grammar
                        .first_delimiter(&argument.role)
                        .map(|delimiter| (argument.role.as_str(), delimiter))
                })
                .collect(),
            None => {
                let mut roles: Vec<(&str, &str)> = Vec::new();
                for marker in &self.grammar.markers {
                    if marker.role != OBJECT
                        && !marker.delimiter.is_empty()
                        && !roles.iter().any(|(role, _)| *role == marker.role)
                    {
                        roles.push((&marker.role, &marker.delimiter));
                    }
                }
                roles
            }
        };
        let verb_takes_object =
            verb.is_some_and(|verb| verb.arguments.iter().any(|a| a.role == OBJECT));

        let mut bases = vec![draft.clone()];
        let mut moved = Vec::new();
        for object in objects {
            if !object.modifier.is_empty() || (!object.from_selection && verb_takes_object) {
                continue;
            }
            let mut next = Vec::new();
            for (role, delimiter) in &roles {
                for base in &bases {
                    if base.args.contains_key(*role) {
                        continue;
                    }
                    let mut copy = base.clone();
                    if let Some(list) = copy.args.get_mut(OBJECT)
                        && let Some(at) = list
                            .iter()
                            .position(|arg| arg.order == object.order && arg.input == object.input)
                    {
                        list.remove(at);
                    }
                    let mut arg = object.clone();
                    arg.modifier = (*delimiter).to_owned();
                    copy.push(role, arg);
                    next.push(copy);
                }
            }
            bases.extend(next.iter().cloned());
            moved.extend(next);
        }
        moved
            .into_iter()
            .filter_map(|mut draft| {
                if draft.args.get(OBJECT).is_some_and(Vec::is_empty) {
                    draft.args.remove(OBJECT);
                }
                let signature: Vec<&str> = draft.args.keys().map(String::as_str).collect();
                self.signatures
                    .contains(&signature.join(","))
                    .then_some(draft)
            })
            .collect()
    }

    /// Step 8 (`suggestVerb`): a parse without a verb gets every verb whose
    /// arguments cover its roles, ranked by how often each was used.
    fn suggest_verbs(&self, draft: &Draft) -> Vec<Draft> {
        if draft.verb.is_some() {
            return vec![draft.clone()];
        }
        if draft.args.values().any(|args| args.len() > 1) {
            return Vec::new();
        }
        let order = if self.grammar.verb_final_display {
            Order::Final
        } else {
            Order::Initial
        };
        self.registry
            .verbs
            .iter()
            .enumerate()
            .filter(|(_, verb)| {
                draft
                    .args
                    .keys()
                    .all(|role| verb.arguments.iter().any(|argument| &argument.role == role))
            })
            .map(|(index, verb)| {
                let used = f64::from(self.memory.score("", &verb.id));
                let mut copy = draft.clone();
                copy.verb = Some(VerbRef {
                    index,
                    name: verb.names.first().cloned().unwrap_or_default(),
                    input: None,
                    order,
                    score: 1.0 - 0.7 / (1.0 + used),
                });
                copy
            })
            .collect()
    }

    /// `updateScoreMultiplierWithArgs`: halve per extra argument in a role,
    /// then scale by the verb match.
    fn apply_penalties(&self, mut draft: Draft) -> Draft {
        for args in draft.args.values() {
            if args.len() > 1 {
                draft.multiplier *= 0.5_f64.powi(args.len() as i32 - 1);
            }
        }
        draft.multiplier *= draft.verb.as_ref().map_or(1.0, |verb| verb.score);
        draft.score = draft.multiplier;
        draft
    }

    /// Steps 9–11 (`suggestArgs`): one parse per combination of argument
    /// readings; a role nothing can read, or the verb does not take, kills
    /// the parse. Empty roles get defaults at half score.
    fn read_arguments(&self, draft: &Draft) -> Vec<Draft> {
        let Some(verb_ref) = &draft.verb else {
            return Vec::new();
        };
        let verb = self.verb(verb_ref);
        let mut combinations: Vec<Vec<(String, Suggestion)>> = vec![Vec::new()];
        for (role, args) in &draft.args {
            let [arg] = args.as_slice() else {
                return Vec::new();
            };
            let Some(argument) = verb
                .arguments
                .iter()
                .find(|argument| &argument.role == role)
            else {
                return Vec::new();
            };
            let readings = noun::detect(
                self.registry,
                &argument.noun,
                &arg.input,
                arg.entity.as_deref(),
            );
            combinations = combinations
                .iter()
                .flat_map(|combination| {
                    readings.iter().map(move |reading| {
                        let mut combination = combination.clone();
                        combination.push((role.clone(), reading.clone()));
                        combination
                    })
                })
                .collect();
        }

        let object_marked = self.grammar.object_delimiter().is_some();
        let this = self
            .context
            .this
            .as_ref()
            .and_then(|this| this.entity.as_deref());
        let defaults: Vec<(String, Arg)> = verb
            .arguments
            .iter()
            .filter(|argument| !draft.args.contains_key(&argument.role))
            .map(|argument| {
                let mut suggestion = noun::default(self.registry, &argument.noun, this);
                suggestion.score /= 2.0;
                let modifier = if suggestion.text.is_empty() || argument.role == OBJECT {
                    ""
                } else {
                    self.grammar.first_delimiter(&argument.role).unwrap_or("")
                };
                (
                    argument.role.clone(),
                    Arg {
                        order: None,
                        input: String::new(),
                        modifier: modifier.to_owned(),
                        from_selection: false,
                        entity: None,
                        inactive_prefix: String::new(),
                        suggestion: Some(suggestion),
                    },
                )
            })
            .collect();

        combinations
            .into_iter()
            .map(|combination| {
                let mut filled = draft.clone();
                for (role, mut reading) in combination {
                    let arg = &mut filled.args.get_mut(&role).expect("role exists")[0];
                    if role == OBJECT && object_marked && arg.modifier.is_empty() {
                        reading.score *= 0.6;
                    }
                    arg.suggestion = Some(reading);
                }
                for (role, arg) in &defaults {
                    filled.push(role, arg.clone());
                }
                filled.score = filled.multiplier
                    + filled
                        .args
                        .values()
                        .filter_map(|args| args.first()?.suggestion.as_ref())
                        .map(|suggestion| suggestion.score * filled.multiplier)
                        .sum::<f64>();
                filled
            })
            .collect()
    }

    fn finish(&self, draft: &Draft) -> Parse {
        let verb_ref = draft.verb.as_ref().expect("scored drafts have a verb");
        let verb = self.verb(verb_ref);
        let arguments = verb
            .arguments
            .iter()
            .map(|argument| {
                let arg = draft.args.get(&argument.role).and_then(|args| args.first());
                let suggestion = arg.and_then(|arg| arg.suggestion.as_ref());
                Filled {
                    role: argument.role.clone(),
                    field: argument.field.clone(),
                    text: suggestion.map(|s| s.text.clone()).unwrap_or_default(),
                    value: suggestion.and_then(|s| s.value.clone()),
                    given: arg.is_some_and(|arg| arg.order.is_some()),
                }
            })
            .collect();
        Parse {
            verb: verb.id.clone(),
            name: verb_ref.name.clone(),
            input: verb_ref.input.clone(),
            score: draft.score,
            arguments,
            display: self.display(draft, verb_ref, verb),
        }
    }

    /// `Parse#displayHtml`: typed arguments in input order, then the
    /// selection, then defaults, then labels for what is still empty.
    fn display(&self, draft: &Draft, verb_ref: &VerbRef, verb: &Verb) -> Vec<Segment> {
        let mut shown: Vec<(&String, &Arg)> = draft
            .args
            .iter()
            .filter_map(|(role, args)| Some((role, args.first()?)))
            .filter(|(_, arg)| arg.suggestion.as_ref().is_some_and(|s| !s.text.is_empty()))
            .collect();
        // Typed arguments keep the order they were typed in, then the
        // selection. Ubiquity appended defaults last, which reads as
        // "rename to [x] [Roadmap]"; here a default sits just before the
        // first typed argument the verb declares after it.
        let typed_after = |role: &str| -> f64 {
            verb.arguments
                .iter()
                .skip_while(|argument| argument.role != role)
                .filter_map(|argument| {
                    draft
                        .args
                        .get(&argument.role)
                        .and_then(|args| args.first())
                        .and_then(|arg| arg.order)
                        .filter(|order| *order > 0)
                })
                .map(f64::from)
                .fold(f64::INFINITY, f64::min)
        };
        shown.sort_by(|(a_role, a), (b_role, b)| {
            let key = |role: &str, arg: &Arg| match arg.order {
                Some(order) if order > 0 => (0, f64::from(order)),
                Some(order) => (1, f64::from(order)),
                None => (0, typed_after(role) - 0.5),
            };
            let (a, b) = (key(a_role, a), key(b_role, b));
            a.0.cmp(&b.0).then(a.1.total_cmp(&b.1))
        });

        let mut segments = Vec::new();
        let verb_segment = Segment {
            kind: SegmentKind::Verb,
            text: verb_ref.name.clone(),
            role: None,
        };
        if verb_ref.order == Order::Initial {
            segments.push(verb_segment.clone());
        }
        for (role, arg) in shown {
            let delimiter = (!arg.modifier.is_empty()).then(|| Segment {
                kind: SegmentKind::Delimiter,
                text: arg.modifier.clone(),
                role: Some(role.clone()),
            });
            let argument = Segment {
                kind: SegmentKind::Argument,
                text: format!(
                    "{}{}",
                    arg.inactive_prefix,
                    arg.suggestion
                        .as_ref()
                        .map(|s| s.text.as_str())
                        .unwrap_or("")
                ),
                role: Some(role.clone()),
            };
            match self.grammar.branching {
                Branching::Right => {
                    segments.extend(delimiter.clone());
                    segments.push(argument);
                }
                Branching::Left => {
                    segments.push(argument);
                    segments.extend(delimiter);
                }
            }
        }
        for argument in &verb.arguments {
            let filled = draft
                .args
                .get(&argument.role)
                .and_then(|args| args.first())
                .and_then(|arg| arg.suggestion.as_ref())
                .is_some_and(|s| !s.text.is_empty());
            if filled {
                continue;
            }
            let delimiter = self
                .grammar
                .first_delimiter(&argument.role)
                .filter(|_| argument.role != OBJECT || self.grammar.object_delimiter().is_some())
                .map(|text| Segment {
                    kind: SegmentKind::Delimiter,
                    text: text.to_owned(),
                    role: Some(argument.role.clone()),
                });
            let missing = Segment {
                kind: SegmentKind::Missing,
                text: argument.label.clone(),
                role: Some(argument.role.clone()),
            };
            match self.grammar.branching {
                Branching::Right => {
                    segments.extend(delimiter);
                    segments.push(missing);
                }
                Branching::Left => {
                    segments.push(missing);
                    segments.extend(delimiter);
                }
            }
        }
        if verb_ref.order == Order::Final {
            segments.push(verb_segment);
        }
        segments
    }
}
