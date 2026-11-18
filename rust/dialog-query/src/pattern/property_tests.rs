//! One pattern says one thing both ways, checked over patterns and
//! rows drawn from a sequence that is the same every run: the value a
//! pattern builds from a row is taken apart by it into bindings that
//! build the value again, whether the pattern's variables were bound
//! before the value was met or by it.

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

use super::*;
use crate::corpus::Entity;

/// Numbers that are the same every run (splitmix64).
struct Dice(u64);

impl Dice {
    fn roll(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut mixed = self.0;
        mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        mixed ^ (mixed >> 31)
    }

    fn below(&mut self, bound: usize) -> usize {
        (self.roll() % bound as u64) as usize
    }

    fn pick<T: Clone>(&mut self, from: &[T]) -> T {
        from[self.below(from.len())].clone()
    }
}

/// A value that is no composite, of a type and a number that a
/// composite keeps another way than it was given.
fn leaf(dice: &mut Dice) -> Value {
    match dice.below(8) {
        0 => Value::String(dice.pick(&["", "a", "Buy milk", "é"]).into()),
        1 => Value::Bytes(vec![dice.roll() as u8; dice.below(4)]),
        2 => Value::Boolean(dice.below(2) == 0),
        3 => Value::UnsignedInt(dice.pick(&[0, 3, 255, u128::from(u64::MAX) + 1])),
        4 => Value::SignedInt(dice.pick(&[-1, 3, -300, i128::MIN])),
        5 => Value::Float(dice.pick(&[0.5, 3.0, -0.0, 1e30, f64::INFINITY])),
        6 => Value::Entity(
            format!("id:{}", dice.pick(&["a", "b", "milk"]))
                .parse::<Entity>()
                .expect("an entity"),
        ),
        _ => Value::Symbol("xyz.todo/open".parse().expect("a symbol")),
    }
}

/// A value: a leaf, or a map or a list of values down to `depth`.
fn value(dice: &mut Dice, depth: usize) -> Value {
    if depth == 0 || dice.below(3) > 0 {
        return leaf(dice);
    }
    let size = dice.below(4);
    if dice.below(2) == 0 {
        Composite::list(
            (0..size)
                .map(|_| value(dice, depth - 1))
                .collect::<Vec<_>>(),
        )
        .into()
    } else {
        Composite::map((0..size).map(|at| (format!("k{at}"), value(dice, depth - 1))))
            .expect("no key twice")
            .into()
    }
}

/// A pattern with no blank in it, and a row that binds its every
/// variable. A rest is bound to a map of keys the pattern does not
/// name, and a run to a list.
fn pattern(dice: &mut Dice, depth: usize, row: &mut Match, names: &mut usize) -> Pattern {
    fn variable(row: &mut Match, names: &mut usize, bound: Value) -> Term<Any> {
        *names += 1;
        let term = Term::<Any>::var(format!("v{names}"));
        row.bind(&term, bound).expect("a new variable binds");
        term
    }
    if depth == 0 || dice.below(3) == 0 {
        return match dice.below(3) {
            0 => Pattern::Term(Term::Constant(leaf(dice))),
            _ => {
                let bound = value(dice, 2);
                Pattern::Term(variable(row, names, bound))
            }
        };
    }
    let size = dice.below(4);
    if dice.below(2) == 0 {
        let items: Vec<Pattern> = (0..size)
            .map(|_| pattern(dice, depth - 1, row, names))
            .collect();
        let list = ListPattern::new(items);
        match dice.below(2) {
            0 => list.into(),
            _ => {
                let others = (0..dice.below(3))
                    .map(|_| value(dice, 1))
                    .collect::<Vec<_>>();
                let run = variable(row, names, Composite::list(others).into());
                list.with_run(run).into()
            }
        }
    } else {
        let entries: Vec<(String, Pattern)> = (0..size)
            .map(|at| (format!("k{at}"), pattern(dice, depth - 1, row, names)))
            .collect();
        let map = MapPattern::new(entries).expect("no key twice");
        match dice.below(2) {
            0 => map.into(),
            _ => {
                let others = (0..dice.below(3))
                    .map(|at| (format!("other{at}"), value(dice, 1)))
                    .collect::<Vec<_>>();
                let rest = variable(
                    row,
                    names,
                    Composite::map(others).expect("no key twice").into(),
                );
                map.with_rest(rest).into()
            }
        }
    }
}

/// `pattern` with some of its variables typed, each under the name it
/// had. A typed variable reads a value through its type, as a scan
/// reads a stored fact.
fn typed(pattern: &Pattern, seed: u64) -> Pattern {
    pattern.map_terms(&|term| {
        let Some(name) = term.name() else {
            return term.clone();
        };
        let mut dice = Dice(seed ^ name.len() as u64 ^ u64::from(name.as_bytes()[name.len() - 1]));
        match dice.below(6) {
            0 => Term::<u32>::var(name).into(),
            1 => Term::<i64>::var(name).into(),
            2 => Term::<f64>::var(name).into(),
            3 => Term::<String>::var(name).into(),
            _ => term.clone(),
        }
    })
}

/// The value a map or a list of patterns builds from a row is taken
/// apart by it, from nothing and by a row that already holds the
/// bindings, and what it binds builds the value again. Whatever value
/// it matches, with its variables typed or its constants other ones,
/// what it binds builds that value: it matches nothing it does not
/// build.
#[dialog_common::test]
fn it_builds_what_it_takes_apart_and_takes_apart_what_it_builds() {
    let mut dice = Dice(3);
    let mut composites = 0;
    let mut typed_matches = 0;
    for round in 0..6000u64 {
        let mut row = Match::new();
        let mut names = 0;
        let drawn = pattern(&mut dice, 3, &mut row, &mut names);
        // A pattern that is a term is matched as a scan reads a value,
        // which is not what is pinned here.
        if !drawn.is_composite() {
            continue;
        }
        composites += 1;
        let Some(built) = drawn.build(&row) else {
            panic!("{round}: {drawn} builds nothing from a row that binds it");
        };

        // Met with nothing bound: the value binds the variables.
        let mut taken = Match::new();
        assert!(
            drawn.unify(&built, &mut taken).expect("is read"),
            "{round}: {drawn} does not match what it built"
        );
        assert_eq!(
            drawn.build(&taken),
            Some(built.clone()),
            "{round}: {drawn} builds another value from what it bound"
        );

        // Met with its variables bound: the value is checked by them.
        let mut checked = row.clone();
        assert!(
            drawn.unify(&built, &mut checked).expect("is read"),
            "{round}: {drawn} does not match what it built from the row that built it"
        );

        // With variables typed, it may match or not. Where it does, it
        // builds the value.
        let narrowed = typed(&drawn, round);
        let mut taken = Match::new();
        if narrowed.unify(&built, &mut taken).expect("is read") {
            typed_matches += 1;
            assert_eq!(
                narrowed.build(&taken),
                Some(built.clone()),
                "{round}: {narrowed} matches {built:?} and builds another value"
            );
        }

        // The same pattern over other constants.
        let other = drawn.map_terms(&|term| match term {
            Term::Constant(_) => Term::Constant(leaf(&mut Dice(round))),
            variable => variable.clone(),
        });
        if let Some(elsewhere) = other.build(&row)
            && elsewhere != built
        {
            let mut taken = Match::new();
            assert!(
                !drawn.unify(&elsewhere, &mut taken).expect("is read")
                    || drawn.build(&taken) == Some(elsewhere.clone()),
                "{round}: {drawn} matches {elsewhere:?} and builds another value"
            );
        }
    }
    assert!(composites > 2000, "{composites} of 6000 were maps or lists");
    assert!(
        typed_matches > 500,
        "{typed_matches} matched with a variable typed"
    );
}
