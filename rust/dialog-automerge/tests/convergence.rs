//! Property tests for the load-bearing facts the integration design rests on
//! (`notes/automerge-integration-spec.md` §3): saved bytes are canonical —
//! replicas holding the same change-set encode byte-identically regardless of
//! the order changes were applied — and merge is idempotent and monotone.

use dialog_artifacts::{Record, RecordFormat};
use dialog_automerge::TextDocument;
use proptest::prelude::*;

/// A single edit: position and deletion seeds (reduced modulo the current
/// document length when applied) plus text to insert.
type Edit = (usize, usize, String);

fn edit() -> impl Strategy<Value = Edit> {
    (any::<usize>(), any::<usize>(), "[a-z ]{0,8}")
}

fn script() -> impl Strategy<Value = Vec<Edit>> {
    prop::collection::vec(edit(), 0..12)
}

/// Apply an edit script, clamping every edit into the document's current
/// bounds so any script is valid against any document.
fn apply(document: &mut TextDocument, script: &[Edit]) {
    for (position_seed, delete_seed, insert) in script {
        let length = document.text().chars().count();
        let position = position_seed % (length + 1);
        let delete = delete_seed % (length - position + 1);
        document
            .splice(position, delete as isize, insert)
            .expect("clamped splice cannot be out of bounds");
    }
}

/// A document with some base history, plus diverged replicas that each
/// applied their own edit script.
fn diverged(
    base_script: &[Edit],
    branch_scripts: &[Vec<Edit>],
) -> (TextDocument, Vec<TextDocument>) {
    let mut base = TextDocument::new();
    apply(&mut base, base_script);

    let branches = branch_scripts
        .iter()
        .map(|branch_script| {
            let mut branch = base.fork();
            apply(&mut branch, branch_script);
            branch
        })
        .collect();

    (base, branches)
}

fn encoded(document: &TextDocument) -> Vec<u8> {
    document.encode().expect("encoding cannot fail")
}

proptest! {
    /// decode(encode(d)) re-encodes to the identical bytes: the stored form
    /// is a fixed point, so hydrating a record and writing it back unchanged
    /// mints the same value.
    #[test]
    fn encode_is_a_fixed_point_of_the_round_trip(base_script in script()) {
        let mut document = TextDocument::new();
        apply(&mut document, &base_script);

        let bytes = encoded(&document);
        let restored = TextDocument::decode(&bytes).unwrap();

        prop_assert_eq!(restored.text(), document.text());
        prop_assert_eq!(encoded(&restored), bytes);
    }

    /// merge(a, b) and merge(b, a) encode byte-identically, so the fold's
    /// stream order is irrelevant to the identity of the fold product.
    #[test]
    fn merge_is_commutative_in_bytes(
        base_script in script(),
        left_script in script(),
        right_script in script(),
    ) {
        let (_, branches) = diverged(&base_script, &[left_script, right_script]);
        let (left, right) = (&branches[0], &branches[1]);

        let left_first = TextDocument::merge(left, right);
        let right_first = TextDocument::merge(right, left);

        prop_assert_eq!(encoded(&left_first), encoded(&right_first));
    }

    /// Merging a document with itself, or re-merging an already-absorbed
    /// side, changes nothing: folding a stale sibling is harmless, so no
    /// concurrency detection is required for correctness.
    #[test]
    fn merge_is_idempotent_and_monotone(
        base_script in script(),
        left_script in script(),
        right_script in script(),
    ) {
        let (base, branches) = diverged(&base_script, &[left_script, right_script]);
        let (left, right) = (&branches[0], &branches[1]);

        prop_assert_eq!(
            encoded(&TextDocument::merge(left, left)),
            encoded(left)
        );

        // The base's changes are a subset of each branch's.
        prop_assert_eq!(
            encoded(&TextDocument::merge(&base, left)),
            encoded(left)
        );

        let merged = TextDocument::merge(left, right);
        prop_assert_eq!(
            encoded(&TextDocument::merge(&merged, right)),
            encoded(&merged)
        );
    }

    /// Replicas that assemble the same change-set along different merge
    /// orders encode byte-identically — the §3 canonical-bytes fact that
    /// lets independently-written merge products collide onto one tree key.
    #[test]
    fn cross_order_build_yields_identical_bytes(
        base_script in script(),
        branch_scripts in prop::collection::vec(script(), 3),
    ) {
        let (_, branches) = diverged(&base_script, &branch_scripts);

        let assemble = |order: &[usize]| {
            let mut replica = branches[order[0]].clone();
            for &index in &order[1..] {
                replica = TextDocument::merge(&replica, &branches[index]);
            }
            encoded(&replica)
        };

        let reference = assemble(&[0, 1, 2]);
        prop_assert_eq!(assemble(&[2, 0, 1]), reference.clone());
        prop_assert_eq!(assemble(&[1, 2, 0]), reference);
    }

    /// The full fold as records: realizing diverged sibling records and
    /// folding them yields equal `Record`s on every replica, whichever
    /// sibling streamed first.
    #[test]
    fn folded_sibling_records_are_equal(
        base_script in script(),
        left_script in script(),
        right_script in script(),
    ) {
        let (_, branches) = diverged(&base_script, &[left_script, right_script]);

        let siblings = (
            Record::from_format(branches[0].clone()).unwrap(),
            Record::from_format(branches[1].clone()).unwrap(),
        );

        let fold = |a: &Record, b: &Record| {
            let a = a.realize::<TextDocument>().unwrap();
            let b = b.realize::<TextDocument>().unwrap();
            Record::from_format(TextDocument::merge(&a, &b)).unwrap()
        };

        prop_assert_eq!(
            fold(&siblings.0, &siblings.1),
            fold(&siblings.1, &siblings.0)
        );
    }
}
