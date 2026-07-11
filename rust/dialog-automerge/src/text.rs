//! A collaboratively editable text document.

use automerge::{
    Automerge, ObjId, ObjType, ROOT, ReadDoc, Value as AutomergeValue, transaction::Transactable,
};
use dialog_artifacts::{RecordError, RecordFormat};

use crate::canonical_bytes;

/// The root map key under which the document's text object lives.
const TEXT_KEY: &str = "text";

/// A collaborative text document backed by an automerge CRDT.
///
/// The document holds a single text object at the root under `"text"`,
/// following the automerge convention for text documents. Concurrent edits on
/// different replicas converge via [`RecordFormat::merge`] instead of
/// surfacing as divergent siblings: both sides' insertions and deletions are
/// preserved in the merged document.
///
/// Rich text (marks, blocks) and any structure beyond plain splicing are
/// available through [`as_automerge`](TextDocument::as_automerge) and
/// [`automerge_mut`](TextDocument::automerge_mut); Dialog itself never sees
/// them — it carries the document as opaque canonical bytes.
#[derive(Debug, Clone)]
pub struct TextDocument(Automerge);

impl TextDocument {
    /// Create a new, empty text document.
    ///
    /// This mints the document's root text object and therefore its identity:
    /// replicas that should converge must descend from one created document
    /// (via storage round-trip or [`fork`](TextDocument::fork)), not from
    /// independent calls to `new` (see the crate docs on shared ancestry).
    pub fn new() -> Self {
        let mut document = Automerge::new();
        document
            .transact(|tx| tx.put_object(ROOT, TEXT_KEY, ObjType::Text))
            .expect("creating the text root on an empty document cannot fail");
        Self(document)
    }

    /// The current text content of the document.
    pub fn text(&self) -> String {
        let root = self.text_root();
        self.0
            .text(&root)
            .expect("the text root is validated at construction")
    }

    /// Splice `insert` into the text at `position`, first deleting `delete`
    /// characters.
    ///
    /// `position` and `delete` are interpreted by automerge (see
    /// [`Transactable::splice_text`]); a negative `delete` removes characters
    /// before `position`. Errors surface automerge's own bounds checking.
    pub fn splice(
        &mut self,
        position: usize,
        delete: isize,
        insert: &str,
    ) -> Result<(), automerge::AutomergeError> {
        let root = self.text_root();
        self.0
            .transact(|tx| tx.splice_text(&root, position, delete, insert))
            .map(|_| ())
            .map_err(|failure| failure.error)
    }

    /// Fork this document into an independent replica with its own actor id.
    ///
    /// The fork shares this document's history, so edits made on either side
    /// converge when the two are merged.
    pub fn fork(&self) -> Self {
        Self(self.0.fork())
    }

    /// The underlying automerge document.
    pub fn as_automerge(&self) -> &Automerge {
        &self.0
    }

    /// Mutable access to the underlying automerge document, for edits beyond
    /// plain text splicing (marks, blocks, additional fields).
    ///
    /// The root text object under `"text"` must be preserved: the accessors
    /// on this type assume it exists, as does every replica that will decode
    /// this document's bytes.
    pub fn automerge_mut(&mut self) -> &mut Automerge {
        &mut self.0
    }

    /// Consume the wrapper, yielding the underlying automerge document.
    pub fn into_automerge(self) -> Automerge {
        self.0
    }

    /// The object id of the root text object.
    fn text_root(&self) -> ObjId {
        self.0
            .get(ROOT, TEXT_KEY)
            .ok()
            .flatten()
            .map(|(_, id)| id)
            .expect("the text root is validated at construction")
    }
}

impl Default for TextDocument {
    fn default() -> Self {
        Self::new()
    }
}

impl TryFrom<Automerge> for TextDocument {
    type Error = RecordError;

    /// Validate that the document has the text-document shape: a text object
    /// at the root under `"text"`.
    ///
    /// This is the shape gate for foreign bytes: any loadable automerge
    /// document is *valid automerge*, but only one carrying a root text
    /// object is a `TextDocument`. Rejecting here keeps the accessors above
    /// infallible and lets the read-side fold drop malformed siblings
    /// deterministically.
    fn try_from(document: Automerge) -> Result<Self, RecordError> {
        match document.get(ROOT, TEXT_KEY) {
            Ok(Some((AutomergeValue::Object(ObjType::Text), _))) => Ok(Self(document)),
            Ok(_) => Err(RecordError::Decode(format!(
                "automerge document has no text object at root key {TEXT_KEY:?}"
            ))),
            Err(error) => Err(RecordError::Decode(error.to_string())),
        }
    }
}

impl RecordFormat for TextDocument {
    fn decode(bytes: &[u8]) -> Result<Self, RecordError> {
        let document =
            Automerge::load(bytes).map_err(|error| RecordError::Decode(error.to_string()))?;
        Self::try_from(document)
    }

    fn encode(&self) -> Result<Vec<u8>, RecordError> {
        // Canonical form: the bytes are a pure function of the change-set,
        // independent of merge order and of any compression library (see
        // `canonical_bytes`).
        canonical_bytes(&self.0)
    }

    fn merge(a: &Self, b: &Self) -> Self {
        let (mut out, mut rhs) = (a.0.clone(), b.0.clone());
        match out.merge(&mut rhs) {
            Ok(_) => Self(out),
            // Merge of two successfully-loaded documents failing means
            // corrupt internal state; degrade to the deterministic default
            // (`b` wins) rather than poisoning the read path.
            Err(_) => Self(b.0.clone()),
        }
    }
}

#[cfg(test)]
mod tests {
    use automerge::transaction::Transactable;
    use dialog_artifacts::Record;

    use super::*;

    fn encoded(document: &TextDocument) -> Vec<u8> {
        document.encode().expect("encoding cannot fail")
    }

    #[test]
    fn new_document_is_empty() {
        assert_eq!(TextDocument::new().text(), "");
    }

    #[test]
    fn splice_edits_the_text() {
        let mut document = TextDocument::new();
        document.splice(0, 0, "hello world").unwrap();
        document.splice(5, 6, " there").unwrap();
        assert_eq!(document.text(), "hello there");
    }

    #[test]
    fn encode_decode_round_trip_is_byte_identical() {
        let mut document = TextDocument::new();
        document.splice(0, 0, "some canonical content").unwrap();

        let bytes = encoded(&document);
        let restored = TextDocument::decode(&bytes).unwrap();

        assert_eq!(restored.text(), document.text());
        assert_eq!(encoded(&restored), bytes);
    }

    #[test]
    fn decode_rejects_garbage() {
        assert!(matches!(
            TextDocument::decode(&[0xde, 0xad, 0xbe, 0xef]),
            Err(RecordError::Decode(_))
        ));
    }

    #[test]
    fn decode_rejects_documents_without_a_text_root() {
        let mut foreign = Automerge::new();
        foreign
            .transact(|tx| tx.put(ROOT, "title", "not a text document"))
            .unwrap();
        let bytes = foreign.save_with_options(crate::canonical_options());

        assert!(matches!(
            TextDocument::decode(&bytes),
            Err(RecordError::Decode(_))
        ));
    }

    /// Automerge's own `save()` lists changes in merge-order-dependent
    /// graph order; `encode` canonicalizes. Bytes saved *without* that
    /// normalization still decode, and re-encode to the canonical form.
    #[test]
    fn decode_normalizes_merge_order_artifacts() {
        let mut base = TextDocument::new();
        base.splice(0, 0, "shared").unwrap();

        let mut left = base.fork();
        let mut right = base.fork();
        left.splice(0, 0, "L").unwrap();
        right.splice(6, 0, "R").unwrap();

        let canonical = encoded(&TextDocument::merge(&left, &right));
        let raw = TextDocument::merge(&right, &left)
            .as_automerge()
            .save_with_options(crate::canonical_options());

        let restored = TextDocument::decode(&raw).unwrap();
        assert_eq!(encoded(&restored), canonical);
    }

    /// Canonical bytes are independent of the form the document arrived in:
    /// loading a DEFLATE-compressed save and re-encoding yields the same
    /// bytes as encoding the original.
    #[test]
    fn decode_normalizes_compressed_input() {
        let mut document = TextDocument::new();
        // Enough content to clear automerge's compression threshold.
        let content = "compressible ".repeat(100);
        document.splice(0, 0, &content).unwrap();

        let compressed = document.as_automerge().save();
        let restored = TextDocument::decode(&compressed).unwrap();

        assert_eq!(encoded(&restored), encoded(&document));
    }

    #[test]
    fn concurrent_edits_both_survive_merge() {
        let mut base = TextDocument::new();
        base.splice(0, 0, "hello").unwrap();

        let mut left = base.fork();
        let mut right = base.fork();
        left.splice(0, 0, ">> ").unwrap();
        right.splice(5, 0, " world").unwrap();

        let merged = TextDocument::merge(&left, &right);
        assert_eq!(merged.text(), ">> hello world");
    }

    #[test]
    fn merge_is_order_insensitive_in_bytes() {
        let mut base = TextDocument::new();
        base.splice(0, 0, "shared").unwrap();

        let mut left = base.fork();
        let mut right = base.fork();
        left.splice(0, 0, "L").unwrap();
        right.splice(6, 0, "R").unwrap();

        let left_first = TextDocument::merge(&left, &right);
        let right_first = TextDocument::merge(&right, &left);
        assert_eq!(encoded(&left_first), encoded(&right_first));
    }

    /// Merge is monotone: when one side's changes are a subset of the
    /// other's, the merge product is byte-identical to the superset — so
    /// folding a stale sibling into a fresh one is harmless.
    #[test]
    fn merging_a_subset_yields_the_superset() {
        let mut base = TextDocument::new();
        base.splice(0, 0, "v1").unwrap();

        let mut ahead = base.fork();
        ahead.splice(2, 0, " v2").unwrap();

        assert_eq!(
            encoded(&TextDocument::merge(&base, &ahead)),
            encoded(&ahead)
        );
        assert_eq!(
            encoded(&TextDocument::merge(&ahead, &base)),
            encoded(&ahead)
        );
        assert_eq!(
            encoded(&TextDocument::merge(&ahead, &ahead)),
            encoded(&ahead)
        );
    }

    #[test]
    fn record_round_trip_realizes_the_document() {
        let mut document = TextDocument::new();
        document.splice(0, 0, "stored as a record").unwrap();

        let record = Record::from_format(document.clone()).unwrap();
        assert_eq!(record.as_bytes(), encoded(&document).as_slice());

        let realized = record.realize::<TextDocument>().unwrap();
        assert_eq!(realized.text(), "stored as a record");
    }

    /// The read-side fold WS4 performs: realize two diverged sibling
    /// records, merge, re-encode. Both fold orders mint the identical
    /// record — and therefore the identical tree key.
    #[test]
    fn diverged_sibling_records_fold_to_one_identity() {
        let mut base = TextDocument::new();
        base.splice(0, 0, "draft").unwrap();

        let mut left = base.fork();
        let mut right = base.fork();
        left.splice(0, 0, "my ").unwrap();
        right.splice(5, 0, " notes").unwrap();

        let fold = |a: &Record, b: &Record| {
            let a = a.realize::<TextDocument>().unwrap();
            let b = b.realize::<TextDocument>().unwrap();
            Record::from_format(TextDocument::merge(&a, &b)).unwrap()
        };

        let left_sibling = Record::from_format(left).unwrap();
        let right_sibling = Record::from_format(right).unwrap();

        let one_replica = fold(&left_sibling, &right_sibling);
        let other_replica = fold(&right_sibling, &left_sibling);
        assert_eq!(one_replica, other_replica);
        assert_eq!(
            one_replica.realize::<TextDocument>().unwrap().text(),
            "my draft notes"
        );
    }

    /// Two documents created independently never share a root text object:
    /// merge keeps both histories, but only one side's content is visible.
    /// This documents the shared-ancestry requirement from the crate docs.
    #[test]
    fn independent_documents_do_not_converge_on_content() {
        let mut one = TextDocument::new();
        let mut other = TextDocument::new();
        one.splice(0, 0, "one").unwrap();
        other.splice(0, 0, "other").unwrap();

        let merged = TextDocument::merge(&one, &other);
        let text = merged.text();
        assert!(text == "one" || text == "other");

        // The merge is still deterministic in bytes across fold orders.
        assert_eq!(
            encoded(&TextDocument::merge(&one, &other)),
            encoded(&TextDocument::merge(&other, &one))
        );
    }
}
