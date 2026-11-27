use anyhow::Result;
use dialog_corpus::Value;
use dialog_notation::live::Live;
use dialog_notation::view::html::{HtmlHost, HtmlMount, HtmlNode};
use dialog_notation::{Analyzer, RecordContent, RecordNode, Step, Syntax, compile, parse};
use dialog_repository::tree_edits::wire::{EditDecoder, EditEncoder, FrameError};
use dialog_repository::{EditedTree, TreeEdit, TreePosition};

#[path = "support/session.rs"]
mod session;
use session::Notebook;

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_browser);

const CONCEPTS: &str = r#"
    (concept (shelf ?l name: ?name)
      (xyz.shelf/name of: ?l is: ?name as: :text cardinality: :one))
    (concept (task ?t list: ?l title: ?title)
      (xyz.task/list of: ?t is: ?l as: :entity cardinality: :one)
      (xyz.task/title of: ?t is: ?title as: :text cardinality: :one))
    (concept (task/tag ?t tag: ?g)
      (xyz.task/tag of: ?t is: ?g as: :text cardinality: :many))"#;

const INITIAL: &str = r#"
    (assert (shelf list:home name: "Home"))
    (assert (shelf list:work name: "Work"))
    (assert (task task:1 list: list:home title: "Buy milk"))
    (assert (task task:2 list: list:home title: "Call mum"))
    (assert (task/tag task:1 tag: "home"))"#;

const RECORD: &str = r#"
    (query (task/data ?t list: ?l is: ?card)
      (task ?t list: ?l title: ?title)
      (task/tag ?t tag: (distinct ?tags))
      (is ?card {:id ?t :title ?title :tags ?tags}))
    (query (shelf/data ?l is: ?cards)
      (shelf ?l name: ?name)
      (task/data _ list: ?l is: (distinct ?cards)))
    (select (shelf/data list:home is: ?cards))"#;

const HTML: &str = r#"
    (query (task/chip ?t tag: ?g is: ?chip)
      (task/tag ?t tag: ?g)
      (view ?chip [:span {:class "tag"} ?g]))
    (query (task/line ?t list: ?l is: ?line)
      (task ?t list: ?l title: ?title)
      (task/chip ?t is: (distinct ?chips))
      (view ?line [:li ?title ?chips]))
    (query (shelf/card ?l is: ?card)
      (shelf ?l name: ?name)
      (task/line _ list: ?l is: (distinct ?lines))
      (view ?card [:section [:h2 ?name] [:ul ?lines]]))
    (select (shelf/card ?l is: ?card))"#;

async fn notebook() -> Result<Notebook> {
    let mut notebook = Notebook::open().await?;
    write(&mut notebook, CONCEPTS).await?;
    write(&mut notebook, INITIAL).await?;
    Ok(notebook)
}

async fn write(notebook: &mut Notebook, text: &str) -> Result<()> {
    let (_, diagnostics) = notebook.submit(text).await?;
    assert!(diagnostics.is_empty(), "{diagnostics:?}");
    Ok(())
}

fn compiled(query: &str) -> Result<Step> {
    let analysis = Analyzer::new()?.analyze(&parse(&format!("{CONCEPTS}{query}"))?);
    assert!(
        analysis.diagnostics.is_empty(),
        "{:?}",
        analysis.diagnostics
    );
    for tree in &analysis.analyzed {
        for step in compile(tree)? {
            if matches!(step, Step::Select(_) | Step::View(_)) {
                return Ok(step);
            }
        }
    }
    anyhow::bail!("missing selection")
}

fn apply(tree: &mut EditedTree<Value>, edits: Vec<TreeEdit<Value>>) -> Result<()> {
    for edit in edits {
        tree.apply(edit)?;
    }
    Ok(())
}

fn materialize(tree: &EditedTree<Value>, id: &str) -> Result<Syntax> {
    let node = RecordNode::decode(tree.value(id).expect("present node"))?;
    match node.content {
        RecordContent::Scalar(value) => Ok(Syntax::Constant(value)),
        RecordContent::Record | RecordContent::Set => {
            let mut entries = Vec::new();
            for child in tree.children(Some(id)) {
                let node = RecordNode::decode(tree.value(child).expect("present child"))?;
                entries.push((
                    Syntax::Reference(node.field.unwrap_or_default()),
                    materialize(tree, child)?,
                ));
            }
            if node.content == RecordContent::Set && entries.is_empty() {
                entries.push((Syntax::Reference(String::new()), Syntax::Blank));
            }
            Ok(Syntax::Map(entries))
        }
    }
}

fn canonical(tree: Syntax) -> Syntax {
    if let Syntax::Map(entries) = tree {
        let mut entries: Vec<_> = entries
            .into_iter()
            .map(|(key, value)| (key, canonical(value)))
            .collect();
        entries.sort_by_key(|(key, value)| (key.to_string(), value.to_string()));
        Syntax::Map(entries)
    } else {
        tree
    }
}

#[dialog_common::test]
fn it_reuses_frame_tokens_and_rejects_skips() -> Result<()> {
    let delta = vec![TreeEdit::Set {
        id: "long-existing-identifier".into(),
        value: Value::String("repeated string in the session".into()),
    }];
    let mut encoder = EditEncoder::default();
    let mut decoder = EditDecoder::default();
    let first = encoder.encode(&delta);
    let second = encoder.encode(&delta);
    assert!(second.len() < first.len());
    assert!(matches!(
        decoder.decode(&second),
        Err(FrameError::Sequence {
            expected: 0,
            actual: 1
        })
    ));
    assert_eq!(decoder.decode(&first)?, delta);
    assert_eq!(decoder.decode(&second)?, delta);
    assert!(matches!(
        decoder.decode(&second),
        Err(FrameError::Sequence {
            expected: 2,
            actual: 1
        })
    ));
    let map = dialog_corpus::Composite::map([
        (Value::String("name".into()), Value::String("first".into())),
        (Value::String("name".into()), Value::String("second".into())),
    ])?;
    let value = vec![TreeEdit::Set {
        id: "long-existing-identifier".into(),
        value: Value::Composite(map),
    }];
    assert_eq!(decoder.decode(&encoder.encode(&value))?, value);
    Ok(())
}

#[dialog_common::test]
async fn it_lowers_a_notation_record_to_one_tag_insert() -> Result<()> {
    let mut notebook = notebook().await?;
    let Step::Select(selection) = compiled(RECORD)? else {
        anyhow::bail!("expected record selection")
    };
    let live = Live::new(notebook.branch.clone());
    let mut watched = live.standing_tree(*selection.clone())?;
    let mut encoder = EditEncoder::default();
    let mut decoder = EditDecoder::default();
    let mut tree = EditedTree::default();
    let frame = watched
        .poll_frame(&notebook.operator, &mut encoder)
        .await?
        .expect("initial record");
    apply(&mut tree, decoder.decode(&frame)?)?;
    let root = tree.children(None).next().unwrap().to_string();
    assert_eq!(tree.children(Some(&root)).count(), 2);

    write(&mut notebook, r#"(assert (task/tag task:1 tag: "urgent"))"#).await?;
    let frame = watched
        .poll_frame(&notebook.operator, &mut encoder)
        .await?
        .expect("tag edit");
    let edits = decoder.decode(&frame)?;
    let [TreeEdit::Insert { at, value, .. }] = edits.as_slice() else {
        anyhow::bail!("expected just a tag insert: {edits:?}")
    };
    assert_eq!(
        RecordNode::decode(value)?.content,
        RecordContent::Scalar(Value::String("urgent".into()))
    );
    let tags = at.parent.clone().expect("tag set");
    apply(&mut tree, edits)?;
    assert_eq!(tree.children(Some(&tags)).count(), 2);
    assert_eq!(tree.children(Some(&root)).count(), 2);
    let selected = live.select(&selection, &notebook.operator).await?;
    let expected = selected[0].operand().unwrap().terms["is"].clone();
    assert_eq!(canonical(materialize(&tree, &root)?), canonical(expected));

    // A card can be visible only through the selected root. Its source
    // tag remains stored when the card leaves this tree.
    write(
        &mut notebook,
        r#"(assert (task task:1 list: list:work title: "Buy milk"))"#,
    )
    .await?;
    let edits = watched.poll(&notebook.operator).await?;
    assert!(matches!(edits.as_slice(), [TreeEdit::Remove { .. }]));
    apply(&mut tree, edits)?;
    assert_eq!(tree.children(Some(&root)).count(), 1);
    assert_eq!(tree.children(Some(&tags)).count(), 0);
    write(
        &mut notebook,
        r#"(assert (task task:1 list: list:home title: "Buy milk"))"#,
    )
    .await?;
    apply(&mut tree, watched.poll(&notebook.operator).await?)?;
    assert_eq!(tree.children(Some(&tags)).count(), 2);

    let unpinned = RECORD.replace(
        "(select (shelf/data list:home is: ?cards))",
        "(select (shelf/data ?l is: ?cards))",
    );
    let Step::Select(selection) = compiled(&unpinned)? else {
        anyhow::bail!("expected record selection")
    };
    let mut both = live.standing_tree(*selection)?;
    let mut both_tree = EditedTree::default();
    apply(&mut both_tree, both.poll(&notebook.operator).await?)?;
    write(
        &mut notebook,
        r#"(assert (task task:1 list: list:work title: "Buy milk"))"#,
    )
    .await?;
    let edits = both.poll(&notebook.operator).await?;
    assert!(matches!(edits.as_slice(), [TreeEdit::Move { .. }]));
    apply(&mut both_tree, edits)?;
    assert_eq!(both_tree.children(Some(&tags)).count(), 2);
    Ok(())
}

#[derive(Default)]
struct NativeHost {
    values: Vec<HtmlNode>,
    placed: Vec<(usize, Option<usize>, Option<usize>)>,
    removed: Vec<usize>,
}

impl HtmlHost for NativeHost {
    type Node = usize;
    fn create(&mut self, value: HtmlNode) -> Result<usize, String> {
        let id = self.values.len();
        self.values.push(value);
        Ok(id)
    }
    fn set(&mut self, node: &usize, value: HtmlNode) -> Result<(), String> {
        self.values[*node] = value;
        Ok(())
    }
    fn place(
        &mut self,
        node: &usize,
        parent: Option<&usize>,
        before: Option<&usize>,
    ) -> Result<(), String> {
        self.placed.push((*node, parent.copied(), before.copied()));
        Ok(())
    }
    fn remove(&mut self, node: &usize) -> Result<(), String> {
        self.removed.push(*node);
        Ok(())
    }
}

async fn exercise_html<H: HtmlHost>(
    host: H,
) -> Result<(HtmlMount<H>, usize, Vec<TreeEdit<Value>>)> {
    let mut notebook = notebook().await?;
    let Step::View(plan) = compiled(HTML)? else {
        anyhow::bail!("expected HTML selection")
    };
    let live = Live::new(notebook.branch.clone());
    let mut watched = live.watch_html(*plan, None)?;
    let mut encoder = EditEncoder::default();
    let mut decoder = EditDecoder::default();
    let mut mount = HtmlMount::new(host);
    let frame = watched
        .poll_frame(&notebook.operator, &mut encoder)
        .await?
        .expect("initial HTML");
    let seed = decoder.decode(&frame)?;
    let seeded = seed.len();
    mount.apply(seed).map_err(anyhow::Error::msg)?;
    write(&mut notebook, r#"(assert (task/tag task:1 tag: "urgent"))"#).await?;
    let frame = watched
        .poll_frame(&notebook.operator, &mut encoder)
        .await?
        .expect("chip frame");
    assert!(matches!(
        decoder.decode(&frame[..frame.len() - 1]),
        Err(FrameError::Malformed)
    ));
    let delta = decoder.decode(&frame)?;
    assert_eq!(delta.len(), 2, "one element and its text");
    mount.apply(delta.clone()).map_err(anyhow::Error::msg)?;
    Ok((mount, seeded, delta))
}

#[dialog_common::test]
async fn it_mounts_one_chip_and_moves_the_same_handle() -> Result<()> {
    let (mut mount, seeded, delta) = exercise_html(NativeHost::default()).await?;
    assert_eq!(mount.host().values.len(), seeded + 2);
    assert!(mount.host().removed.is_empty());
    assert!(matches!(&mount.host().values[seeded], HtmlNode::Element { tag, .. } if tag == "span"));
    assert_eq!(
        mount.host().values[seeded + 1],
        HtmlNode::Text("urgent".into())
    );
    let TreeEdit::Insert { id, at, .. } = &delta[0] else {
        anyhow::bail!("expected insert")
    };
    // A move gives the host the same element handle and no child payload.
    mount
        .apply(vec![TreeEdit::Move {
            id: id.clone(),
            to: TreePosition {
                parent: at.parent.clone(),
                before: None,
            },
        }])
        .map_err(anyhow::Error::msg)?;
    assert_eq!(mount.host().values.len(), seeded + 2);
    assert_eq!(mount.host().placed.last().unwrap().0, seeded);
    Ok(())
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
#[wasm_bindgen_test::wasm_bindgen_test]
async fn it_inserts_the_chip_into_the_actual_dom() -> Result<()> {
    use dialog_notation::view::html::BrowserHost;
    use web_sys::Element;
    let document: web_sys::Document = js_sys::Reflect::get(&js_sys::global(), &"document".into())
        .map_err(|err| anyhow::anyhow!("{err:?}"))?
        .into();
    let root = document
        .create_element("main")
        .map_err(|err| anyhow::anyhow!("{err:?}"))?;
    let host =
        BrowserHost::new(root.clone(), |_: &Element, _| Ok(())).map_err(anyhow::Error::msg)?;
    let (mut mount, _, delta) = exercise_html(host).await?;
    assert!(root.inner_html().contains(
        r#"<li>Buy milk<span class="tag">home</span><span class="tag">urgent</span></li>"#
    ));
    let chip = root
        .query_selector("li > span:last-child")
        .map_err(|err| anyhow::anyhow!("{err:?}"))?
        .expect("tag chip");
    let [TreeEdit::Insert { id, .. }, _] = delta.as_slice() else {
        anyhow::bail!("expected chip")
    };
    mount
        .apply(vec![TreeEdit::Move {
            id: id.clone(),
            to: TreePosition {
                parent: None,
                before: None,
            },
        }])
        .map_err(anyhow::Error::msg)?;
    assert!(chip.is_same_node(root.last_child().as_ref()));
    assert_eq!(chip.text_content().as_deref(), Some("urgent"));
    Ok(())
}
