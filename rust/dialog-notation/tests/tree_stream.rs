use anyhow::Result;
use dialog_corpus::Value;
use dialog_notation::live::{Live, WatchedHtml};
use dialog_notation::view::html::{HtmlHost, HtmlMount, HtmlNode};
use dialog_notation::{Analyzer, RecordContent, RecordNode, Step, Syntax, compile, parse};
use dialog_repository::tree_edits::wire::{EditDecoder, EditEncoder, FrameError};
use dialog_repository::{EditedTree, TreeEdit};

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

struct HtmlSession<H: HtmlHost> {
    notebook: Notebook,
    watched: WatchedHtml,
    encoder: EditEncoder,
    decoder: EditDecoder,
    mount: HtmlMount<H>,
}

impl<H: HtmlHost> HtmlSession<H> {
    async fn open(host: H, notebook: Notebook, query: &str) -> Result<Self> {
        let Step::View(plan) = compiled(query)? else {
            anyhow::bail!("expected HTML selection")
        };
        let watched = Live::new(notebook.branch.clone()).watch_html(*plan, None)?;
        let mut session = Self {
            notebook,
            watched,
            encoder: EditEncoder::default(),
            decoder: EditDecoder::default(),
            mount: HtmlMount::new(host),
        };
        let seed = session.frame().await?;
        session.apply(&seed)?;
        Ok(session)
    }

    async fn frame(&mut self) -> Result<Vec<u8>> {
        Ok(self
            .watched
            .poll_frame(&self.notebook.operator, &mut self.encoder)
            .await?
            .expect("HTML frame"))
    }

    fn apply(&mut self, frame: &[u8]) -> Result<Vec<TreeEdit<Value>>> {
        let edits = self.decoder.decode(frame)?;
        self.mount
            .apply(edits.clone())
            .map_err(anyhow::Error::msg)?;
        Ok(edits)
    }

    async fn update(&mut self, source: &str) -> Result<Vec<TreeEdit<Value>>> {
        write(&mut self.notebook, source).await?;
        let frame = self.frame().await?;
        self.apply(&frame)
    }

    async fn add_chip(&mut self) -> Result<Vec<TreeEdit<Value>>> {
        write(
            &mut self.notebook,
            r#"(assert (task/tag task:1 tag: "urgent"))"#,
        )
        .await?;
        let frame = self.frame().await?;
        assert!(matches!(
            self.decoder.decode(&frame[..frame.len() - 1]),
            Err(FrameError::Malformed)
        ));
        let delta = self.apply(&frame)?;
        assert_eq!(delta.len(), 2, "one element and its text");
        Ok(delta)
    }
}

#[dialog_common::test]
async fn it_mounts_one_chip_and_moves_its_card_with_local_edits() -> Result<()> {
    let mut session = HtmlSession::open(NativeHost::default(), notebook().await?, HTML).await?;
    let seeded = session.mount.host().values.len();
    let delta = session.add_chip().await?;
    let host = session.mount.host();
    assert_eq!(host.values.len(), seeded + 2);
    assert!(host.removed.is_empty());
    assert!(matches!(&host.values[seeded], HtmlNode::Element { tag, .. } if tag == "span"));
    assert_eq!(host.values[seeded + 1], HtmlNode::Text("urgent".into()));
    let TreeEdit::Insert { at, .. } = &delta[0] else {
        anyhow::bail!("expected chip insert")
    };
    let card = at.parent.as_ref().expect("parent card");
    let card_handle = host.placed[seeded].1.expect("mounted parent card");
    let delta = session
        .update(
            r#"(assert (task task:1 list: list:work title: "Buy oat milk"))
               (retract (task/tag task:1 tag: "home"))
               (assert (task/tag task:1 tag: "work"))"#,
        )
        .await?;
    let [
        TreeEdit::Move { id, .. },
        TreeEdit::Set { value, .. },
        TreeEdit::Remove { .. },
        TreeEdit::Insert { at, .. },
        TreeEdit::Insert { .. },
    ] = delta.as_slice()
    else {
        anyhow::bail!("expected a card move with local edits: {delta:?}")
    };
    assert_eq!(id, card);
    assert_eq!(at.parent.as_ref(), Some(card));
    assert_eq!(
        HtmlNode::decode(value.clone())?,
        HtmlNode::Text("Buy oat milk".into())
    );
    assert_eq!(session.mount.host().placed[seeded + 2].0, card_handle);
    assert_eq!(session.mount.host().removed.len(), 1);

    let delta = session
        .update(
            r#"(retract (task/tag task:1 tag: "urgent"))
               (assert (task/tag task:2 tag: "urgent"))"#,
        )
        .await?;
    assert!(matches!(
        delta.as_slice(),
        [
            TreeEdit::Remove { .. },
            TreeEdit::Insert { .. },
            TreeEdit::Insert { .. }
        ]
    ));
    assert_eq!(session.mount.host().removed.last(), Some(&seeded));
    Ok(())
}

#[dialog_common::test]
async fn it_leaves_duplicate_card_instances_independent() -> Result<()> {
    let membership = r#"
        (concept (task/link ?t list: ?l)
          (xyz.task/link of: ?t is: ?l as: :entity cardinality: :many))"#;
    let query = format!("{membership}{HTML}").replace(
        "(task ?t list: ?l title: ?title)",
        "(task ?t title: ?title ...) (task/link ?t list: ?l)",
    );
    let mut notebook = notebook().await?;
    write(&mut notebook, membership).await?;
    write(
        &mut notebook,
        r#"(assert (shelf list:third name: "Third"))
           (assert (task/link task:1 list: list:home))
           (assert (task/link task:1 list: list:work))"#,
    )
    .await?;
    let mut session = HtmlSession::open(NativeHost::default(), notebook, &query).await?;
    let edits = session
        .update(
            r#"(retract (task/link task:1 list: list:home))
               (assert (task/link task:1 list: list:third))"#,
        )
        .await?;
    assert_eq!(edits.len(), 5);
    assert!(matches!(edits.first(), Some(TreeEdit::Remove { .. })));
    assert!(
        edits[1..]
            .iter()
            .all(|edit| matches!(edit, TreeEdit::Insert { .. }))
    );

    session
        .update(r#"(retract (task/link task:1 list: list:work))"#)
        .await?;
    let edits = session
        .update(
            r#"(retract (task/link task:1 list: list:third))
               (assert (task/link task:1 list: list:home))
               (assert (task/link task:1 list: list:work))"#,
        )
        .await?;
    assert_eq!(edits.len(), 9);
    assert!(matches!(edits.first(), Some(TreeEdit::Remove { .. })));
    assert!(
        edits[1..]
            .iter()
            .all(|edit| matches!(edit, TreeEdit::Insert { .. }))
    );
    Ok(())
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
#[wasm_bindgen_test::wasm_bindgen_test]
async fn it_moves_the_card_and_its_chip_in_the_actual_dom() -> Result<()> {
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
    let mut session = HtmlSession::open(host, notebook().await?, HTML).await?;
    session.add_chip().await?;
    assert!(root.inner_html().contains(
        r#"<li>Buy milk<span class="tag">home</span><span class="tag">urgent</span></li>"#
    ));
    let select = |selector| -> Result<Element> {
        Ok(root
            .query_selector(selector)
            .map_err(|err| anyhow::anyhow!("{err:?}"))?
            .expect("mounted element"))
    };
    let card = select("section:first-child li:first-child")?;
    let chip = select("section:first-child li:first-child > span:last-child")?;
    let delta = session
        .update(r#"(assert (task task:1 list: list:work title: "Buy milk"))"#)
        .await?;
    assert!(
        matches!(delta.as_slice(), [TreeEdit::Move { .. }]),
        "{delta:?}"
    );
    let moved = select("section:last-child li")?;
    let urgent = select("section:last-child li > span:last-child")?;
    assert!(card.is_same_node(Some(moved.as_ref())));
    assert!(chip.is_same_node(Some(urgent.as_ref())));
    assert_eq!(chip.text_content().as_deref(), Some("urgent"));

    let other = select("section:first-child li")?;
    let delta = session
        .update(
            r#"(assert (task task:1 list: list:home title: "Buy milk"))
               (assert (task task:2 list: list:work title: "Call mum"))"#,
        )
        .await?;
    assert!(
        matches!(
            delta.as_slice(),
            [TreeEdit::Move { .. }, TreeEdit::Move { .. }]
        ),
        "{delta:?}"
    );
    assert!(card.is_same_node(Some(select("section:first-child li")?.as_ref())));
    assert!(other.is_same_node(Some(select("section:last-child li")?.as_ref())));
    Ok(())
}
