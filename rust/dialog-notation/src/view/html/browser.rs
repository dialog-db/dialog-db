use wasm_bindgen::{JsCast as _, JsValue};
use web_sys::{Document, Element, Node};

use super::{HtmlHost, HtmlNode, Posted};

/// Mounts trusted notation view edits into a real browser element.
/// The supplied callback reconciles notation event bindings with the
/// application's command dispatcher.
pub struct BrowserHost<F> {
    root: Element,
    document: Document,
    events: F,
}

impl<F> BrowserHost<F> {
    /// Use an empty element as the mount point.
    pub fn new(root: Element, events: F) -> Result<Self, String> {
        if root.has_child_nodes() {
            return Err("the HTML mount root must be empty".into());
        }
        let document = root
            .owner_document()
            .ok_or("the mount root has no document")?;
        Ok(Self {
            root,
            document,
            events,
        })
    }
}

fn dom(error: JsValue) -> String {
    error.as_string().unwrap_or_else(|| format!("{error:?}"))
}

impl<F: FnMut(&Element, Vec<Posted>) -> Result<(), String>> HtmlHost for BrowserHost<F> {
    type Node = Node;

    fn create(&mut self, value: HtmlNode) -> Result<Node, String> {
        let node = match &value {
            HtmlNode::Text(text) => return Ok(self.document.create_text_node(text).into()),
            HtmlNode::Element { tag, .. } => {
                Node::from(self.document.create_element(tag).map_err(dom)?)
            }
        };
        self.set(&node, value)?;
        Ok(node)
    }

    fn set(&mut self, node: &Node, value: HtmlNode) -> Result<(), String> {
        match value {
            HtmlNode::Text(text) if node.node_type() == Node::TEXT_NODE => {
                node.set_node_value(Some(&text));
                Ok(())
            }
            HtmlNode::Text(_) => Err("an HTML element cannot become text".into()),
            HtmlNode::Element {
                tag,
                attributes,
                events,
            } => {
                let element = node
                    .dyn_ref::<Element>()
                    .ok_or("expected an HTML element")?;
                if !element.local_name().eq_ignore_ascii_case(&tag) {
                    return Err("an HTML edit cannot change an element's tag".into());
                }
                for key in element
                    .get_attribute_names()
                    .iter()
                    .filter_map(|key| key.as_string())
                {
                    if !attributes.iter().any(|(name, _)| name == &key) {
                        element.remove_attribute(&key).map_err(dom)?;
                    }
                }
                for (key, value) in attributes {
                    if element.get_attribute(&key).as_deref() != Some(&value) {
                        element.set_attribute(&key, &value).map_err(dom)?;
                    }
                }
                (self.events)(element, events)
            }
        }
    }

    fn place(
        &mut self,
        node: &Node,
        parent: Option<&Node>,
        before: Option<&Node>,
    ) -> Result<(), String> {
        let parent = parent.unwrap_or(self.root.as_ref());
        if node.parent_node().is_some() && node.is_connected() == parent.is_connected() {
            let method =
                js_sys::Reflect::get(parent, &JsValue::from_str("moveBefore")).map_err(dom)?;
            if let Some(method) = method.dyn_ref::<js_sys::Function>() {
                let before = before.map_or(JsValue::NULL, |node| JsValue::from(node.clone()));
                method.call2(parent, node, &before).map_err(dom)?;
                return Ok(());
            }
        }
        parent.insert_before(node, before).map(|_| ()).map_err(dom)
    }

    fn remove(&mut self, node: &Node) -> Result<(), String> {
        node.parent_node()
            .ok_or("the removed HTML node has no parent")?
            .remove_child(node)
            .map(|_| ())
            .map_err(dom)
    }
}
