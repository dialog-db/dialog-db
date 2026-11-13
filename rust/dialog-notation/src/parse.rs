//! Parsing: the first phase, from text to syntax trees.
//!
//! Nothing here knows what a form means. A name is a
//! [`Syntax::Reference`] whatever it names, and whether a form defines
//! one is for the [analyzer](crate::analyze) to say.

use crate::form::read;
use crate::{Error, Syntax};

/// The syntax trees `text` says, one a top-level form, in the order
/// written.
pub fn parse(text: &str) -> Result<Vec<Syntax>, Error> {
    read(text)?.iter().map(Syntax::try_from).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[dialog_common::test]
    fn it_parses_each_top_level_form_to_a_tree() -> Result<(), Error> {
        let trees = parse("(todo ?t state: :open) (xyz.todo todo:1 text: \"milk\")")?;
        assert_eq!(trees.len(), 2);
        assert!(matches!(&trees[0], Syntax::Application(operator, _)
            if **operator == Syntax::Reference("todo".into())));
        Ok(())
    }

    #[dialog_common::test]
    fn it_says_the_same_tree_however_the_labels_are_ordered() -> Result<(), Error> {
        assert_eq!(
            parse("(todo ?t state: :open text: ?x)")?,
            parse("(todo text: ?x of: ?t state: :open)")?
        );
        Ok(())
    }

    #[dialog_common::test]
    fn it_refuses_text_that_is_not_forms() {
        assert!(matches!(parse("(todo ?t"), Err(Error::Read(_))));
    }
}
