//! Overlay support for query evaluation.
//!
//! An *overlay* is an auxiliary fact + rule source that gets unioned with a
//! primary source during query evaluation. Overlays expose synthetic,
//! in-memory information — branch metadata, system state, derived views —
//! alongside the real artifact store, queried with the same
//! [`Application`](crate::query::Application) API.
//!
//! The Datomic comparison: just as `:db/ident` and similar attributes show up
//! as regular facts that participate in queries, an overlay's artifacts are
//! indistinguishable from stored ones from the query engine's perspective.
//!
//! # Types
//!
//! - [`InMemoryFacts`] — a `Vec<Artifact>` exposed as a [`Provider<Select<'a>>`].
//! - [`Overlay`] — facts + a [`RuleRegistry`], implements both providers.
//! - [`Overlaid<P, O>`] — combines two sources; selects union, rules merge.
//!
//! ```ignore
//! use dialog_query::overlay::{Overlay, Overlaid};
//!
//! let overlay = Overlay::new()
//!     .fact("dialog.meta/name", "dialog:branch", "main".to_string())?
//!     .register(my_rule)?;
//!
//! let env = Overlaid::new(branch_env, overlay);
//! let results = my_query.perform(&env).try_vec().await?;
//! ```

use dialog_artifacts::selector::Constrained;
use dialog_artifacts::{
    Artifact, ArtifactSelector, ArtifactStream, DialogArtifactsError, Entity, Select, Value,
};
use dialog_capability::Provider;
use dialog_common::ConditionalSync;
use futures_util::stream::{self, StreamExt};

use crate::concept::descriptor::ConceptDescriptor;
use crate::concept::query::ConceptRules;
use crate::error::EvaluationError;
use crate::rule::deductive::DeductiveRule;
use crate::session::RuleRegistry;
use crate::source::SelectRules;

/// An in-memory collection of artifacts that serves as a fact source.
///
/// `InMemoryFacts` implements [`Provider<Select<'a>>`] by filtering its facts
/// against the selector. Use [`Overlay`] when you also need to attach rules.
#[derive(Debug, Default, Clone)]
pub struct InMemoryFacts {
    facts: Vec<Artifact>,
}

impl InMemoryFacts {
    /// Create an empty in-memory fact store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Append an artifact to the in-memory store.
    pub fn assert(mut self, artifact: Artifact) -> Self {
        self.facts.push(artifact);
        self
    }

    /// Append an artifact constructed from triple parts.
    pub fn fact(
        self,
        the: impl AsRef<str>,
        of: impl AsRef<str>,
        is: impl Into<Value>,
    ) -> Result<Self, DialogArtifactsError> {
        Ok(self.assert(Artifact {
            the: the.as_ref().parse()?,
            of: of.as_ref().parse::<Entity>()?,
            is: is.into(),
            cause: None,
        }))
    }

    /// The stored artifacts.
    pub fn facts(&self) -> &[Artifact] {
        &self.facts
    }
}

fn matches(artifact: &Artifact, selector: &ArtifactSelector<Constrained>) -> bool {
    if let Some(entity) = selector.entity()
        && entity != &artifact.of
    {
        return false;
    }
    if let Some(attribute) = selector.attribute()
        && attribute != &artifact.the
    {
        return false;
    }
    if let Some(value) = selector.value()
        && value != &artifact.is
    {
        return false;
    }
    true
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<'a> Provider<Select<'a>> for InMemoryFacts {
    async fn execute(
        &self,
        input: ArtifactSelector<Constrained>,
    ) -> Result<ArtifactStream<'a>, DialogArtifactsError> {
        let matching: Vec<Artifact> = self
            .facts
            .iter()
            .filter(|a| matches(a, &input))
            .cloned()
            .collect();
        Ok(Box::pin(stream::iter(matching.into_iter().map(Ok))))
    }
}

/// An in-memory overlay carrying both synthetic facts and deductive rules.
///
/// `Overlay` is the bundle a [`QuerySession`](crate::session) or similar wires
/// onto a real fact source. Facts asserted here are unioned with the primary
/// during evaluation; rules registered here are merged with the primary's
/// rules per concept so both sets contribute candidates.
#[derive(Debug, Default, Clone)]
pub struct Overlay {
    facts: InMemoryFacts,
    rules: RuleRegistry,
}

impl Overlay {
    /// Create an empty overlay.
    pub fn new() -> Self {
        Self::default()
    }

    /// Append an artifact.
    pub fn assert(mut self, artifact: Artifact) -> Self {
        self.facts = self.facts.assert(artifact);
        self
    }

    /// Append an artifact constructed from triple parts.
    pub fn fact(
        mut self,
        the: impl AsRef<str>,
        of: impl AsRef<str>,
        is: impl Into<Value>,
    ) -> Result<Self, DialogArtifactsError> {
        self.facts = self.facts.fact(the, of, is)?;
        Ok(self)
    }

    /// Register a pre-built deductive rule on this overlay.
    pub fn register(mut self, rule: DeductiveRule) -> Result<Self, EvaluationError> {
        self.rules.register(rule)?;
        Ok(self)
    }

    /// Borrow the underlying fact store.
    pub fn facts(&self) -> &InMemoryFacts {
        &self.facts
    }

    /// Borrow the underlying rule registry.
    pub fn rules(&self) -> &RuleRegistry {
        &self.rules
    }

    /// Mutably borrow the rule registry.
    pub fn rules_mut(&mut self) -> &mut RuleRegistry {
        &mut self.rules
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<'a> Provider<Select<'a>> for Overlay {
    async fn execute(
        &self,
        input: ArtifactSelector<Constrained>,
    ) -> Result<ArtifactStream<'a>, DialogArtifactsError> {
        Provider::<Select<'a>>::execute(&self.facts, input).await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<SelectRules> for Overlay {
    async fn execute(&self, input: ConceptDescriptor) -> Result<ConceptRules, EvaluationError> {
        self.rules.acquire(&input)
    }
}

/// A query environment that unions a primary source with an overlay.
///
/// `Overlaid` is the bridge that lets a query see both stored artifacts and
/// overlay-supplied ones, plus rules from either side. Selects yield results
/// from both sources (primary first, then overlay). Rules are merged per
/// concept: every installed rule from both sides contributes alternatives.
pub struct Overlaid<P, O> {
    primary: P,
    overlay: O,
}

impl<P, O> Overlaid<P, O> {
    /// Wrap a primary source with an overlay.
    pub fn new(primary: P, overlay: O) -> Self {
        Self { primary, overlay }
    }

    /// Borrow the primary source.
    pub fn primary(&self) -> &P {
        &self.primary
    }

    /// Borrow the overlay source.
    pub fn overlay(&self) -> &O {
        &self.overlay
    }
}

impl<P: Clone, O: Clone> Clone for Overlaid<P, O> {
    fn clone(&self) -> Self {
        Self {
            primary: self.primary.clone(),
            overlay: self.overlay.clone(),
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<'a, P, O> Provider<Select<'a>> for Overlaid<P, O>
where
    P: Provider<Select<'a>> + ConditionalSync,
    O: Provider<Select<'a>> + ConditionalSync,
{
    async fn execute(
        &self,
        input: ArtifactSelector<Constrained>,
    ) -> Result<ArtifactStream<'a>, DialogArtifactsError> {
        let primary = self.primary.execute(input.clone()).await?;
        let overlay = self.overlay.execute(input).await?;
        Ok(Box::pin(primary.chain(overlay)))
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<P, O> Provider<SelectRules> for Overlaid<P, O>
where
    P: Provider<SelectRules> + ConditionalSync,
    O: Provider<SelectRules> + ConditionalSync,
{
    async fn execute(&self, input: ConceptDescriptor) -> Result<ConceptRules, EvaluationError> {
        let mut primary = self.primary.execute(input.clone()).await?;
        let overlay = self.overlay.execute(input).await?;
        primary.extend(&overlay);
        Ok(primary)
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use crate::artifact::Entity;
    use crate::attribute::query::AttributeQuery;
    use crate::query::Output;
    use crate::session::RuleRegistry;
    use crate::source::test::TestEnv;
    use crate::{Term, the};
    use dialog_repository::helpers::{test_operator_with_profile, test_repo};

    #[dialog_common::test]
    async fn in_memory_facts_filters_by_attribute() -> anyhow::Result<()> {
        let entity: Entity = "dialog:branch".parse()?;
        let overlay = InMemoryFacts::new()
            .fact("dialog.meta/name", "dialog:branch", "main".to_string())?
            .fact(
                "dialog.meta/upstream",
                "dialog:branch",
                "origin".to_string(),
            )?;

        let selector = ArtifactSelector::new().the("dialog.meta/name".parse()?);
        let stream = Provider::<Select<'_>>::execute(&overlay, selector).await?;
        let results: Vec<_> = stream.collect::<Vec<_>>().await;

        assert_eq!(results.len(), 1);
        let artifact = results[0].as_ref().expect("artifact ok");
        assert_eq!(artifact.of, entity);
        assert_eq!(artifact.is, Value::String("main".into()));
        Ok(())
    }

    #[dialog_common::test]
    async fn overlay_unions_primary_and_overlay_facts() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        // Write one real fact under dialog.meta/name on a user entity.
        let alice = Entity::new()?;
        branch
            .transaction()
            .assert(
                the!("dialog.meta/name")
                    .of(alice.clone())
                    .is("Alice".to_string()),
            )
            .commit()
            .perform(&operator)
            .await?;

        // Overlay adds a synthetic "main" branch name fact.
        let synthetic: Entity = "dialog:branch".parse()?;
        let overlay = Overlay::new().fact(
            "dialog.meta/name",
            "dialog:branch",
            "main".to_string(),
        )?;

        let primary = TestEnv::new(&branch, &operator, RuleRegistry::new());
        let env = Overlaid::new(primary, overlay);

        let query = AttributeQuery::new(
            Term::from(the!("dialog.meta/name")),
            Term::var("of"),
            Term::var("is"),
            Term::blank(),
            None,
        );

        let results = query.perform(&env).try_vec().await?;
        assert_eq!(results.len(), 2, "should see both real and overlay facts");

        let names: Vec<_> = results.iter().map(|c| c.is().clone()).collect();
        assert!(names.contains(&Value::String("Alice".into())));
        assert!(names.contains(&Value::String("main".into())));

        // Sanity check: overlay-only entity must come back when filtered.
        let only_branch = AttributeQuery::new(
            Term::from(the!("dialog.meta/name")),
            Term::from(synthetic),
            Term::var("is"),
            Term::blank(),
            None,
        );
        let scoped = only_branch.perform(&env).try_vec().await?;
        assert_eq!(scoped.len(), 1);
        assert_eq!(scoped[0].is(), &Value::String("main".into()));
        Ok(())
    }
}
