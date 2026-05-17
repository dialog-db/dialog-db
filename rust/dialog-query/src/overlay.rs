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
//! Both [`InMemoryFacts`] and [`Overlay`] implement
//! [`Update`](dialog_artifacts::Update), so the `assert<C: Statement>` /
//! `retract<C: Statement>` API mirrors the one on
//! [`Transaction`](#crate-tx) — any concept instance, attribute expression,
//! or other [`Statement`](dialog_artifacts::Statement) writes the same way.
//!
//! ```ignore
//! use dialog_query::overlay::{Overlay, Overlaid};
//!
//! let overlay = Overlay::new()
//!     .assert(Employee { this: id, name: Name("Alice".into()), role: Role("PM".into()) })
//!     .register(my_rule)?;
//!
//! let env = Overlaid::new(branch_env, overlay);
//! let results = my_query.perform(&env).try_vec().await?;
//! ```

use dialog_artifacts::selector::Constrained;
use dialog_artifacts::{
    Artifact, ArtifactSelector, ArtifactStream, Attribute, DialogArtifactsError, Entity, Select,
    Statement, Update, Value,
};
use dialog_capability::Provider;
use dialog_common::ConditionalSync;
use futures_util::stream::{self, StreamExt};

use crate::concept::descriptor::ConceptDescriptor;
use crate::concept::query::ConceptRules;
use crate::error::EvaluationError;
use crate::query::Application;
use crate::rule::When;
use crate::rule::deductive::DeductiveRule;
use crate::session::RuleRegistry;
use crate::source::SelectRules;

/// An in-memory collection of artifacts that serves as a fact source.
///
/// Implements [`Update`] so [`assert`](Self::assert) and
/// [`retract`](Self::retract) accept any [`Statement`] — the same surface
/// used by transactions. Implements [`Provider<Select<'a>>`] by filtering
/// its facts against the selector.
#[derive(Debug, Default, Clone)]
pub struct InMemoryFacts {
    facts: Vec<Artifact>,
}

impl InMemoryFacts {
    /// Create an empty in-memory fact store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Assert a [`Statement`] into the overlay — same shape as
    /// [`Transaction::assert`](#crate-tx).
    pub fn assert<S: Statement>(mut self, statement: S) -> Self {
        statement.assert(&mut self);
        self
    }

    /// Retract a [`Statement`] from the overlay.
    ///
    /// Removes any artifact whose `(the, of, is)` triple was previously
    /// asserted by the same statement.
    pub fn retract<S: Statement>(mut self, statement: S) -> Self {
        statement.retract(&mut self);
        self
    }

    /// The stored artifacts.
    pub fn facts(&self) -> &[Artifact] {
        &self.facts
    }

    /// Append every artifact from `other` to this store.
    pub fn extend(mut self, other: InMemoryFacts) -> Self {
        self.facts.extend(other.facts);
        self
    }
}

impl Update for InMemoryFacts {
    fn associate(&mut self, the: Attribute, of: Entity, is: Value) {
        self.facts.push(Artifact {
            the,
            of,
            is,
            cause: None,
        });
    }

    fn associate_unique(&mut self, the: Attribute, of: Entity, is: Value) {
        // Cardinality-one supersedes any prior (the, of) entry.
        self.facts.retain(|a| !(a.the == the && a.of == of));
        self.facts.push(Artifact {
            the,
            of,
            is,
            cause: None,
        });
    }

    fn dissociate(&mut self, the: Attribute, of: Entity, is: Value) {
        self.facts
            .retain(|a| !(a.the == the && a.of == of && a.is == is));
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
/// `Overlay` is the bundle a [`QuerySession`](crate::session) wires onto a
/// real fact source. Facts asserted here are unioned with the primary during
/// evaluation; rules registered here are merged with the primary's rules
/// per concept so both sets contribute candidates.
///
/// The fact-mutation surface mirrors
/// [`Transaction`](#crate-tx): use [`assert`](Self::assert) /
/// [`retract`](Self::retract) with any [`Statement`].
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

    /// Assert a [`Statement`] into the overlay.
    pub fn assert<S: Statement>(mut self, statement: S) -> Self {
        statement.assert(&mut self);
        self
    }

    /// Retract a [`Statement`] from the overlay.
    pub fn retract<S: Statement>(mut self, statement: S) -> Self {
        statement.retract(&mut self);
        self
    }

    /// Register a pre-built deductive rule on this overlay.
    pub fn register(mut self, rule: DeductiveRule) -> Result<Self, EvaluationError> {
        self.rules.register(rule)?;
        Ok(self)
    }

    /// Install a deductive rule from a closure that builds its body from a
    /// fresh query of the conclusion concept.
    ///
    /// The closure receives a default-constructed `Query<M>`; whatever
    /// premises it returns become the rule body.
    ///
    /// ```ignore
    /// let overlay = Overlay::new()
    ///     .install(|employee: Query<Employee>| {
    ///         (
    ///             Query::<Stuff> { this: employee.this.clone(), ... },
    ///             ...
    ///         )
    ///     })?;
    /// ```
    pub fn install<M, W>(self, rule: impl Fn(M) -> W) -> Result<Self, EvaluationError>
    where
        M: Application + Default + Into<ConceptDescriptor>,
        W: When,
    {
        let query = M::default();
        let concept: ConceptDescriptor = query.clone().into();
        let when = rule(query).into_premises();
        let premises = when.into_vec();
        let rule =
            DeductiveRule::new(concept, premises).map_err(|e| EvaluationError::Planning {
                message: e.to_string(),
            })?;
        self.register(rule)
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

    /// Merge another overlay's facts and rules into this one.
    ///
    /// Facts append; rules merge per-concept via [`RuleRegistry::extend`].
    pub fn extend(mut self, other: Overlay) -> Result<Self, EvaluationError> {
        self.facts = self.facts.extend(other.facts);
        self.rules.extend(&other.rules)?;
        Ok(self)
    }
}

impl Update for Overlay {
    fn associate(&mut self, the: Attribute, of: Entity, is: Value) {
        self.facts.associate(the, of, is);
    }

    fn associate_unique(&mut self, the: Attribute, of: Entity, is: Value) {
        self.facts.associate_unique(the, of, is);
    }

    fn dissociate(&mut self, the: Attribute, of: Entity, is: Value) {
        self.facts.dissociate(the, of, is);
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

    extern crate self as dialog_query;

    use super::*;
    use crate::artifact::Entity;
    use crate::attribute::query::AttributeQuery;
    use crate::attribute::{AttributeDescriptor, Cardinality, Type};
    use crate::concept::descriptor::ConceptDescriptor;
    use crate::query::Output;
    use crate::session::RuleRegistry;
    use crate::source::test::TestEnv;
    use crate::{Term, the};
    use dialog_repository::helpers::{test_operator_with_profile, test_repo};

    // Helper: build a simple two-attribute concept descriptor.
    fn person_concept() -> ConceptDescriptor {
        ConceptDescriptor::from([
            (
                "name",
                AttributeDescriptor::new(
                    the!("person/name"),
                    "person name",
                    Cardinality::One,
                    Some(Type::String),
                ),
            ),
            (
                "age",
                AttributeDescriptor::new(
                    the!("person/age"),
                    "person age",
                    Cardinality::One,
                    Some(Type::UnsignedInt),
                ),
            ),
        ])
    }

    // -- InMemoryFacts -----------------------------------------------------

    #[dialog_common::test]
    async fn in_memory_facts_filters_by_attribute() -> anyhow::Result<()> {
        let entity: Entity = "id:branch".parse()?;
        let overlay = InMemoryFacts::new()
            .assert(
                the!("dialog.meta/name")
                    .of(entity.clone())
                    .is("main".to_string()),
            )
            .assert(
                the!("dialog.meta/upstream")
                    .of(entity.clone())
                    .is("origin".to_string()),
            );

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
    async fn in_memory_facts_filters_by_entity() -> anyhow::Result<()> {
        let alice: Entity = "id:alice".parse()?;
        let bob: Entity = "id:bob".parse()?;
        let overlay = InMemoryFacts::new()
            .assert(
                the!("person/name")
                    .of(alice.clone())
                    .is("Alice".to_string()),
            )
            .assert(the!("person/name").of(bob).is("Bob".to_string()));

        let selector = ArtifactSelector::new().of(alice.clone());
        let stream = Provider::<Select<'_>>::execute(&overlay, selector).await?;
        let results: Vec<_> = stream.collect::<Vec<_>>().await;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].as_ref().unwrap().of, alice);
        Ok(())
    }

    #[dialog_common::test]
    async fn in_memory_facts_filters_by_value() -> anyhow::Result<()> {
        let alice: Entity = "id:alice".parse()?;
        let bob: Entity = "id:bob".parse()?;
        let overlay = InMemoryFacts::new()
            .assert(
                the!("person/name")
                    .of(alice)
                    .is("Alice".to_string()),
            )
            .assert(the!("person/name").of(bob).is("Bob".to_string()));

        let selector = ArtifactSelector::new().is(Value::String("Bob".into()));
        let stream = Provider::<Select<'_>>::execute(&overlay, selector).await?;
        let results: Vec<_> = stream.collect::<Vec<_>>().await;
        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0].as_ref().unwrap().is,
            Value::String("Bob".into())
        );
        Ok(())
    }

    #[dialog_common::test]
    async fn in_memory_facts_retract_removes_matching_fact() -> anyhow::Result<()> {
        let entity: Entity = "id:branch".parse()?;
        let overlay = InMemoryFacts::new()
            .assert(
                the!("dialog.meta/name")
                    .of(entity.clone())
                    .is("main".to_string()),
            )
            .retract(
                the!("dialog.meta/name")
                    .of(entity.clone())
                    .is("main".to_string()),
            );

        assert!(overlay.facts().is_empty(), "retracted fact should be gone");
        Ok(())
    }

    #[dialog_common::test]
    async fn in_memory_facts_cardinality_one_supersedes_prior_value() -> anyhow::Result<()> {
        // Asserting a cardinality-one attribute twice should keep only the
        // latest value — `Update::associate_unique` retains nothing for
        // the matching (the, of) pair before appending.
        let alice: Entity = "id:alice".parse()?;
        let mut overlay = InMemoryFacts::new();
        // Bypass the Statement layer to drive `associate_unique` directly.
        overlay.associate_unique(
            "person/name".parse()?,
            alice.clone(),
            Value::String("Alice".into()),
        );
        overlay.associate_unique(
            "person/name".parse()?,
            alice.clone(),
            Value::String("Alicia".into()),
        );

        assert_eq!(overlay.facts().len(), 1, "prior value should be replaced");
        assert_eq!(overlay.facts()[0].is, Value::String("Alicia".into()));
        Ok(())
    }

    #[dialog_common::test]
    async fn in_memory_facts_extend_appends_other_facts() -> anyhow::Result<()> {
        let alice: Entity = "id:alice".parse()?;
        let bob: Entity = "id:bob".parse()?;
        let a = InMemoryFacts::new().assert(
            the!("person/name")
                .of(alice.clone())
                .is("Alice".to_string()),
        );
        let b =
            InMemoryFacts::new().assert(the!("person/name").of(bob.clone()).is("Bob".to_string()));

        let merged = a.extend(b);
        assert_eq!(merged.facts().len(), 2);
        let entities: Vec<_> = merged.facts().iter().map(|f| f.of.clone()).collect();
        assert!(entities.contains(&alice));
        assert!(entities.contains(&bob));
        Ok(())
    }

    // -- Overlay -----------------------------------------------------------

    #[dialog_common::test]
    async fn overlay_assert_routes_to_underlying_facts() -> anyhow::Result<()> {
        let alice: Entity = "id:alice".parse()?;
        let overlay = Overlay::new().assert(
            the!("person/name")
                .of(alice.clone())
                .is("Alice".to_string()),
        );

        assert_eq!(overlay.facts().facts().len(), 1);
        assert_eq!(overlay.facts().facts()[0].of, alice);
        Ok(())
    }

    #[dialog_common::test]
    async fn overlay_register_records_rule() -> anyhow::Result<()> {
        let descriptor = person_concept();
        let rule = DeductiveRule::from(&descriptor);
        let overlay = Overlay::new().register(rule.clone())?;

        let acquired = overlay.rules().acquire(&descriptor)?;
        assert_eq!(acquired.installed().len(), 1);
        assert_eq!(acquired.installed()[0], rule);
        Ok(())
    }

    #[dialog_common::test]
    async fn overlay_install_closure_records_derived_rule() -> anyhow::Result<()> {
        // `install` is a sugar over `register` that builds a DeductiveRule
        // from a closure. We verify it ends up in the registry.
        use crate::rule::When;
        use crate::{Concept, Query};

        mod attrs {
            #[derive(crate::Attribute, Clone, PartialEq)]
            pub struct Name(pub String);
        }

        #[derive(Clone, Debug, PartialEq, Concept)]
        pub struct Greeter {
            pub this: Entity,
            pub name: attrs::Name,
        }

        // Body re-states the implicit attribute query — keeps the test
        // focused on install plumbing, not derivation semantics (covered
        // by the session-level tests).
        fn greeter_rule(g: Query<Greeter>) -> impl When {
            (AttributeQuery::new(
                Term::from(the!("attrs/name")),
                g.this,
                g.name.into(),
                Term::blank(),
                None,
            ),)
        }

        let overlay = Overlay::new().install(greeter_rule)?;
        let descriptor: ConceptDescriptor = Query::<Greeter>::default().into();
        let acquired = overlay.rules().acquire(&descriptor)?;
        assert_eq!(
            acquired.installed().len(),
            1,
            "install should add exactly one rule"
        );
        Ok(())
    }

    #[dialog_common::test]
    async fn overlay_extend_merges_facts_and_rules() -> anyhow::Result<()> {
        let alice: Entity = "id:alice".parse()?;
        let bob: Entity = "id:bob".parse()?;
        let descriptor = person_concept();
        let rule = DeductiveRule::from(&descriptor);

        let a = Overlay::new().assert(
            the!("person/name")
                .of(alice.clone())
                .is("Alice".to_string()),
        );
        let b = Overlay::new()
            .assert(the!("person/name").of(bob.clone()).is("Bob".to_string()))
            .register(rule.clone())?;

        let merged = a.extend(b)?;
        assert_eq!(merged.facts().facts().len(), 2);
        assert_eq!(merged.rules().acquire(&descriptor)?.installed().len(), 1);
        Ok(())
    }

    #[dialog_common::test]
    async fn overlay_rules_mut_allows_in_place_registration() -> anyhow::Result<()> {
        let descriptor = person_concept();
        let rule = DeductiveRule::from(&descriptor);

        let mut overlay = Overlay::new();
        overlay.rules_mut().register(rule.clone())?;
        assert_eq!(overlay.rules().acquire(&descriptor)?.installed()[0], rule);
        Ok(())
    }

    #[dialog_common::test]
    async fn overlay_select_rules_returns_default_for_unknown_concept() -> anyhow::Result<()> {
        // Overlay::SelectRules should always return a ConceptRules — the
        // implicit per-concept rule is created on demand.
        let overlay = Overlay::new();
        let descriptor = person_concept();
        let rules = Provider::<SelectRules>::execute(&overlay, descriptor).await?;
        assert!(
            rules.installed().is_empty(),
            "no rules installed, only implicit"
        );
        Ok(())
    }

    // -- Overlaid ----------------------------------------------------------

    #[dialog_common::test]
    async fn overlaid_chains_select_streams_from_both_sides() -> anyhow::Result<()> {
        let alice: Entity = "id:alice".parse()?;
        let bob: Entity = "id:bob".parse()?;
        let primary = InMemoryFacts::new().assert(
            the!("person/name")
                .of(alice.clone())
                .is("Alice".to_string()),
        );
        let overlay =
            InMemoryFacts::new().assert(the!("person/name").of(bob.clone()).is("Bob".to_string()));

        let combined = Overlaid::new(primary, overlay);
        let selector = ArtifactSelector::new().the("person/name".parse()?);
        let stream = Provider::<Select<'_>>::execute(&combined, selector).await?;
        let results: Vec<_> = stream.collect::<Vec<_>>().await;
        assert_eq!(results.len(), 2);
        let entities: Vec<_> = results
            .into_iter()
            .map(|r| r.unwrap().of)
            .collect();
        assert!(entities.contains(&alice));
        assert!(entities.contains(&bob));
        Ok(())
    }

    #[dialog_common::test]
    async fn overlaid_merges_rules_from_both_sides() -> anyhow::Result<()> {
        // Two RuleRegistries each carrying a distinct deductive rule for the
        // same concept; Overlaid should expose both via its SelectRules
        // provider so planning sees every alternative.
        let descriptor = person_concept();

        // Build two distinct rules so install dedup is no-op.
        let mut primary = RuleRegistry::new();
        let mut overlay_rules = RuleRegistry::new();

        let rule_a = DeductiveRule::from(&descriptor);
        // Construct a second, distinct rule for the same concept by adding
        // an extra premise.
        let rule_b = DeductiveRule::new(
            descriptor.clone(),
            vec![
                AttributeQuery::new(
                    Term::from(the!("person/name")),
                    Term::var("this"),
                    Term::var("name"),
                    Term::blank(),
                    None,
                )
                .into(),
                AttributeQuery::new(
                    Term::from(the!("person/age")),
                    Term::var("this"),
                    Term::var("age"),
                    Term::blank(),
                    None,
                )
                .into(),
            ],
        )?;
        assert_ne!(rule_a, rule_b);

        primary.register(rule_a.clone())?;
        overlay_rules.register(rule_b.clone())?;

        let combined = Overlaid::new(primary, overlay_rules);
        let acquired = Provider::<SelectRules>::execute(&combined, descriptor).await?;
        let installed = acquired.installed();
        assert_eq!(installed.len(), 2);
        assert!(installed.contains(&rule_a));
        assert!(installed.contains(&rule_b));
        Ok(())
    }

    #[dialog_common::test]
    async fn overlaid_primary_and_overlay_accessors_borrow_underlying() {
        let primary = InMemoryFacts::new();
        let overlay = InMemoryFacts::new();
        let combined = Overlaid::new(primary, overlay);
        // Accessors compile-check the return types and don't move the
        // underlying values.
        assert!(combined.primary().facts().is_empty());
        assert!(combined.overlay().facts().is_empty());
    }

    #[dialog_common::test]
    async fn overlaid_clone_clones_both_sides() -> anyhow::Result<()> {
        let alice: Entity = "id:alice".parse()?;
        let primary = InMemoryFacts::new().assert(
            the!("person/name")
                .of(alice)
                .is("Alice".to_string()),
        );
        let combined = Overlaid::new(primary, InMemoryFacts::new());
        let cloned = combined.clone();
        assert_eq!(cloned.primary().facts().len(), 1);
        assert!(cloned.overlay().facts().is_empty());
        Ok(())
    }

    #[dialog_common::test]
    async fn overlay_unions_primary_and_overlay_facts() -> anyhow::Result<()> {
        // End-to-end: a real branch as primary, an in-memory Overlay as the
        // overlay; queries see both sides.
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

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

        let synthetic: Entity = "id:branch".parse()?;
        let overlay = Overlay::new().assert(
            the!("dialog.meta/name")
                .of(synthetic.clone())
                .is("main".to_string()),
        );

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
