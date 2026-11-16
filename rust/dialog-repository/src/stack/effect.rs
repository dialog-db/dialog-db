//! Effects: running a stack's providers over the commands a dispatch
//! landed, as a step of its own.
//!
//! A **provider** performs an effect for the commands of its concepts
//! ([`Provider`]): a function from the rows of one landed command to the
//! statements that answer it. [`Stack::provide`] registers one, and
//! records it in the stack's own layer, [`SESSION`](crate::SESSION), as
//! `session: dialog.provider/command <concept>`, one row per concept a
//! provider serves: what this process can perform, discoverable by a
//! query like any other row.
//!
//! A dispatch's landed rows of the [`control:`](crate::CONTROL) entity
//! itself are no command: they are the **tick**'s own rows (who asked,
//! and whatever else an embedder stamps on the loopback), which
//! [`InvocationReport::tick`] reports and every command of the tick
//! carries ([`Command::tick`]).
//!
//! Performing an invocation runs no provider. Its report
//! ([`InvocationReport`]) names the commands the dispatch landed on the
//! outside end of [`control:`](crate::CONTROL), and
//! [`InvocationReport::effect`] hands them, with the stack's providers,
//! to an [`Effect`]: a separate step an embedder performs when it
//! chooses, after its request returned (a worker's `waitUntil`), or
//! never. Performing the effect runs each command's provider and then
//! one answer dispatch through the stack that writes every answer, and
//! reports whether that dispatch landed commands of its own
//! ([`EffectReport::remaining`]), whose effect continues the chain.
//!
//! Every change in a command's life is a row of the command's entity in
//! `control:`, for one tick, so a rule premised on it fires and copies
//! what it wants into a layer that keeps it:
//!
//! ```text
//! <cmd>  dialog.command/invoked    <tick>          ; the tick the command is stimulus of
//! <cmd>  dialog.command/answered   <concept>       ; the answer dispatch, one per provider that answered
//! <cmd>  dialog.command/failed     "<reason>"      ; the answer dispatch, one per provider that failed
//! <cmd>  dialog.command/unhandled  true            ; the answer dispatch, when no provider claims it
//! <cmd>  dialog.command/progress   <value>         ; a provider's own intermediate answer
//! ```
//!
//! `invoked` names the tick of its dispatch, counted from 1, that the
//! command is stimulus of: the first, for one invoked, the one after
//! the tick whose rule dispatched it, for one a rule dispatched. It is
//! written by the dispatch's own commit, beside the command's rows, and
//! lands nowhere else. `answered`, `failed` and `unhandled` are written
//! by the effect's answer dispatch, and `progress` by the dispatch a
//! provider runs with [`Command::progress`]. None of them is a command:
//! a report leaves them out of what it names, and no provider is run
//! for them.

use std::collections::BTreeSet;
use std::error::Error;
use std::fmt::{Debug, Display, Formatter, Result as FmtResult};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, LazyLock};

use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_corpus::{Change, Changes, Entity, Fact, Pick, Relation, Update as _, Value};
use dialog_query::attribute::The;
use dialog_query::type_system::Type as Kind;
use dialog_query::{ConceptDescriptor, EvaluationError};

use super::invoke::{Invocation, InvocationReport};
use crate::placement::{control_layer, session_layer};
use crate::{CommitError, Revision, Stack, StackEnv};

static INVOKED_ATTR: LazyLock<Relation> =
    LazyLock::new(|| dialog_query::the!("dialog.command/invoked").into());
static ANSWERED_ATTR: LazyLock<Relation> =
    LazyLock::new(|| dialog_query::the!("dialog.command/answered").into());
static FAILED_ATTR: LazyLock<Relation> =
    LazyLock::new(|| dialog_query::the!("dialog.command/failed").into());
static UNHANDLED_ATTR: LazyLock<Relation> =
    LazyLock::new(|| dialog_query::the!("dialog.command/unhandled").into());
static PROGRESS_ATTR: LazyLock<Relation> =
    LazyLock::new(|| dialog_query::the!("dialog.command/progress").into());
static PROVIDED_ATTR: LazyLock<Relation> =
    LazyLock::new(|| dialog_query::the!("dialog.provider/command").into());

/// The domain of a command's lifecycle rows.
const LIFECYCLE: &str = "dialog.command/";

/// `dialog.command/invoked`: of a command's entity, the tick of its
/// dispatch it is stimulus of, counted from 1, as an unsigned integer.
pub fn invoked_attr() -> Relation {
    INVOKED_ATTR.clone()
}

/// `dialog.command/answered`: of a command's entity, the concept whose
/// provider answered it, as its entity. Cardinality-many: one row per
/// provider that answered.
pub fn answered_attr() -> Relation {
    ANSWERED_ATTR.clone()
}

/// `dialog.command/failed`: of a command's entity, the reason its
/// provider failed, as text. Cardinality-many: one row per provider that
/// failed.
pub fn failed_attr() -> Relation {
    FAILED_ATTR.clone()
}

/// `dialog.command/unhandled`: of a command's entity, `true` when no
/// provider of the stack claims it.
pub fn unhandled_attr() -> Relation {
    UNHANDLED_ATTR.clone()
}

/// `dialog.command/progress`: of a command's entity, an intermediate
/// answer its provider gave ([`Command::progress`]).
pub fn progress_attr() -> Relation {
    PROGRESS_ATTR.clone()
}

/// `dialog.provider/command`: of [`SESSION`](crate::SESSION), each
/// concept a provider registered with [`Stack::provide`] serves, as its
/// entity. Cardinality-many: one row per concept.
pub fn provided_attr() -> Relation {
    PROVIDED_ATTR.clone()
}

/// Whether `the` is a command's lifecycle row rather than a row of the
/// command itself.
pub(crate) fn is_lifecycle(the: &Relation) -> bool {
    the.as_str().starts_with(LIFECYCLE)
}

/// Whether `of` is the [`control:`](crate::CONTROL) entity itself, whose
/// rows are the tick's own ([`Command::tick`]) rather than a command's.
pub(crate) fn is_tick(of: &Entity) -> bool {
    *of == control_layer()
}

/// The `dialog.command/invoked` rows marking each command `transients`
/// carries as stimulus of the dispatch's tick `tick`: one per entity with
/// a row bound for no layer, a lifecycle row and the tick's own rows
/// aside.
pub(crate) fn invoked(transients: &Changes, tick: u32) -> Changes {
    let mut marked = Changes::new();
    let mut commands = BTreeSet::new();
    for (of, the, change) in transients.iter() {
        if matches!(change, Change::Assert(..))
            && !is_lifecycle(the)
            && !is_tick(of)
            && commands.insert(of)
        {
            marked.associate(
                invoked_attr(),
                of.clone(),
                Value::UnsignedInt(tick.into()),
                Pick::Last,
            );
        }
    }
    marked
}

/// The future a provider's handler returns.
#[cfg(not(target_arch = "wasm32"))]
type Answering = Pin<Box<dyn Future<Output = Result<Changes, Failure>> + Send>>;
/// The future a provider's handler returns.
#[cfg(target_arch = "wasm32")]
type Answering = Pin<Box<dyn Future<Output = Result<Changes, Failure>>>>;

/// A provider's handler.
#[cfg(not(target_arch = "wasm32"))]
type Handler = Arc<dyn Fn(Command) -> Answering + Send + Sync>;
/// A provider's handler.
#[cfg(target_arch = "wasm32")]
type Handler = Arc<dyn Fn(Command) -> Answering>;

/// Something that performs the effect of the commands of its concepts:
/// a handler from the rows of one landed command to the statements that
/// answer it. Registered with [`Stack::provide`], run by
/// [`Effect::perform`].
///
/// A provider claims a landed command when the command's rows satisfy
/// one of its concepts, the test decoding the concept from those rows
/// would pass: every *required* field of the concept has a row whose
/// attribute is one the field reads (for a collection field, one in its
/// domain) and, when the field declares a value type, whose value that
/// type reads (as is, or through a number view). Optional fields are not
/// needed, and rows of attributes outside the concept do not disqualify.
/// Sharing one attribute with a concept is not satisfying it.
///
/// A provider answers one concept ([`new`](Self::new)) or several
/// ([`also`](Self::also)): the current shape of a command and its legacy
/// ones, say. It runs at most once per command, however many of its
/// concepts the command satisfies, and its `dialog.command/answered`
/// row names the first of them in the provider's own order. When several
/// distinct providers are each satisfied, every one of them runs, in the
/// order they were registered, and each writes its own `answered` (or
/// `failed`) row.
///
/// The answer is written through the stack as a
/// [`transact`](Stack::transact) body's writes are: a statement lands in
/// the layer it names, the primary with none named, and one placed on
/// [`control:`](crate::CONTROL) (`.at(control_layer())`) is a command
/// of its own, which the next effect runs. A handler that fails answers
/// with the failure ([`Failure`]); it does not stop the others.
///
/// ```no_run
/// # use dialog_repository::{Provider, Stack, StackEnv, Failure};
/// # use dialog_corpus::{Changes, Statement as _, Value};
/// # async fn example(stack: Stack, env: impl StackEnv + Clone + dialog_common::ConditionalSend, fetch: dialog_query::ConceptDescriptor, legacy_fetch: dialog_query::ConceptDescriptor) -> anyhow::Result<()> {
/// stack.provide(
///     Provider::new(&fetch, |command| async move {
///         let Some(Value::Entity(node)) = command.rows().first().map(|row| row.is.clone()) else {
///             return Err(Failure::new("a fetch names its node"));
///         };
///         let mut answer = Changes::new();
///         dialog_query::the!("cmd.fetched/node")
///             .of(command.entity().clone())
///             .is(node)
///             .assert(&mut answer);
///         Ok(answer)
///     })
///     // The same handler answers the shape fetches had before.
///     .also(&legacy_fetch),
/// )?;
/// let report = stack
///     .invoke(dialog_query::the!("cmd.fetch/node").of("cmd:1".parse::<dialog_corpus::Entity>()?).is("node:1".parse::<dialog_corpus::Entity>()?))
///     .perform(&env)
///     .await?;
/// let mut effect = report.effect().perform(&env).await?;
/// while effect.remaining() {
///     effect = effect.effect().perform(&env).await?;
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct Provider {
    concepts: Vec<Shape>,
    handler: Handler,
}

/// What a command's rows must hold to satisfy one concept: a row for
/// each of its required fields.
#[derive(Clone)]
struct Shape {
    concept: Entity,
    required: Vec<Requirement>,
}

/// One required field of a concept, as a command's row satisfies it.
#[derive(Clone)]
struct Requirement {
    /// The relations the field reads, the first ranked highest.
    relations: Vec<The>,
    /// The type the field's value is read as, when it declares one.
    kind: Option<Kind>,
}

impl Shape {
    fn of(concept: &ConceptDescriptor) -> Self {
        Self {
            concept: concept.this(),
            required: concept
                .with()
                .iter()
                .filter(|(_, field)| !field.is_optional())
                .map(|(_, field)| Requirement {
                    relations: field.descriptor().relations().cloned().collect(),
                    kind: field.content_type().map(Kind::from),
                })
                .collect(),
        }
    }

    /// Whether `rows` satisfy this concept: every required field has a
    /// row.
    fn satisfied(&self, rows: &[Fact]) -> bool {
        self.required
            .iter()
            .all(|field| rows.iter().any(|row| field.holds(row)))
    }
}

impl Requirement {
    /// Whether `row` is a value of this field: of a relation it reads,
    /// with a value its type reads.
    fn holds(&self, row: &Fact) -> bool {
        let related = self.relations.iter().any(|the| match the {
            The::Relation(_) => the.attribute().as_ref() == Some(&row.the),
            The::Collection { domain, .. } => row.the.domain() == domain.as_str(),
        });
        related
            && self
                .kind
                .as_ref()
                .is_none_or(|kind| kind.view(row.is.clone()).is_some())
    }
}

impl Provider {
    /// A provider of the commands of `concept`, answered by `handler`.
    pub fn new<F, Fut>(concept: &ConceptDescriptor, handler: F) -> Self
    where
        F: Fn(Command) -> Fut + ConditionalSend + ConditionalSync + 'static,
        Fut: Future<Output = Result<Changes, Failure>> + ConditionalSend + 'static,
    {
        Self {
            concepts: vec![Shape::of(concept)],
            handler: Arc::new(move |command| -> Answering { Box::pin(handler(command)) }),
        }
    }

    /// This provider answering the commands of `concept` too, after the
    /// concepts it already answers: a command satisfying any of them is
    /// claimed, and runs the handler once. A concept it already answers
    /// is not added again.
    pub fn also(mut self, concept: &ConceptDescriptor) -> Self {
        let shape = Shape::of(concept);
        if !self.concepts.iter().any(|held| held.concept == shape.concept) {
            self.concepts.push(shape);
        }
        self
    }

    /// The entity of the first concept whose commands this provider
    /// performs: the one it was built with.
    pub fn concept(&self) -> &Entity {
        &self.concepts[0].concept
    }

    /// The entities of every concept whose commands this provider
    /// performs, in the order they were added.
    pub fn concepts(&self) -> impl Iterator<Item = &Entity> {
        self.concepts.iter().map(|shape| &shape.concept)
    }

    /// Whether this provider claims the command whose rows are `rows`:
    /// whether they satisfy one of its concepts, every required field of
    /// it having a row of its attribute and of its value type (see
    /// [`Provider`]).
    pub fn claims(&self, rows: &[Fact]) -> bool {
        self.matched(rows).is_some()
    }

    /// The first of this provider's concepts `rows` satisfy, if any: the
    /// concept its `answered` row names.
    fn matched(&self, rows: &[Fact]) -> Option<&Entity> {
        self.concepts
            .iter()
            .find(|shape| shape.satisfied(rows))
            .map(|shape| &shape.concept)
    }
}

impl Debug for Provider {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("Provider")
            .field("concepts", &self.concepts().collect::<Vec<_>>())
            .finish()
    }
}

/// Why a provider could not answer a command: written as the command's
/// `dialog.command/failed` row. Any error converts into one, by its
/// message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Failure {
    reason: String,
}

impl Failure {
    /// A failure for `reason`.
    pub fn new(reason: impl Into<String>) -> Self {
        Self {
            reason: reason.into(),
        }
    }

    /// Why the provider failed.
    pub fn reason(&self) -> &str {
        &self.reason
    }
}

impl Display for Failure {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.write_str(&self.reason)
    }
}

impl<E: Error> From<E> for Failure {
    fn from(error: E) -> Self {
        Self::new(error.to_string())
    }
}

/// One landed command, as a provider's handler is handed it: its entity,
/// its rows on [`control:`](crate::CONTROL), and the rows of the tick it
/// landed in ([`tick`](Self::tick)).
#[derive(Clone)]
pub struct Command {
    entity: Entity,
    rows: Vec<Fact>,
    tick: Arc<[Fact]>,
    stack: Stack,
}

impl Command {
    /// The command's entity: what an answer names to say which command
    /// it answers.
    pub fn entity(&self) -> &Entity {
        &self.entity
    }

    /// The command's rows, as they landed.
    pub fn rows(&self) -> &[Fact] {
        &self.rows
    }

    /// The rows of the [`control:`](crate::CONTROL) entity itself that
    /// landed in the same tick as the command: the tick's own facts, who
    /// asked (`control: tonk.session/client client:<id>`, written
    /// `.at(control_layer())` beside the command) and anything else an
    /// embedder stamps on the loopback. Every command of the tick sees
    /// the same rows; they are no command of their own
    /// ([`InvocationReport::tick`]).
    pub fn tick(&self) -> &[Fact] {
        &self.tick
    }

    /// Report `value` as an intermediate answer before the provider's
    /// own: an invocation, to perform while the handler runs, writing the
    /// command's `dialog.command/progress` row into
    /// [`control:`](crate::CONTROL) for one tick, where a rule premised
    /// on it copies it where it should stay.
    pub fn progress(&self, value: Value) -> Invocation {
        let mut row = Changes::new();
        row.associate(progress_attr(), self.entity.clone(), value, Pick::Last);
        self.stack.invoke(row)
    }
}

impl Debug for Command {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("Command")
            .field("entity", &self.entity)
            .field("rows", &self.rows)
            .field("tick", &self.tick)
            .finish()
    }
}

impl Stack {
    /// Register `provider`: from now on the stack's effects
    /// ([`Effect::perform`]) run it for the commands it claims. The
    /// first registration of a concept writes `session:
    /// dialog.provider/command <concept>` into the stack's own layer,
    /// [`SESSION`](crate::SESSION), one row per concept, as one instant.
    /// A provider sharing a concept with one already registered replaces
    /// the first such, its handler and concepts, and keeps its place
    /// among the providers; only the concepts no provider registered
    /// before are written.
    pub fn provide(&self, provider: Provider) -> Result<&Self, CommitError> {
        let mut providers = self.providers().write();
        let fresh: Vec<Entity> = provider
            .concepts()
            .filter(|concept| {
                !providers
                    .iter()
                    .any(|held| held.concepts().any(|known| known == *concept))
            })
            .cloned()
            .collect();
        let wrote = !fresh.is_empty();
        if wrote {
            let mut rows = Changes::new();
            for concept in fresh {
                rows.associate(
                    provided_attr(),
                    session_layer(),
                    Value::Entity(concept),
                    Pick::All,
                );
            }
            self.own().apply(rows)?;
        }
        match providers.iter_mut().find(|held| {
            held.concepts()
                .any(|known| provider.concepts().any(|concept| concept == known))
        }) {
            Some(held) => *held = provider,
            None => providers.push(provider),
        }
        drop(providers);
        if !wrote {
            return Ok(self);
        }
        // The provider's rows are rows of `session:`, which a
        // subscription may read.
        self.schedule_poll();
        Ok(self)
    }
}

/// The commands one [`InvocationReport`] landed, with the rows of their
/// tick and the stack's providers: the step that performs their effects. Built by
/// [`InvocationReport::effect`]; run with [`perform`](Self::perform),
/// which needs nothing of the invocation and can run long after it.
#[derive(Clone)]
pub struct Effect {
    stack: Stack,
    revision: Revision,
    commands: Vec<(Entity, Vec<Fact>)>,
    tick: Arc<[Fact]>,
}

impl Effect {
    /// The effect of `landed`, the rows a dispatch that left the primary
    /// at `revision` landed on [`control:`](crate::CONTROL): the rows of
    /// the `control:` entity itself are the tick's, every other entity's
    /// rows a command's.
    pub(crate) fn new(stack: Stack, revision: Revision, landed: Vec<Fact>) -> Self {
        let mut grouped: Vec<(Entity, Vec<Fact>)> = Vec::new();
        let mut tick = Vec::new();
        for fact in landed {
            if is_tick(&fact.of) {
                tick.push(fact);
                continue;
            }
            match grouped.iter_mut().find(|(entity, _)| *entity == fact.of) {
                Some((_, rows)) => rows.push(fact),
                None => grouped.push((fact.of.clone(), vec![fact])),
            }
        }
        Self {
            stack,
            revision,
            commands: grouped,
            tick: tick.into(),
        }
    }

    /// The commands this effect performs, each by its entity with its
    /// rows, in the order they landed.
    pub fn commands(&self) -> &[(Entity, Vec<Fact>)] {
        &self.commands
    }

    /// The rows of the tick the commands landed in: the
    /// [`control:`](crate::CONTROL) entity's own, which every command
    /// carries ([`Command::tick`]) and none is.
    pub fn tick(&self) -> &[Fact] {
        &self.tick
    }

    /// Whether there is nothing to perform. Rows of the tick alone are
    /// nothing to perform.
    pub fn is_empty(&self) -> bool {
        self.commands.is_empty()
    }

    /// Run, for each command in order, every provider claiming it (see
    /// [`Provider`]), in the order they were registered, one after
    /// another, then one answer dispatch through the stack that writes
    /// every answer with the lifecycle rows of each command:
    /// `dialog.command/answered` naming the concept each provider that
    /// answered matched first, `dialog.command/failed` with the reason
    /// of each provider that failed, or `dialog.command/unhandled` for a
    /// command no provider claims. The answer dispatch is re-run from a moved head as any
    /// invocation is; the providers are not. Then it settles the stack
    /// ([`Stack::settle`]), delivering to every reader of a subscription
    /// what the polls scheduled since the last `settle` moved; a failed
    /// poll is reported by [`EffectReport::unsettled`], not as an error
    /// of the effect. Returns the [`EffectReport`]. An effect with no
    /// commands runs no dispatch, reports nothing remaining, and
    /// settles all the same.
    pub async fn perform<Env>(self, env: &Env) -> Result<EffectReport, CommitError>
    where
        Env: StackEnv + Clone + ConditionalSend,
    {
        let Effect {
            stack,
            revision,
            commands,
            tick,
        } = self;
        if commands.is_empty() {
            let unsettled = Box::pin(stack.settle(env)).await.err();
            return Ok(EffectReport {
                invocation: InvocationReport::unmoved(stack, revision),
                answered: Vec::new(),
                failed: Vec::new(),
                unhandled: Vec::new(),
                unsettled,
            });
        }
        let providers = stack.providers().read().clone();
        let mut answers = Changes::new();
        let mut lifecycle = Changes::new();
        let mut answered = Vec::new();
        let mut failed = Vec::new();
        let mut unhandled = Vec::new();
        for (entity, rows) in commands {
            let mut claimed = false;
            for provider in &providers {
                let Some(concept) = provider.matched(&rows) else {
                    continue;
                };
                claimed = true;
                let command = Command {
                    entity: entity.clone(),
                    rows: rows.clone(),
                    tick: tick.clone(),
                    stack: stack.clone(),
                };
                match (provider.handler)(command).await {
                    Ok(answer) => {
                        answers.merge(answer);
                        lifecycle.associate(
                            answered_attr(),
                            entity.clone(),
                            Value::Entity(concept.clone()),
                            Pick::All,
                        );
                        answered.push(entity.clone());
                    }
                    Err(failure) => {
                        lifecycle.associate(
                            failed_attr(),
                            entity.clone(),
                            Value::String(failure.reason.clone()),
                            Pick::All,
                        );
                        failed.push((entity.clone(), failure.reason));
                    }
                }
            }
            if !claimed {
                lifecycle.associate(
                    unhandled_attr(),
                    entity.clone(),
                    Value::Boolean(true),
                    Pick::Last,
                );
                unhandled.push(entity);
            }
        }
        let invocation = Box::pin(stack.assert(answers).invoke(lifecycle).perform(env)).await?;
        // The polls the dispatches before this one and the answer
        // dispatch scheduled run once, here, so an embedder that drives
        // effects is delivered to without settling itself.
        let unsettled = Box::pin(stack.settle(env)).await.err();
        Ok(EffectReport {
            invocation,
            answered,
            failed,
            unhandled,
            unsettled,
        })
    }
}

impl Debug for Effect {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("Effect")
            .field("commands", &self.commands)
            .field("tick", &self.tick)
            .finish()
    }
}

/// What performing an [`Effect`] did: the answer dispatch's
/// [`InvocationReport`], which commands a provider answered, which
/// failed and why, which no provider claimed, and whether the answer
/// landed commands of its own ([`remaining`](Self::remaining)). The
/// tick's own rows are no command, and it reports nothing for them.
#[derive(Clone, Debug)]
pub struct EffectReport {
    invocation: InvocationReport,
    answered: Vec<Entity>,
    failed: Vec<(Entity, String)>,
    unhandled: Vec<Entity>,
    unsettled: Option<EvaluationError>,
}

impl EffectReport {
    /// The answer dispatch's report. For an effect that had no commands,
    /// a report of no dispatch: the primary where the invocation left
    /// it, no tick, no command.
    pub fn invocation(&self) -> &InvocationReport {
        &self.invocation
    }

    /// The commands a provider answered, in the order they ran: one
    /// entry per provider that answered, so a command several providers
    /// answered is named once for each.
    pub fn answered(&self) -> &[Entity] {
        &self.answered
    }

    /// The commands a provider failed, each with its reason: one entry
    /// per provider that failed.
    pub fn failed(&self) -> &[(Entity, String)] {
        &self.failed
    }

    /// The commands no provider claims.
    pub fn unhandled(&self) -> &[Entity] {
        &self.unhandled
    }

    /// Why the [`Stack::settle`] the effect ran after its answer
    /// dispatch failed, if it did: a subscription's poll failed. The
    /// answer dispatch has published all the same, and the poll stays
    /// scheduled for the next `settle`.
    pub fn unsettled(&self) -> Option<&EvaluationError> {
        self.unsettled.as_ref()
    }

    /// Whether the answer dispatch landed commands of its own on
    /// [`control:`](crate::CONTROL): then [`effect`](Self::effect)
    /// performs them, and the embedder decides when to stop.
    pub fn remaining(&self) -> bool {
        !self.invocation.commands().is_empty()
    }

    /// The next step of the chain: the stack's providers over the
    /// commands the answer dispatch landed.
    pub fn effect(&self) -> Effect {
        self.invocation.effect()
    }
}
