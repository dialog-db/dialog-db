//! Replicating a branch: pulling from its upstreams as they move, and
//! pushing to them as it does.

use async_stream::stream;
use dialog_capability::{Fork, Provider};
use dialog_common::ConditionalSend;
use dialog_common::time::{self, Duration, Instant};
use dialog_effects::Rejection;
use dialog_effects::archive::{Get, Put};
use dialog_effects::blob::{Import as BlobImport, Read as BlobRead};
use dialog_effects::memory::{self, MemoryError, Publish};
#[cfg(not(target_arch = "wasm32"))]
use futures_util::stream::BoxStream;
#[cfg(target_arch = "wasm32")]
use futures_util::stream::LocalBoxStream;
use futures_util::stream::{self, Pending, SelectAll};
use futures_util::{Stream, StreamExt as _};

use super::resolve::resolve;
use crate::{
    Branch, ConnectedBranch, Observation, RemoteSite, ReplicateError, ResolveEnv, Revision, Target,
    Upstream, WatchRemoteBranchError,
};

/// What replicating a branch did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Replicated {
    /// The branch took what its upstreams brought, and is now at this
    /// head.
    Pulled(Revision),
    /// The branch's head reached its upstreams.
    Pushed(Revision),
    /// The watch of this upstream ended (its connection dropped, say). It
    /// is begun again after a wait that grows each time in a row the watch
    /// ends or cannot be begun, and meanwhile the upstream is pulled from
    /// on the host's ticks, if any.
    Unwatched(Target),
}

/// Command that keeps a branch in step with its upstreams for as long as
/// its stream is read.
///
/// An upstream that can be watched is followed as it moves: each head it
/// delivers is recorded (see [`ConnectedBranch::watch`]) and pulled, with
/// no round trip of its own. A watch that ends, or cannot be begun, is
/// begun again after a wait, which doubles each time in a row up to a
/// minute and starts over once a watch delivers; the first state of the
/// new watch is where the upstream stands, so what it took meanwhile is
/// pulled then. One that cannot be watched at all, a local one, and one
/// waiting to be watched again are pulled from on the host's
/// [`every`](Self::every) ticks. Each tick also pushes the branch's own
/// commits.
///
/// The stream does the work only while it is read, and holds nothing of
/// the environment but the borrow: a host that stops reading it pauses
/// replication, and the next head it reads is the newest. Nothing it
/// answers ends it but the ticks ending with nothing left to watch.
pub struct Replicate<'a, Ticks = Pending<()>> {
    branch: &'a Branch,
    pull: bool,
    push: bool,
    ticks: Ticks,
}

impl Branch {
    /// Keep this branch in step with its upstreams: pull from them as they
    /// move and push to them as this one does.
    ///
    /// Chain [`Replicate::pull`] or [`Replicate::push`] to do only one;
    /// a bare replicate does both.
    ///
    /// Without [`Replicate::every`], upstreams that can be watched are
    /// still followed, a watch that ends being begun again on its own, but
    /// an upstream that cannot be watched at all (or a local one) is pulled
    /// from only as the replication begins. The stream answers
    /// [`Replicated::Unwatched`] when a watch ends, so a host can tell.
    pub fn replicate(&self) -> Replicate<'_> {
        Replicate {
            branch: self,
            pull: false,
            push: false,
            ticks: stream::pending(),
        }
    }
}

impl<'a, Ticks> Replicate<'a, Ticks> {
    /// Pull from the upstreams as they move.
    pub fn pull(mut self) -> Self {
        self.pull = true;
        self
    }

    /// Push to the upstreams as this branch moves.
    pub fn push(mut self) -> Self {
        self.push = true;
        self
    }

    /// Check on each of `ticks` what is not watched: pull from upstreams
    /// that cannot be watched or whose watch ended, begin those watches
    /// again, and push what was committed since.
    ///
    /// How often is the host's to decide, from what it knows that this
    /// does not: whether anyone is looking, what else is waiting.
    pub fn every<Next>(self, ticks: Next) -> Replicate<'a, Next>
    where
        Next: Stream<Item = ()>,
    {
        Replicate {
            branch: self.branch,
            pull: self.pull,
            push: self.push,
            ticks,
        }
    }
}

/// What the replication reacts to.
enum Event {
    /// A watched upstream delivered a head, or failed to.
    Observed(Target, Result<Observation, WatchRemoteBranchError>),
    /// A watch ended.
    Ended(Target),
    /// The time to begin again a watch that ended or could not be begun
    /// came.
    Rewatch,
    /// The host's tick.
    Tick,
}

/// How long to wait before beginning again the first watch of an upstream
/// that ended or could not be begun.
const FIRST_REWATCH: Duration = Duration::from_secs(1);

/// The longest wait before beginning a watch again, however many times in
/// a row it ended or could not be begun.
const LAST_REWATCH: Duration = Duration::from_secs(60);

/// When the watch of each upstream that is not watched may be begun again:
/// after a wait that doubles each time in a row it ends or cannot be
/// begun, up to [`LAST_REWATCH`], and starts over once a watch delivers.
#[derive(Default)]
struct Rewatch {
    /// The next wait for each upstream.
    delays: Vec<(Target, Duration)>,
    /// When each upstream that is waiting may be watched again.
    until: Vec<(Target, Instant)>,
}

impl Rewatch {
    /// Wait before watching `target` again: answers the event that says
    /// the wait is over.
    fn schedule<'a>(&mut self, target: Target) -> Source<'a> {
        let delay = match self.delays.iter_mut().find(|(known, _)| known == &target) {
            Some((_, delay)) => {
                let now = *delay;
                *delay = (*delay * 2).min(LAST_REWATCH);
                now
            }
            None => {
                self.delays
                    .push((target.clone(), (FIRST_REWATCH * 2).min(LAST_REWATCH)));
                FIRST_REWATCH
            }
        };
        self.until.retain(|(known, _)| known != &target);
        self.until.push((target.clone(), Instant::now() + delay));
        source(stream::once(async move {
            time::sleep(delay).await;
            Event::Rewatch
        }))
    }

    /// A watch of `target` delivered: the next time it ends, wait the
    /// shortest time again.
    fn delivered(&mut self, target: &Target) {
        self.delays.retain(|(known, _)| known != target);
    }

    /// Whether `target` is waiting to be watched again.
    fn waiting(&self, target: &Target) -> bool {
        self.until
            .iter()
            .any(|(known, until)| known == target && Instant::now() < *until)
    }
}

#[cfg(not(target_arch = "wasm32"))]
type Source<'a> = BoxStream<'a, Event>;
#[cfg(target_arch = "wasm32")]
type Source<'a> = LocalBoxStream<'a, Event>;

fn source<'a>(events: impl Stream<Item = Event> + ConditionalSend + 'a) -> Source<'a> {
    #[cfg(not(target_arch = "wasm32"))]
    return events.boxed();
    #[cfg(target_arch = "wasm32")]
    return events.boxed_local();
}

impl<'a, Ticks> Replicate<'a, Ticks>
where
    Ticks: Stream<Item = ()> + ConditionalSend + 'a,
{
    /// Replicate, answering what each step did, and what went wrong, for
    /// as long as the stream is read.
    pub fn perform<Env>(
        self,
        env: &'a Env,
    ) -> impl Stream<Item = Result<Replicated, ReplicateError>> + 'a
    where
        Env: ResolveEnv
            + Provider<BlobRead>
            + Provider<Fork<RemoteSite, Get>>
            + Provider<Fork<RemoteSite, Put>>
            + Provider<Fork<RemoteSite, Publish>>
            + Provider<Fork<RemoteSite, BlobImport>>
            + Provider<Fork<RemoteSite, BlobRead>>
            + Provider<Fork<RemoteSite, memory::Watch>>,
    {
        let branch = self.branch;
        let (pull, push) = match (self.pull, self.push) {
            (false, false) => (true, true),
            chosen => chosen,
        };
        let ticks = self.ticks;
        stream! {
            let mut sources: SelectAll<Source<'a>> = SelectAll::new();
            sources.push(source(ticks.map(|()| Event::Tick)));

            // Upstreams to watch, by target, and those a watch of is not
            // running: never begun, refused, or ended.
            let mut unwatched: Vec<Target> = Vec::new();
            let mut unsupported: Vec<Target> = Vec::new();
            let mut rewatch = Rewatch::default();
            let mut local = false;
            if pull {
                if let Err(error) = resolve(branch, env).await {
                    yield Err(ReplicateError::from(crate::PullError::from(error)));
                }
                for upstream in branch.pulls().iter() {
                    match upstream {
                        Upstream::Remote { .. } => {
                            unwatched.push(upstream.target());
                        }
                        _ => local = true,
                    }
                }
                for item in begin(branch, env, &mut unwatched, &mut unsupported, &mut rewatch, &mut sources).await {
                    yield item;
                }
            }

            // Catch up once, as the stream begins.
            for item in sync(branch, env, pull, push, true).await {
                yield item;
            }

            while let Some(event) = sources.next().await {
                match event {
                    // A stale head is one already recorded, not one this
                    // branch already took: another handle of the upstream
                    // (another branch's push or pull, say) may have
                    // recorded it. Merging the recorded head asks nothing
                    // of the remote, and takes nothing when the branch has
                    // it.
                    Event::Observed(target, Ok(Observation::Advanced(_) | Observation::Stale)) => {
                        rewatch.delivered(&target);
                        for item in sync(branch, env, pull, push, false).await {
                            yield item;
                        }
                    }
                    Event::Observed(target, Ok(Observation::Diverged)) => {
                        rewatch.delivered(&target);
                        for item in sync(branch, env, pull, push, true).await {
                            yield item;
                        }
                    }
                    // A head that could not be recorded may still be the
                    // remote's newest, and the upstream counts as watched,
                    // so no tick asks where it stands: ask now.
                    Event::Observed(_, Err(error)) => {
                        yield Err(error.into());
                        for item in sync(branch, env, pull, push, true).await {
                            yield item;
                        }
                    }
                    // The watch is begun again after a while, whether or
                    // not the host ticks: a socket that dropped is not the
                    // end of following the upstream. Its first state is
                    // where the upstream stands, so what it took meanwhile
                    // is pulled then.
                    Event::Ended(target) => {
                        if !unwatched.contains(&target) {
                            unwatched.push(target.clone());
                        }
                        sources.push(rewatch.schedule(target.clone()));
                        yield Ok(Replicated::Unwatched(target));
                    }
                    Event::Rewatch => {
                        for item in begin(branch, env, &mut unwatched, &mut unsupported, &mut rewatch, &mut sources).await {
                            yield item;
                        }
                    }
                    Event::Tick => {
                        if pull {
                            for item in begin(branch, env, &mut unwatched, &mut unsupported, &mut rewatch, &mut sources).await {
                                yield item;
                            }
                        }
                        let confirm = pull && (local || !unwatched.is_empty());
                        for item in sync(branch, env, confirm, push, confirm).await {
                            yield item;
                        }
                    }
                }
            }
        }
    }
}

/// Begin a watch of each remote upstream that has none running, except
/// those whose remote cannot follow a cell and those waiting to be watched
/// again. One that cannot be begun waits, longer each time in a row.
async fn begin<'a, Env>(
    branch: &'a Branch,
    env: &'a Env,
    unwatched: &mut Vec<Target>,
    unsupported: &mut Vec<Target>,
    rewatch: &mut Rewatch,
    sources: &mut SelectAll<Source<'a>>,
) -> Vec<Result<Replicated, ReplicateError>>
where
    Env: ResolveEnv + Provider<Fork<RemoteSite, memory::Watch>>,
{
    let mut failures = Vec::new();
    for upstream in branch.pulls().iter() {
        let Upstream::Remote {
            remote,
            branch: name,
            ..
        } = upstream
        else {
            continue;
        };
        let target = upstream.target();
        if !unwatched.contains(&target) || unsupported.contains(&target) || rewatch.waiting(&target)
        {
            continue;
        }
        let watched = match remote.branch(name.clone()).open().perform(env).await {
            Ok(watched) => watched,
            Err(error) => {
                sources.push(rewatch.schedule(target));
                failures.push(Err(error.into()));
                continue;
            }
        };
        match watch(watched, env, target.clone()).await {
            Ok(events) => {
                unwatched.retain(|unwatched| unwatched != &target);
                sources.push(events);
            }
            Err(WatchRemoteBranchError::Watch(MemoryError::Rejected(Rejection::Unsupported {
                ..
            }))) => {
                unsupported.push(target);
            }
            Err(error) => {
                sources.push(rewatch.schedule(target));
                failures.push(Err(error.into()));
            }
        }
    }
    failures
}

/// The events of a watch of `branch`, ending with its end.
async fn watch<'a, Env>(
    branch: ConnectedBranch,
    env: &'a Env,
    target: Target,
) -> Result<Source<'a>, WatchRemoteBranchError>
where
    Env: ResolveEnv + Provider<Fork<RemoteSite, memory::Watch>>,
{
    let observations = branch.watch().perform(env).await?;
    let observed = target.clone();
    Ok(source(
        observations
            .map(move |observation| Event::Observed(observed.clone(), observation))
            .chain(stream::once(async move { Event::Ended(target) })),
    ))
}

/// Pull, when replicating pulls, then push what the branch has that its
/// upstreams do not, when replicating pushes.
///
/// Unless `confirm`, a remote upstream is not asked where it stands: the
/// head last recorded for it, which a watch keeps current, is merged and
/// pushed onto.
async fn sync<Env>(
    branch: &Branch,
    env: &Env,
    pull: bool,
    push: bool,
    confirm: bool,
) -> Vec<Result<Replicated, ReplicateError>>
where
    Env: ResolveEnv
        + Provider<BlobRead>
        + Provider<Fork<RemoteSite, Get>>
        + Provider<Fork<RemoteSite, Put>>
        + Provider<Fork<RemoteSite, Publish>>
        + Provider<Fork<RemoteSite, BlobImport>>
        + Provider<Fork<RemoteSite, BlobRead>>,
{
    let mut done = Vec::new();
    if pull {
        let pulled = if confirm {
            branch.pull().perform(env).await
        } else {
            branch.pull().assuming_upstream().perform(env).await
        };
        match pulled {
            Ok(Some(revision)) => done.push(Ok(Replicated::Pulled(revision))),
            Ok(None) => {}
            Err(error) => done.push(Err(error.into())),
        }
    }
    if push && ahead(branch) {
        // A pull just now, or a watch, knows where the upstreams stand.
        let pushed = if pull {
            branch.push().assuming_upstream().perform(env).await
        } else {
            branch.push().perform(env).await
        };
        match pushed {
            Ok(Some(revision)) => done.push(Ok(Replicated::Pushed(revision))),
            Ok(None) => {}
            Err(error) => done.push(Err(error.into())),
        }
    }
    done
}

/// Whether the branch holds a head some upstream it pushes to was not last
/// in sync at.
fn ahead(branch: &Branch) -> bool {
    let Some(head) = branch.revision() else {
        return false;
    };
    branch
        .pushes()
        .iter()
        .any(|upstream| upstream.tree() != Some(&head.tree))
}

#[cfg(all(
    test,
    any(feature = "integration-tests", feature = "web-integration-tests")
))]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::Replicated;
    use crate::helpers::connect;
    use crate::{Branch, ReplicateError, RepositoryExt as _, ResolveEnv, Revision, SiteAddress};
    use anyhow::{Result, bail};
    use dialog_artifacts::{Artifact, Instruction, Value};
    use dialog_peer::helpers::{test_session_with_peer, unique_name};
    use dialog_remote_ucan::UcanAddress;
    use dialog_remote_ucan::helpers::UcanServiceAddress;
    use futures_util::{Stream, StreamExt as _, pin_mut, stream};
    use tokio::sync::mpsc::unbounded_channel;

    async fn commit<Env: ResolveEnv>(branch: &Branch, name: &str, env: &Env) -> Result<Revision> {
        branch
            .commit(stream::iter(vec![Instruction::Assert(
                Artifact {
                    the: "user/name".parse()?,
                    of: format!("user:{name}").parse()?,
                    is: Value::String(name.into()),
                    cause: None,
                },
                dialog_artifacts::Pick::All,
            )]))
            .perform(env)
            .await?;
        Ok(branch.revision().expect("a commit leaves a head"))
    }

    /// The next thing replication did, which must have gone right.
    async fn next(
        events: &mut (impl Stream<Item = Result<Replicated, ReplicateError>> + Unpin),
    ) -> Result<Replicated> {
        match events.next().await {
            Some(Ok(done)) => Ok(done),
            Some(Err(error)) => bail!("replication failed: {error}"),
            None => bail!("replication ended"),
        }
    }

    /// Two branches of one repository tracking one branch at the service:
    /// a writer that pushes to it, and a mirror that replicates from it.
    macro_rules! rig {
        ($operator:ident, $profile:ident, $site:expr, $writer:ident, $mirror:ident) => {
            let ($operator, $profile) = test_session_with_peer().await;
            let repo = $profile
                .space(unique_name("replicate"))
                .create()
                .perform(&$operator)
                .await?;
            let chain = repo
                .access()
                .claim(&repo)
                .delegate($profile.did())
                .perform(&$operator)
                .await?;
            $profile.access().save(chain).perform(&$operator).await?;
            let origin = connect("origin", $site, repo.did(), &$operator).await?;
            let remote = origin.branch("main").open().perform(&$operator).await?;
            let $writer = repo.branch("main").open().perform(&$operator).await?;
            $writer
                .set_upstream(remote.clone())
                .perform(&$operator)
                .await?;
            let $mirror = repo.branch("mirror").open().perform(&$operator).await?;
            $mirror.set_upstream(remote).perform(&$operator).await?;
        };
    }

    /// Where the service has a socket, a branch replicating from it is
    /// told of each head its upstream takes: with no ticks at all, a push
    /// after replication began still reaches it.
    #[dialog_common::test]
    async fn it_replicates_what_a_watched_upstream_delivers(
        service: UcanServiceAddress,
    ) -> Result<()> {
        let site =
            SiteAddress::Ucan(UcanAddress::new(&service.endpoint).with_socket(&service.socket));
        rig!(operator, profile, site, writer, mirror);

        let first = commit(&writer, "first", &operator).await?;
        writer.push().perform(&operator).await?;

        let events = mirror.replicate().pull().perform(&operator);
        pin_mut!(events);
        let Replicated::Pulled(caught_up) = next(&mut events).await? else {
            bail!("expected the mirror to catch up first");
        };
        assert_eq!(caught_up.tree, first.tree, "it catches up as it begins");

        let second = commit(&writer, "second", &operator).await?;
        writer.push().perform(&operator).await?;
        let Replicated::Pulled(delivered) = next(&mut events).await? else {
            bail!("expected the delivered head pulled");
        };
        assert_eq!(delivered.tree, second.tree, "the watch delivers the push");
        assert_eq!(mirror.revision().map(|head| head.tree), Some(second.tree));
        Ok(())
    }

    /// Where the service has no socket, the upstream cannot be watched, and
    /// a branch replicating from it pulls on the host's ticks instead; on
    /// a tick it also pushes what it committed.
    #[dialog_common::test]
    async fn it_replicates_on_ticks_where_the_upstream_cannot_be_watched(
        service: UcanServiceAddress,
    ) -> Result<()> {
        let site = SiteAddress::Ucan(UcanAddress::new(&service.endpoint));
        rig!(operator, profile, site, writer, mirror);

        let (tick, ticks) = unbounded_channel::<()>();
        let ticks = stream::unfold(ticks, |mut ticks| async move {
            ticks.recv().await.map(|tick| (tick, ticks))
        });
        let events = mirror.replicate().every(ticks).perform(&operator);
        pin_mut!(events);

        let pushed = commit(&writer, "elsewhere", &operator).await?;
        writer.push().perform(&operator).await?;
        tick.send(())?;
        let Replicated::Pulled(pulled) = next(&mut events).await? else {
            bail!("expected the tick to pull");
        };
        assert_eq!(pulled.tree, pushed.tree);

        let mine = commit(&mirror, "here", &operator).await?;
        tick.send(())?;
        let Replicated::Pushed(reached) = next(&mut events).await? else {
            bail!("expected the tick to push");
        };
        assert_eq!(reached.tree, mine.tree);
        Ok(())
    }

    /// A branch replicating from a watched upstream follows it again after
    /// the connection carrying the watch is cut, with no ticks at all: the
    /// watch is begun again on its own, and a head pushed while it was
    /// down is pulled.
    #[dialog_common::test(
        fault = dialog_remote_ucan::helpers::Fault::Cut {
            after: std::time::Duration::from_millis(300),
            times: 1,
        }
    )]
    async fn it_follows_an_upstream_again_after_its_connection_is_cut(
        service: UcanServiceAddress,
    ) -> Result<()> {
        let site =
            SiteAddress::Ucan(UcanAddress::new(&service.endpoint).with_socket(&service.socket));
        rig!(operator, profile, site, writer, mirror);

        let first = commit(&writer, "first", &operator).await?;
        writer.push().perform(&operator).await?;

        let events = mirror.replicate().pull().perform(&operator);
        pin_mut!(events);
        let Replicated::Pulled(caught_up) = next(&mut events).await? else {
            bail!("expected the mirror to catch up first");
        };
        assert_eq!(caught_up.tree, first.tree);

        // The connection carrying the watch is cut.
        loop {
            if let Replicated::Unwatched(_) = next(&mut events).await? {
                break;
            }
        }

        let second = commit(&writer, "second", &operator).await?;
        writer.push().perform(&operator).await?;
        loop {
            match next(&mut events).await? {
                Replicated::Pulled(head) if head.tree == second.tree => break,
                _ => {}
            }
        }
        assert_eq!(mirror.revision().map(|head| head.tree), Some(second.tree));
        Ok(())
    }

    /// A push whose publish landed at the remote but whose answer was lost
    /// with its connection is found to have landed by the next push, which
    /// then has nothing to send, and a branch pulling from the upstream
    /// takes the head.
    #[dialog_common::test(fault = dialog_remote_ucan::helpers::Fault::LosePublish { times: 1 })]
    async fn it_heals_a_push_whose_answer_was_lost(service: UcanServiceAddress) -> Result<()> {
        let site =
            SiteAddress::Ucan(UcanAddress::new(&service.endpoint).with_socket(&service.socket));
        rig!(operator, profile, site, writer, mirror);

        let head = commit(&writer, "lost", &operator).await?;
        let lost = writer.push().perform(&operator).await;
        assert!(lost.is_err(), "the push's answer was lost: {lost:?}");

        let pushed = writer.push().perform(&operator).await?;
        assert_eq!(pushed.map(|pushed| pushed.tree), Some(head.tree.clone()));
        mirror.pull().perform(&operator).await?;
        assert_eq!(mirror.revision().map(|pulled| pulled.tree), Some(head.tree));
        Ok(())
    }

    /// A service whose clock reads ten minutes behind this one refuses
    /// the first invocation it is sent as issued too far ahead. The refusal
    /// says what its clock read, and the branch's push and the mirror's
    /// pull go through, stamped by the service's clock.
    #[dialog_common::test(clock_offset = -600i64)]
    async fn it_pushes_and_pulls_through_a_service_whose_clock_differs(
        service: UcanServiceAddress,
    ) -> Result<()> {
        let site =
            SiteAddress::Ucan(UcanAddress::new(&service.endpoint).with_socket(&service.socket));
        rig!(operator, profile, site, writer, mirror);

        let head = commit(&writer, "skewed", &operator).await?;
        writer.push().perform(&operator).await?;
        mirror.pull().perform(&operator).await?;
        assert_eq!(mirror.revision().map(|pulled| pulled.tree), Some(head.tree));
        Ok(())
    }

    /// A watch over the socket of a service whose clock reads ten minutes
    /// ahead is begun, the refusal of its first invocation having said
    /// what the clock read, and delivers what is pushed after.
    #[dialog_common::test(clock_offset = 600i64)]
    async fn it_watches_an_upstream_whose_service_clock_differs(
        service: UcanServiceAddress,
    ) -> Result<()> {
        let site =
            SiteAddress::Ucan(UcanAddress::new(&service.endpoint).with_socket(&service.socket));
        rig!(operator, profile, site, writer, mirror);

        let events = mirror.replicate().pull().perform(&operator);
        pin_mut!(events);
        let pushed = commit(&writer, "ahead", &operator).await?;
        writer.push().perform(&operator).await?;
        loop {
            match next(&mut events).await? {
                Replicated::Pulled(head) if head.tree == pushed.tree => break,
                _ => {}
            }
        }
        Ok(())
    }
}
