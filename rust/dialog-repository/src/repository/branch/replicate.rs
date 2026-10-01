//! Replicating a branch: pulling from its upstreams as they move, and
//! pushing to them as it does.

use async_stream::stream;
use dialog_capability::{Fork, Provider};
use dialog_common::ConditionalSend;
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
}

/// Command that keeps a branch in step with its upstreams for as long as
/// its stream is read.
///
/// An upstream that can be watched is followed as it moves: each head it
/// delivers is recorded (see [`ConnectedBranch::watch`]) and pulled, with
/// no round trip of its own. One that cannot be watched, a local one, and
/// one whose watch ended are pulled from on the host's
/// [`every`](Self::every) ticks, which is also when a watch that ended is
/// begun again. Each tick also pushes the branch's own commits.
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
    Observed(Result<Observation, WatchRemoteBranchError>),
    /// A watch ended.
    Ended(Target),
    /// The host's tick.
    Tick,
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
                for item in begin(branch, env, &mut unwatched, &mut unsupported, &mut sources).await {
                    yield item;
                }
            }

            // Catch up once, as the stream begins.
            for item in sync(branch, env, pull, push, true).await {
                yield item;
            }

            while let Some(event) = sources.next().await {
                match event {
                    Event::Observed(Ok(Observation::Advanced(_))) => {
                        for item in sync(branch, env, pull, push, false).await {
                            yield item;
                        }
                    }
                    Event::Observed(Ok(Observation::Stale)) => {}
                    Event::Observed(Ok(Observation::Diverged)) => {
                        for item in sync(branch, env, pull, push, true).await {
                            yield item;
                        }
                    }
                    Event::Observed(Err(error)) => yield Err(error.into()),
                    Event::Ended(target) => {
                        if !unwatched.contains(&target) {
                            unwatched.push(target);
                        }
                    }
                    Event::Tick => {
                        if pull {
                            for item in begin(branch, env, &mut unwatched, &mut unsupported, &mut sources).await {
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
/// those whose remote cannot follow a cell.
async fn begin<'a, Env>(
    branch: &'a Branch,
    env: &'a Env,
    unwatched: &mut Vec<Target>,
    unsupported: &mut Vec<Target>,
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
        if !unwatched.contains(&target) || unsupported.contains(&target) {
            continue;
        }
        let watched = match remote.branch(name.clone()).open().perform(env).await {
            Ok(watched) => watched,
            Err(error) => {
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
            Err(error) => failures.push(Err(error.into())),
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
    Ok(source(
        observations
            .map(Event::Observed)
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
            .commit(stream::iter(vec![Instruction::Assert(Artifact {
                the: "user/name".parse()?,
                of: format!("user:{name}").parse()?,
                is: Value::String(name.into()),
                cause: None,
            })]))
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
}
