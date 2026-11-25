//! Commits, durable writes and pulls racing through one replica on one
//! thread.
//!
//! A browser runs a replica on one thread with no preemption: a future
//! that awaits what only another task on that thread can complete, while
//! that task waits on it, never returns there, though it would on a
//! multi-threaded runtime. These tests drive concurrent work through one
//! replica on the executor each target gives a test, a current-thread
//! tokio runtime natively and the browser's event loop on wasm, and fail
//! instead of blocking when it does not finish (see [`bounded`]).

use super::*;
use dialog_common::ConditionalSync;
use dialog_common::time::sleep;
use dialog_corpus::Relation;
use dialog_corpus::history::Version as RevisionVersion;
use dialog_corpus::tree::CorpusNode;
use dialog_effects::storage::Directory;
use dialog_peer::helpers::test_owned;
use dialog_query::attribute::Relation as QueryRelation;
use dialog_query::query::Output as _;
use dialog_query::{AttributeQuery, Term};
use dialog_search_tree::nodes;
use futures_util::future::{Either, select};
use std::pin::pin;
use std::time::Duration;
#[cfg(not(target_arch = "wasm32"))]
use watchdog::Watchdog;

/// Rounds each race runs.
const ROUNDS: usize = 5;

/// How many times a pull racing commits may make itself again: one per
/// commit it can lose to in a round, and one to spare.
const RETRIES: usize = 3;

/// How long a race may run before it fails instead of blocking.
const BOUND: Duration = Duration::from_secs(60);

/// `work`, or an error naming `what` when it has not finished within
/// [`BOUND`].
///
/// The deadline is a timer raced against the work on the work's own
/// executor, so it fails, on both targets, work that waits on something
/// that never comes. Work that never yields never lets the timer run:
/// natively a `Watchdog` thread ends the test process for that, and in
/// the browser, which gives a test no second thread, the runner's own
/// limit on a test run does (`WASM_BINDGEN_TEST_TIMEOUT`, 180 seconds in
/// CI, 20 by default).
async fn bounded<T>(what: &str, work: impl Future<Output = Result<T>>) -> Result<T> {
    let work = pin!(work);
    let deadline = pin!(sleep(BOUND));
    match select(work, deadline).await {
        Either::Left((done, _)) => done,
        Either::Right(((), _)) => Err(anyhow::anyhow!(
            "{what} did not finish within {BOUND:?}: something it awaits never completes"
        )),
    }
}

/// The bound for work that never yields, natively.
#[cfg(not(target_arch = "wasm32"))]
mod watchdog {
    use super::BOUND;
    use std::process;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::thread;
    use std::time::{Duration, Instant};

    /// Ends the test process when the test that armed it has not finished
    /// within twice [`BOUND`]: the bound for work that never yields, which
    /// a timer on the work's own thread cannot fire for. Native only; see
    /// [`bounded`](super::bounded) for the browser.
    pub(super) struct Watchdog(Arc<AtomicBool>);

    impl Watchdog {
        pub(super) fn arm(what: &'static str) -> Self {
            let done = Arc::new(AtomicBool::new(false));
            let finished = done.clone();
            thread::spawn(move || {
                let deadline = Instant::now() + 2 * BOUND;
                while Instant::now() < deadline {
                    if finished.load(Ordering::SeqCst) {
                        return;
                    }
                    thread::sleep(Duration::from_millis(100));
                }
                eprintln!(
                    "{what} did not finish within {:?}: it never yields",
                    2 * BOUND
                );
                process::exit(1);
            });
            Self(done)
        }
    }

    impl Drop for Watchdog {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }
}

/// The profile `seed` keys, on `storage` at `location`, and the session
/// a test performs through on it.
async fn seeded<S: PeerSpace>(
    seed: &Seed,
    storage: Storage<S>,
    location: Location,
) -> Result<(Peer<S, dialog_peer::Session>, Peer<S>)> {
    let profile = open_peer_as(
        test_owned(storage).await,
        location,
        seed.credential("profile").await?,
    )
    .await?;
    Ok((session_of(&profile).await?, profile))
}

/// A replica commits twice while it pulls a head another device moved,
/// and every commit and pull returns, on one thread.
///
/// Each round the author renames `user:0-0`, settles more residents
/// where the follower never read, and publishes. The follower then pulls
/// with `Download::QueriedRanges` while two commits of its own run beside
/// the pull, each to a fact it never wrote, then pulls again and writes
/// `user:0-0` once more. That last write reads its parent's revision
/// record and the leaves it writes to, which the follower holds: it wrote
/// there before and the pull keeps what it wrote, so it fetches nothing.
/// Every name either device wrote is in the follower's tree at the end.
///
/// A pull merges onto the head it read and publishes only if the head is
/// still that one, so a pull racing a commit fails with
/// `PublishError::VersionMismatch` unless it is asked to retry
/// ([`Pull::retry`](crate::Pull::retry)). The pull here asks: it is a
/// replica that goes on writing while it syncs.
///
/// The durable-storage run of this race is
/// [`it_commits_while_it_adopts_a_moved_head_on_durable_storage`].
#[dialog_common::test]
async fn it_commits_while_it_adopts_a_moved_head(s3: S3Address) -> Result<()> {
    let seed = Seed::new(61);
    let _entropy = seed.entropy();
    #[cfg(not(target_arch = "wasm32"))]
    let _watchdog = Watchdog::arm("it_commits_while_it_adopts_a_moved_head");
    let (operator, profile) = seed.session_with_peer().await;
    bounded(
        "the race of commits and an adoption",
        race_an_adoption(&seed, operator, profile, &s3),
    )
    .await
}

/// [`it_commits_while_it_adopts_a_moved_head`] on the platform's durable
/// storage: the filesystem natively, and IndexedDB in the browser, where
/// every block read is a request the event loop answers later, so the
/// commits and the pull interleave at every read rather than only where
/// the pull waits on the remote.
#[dialog_common::test]
async fn it_commits_while_it_adopts_a_moved_head_on_durable_storage(s3: S3Address) -> Result<()> {
    let seed = Seed::new(61);
    let _entropy = seed.entropy();
    #[cfg(not(target_arch = "wasm32"))]
    {
        use dialog_storage::provider::storage::NativeSpace;
        let _watchdog = Watchdog::arm("it_commits_while_it_adopts_a_moved_head_on_durable_storage");
        let root = tempfile::tempdir()?;
        let directory = Directory::At(root.path().to_string_lossy().into_owned());
        let (operator, profile) = seeded(
            &seed,
            Storage::<NativeSpace>::default(),
            Location::new(directory, unique_name("racing")),
        )
        .await?;
        bounded(
            "the race of commits and an adoption on the filesystem",
            race_an_adoption(&seed, operator, profile, &s3),
        )
        .await
    }
    #[cfg(target_arch = "wasm32")]
    {
        use dialog_storage::provider::storage::WebSpace;
        let (operator, profile) = seeded(
            &seed,
            Storage::<WebSpace>::default(),
            Location::new(Directory::Temp, unique_name("racing")),
        )
        .await?;
        bounded(
            "the race of commits and an adoption on IndexedDB",
            race_an_adoption(&seed, operator, profile, &s3),
        )
        .await
    }
}

/// The race of [`it_commits_while_it_adopts_a_moved_head`], for both
/// devices on `profile`.
async fn race_an_adoption<S: PeerSpace>(
    seed: &Seed,
    operator: Peer<S, dialog_peer::Session>,
    profile: Peer<S>,
    s3: &S3Address,
) -> Result<()> {
    let repository = profile
        .space(seed.name("race-a"))
        .create()
        .with_credential(seed.signer("repository").await?)
        .perform(&operator)
        .await?;
    let follower = seed.name("race-b");
    let devices = following(
        operator,
        profile,
        repository,
        s3,
        Follower::Created(&follower, Box::new(seed.signer("follower").await?)),
    )
    .await?;

    let mut written: Vec<(&str, String)> = vec![("user:0-0", "first".into())];
    devices.name("user:0-0", "first").await?;
    for round in 0..ROUNDS {
        devices.bob.push().perform(&devices.env).await?;
        devices
            .alice
            .pull(crate::Download::MinimalRanges)
            .perform(&devices.operator)
            .await?;
        let renamed = format!("renamed {round}");
        devices
            .move_upstream("user:0-0", &renamed, (4 + 2 * round)..(6 + 2 * round))
            .await?;
        written.push(("user:0-0", renamed));

        let (x, y) = (format!("x {round}"), format!("y {round}"));
        let (pulled, first, second) = futures_util::join!(
            devices
                .bob
                .pull(crate::Download::QueriedRanges)
                .retry(crate::Limit::Times(RETRIES))
                .perform(&devices.env),
            devices
                .bob
                .commit(stream::iter(vec![named("user:0-1", &x)]))
                .perform(&devices.env),
            devices
                .bob
                .commit(stream::iter(vec![named("user:0-2", &y)]))
                .perform(&devices.env),
        );
        pulled?.context("the pull adopts the head the author moved")?;
        first?;
        second?;
        written.push(("user:0-1", x));
        written.push(("user:0-2", y));

        devices
            .bob
            .pull(crate::Download::QueriedRanges)
            .perform(&devices.env)
            .await?;
        let z = format!("z {round}");
        assert_eq!(
            devices.name("user:0-0", &z).await?,
            0,
            "round {round}: after the pull the follower writes where it wrote \
             before from its own blocks, its parent's record included"
        );
        written.push(("user:0-0", z));
    }

    for (entity, name) in &written {
        let (names, _) = devices.names_of(entity).await?;
        assert!(
            names.contains(&Value::String(name.clone())),
            "{entity} keeps {name:?}, written in the race: {names:?}"
        );
    }
    Ok(())
}

/// A replica writes into a durable layer twice while it syncs, and every
/// write and the sync return, on one thread.
///
/// Each round two writes into `memory:notes`, each to a note of its own,
/// run beside a `Download::QueriedRanges` sync of the stack, and then the
/// replica writes its own note again. That write builds on a layer
/// revision this replica minted a moment before, whose record rides a
/// buffer of the layer's tree, above a leaf the replica does not hold
/// under this seed: the write reads the record from the buffer and
/// fetches nothing. Every note written is in the layer at the end, each
/// write of the replica's own note among them: a note here keeps every
/// value asserted, not only the last.
///
/// The race of a write and a sync on durable storage is
/// [`it_lands_a_durable_write_racing_a_sync`](super::it_lands_a_durable_write_racing_a_sync).
#[dialog_common::test]
async fn it_writes_a_durable_layer_while_it_syncs(s3: S3Address) -> Result<()> {
    let seed = Seed::new(415);
    let _entropy = seed.entropy();
    #[cfg(not(target_arch = "wasm32"))]
    let _watchdog = Watchdog::arm("it_writes_a_durable_layer_while_it_syncs");
    let storage = test_storage().await;
    let location = Location::profile(seed.name("layered"));
    let layered = layered(&seed, &s3, &storage, &location).await?;
    bounded(
        "the race of durable writes and a sync",
        race_durable_writes(&layered),
    )
    .await
}

/// The race of [`it_writes_a_durable_layer_while_it_syncs`].
async fn race_durable_writes<S: PeerSpace>(layered: &Layered<S>) -> Result<()> {
    let mut notes: Vec<(String, String)> = vec![("doc:follower".into(), "first".into())];
    layered.write("first").await?;
    for round in 0..ROUNDS {
        let (a, b) = (format!("doc:a-{round}"), format!("doc:b-{round}"));
        let (first, second, synced) = futures_util::join!(
            layered
                .follower
                .assert(noted(&a.parse()?, &format!("a {round}"), &layered.notes))
                .perform(&layered.env),
            layered
                .follower
                .assert(noted(&b.parse()?, &format!("b {round}"), &layered.notes))
                .perform(&layered.env),
            layered
                .follower
                .sync(crate::Download::QueriedRanges)
                .perform(&layered.env),
        );
        first?;
        second?;
        synced?;
        notes.push((a, format!("a {round}")));
        notes.push((b, format!("b {round}")));

        let after = format!("after {round}");
        assert_eq!(
            layered.write(&after).await?,
            0,
            "round {round}: the replica writes into the layer again from its \
             own blocks, its last revision's record read from the buffer"
        );
        notes.push(("doc:follower".into(), after));
    }

    let query = AttributeQuery::from(
        Term::<QueryRelation>::from(QueryRelation::from("doc/note".parse::<Relation>()?))
            .of(Term::<Entity>::var("e"))
            .is(Term::<String>::var("v")),
    )
    .with_layer(Term::Constant(Value::Entity(layered.notes.clone())));
    let claims = layered
        .follower
        .query()
        .select(query)
        .perform(&layered.env)
        .try_vec()
        .await?;
    let mut read: Vec<(String, String)> = claims
        .into_iter()
        .filter(|claim| !claim.of.to_string().starts_with("doc:bulk-"))
        .map(|claim| match claim.is {
            Value::String(text) => Ok((claim.of.to_string(), text)),
            other => Err(anyhow::anyhow!("a note that is no text: {other:?}")),
        })
        .collect::<Result<_>>()?;
    read.sort();
    notes.sort();
    assert_eq!(
        read, notes,
        "the layer holds every note written in the race, each write of the \
         replica's own note among them"
    );
    Ok(())
}

/// A node a lookup of a key descends through, from the root, as a
/// [local](dialog_search_tree::Reading::locally) read finds it.
#[derive(Debug, PartialEq, Eq)]
enum Step {
    /// An index node the replica holds.
    Index,
    /// A leaf the replica holds.
    Leaf,
    /// A node the replica does not hold, by its hash: the local read ends
    /// there, not knowing what the node is.
    Unheld(NodeHash),
}

/// The route a lookup of `version`'s revision record descends in the tree
/// at `root` of `branch`'s subject, read locally through `env`: nothing
/// is fetched, so reading a route changes nothing the replica holds.
async fn route_to_record<Env>(
    branch: &Branch,
    root: &crate::TreeReference,
    version: &RevisionVersion,
    env: &Env,
) -> Result<Vec<Step>>
where
    Env: dialog_capability::Provider<nodes::Read<CorpusNode>> + ConditionalSync,
{
    use dialog_corpus::{
        AttributeKey, AttributeKeyPart, EntityKeyPart, KeyViewConstruct, KeyViewMut as _,
        history::REVISION_ATTRIBUTE,
    };
    use dialog_search_tree::Reading;

    let revision: Relation = REVISION_ATTRIBUTE.parse()?;
    let key = <AttributeKey<Key> as KeyViewConstruct>::min()
        .set_attribute(AttributeKeyPart::from(&revision))
        .set_entity(EntityKeyPart::from(&version.entity()))
        .into_key();
    let reading = Reading::new(branch.subject()).locally();
    let mut at = NodeHash::from(*root.hash());
    let mut route = Vec::new();
    loop {
        let Some(node) = reading.read::<CorpusNode>(&at).perform(env).await? else {
            route.push(Step::Unheld(at));
            return Ok(route);
        };
        match node.body() {
            NodeBody::Index(index) if !index.is_empty() => {
                route.push(Step::Index);
                at = index.hash_at(index.route(key.as_ref())?)?.clone();
            }
            _ => {
                route.push(Step::Leaf);
                return Ok(route);
            }
        }
    }
}

/// Whether the node `name` names in `branch`'s subject is a leaf, read
/// through `env`, which may fetch it.
async fn is_a_leaf<Env>(branch: &Branch, name: &NodeHash, env: &Env) -> Result<bool>
where
    Env: dialog_capability::Provider<nodes::Read<CorpusNode>> + ConditionalSync,
{
    let node = dialog_search_tree::Reading::new(branch.subject())
        .read::<CorpusNode>(name)
        .perform(env)
        .await?
        .context("the node is on the remote")?;
    Ok(!matches!(node.body(), NodeBody::Index(index) if !index.is_empty()))
}

/// A replica that read its layer revision's record from a buffer holds
/// the index nodes on the route to that record and not the leaf the
/// route ends at: what is true today, pinned.
///
/// A write into a durable layer reads the record of the revision it
/// builds on. A record the replica minted a moment ago rides a buffer of
/// the layer's tree, in an index node on the route to its key, and the
/// write reads it there, without the leaf below. Before the record was
/// read from the buffer the write walked to that leaf and, when the
/// replica did not hold it, fetched it and kept it, so afterwards the
/// replica held the leaf as well as the route. It no longer fetches it,
/// so a reader that waits on local blocks alone (a
/// [local](dialog_search_tree::nodes::Read::locally) read, a difference
/// bounded by what the replica holds, an export that does not download)
/// sees one fewer leaf than it did: the leaf the record's key routes to.
/// Here the write leaves that leaf as it was, so it is one leaf, unheld,
/// in the tree of the revision written and in the tree it builds on.
///
/// Both routes are read locally before anything is fetched: naming the
/// unheld node, which the test does by reading it from the remote, keeps
/// it, so a route read after that would find it held.
///
/// The keys derive from the seed of
/// [`it_writes_a_durable_layer_again_from_its_own_blocks`], under which
/// the replica does not hold that leaf after its first write; the test
/// asserts that before relying on it.
#[dialog_common::test]
async fn it_holds_the_route_to_a_record_it_read_from_a_buffer_and_not_its_leaf(
    s3: S3Address,
) -> Result<()> {
    let seed = Seed::new(415);
    let _entropy = seed.entropy();
    let storage = test_storage().await;
    let location = Location::profile(seed.name("layered"));
    let layered = layered(&seed, &s3, &storage, &location).await?;

    layered.write("first").await?;
    let parent = layered
        .follower
        .pointed(&layered.notes)
        .context("the first write moved the layer")?;
    assert!(
        !holds_the_leaf_of_its_record(layered.follower.primary(), &parent, &layered.env).await?,
        "the fixture's layout moved: under seed {} the replica holds the leaf \
         a lookup of its layer revision's record starts at, so choose the \
         seed again",
        seed.value()
    );
    assert_eq!(
        layered.write("second").await?,
        0,
        "the second write read its parent's record without the leaf: from \
         the buffer"
    );
    let written = layered
        .follower
        .pointed(&layered.notes)
        .context("the second write moved the layer")?;

    // Both routes are read before anything is fetched: naming the node
    // the replica does not hold fetches it, and would make the second
    // route find it held.
    let primary = layered.follower.primary();
    let on_parent = route_to_record(primary, &parent.tree, &parent.version(), &layered.env).await?;
    let on_written =
        route_to_record(primary, &written.tree, &parent.version(), &layered.env).await?;
    for (tree, route) in [("parent", &on_parent), ("written", &on_written)] {
        let (last, indexes) = route.split_last().context("a route has a node")?;
        assert!(
            !indexes.is_empty() && indexes.iter().all(|step| *step == Step::Index),
            "in the {tree} revision's tree the replica holds every index node \
             on the route to the parent's record: {route:?}"
        );
        let Step::Unheld(name) = last else {
            anyhow::bail!(
                "in the {tree} revision's tree the replica holds the node the \
                 parent's record routes to: {route:?}"
            );
        };
        assert!(
            is_a_leaf(primary, name, &layered.env).await?,
            "the node the replica does not hold, in the {tree} revision's \
             tree, is the leaf the parent's record routes to"
        );
    }
    assert_eq!(
        on_parent.last(),
        on_written.last(),
        "the write left that leaf as it was: one leaf, unheld, in both trees"
    );
    Ok(())
}

/// Writers past the stack's rerun cap, racing through one replica on
/// durable storage, either land or fail with `VersionMismatch`, and
/// nothing between: what an embedder sees when it runs more writes at
/// once than [`MAX_RERUNS`](crate::MAX_RERUNS) lets the stack run again.
///
/// A write through the stack that loses the head's compare-and-swap is
/// run again on the new head, at most [`MAX_RERUNS`](crate::MAX_RERUNS)
/// times, so it has `MAX_RERUNS + 1` attempts. Every publish round has a
/// winner, so at least `MAX_RERUNS + 1` of the writers land. A write that
/// loses every attempt fails with
/// `CommitError::Publish(PublishError::VersionMismatch { .. })`, and
/// nothing it wrote is in the layer: the embedder runs it again, or keeps
/// fewer writes in flight. How many fail depends on how the writes
/// interleave, so the test does not count them.
///
/// On durable storage because only there do the writes overlap: in
/// memory a write never waits, so they run one after another and none
/// loses.
#[dialog_common::test]
async fn it_lands_or_refuses_writers_past_the_stacks_rerun_cap(s3: S3Address) -> Result<()> {
    let seed = Seed::new(415);
    let _entropy = seed.entropy();
    on_durable_storage!(&seed, &s3, race_past_the_rerun_cap)
}

/// [`it_lands_or_refuses_writers_past_the_stacks_rerun_cap`] over
/// `layered`: a round of more writers than the stack can run again.
async fn race_past_the_rerun_cap<S: PeerSpace>(layered: &Layered<S>) -> Result<()> {
    use futures_util::future::join_all;

    let attempts = usize::try_from(crate::MAX_RERUNS)? + 1;
    let writers = attempts + 3;
    let docs = (0..writers)
        .map(|writer| format!("doc:capped-{writer}"))
        .collect::<Vec<_>>();
    let entities = docs
        .iter()
        .map(|doc| doc.parse::<Entity>())
        .collect::<Result<Vec<_>, _>>()?;
    let written = join_all(entities.iter().map(|doc| {
        layered
            .follower
            .assert(noted(doc, "capped", &layered.notes))
            .perform(&layered.env)
    }))
    .await;

    let mut landed = Vec::new();
    let mut failed = 0;
    for (doc, result) in docs.into_iter().zip(written) {
        match result {
            Ok(_) => landed.push((doc, "capped".to_string())),
            Err(crate::CommitError::Publish(crate::PublishError::VersionMismatch { .. })) => {
                failed += 1;
            }
            Err(other) => {
                anyhow::bail!("{doc} failed other than by losing every attempt: {other:?}")
            }
        }
    }
    assert!(
        landed.len() >= attempts,
        "at least MAX_RERUNS + 1 = {attempts} of {writers} writers land, {} did",
        landed.len()
    );
    assert_eq!(
        landed.len() + failed,
        writers,
        "every writer lands or fails"
    );
    landed.sort();
    assert_eq!(
        notes_in(layered).await?,
        landed,
        "the layer holds the note of each writer that landed, and of none that failed"
    );
    Ok(())
}
