//! A channel: an ephemeral layer whose instants replicate to peers.
//!
//! A local ephemeral layer fans instants out to in-process observers and
//! keeps no log. A channel is the same layer with the one thing peers
//! need added: a bounded **log** of its instants with a per-peer
//! **offset** into it. A catch-up is "the instants past my offset"; retention
//! is the lowest peer offset, under a ring bound; a peer whose offset
//! fell off the ring resyncs from the fold. That is the shape presence
//! and awareness protocols already have, with the sequence standing in
//! for a per-peer clock, and it is what makes a replicated scope
//! meaningful without a tree: there is nothing to push but the log.
//!
//! What lands here is the log and its offsets, exercised with peers in
//! one process: a channel on each side and the [`Catchup`] carried between
//! them by the test. Carrying it over the remote transport is the part
//! that remains; the [`Catchup`] is serializable so that binding is a
//! transport concern, not a change to this shape.
//!
//! Echo is prevented by origin: an instant a channel applied on a
//! peer's behalf is logged as that peer's, and a sync to that peer skips
//! it. Two channels exchanging syncs therefore converge rather than
//! bouncing each other's writes.
//!
//! See [`Channel`] for why a channel waits for the publish of a commit.

use std::collections::{BTreeSet, HashMap, VecDeque};
use std::sync::Arc;

use dialog_artifacts::{Artifact, Changes, Entity, Pick, Update as _};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use super::{Delta, Ephemeral, Held, Instant, Observer, StoreChange};

/// How many instants a channel's log holds at most, whatever its
/// slowest peer has seen. A peer further behind than this resyncs.
pub const DEFAULT_LOG_CAPACITY: usize = 4096;

/// One entry of the log: an instant and, when it was applied on a
/// peer's behalf, that peer.
#[derive(Clone, Debug)]
struct Entry {
    instant: Instant,
    origin: Option<Entity>,
}

/// What a peer receives when it catches up: the instants past its
/// offset, or the whole fold when the offset fell off the log.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Catchup {
    /// The instants past the peer's offset that did not originate with
    /// it, oldest first; empty when the peer is current. `sequence` is
    /// the channel's sequence after the last of them, the peer's new
    /// offset.
    Instants {
        /// The instants to apply, in order.
        instants: Vec<Instant>,
        /// The offset the peer stands at once they are applied.
        sequence: u64,
    },
    /// The peer's offset fell off the log: replace the peer's layer
    /// with the fold, which stands at `sequence`.
    Resync {
        /// Every fact the layer holds.
        facts: Vec<Artifact>,
        /// Every tombstone hiding a fact in a lower layer.
        shadowed: Vec<Artifact>,
        /// The offset the peer stands at once the fold is applied.
        sequence: u64,
    },
}

impl Catchup {
    /// The offset a peer stands at after applying this.
    pub fn sequence(&self) -> u64 {
        match self {
            Catchup::Instants { sequence, .. } | Catchup::Resync { sequence, .. } => *sequence,
        }
    }
}

#[derive(Debug)]
struct Log {
    entries: VecDeque<Entry>,
    capacity: usize,
    /// Each peer's offset: the sequence of the last instant it has.
    offsets: HashMap<Entity, u64>,
    /// Sequences minted by [`Channel::receive`] on a peer's behalf,
    /// awaiting their drain into the log, where they are logged as that
    /// peer's.
    received: HashMap<u64, Entity>,
    /// For each peer, the witnesses at or below its offset whose commit
    /// had not published when a join or a resync placed the peer past
    /// them: the peer is owed each one once its commit publishes.
    owed: HashMap<Entity, BTreeSet<u64>>,
}

/// A replicated ephemeral layer. Cheap to clone: clones share the log.
///
/// # A channel waits for the publish
///
/// A stack commit stages, then publishes. The share of an ephemeral
/// layer, and the witness of each round of induction, land at stage
/// time, so rules and observers in the process see them at once. If a
/// channel sent them then, a peer could act on a command whose commit
/// never publishes:
///
/// ```text
/// stage: intent zI, witness create-space
/// channel sends create-space ──▶ daemon makes Garden, origin zI
/// publish: compare and set lost (another device moved the head)
/// the worker crashes before it runs the transaction again
/// ⇒ Garden exists with origin zI, and no replica holds zI
/// ```
///
/// The space then has no pending row, no follow-up facts, and nothing to
/// resume. So each instant that a stack commit mints carries the hold of
/// that commit, and a catch-up stops at the first instant whose commit
/// has not published. Once the commit publishes, the next catch-up
/// carries its instants. If the publish fails, or the stack drops the
/// staged commit on an advance or a pull, its witnesses never leave: the
/// run again witnesses the command again, and the peer receives it once,
/// after the intent lands. The facts that such a commit stored in the
/// layer stay there, and its instants of them still go, so that a peer
/// holds what the layer holds; the run again stores nothing new of them.
/// A commit that changes no durable layer, while no earlier commit of the
/// stack waits to publish, has nothing to wait for, and its instants go at
/// once.
///
/// So a writer sends what must wait for its commit, a command, as a
/// transient: its witness waits, and goes once, or never. What may go at
/// once, and must reach every peer that the layer reaches, such as a
/// result or a notice, is a stored fact of the layer.
///
/// A [`Catchup::Resync`] carries the fold, which already holds the facts
/// of a staged commit: only the instants wait. A command is a witness and
/// never a fact of the layer, so it reaches no fold. So the log keeps each
/// instant from the first one that waits, past its ring bound if it must,
/// and a resync leaves a peer just before that instant, so the held
/// instants still follow once their commit publishes.
#[derive(Clone, Debug)]
pub struct Channel {
    layer: Ephemeral,
    observer: Arc<Observer>,
    log: Arc<Mutex<Log>>,
}

impl Channel {
    /// Replicate `layer`, keeping at most `capacity` instants for peers
    /// that fall behind. Instants minted before this call are not in
    /// the log; a peer joining now starts from the fold. The channel
    /// observes the layer through a queue as large as its log, and no
    /// smaller than an observer's: more instants than that between two
    /// calls of the channel make a gap, and every peer resyncs from the
    /// fold.
    pub fn over(layer: Ephemeral, capacity: usize) -> Self {
        let capacity = capacity.max(1);
        let observer =
            Arc::new(layer.observe_everything_within(capacity.max(super::QUEUE_CAPACITY)));
        Self {
            layer,
            observer,
            log: Arc::new(Mutex::new(Log {
                entries: VecDeque::new(),
                capacity,
                offsets: HashMap::new(),
                received: HashMap::new(),
                owed: HashMap::new(),
            })),
        }
    }

    /// Replicate `layer` with [`DEFAULT_LOG_CAPACITY`].
    pub fn new(layer: Ephemeral) -> Self {
        Self::over(layer, DEFAULT_LOG_CAPACITY)
    }

    /// The layer replicated.
    pub fn layer(&self) -> &Ephemeral {
        &self.layer
    }

    /// Admit `peer` at the layer's current sequence: its first sync
    /// delivers only what happens from now on. A peer that needs the
    /// fold too takes it with [`snapshot`](Self::snapshot), or is admitted
    /// behind with [`join_at`](Self::join_at).
    pub fn join(&self, peer: Entity) -> u64 {
        let sequence = self.layer.revision().sequence;
        self.join_at(peer, sequence);
        sequence
    }

    /// Admit `peer` claiming to stand at `sequence`. A claim the log no
    /// longer covers makes the peer's next sync a resync. A witness at or
    /// below `sequence` whose commit has not published is owed to the
    /// peer, and goes once it does.
    pub fn join_at(&self, peer: Entity, sequence: u64) {
        let mut log = self.log.lock();
        self.drain(&mut log);
        Self::place(&mut log, peer, sequence);
    }

    /// Set the offset of `peer` to `sequence`, and owe it each witness at
    /// or below that whose commit has not published yet.
    fn place(log: &mut Log, peer: Entity, sequence: u64) {
        let held: Vec<u64> = log
            .entries
            .iter()
            .filter(|entry| {
                entry.instant.transient
                    && entry.instant.sequence <= sequence
                    && entry.instant.hold.state() == Held::Open
            })
            .map(|entry| entry.instant.sequence)
            .collect();
        // A peer placed lower than before reads again what lies past it,
        // so it is owed nothing there.
        if let Some(owed) = log.owed.get_mut(&peer) {
            owed.retain(|owed| *owed <= sequence);
        }
        if !held.is_empty() {
            log.owed.entry(peer.clone()).or_default().extend(held);
        }
        log.offsets.insert(peer, sequence);
    }

    /// The witnesses owed to `peer` whose commits published, oldest
    /// first; each goes once. One whose commit never publishes is no
    /// longer owed.
    fn repay(log: &mut Log, peer: &Entity) -> Vec<Instant> {
        let Some(owed) = log.owed.remove(peer) else {
            return Vec::new();
        };
        let mut paid = Vec::new();
        let mut still = BTreeSet::new();
        for sequence in owed {
            let Some(entry) = log
                .entries
                .iter()
                .find(|entry| entry.instant.sequence == sequence)
            else {
                continue;
            };
            match entry.instant.hold.state() {
                Held::Open => {
                    still.insert(sequence);
                }
                Held::Released => paid.push(entry.instant.clone()),
                Held::Dropped => {}
            }
        }
        if !still.is_empty() {
            log.owed.insert(peer.clone(), still);
        }
        paid
    }

    /// Forget `peer`: its offset no longer holds instants back. A transport
    /// calls this when its link to the peer closes: a peer that never
    /// reads keeps every witness it has not read in the log.
    pub fn leave(&self, peer: &Entity) -> bool {
        let mut log = self.log.lock();
        let left = log.offsets.remove(peer).is_some();
        log.owed.remove(peer);
        drop(log);
        self.retain();
        left
    }

    /// The offset `peer` stands at, if admitted.
    pub fn offset(&self, peer: &Entity) -> Option<u64> {
        self.log.lock().offsets.get(peer).copied()
    }

    /// The instants the log holds now.
    pub fn len(&self) -> usize {
        let mut log = self.log.lock();
        self.absorb(&mut log);
        log.entries.len()
    }

    /// Whether the log holds no instants.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Every fact the layer holds, and the sequence it stands at: what
    /// a peer that cannot be caught up from the log applies instead.
    pub fn fold(&self) -> (Vec<Artifact>, u64) {
        self.layer.fold()
    }

    /// A complete replication snapshot: held facts, tombstones, and a
    /// sequence captured together. Use this to initialize a peer.
    pub fn snapshot(&self) -> Catchup {
        let (facts, shadowed, sequence) = self.layer.snapshot();
        Catchup::Resync {
            facts,
            shadowed,
            sequence,
        }
    }

    /// What `peer` needs to be current: the instants past its offset
    /// that it did not itself send, up to the first one whose stack
    /// commit has not published, or the fold if its offset fell off the
    /// log. Moves the peer's offset to the sequence the sync reaches; a
    /// peer not admitted is admitted by this.
    pub fn since(&self, peer: &Entity) -> Catchup {
        let mut log = self.log.lock();
        let current = self.absorb(&mut log);
        let offset = log.offsets.get(peer).copied().unwrap_or(0);
        let oldest = log.entries.front().map(|entry| entry.instant.sequence);
        // The log covers the peer when every instant after its offset
        // is still held: the oldest held is at most offset + 1, or the
        // log is empty because nothing happened past the offset.
        let covered = match oldest {
            Some(oldest) => oldest <= offset.saturating_add(1),
            None => offset >= current,
        };
        let sync = if covered {
            // The catch-up stops before the first instant whose commit
            // has not published, and skips each witness whose commit never
            // will (see the module docs).
            let mut instants: Vec<Instant> = Self::repay(&mut log, peer);
            let mut sequence = current;
            for entry in log
                .entries
                .iter()
                .filter(|entry| entry.instant.sequence > offset)
            {
                match entry.instant.hold.state() {
                    Held::Open => {
                        sequence = entry.instant.sequence - 1;
                        break;
                    }
                    // The witness of a commit that never publishes never
                    // leaves; the facts it stored are the layer's.
                    Held::Dropped if entry.instant.transient => continue,
                    Held::Dropped | Held::Released => {}
                }
                if entry.origin.as_ref() != Some(peer) {
                    instants.push(entry.instant.clone());
                }
            }
            Catchup::Instants { instants, sequence }
        } else {
            // A resync carries the fold, which holds no witness. A peer
            // stands just before the first instant whose commit has not
            // published, so the held instants follow it once it does.
            match (self.snapshot(), Self::first_held(&log)) {
                (
                    Catchup::Resync {
                        facts,
                        shadowed,
                        sequence,
                    },
                    Some(held),
                ) => Catchup::Resync {
                    facts,
                    shadowed,
                    sequence: sequence.min(held - 1),
                },
                (sync, _) => sync,
            }
        };
        log.offsets.insert(peer.clone(), sync.sequence());
        drop(log);
        self.retain();
        sync
    }

    /// Apply what `peer` sent. Instants land on the layer as writes of
    /// its facts, and are logged as the peer's so they are not sent
    /// back to it; a resync replaces the layer's facts with the fold.
    /// Returns how many instants changed what readers see.
    pub fn receive(&self, peer: &Entity, sync: Catchup) -> usize {
        match sync {
            Catchup::Instants { instants, .. } => {
                let mut applied = 0;
                for instant in instants {
                    let mut log = self.log.lock();
                    let minted = if instant.transient {
                        let mut changes = Changes::new();
                        for fact in instant.retracted {
                            if !instant.asserted.contains(&fact) {
                                changes.dissociate(fact.the, fact.of, fact.is);
                            }
                        }
                        for fact in instant.asserted {
                            changes.associate(fact.the, fact.of, fact.is, Pick::All);
                        }
                        self.layer.witness(changes)
                    } else {
                        let mut state = self.layer.state.write();
                        let mut delta = Delta::default();
                        for change in instant.changes {
                            match change {
                                StoreChange::Insert(fact) => state.insert(fact, &mut delta),
                                StoreChange::Remove(fact) => {
                                    state.remove(&fact, &mut delta);
                                }
                                StoreChange::Shadow(fact) => state.shadow(fact, &mut delta),
                                StoreChange::Unshadow(fact) => {
                                    let key = state.key(&fact);
                                    state.unshadow(&key, &mut delta)
                                }
                            }
                        }
                        state.mint(delta)
                    };
                    if let Some(minted) = minted {
                        log.received.insert(minted.sequence, peer.clone());
                        applied += 1;
                    }
                }
                self.absorb(&mut self.log.lock());
                applied
            }
            Catchup::Resync {
                facts, shadowed, ..
            } => {
                self.resync(peer, facts, shadowed);
                1
            }
        }
    }

    /// Replace the layer's facts with `facts`, as `peer`'s fold.
    fn resync(&self, peer: &Entity, facts: Vec<Artifact>, shadowed: Vec<Artifact>) {
        let mut log = self.log.lock();
        let mut state = self.layer.state.write();
        let mut delta = Delta::default();
        let held: Vec<_> = state.facts.iter().cloned().collect();
        for fact in held {
            state.remove(&fact, &mut delta);
        }
        let hidden: Vec<_> = state.shadowed.keys().cloned().collect();
        for key in hidden {
            state.unshadow(&key, &mut delta);
        }
        for fact in facts {
            state.insert(fact, &mut delta);
        }
        for fact in shadowed {
            state.shadow(fact, &mut delta);
        }
        if let Some(minted) = state.mint(delta) {
            log.received.insert(minted.sequence, peer.clone());
        }
        // This snapshot supersedes our earlier writes. Sending those back
        // to its author would resurrect facts its snapshot just removed. A
        // witness that waits for its commit is no fact of the fold, and is
        // owed to the peer still.
        let sequence = state.sequence;
        drop(state);
        self.absorb(&mut log);
        Self::place(&mut log, peer.clone(), sequence);
    }

    /// Drain the layer's instants into the log, tagging the ones that
    /// were applied on a peer's behalf, and bound the log.
    fn absorb(&self, log: &mut Log) -> u64 {
        let sequence = self.drain(log);
        Self::bound(log);
        sequence
    }

    /// Drain the layer's instants into the log, as [`absorb`](Self::absorb)
    /// does, without bounding it.
    fn drain(&self, log: &mut Log) -> u64 {
        let (drained, sequence) = self.observer.drain_at();
        match drained {
            super::Drained::Instants(instants) => {
                for instant in instants {
                    let origin = log.received.remove(&instant.sequence);
                    log.entries.push_back(Entry { instant, origin });
                }
            }
            super::Drained::Gap { .. } => {
                // The channel's own observer fell behind: the log has a
                // hole nothing can fill, so every peer resyncs. The fold
                // holds every fact, but no witness: a command that waited
                // for its commit to publish is lost with the log.
                if Self::first_held(log).is_some() {
                    tracing::warn!(
                        "a channel fell behind its layer with a command waiting; the command is lost"
                    );
                }
                log.entries.clear();
                log.received.clear();
                log.owed.clear();
            }
        }
        sequence
    }

    /// Drop what every peer has and what the ring bound excludes.
    fn retain(&self) {
        let mut log = self.log.lock();
        Self::bound(&mut log);
    }

    /// The sequence of the first instant of the log whose commit has not
    /// published.
    fn first_held(log: &Log) -> Option<u64> {
        log.entries
            .iter()
            .find(|entry| entry.instant.hold.state() == Held::Open)
            .map(|entry| entry.instant.sequence)
    }

    /// Whether some peer is owed the witness at `sequence`.
    fn is_owed(log: &Log, sequence: u64) -> bool {
        log.owed.values().any(|owed| owed.contains(&sequence))
    }

    /// Drop what no peer needs, and what is past the ring bound, but never
    /// an instant whose commit has not published, or one after it, nor a
    /// witness that a peer has not read: no peer has those yet, and none
    /// could get them from the fold, which holds no witness.
    fn bound(log: &mut Log) {
        let slowest = log.offsets.values().copied().min();
        while let Some(front) = log.entries.front() {
            let held = front.instant.hold.state();
            // A witness owed to a peer stays until it is repaid, whatever
            // the offsets say: the peer stands past it already.
            if held == Held::Open || Self::is_owed(log, front.instant.sequence) {
                break;
            }
            let seen_by_all = slowest.is_some_and(|offset| front.instant.sequence <= offset);
            let unread_witness = front.instant.transient
                && held == Held::Released
                && slowest.is_some_and(|offset| front.instant.sequence > offset);
            let over = log.entries.len() > log.capacity && !unread_witness;
            if seen_by_all || over {
                log.entries.pop_front();
            } else {
                break;
            }
        }
        if log.offsets.is_empty() {
            // Nobody is waiting: keep only the ring, so a late joiner
            // can still be caught up from it.
            while log.entries.len() > log.capacity
                && log.entries.front().is_some_and(|front| {
                    front.instant.hold.state() != Held::Open
                        && !Self::is_owed(log, front.instant.sequence)
                })
            {
                log.entries.pop_front();
            }
        }
    }
}

/// Apply a statement of raw facts to `changes`, for tests and callers
/// that already hold artifacts.
#[cfg(test)]
pub(crate) fn assert_all(changes: &mut Changes, facts: impl IntoIterator<Item = Artifact>) {
    for fact in facts {
        changes.associate(fact.the, fact.of, fact.is, Pick::All);
    }
}

#[cfg(test)]
impl Channel {
    /// Every instant the log holds, oldest first.
    pub(crate) fn entries(&self) -> Vec<Instant> {
        let mut log = self.log.lock();
        self.absorb(&mut log);
        log.entries
            .iter()
            .map(|entry| entry.instant.clone())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use dialog_artifacts::{ArtifactSelector, Value};
    use dialog_common::Holdings;
    use dialog_search_tree::Manifest;

    fn fact(of: &str, the_: &str, is: &str) -> Artifact {
        Artifact {
            the: the_.parse().expect("attribute"),
            of: of.parse().expect("entity"),
            is: Value::String(is.into()),
            cause: None,
        }
    }

    fn peer(name: &str) -> Entity {
        format!("peer:{name}").parse().expect("peer entity")
    }

    fn write(channel: &Channel, facts: Vec<Artifact>) {
        let mut changes = Changes::new();
        assert_all(&mut changes, facts);
        channel.layer().apply(changes).unwrap();
    }

    fn titles(channel: &Channel) -> Vec<String> {
        let mut titles: Vec<String> = channel
            .layer()
            .scan(&ArtifactSelector::new().the("doc/title".parse().unwrap()))
            .into_iter()
            .filter_map(|fact| match fact.is {
                Value::String(title) => Some(title),
                _ => None,
            })
            .collect();
        titles.sort();
        titles
    }

    async fn two_channels() -> (Channel, Channel, Holdings) {
        let env = Holdings::default();
        let a = Channel::over(Ephemeral::create().perform(&env).await, 8);
        let b = Channel::over(Ephemeral::create().perform(&env).await, 8);
        (a, b, env)
    }

    /// Instants written on one side reach the other through a sync, and
    /// the receiver logs them as the sender's so they do not come back.
    #[dialog_common::test]
    async fn it_replicates_instants_to_a_peer_without_echo() {
        let (a, b, _env) = two_channels().await;
        let (pa, pb) = (peer("a"), peer("b"));
        a.join(pb.clone());
        b.join(pa.clone());

        write(&a, vec![fact("doc:1", "doc/title", "Notes")]);
        write(&a, vec![fact("doc:2", "doc/title", "Plans")]);

        let sync = a.since(&pb);
        let Catchup::Instants { instants, sequence } = &sync else {
            panic!("a covers b: {sync:?}");
        };
        assert_eq!(instants.len(), 2);
        assert_eq!(*sequence, 2);
        assert_eq!(b.receive(&pa, sync), 2);
        assert_eq!(titles(&b), vec!["Notes".to_string(), "Plans".to_string()]);

        // b has nothing of its own to send back: what it received from
        // a is a's, and a is current.
        let back = b.since(&pa);
        assert_eq!(
            back,
            Catchup::Instants {
                instants: Vec::new(),
                sequence: 2
            }
        );
        assert_eq!(a.receive(&pb, back), 0);
        assert_eq!(a.since(&pb).sequence(), 2);
        assert!(matches!(a.since(&pb), Catchup::Instants { instants, .. } if instants.is_empty()));
    }

    /// A write on each side converges both ways: each sync carries only
    /// the other side's own instants.
    #[dialog_common::test]
    async fn it_converges_writes_from_both_sides() {
        let (a, b, _env) = two_channels().await;
        let (pa, pb) = (peer("a"), peer("b"));
        a.join(pb.clone());
        b.join(pa.clone());

        write(&a, vec![fact("doc:1", "doc/title", "From a")]);
        write(&b, vec![fact("doc:2", "doc/title", "From b")]);
        b.receive(&pa, a.since(&pb));
        a.receive(&pb, b.since(&pa));
        assert_eq!(titles(&a), titles(&b));
        assert_eq!(titles(&a), vec!["From a".to_string(), "From b".to_string()]);

        // Another round carries nothing: both are current.
        let (ab, ba) = (a.since(&pb), b.since(&pa));
        assert!(matches!(&ab, Catchup::Instants { instants, .. } if instants.is_empty()));
        assert!(matches!(&ba, Catchup::Instants { instants, .. } if instants.is_empty()));
    }

    /// A retract replicates as a retract: the fact leaves the peer too.
    #[dialog_common::test]
    async fn it_replicates_retracts() {
        let (a, b, _env) = two_channels().await;
        let (pa, pb) = (peer("a"), peer("b"));
        a.join(pb.clone());
        b.join(pa.clone());
        write(&a, vec![fact("doc:1", "doc/title", "Notes")]);
        b.receive(&pa, a.since(&pb));
        assert_eq!(titles(&b), vec!["Notes".to_string()]);

        let mut changes = Changes::new();
        let gone = fact("doc:1", "doc/title", "Notes");
        changes.dissociate(gone.the, gone.of, gone.is);
        a.layer().apply(changes).unwrap();
        b.receive(&pa, a.since(&pb));
        assert!(titles(&b).is_empty());
    }

    /// A peer further behind than the log holds resyncs from the fold
    /// and ends up current, with the fold's facts and nothing stale.
    #[dialog_common::test]
    async fn it_resyncs_a_peer_that_fell_off_the_log() {
        let (a, b, _env) = two_channels().await;
        let (pa, pb) = (peer("a"), peer("b"));
        a.join(pb.clone());
        b.join(pa.clone());
        write(&a, vec![fact("doc:0", "doc/title", "Early")]);
        b.receive(&pa, a.since(&pb));
        write(&b, vec![fact("doc:stale", "doc/title", "Only on b")]);

        // Twelve writes against a log of eight: b's offset (1) is gone.
        for i in 1..=12 {
            write(
                &a,
                vec![fact(&format!("doc:{i}"), "doc/title", &format!("T{i}"))],
            );
        }
        let sync = a.since(&pb);
        let Catchup::Resync {
            facts, sequence, ..
        } = &sync
        else {
            panic!("b fell off the log: {sync:?}");
        };
        assert_eq!(*sequence, 13);
        assert_eq!(facts.len(), 13);
        b.receive(&pa, sync);
        assert_eq!(titles(&b), titles(&a), "b holds exactly a's fold");
        assert!(!titles(&b).contains(&"Only on b".to_string()));
        assert_eq!(a.offset(&pb), Some(13));

        // b is current now: the next sync is an empty instants list.
        assert!(matches!(a.since(&pb), Catchup::Instants { instants, .. } if instants.is_empty()));
    }

    /// Tombstone maintenance is distinct from storing or retracting a
    /// held fact, both in incremental replication and after a resync.
    #[dialog_common::test]
    async fn it_replicates_tombstones_and_their_removal() {
        let (a, b, _env) = two_channels().await;
        let (pa, pb) = (peer("a"), peer("b"));
        a.join(pb.clone());
        b.join(pa.clone());
        let hidden = fact("doc:lower", "doc/title", "Below");
        let mut changes = Changes::new();
        changes.dissociate(hidden.the.clone(), hidden.of.clone(), hidden.is.clone());
        a.layer().apply(changes).unwrap();
        b.receive(&pa, a.since(&pb));
        assert_eq!(
            a.layer().tombstones(&Manifest::default()),
            b.layer().tombstones(&Manifest::default())
        );
        assert_eq!(b.layer().tombstones(&Manifest::default()).len(), 1);
        a.layer().clear();
        b.receive(&pa, a.since(&pb));
        assert!(
            b.layer().is_empty(),
            "lifting a tombstone must not store its fact"
        );
        assert!(b.layer().tombstones(&Manifest::default()).is_empty());

        let mut changes = Changes::new();
        changes.dissociate(hidden.the.clone(), hidden.of.clone(), hidden.is.clone());
        a.layer().apply(changes).unwrap();
        write(&b, vec![fact("doc:stale", "doc/title", "Stale")]);
        b.receive(&pa, a.snapshot());
        assert_eq!(
            a.layer().tombstones(&Manifest::default()),
            b.layer().tombstones(&Manifest::default())
        );
        assert!(b.layer().is_empty());
        a.receive(&pb, b.since(&pa));
        assert!(
            a.layer().is_empty(),
            "a resync cannot echo superseded writes"
        );
        a.layer().clear();
        b.receive(&pa, a.snapshot());
        assert!(
            b.layer().tombstones(&Manifest::default()).is_empty(),
            "a resync clears stale tombstones"
        );
    }

    /// Retention: what every peer has seen leaves the log, the slowest
    /// peer holds the rest, and the ring bound caps it regardless.
    #[dialog_common::test]
    async fn it_retains_only_what_the_slowest_peer_needs_under_the_bound() {
        let (a, _b, _env) = two_channels().await;
        let (fast, slow) = (peer("fast"), peer("slow"));
        a.join(fast.clone());
        a.join(slow.clone());
        for i in 1..=5 {
            write(
                &a,
                vec![fact(&format!("doc:{i}"), "doc/title", &format!("T{i}"))],
            );
        }
        assert_eq!(a.len(), 5);
        a.since(&fast);
        assert_eq!(a.len(), 5, "slow still needs every instant");
        a.since(&slow);
        assert_eq!(a.len(), 0, "everyone has everything");

        for i in 6..=20 {
            write(
                &a,
                vec![fact(&format!("doc:{i}"), "doc/title", &format!("T{i}"))],
            );
        }
        assert_eq!(
            a.len(),
            8,
            "the ring bound caps what a slow peer holds back"
        );
        a.leave(&slow);
        a.since(&fast);
        assert_eq!(a.len(), 0);
    }

    /// A peer admitted at the current sequence sees only what follows;
    /// one admitted at zero is behind by everything and, while the log
    /// still covers it, is caught up from the log.
    #[dialog_common::test]
    async fn it_admits_peers_at_an_offset() {
        let (a, _b, _env) = two_channels().await;
        write(&a, vec![fact("doc:1", "doc/title", "Before")]);
        let late = peer("late");
        let early = peer("early");
        assert_eq!(a.join(late.clone()), 1);
        a.join_at(early.clone(), 0);
        write(&a, vec![fact("doc:2", "doc/title", "After")]);

        let Catchup::Instants { instants, .. } = a.since(&late) else {
            panic!("late is covered");
        };
        assert_eq!(instants.len(), 1);
        assert_eq!(instants[0].asserted[0].of.to_string(), "doc:2");

        let Catchup::Instants { instants, .. } = a.since(&early) else {
            panic!("early is covered by the log");
        };
        assert_eq!(instants.len(), 2);
        assert_eq!(a.entries().len(), 0, "both peers have everything now");
    }

    #[dialog_common::test]
    async fn it_replicates_a_witness_without_storing_or_echoing_it() {
        let (a, b, _env) = two_channels().await;
        let (pa, pb) = (peer("a"), peer("b"));
        a.join(pb.clone());
        b.join(pa.clone());
        let observer = b.layer().observe_everything();
        let command = fact("cmd:1", "cmd/target", "doc:1");
        let mut changes = Changes::new();
        assert_all(&mut changes, [command.clone()]);
        a.layer().witness(changes);

        assert_eq!(b.receive(&pa, a.since(&pb)), 1);
        assert!(a.layer().is_empty());
        assert!(
            b.layer().is_empty(),
            "a witnessed command must not become stored state"
        );
        assert!(b.layer().tombstones(&Manifest::default()).is_empty());
        let super::super::Drained::Instants(instants) = observer.drain() else {
            panic!("one command cannot overflow the observer");
        };
        assert_eq!(instants.len(), 1);
        assert!(instants[0].transient);
        assert_eq!(instants[0].asserted, vec![command.clone()]);
        assert_eq!(instants[0].retracted, vec![command]);
        assert!(matches!(b.since(&pa), Catchup::Instants { instants, .. } if instants.is_empty()));
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn it_does_not_acknowledge_concurrent_writes_before_draining_them() {
        use std::thread;
        let a = Channel::new(Ephemeral::detached());
        let b = Channel::new(Ephemeral::detached());
        let (pa, pb) = (peer("a"), peer("b"));
        a.join(pb.clone());
        b.join(pa.clone());
        let writer = a.clone();
        let writing = thread::spawn(move || {
            for i in 0..2048 {
                write(
                    &writer,
                    vec![fact(&format!("doc:{i}"), "doc/title", "Concurrent")],
                );
                thread::yield_now();
            }
        });
        while !writing.is_finished() {
            b.receive(&pa, a.since(&pb));
            thread::yield_now();
        }
        writing.join().unwrap();
        b.receive(&pa, a.since(&pb));
        assert_eq!(a.layer().len(), 2048);
        assert_eq!(b.layer().facts(), a.layer().facts());
    }

    /// The sync is what crosses a wire: it round-trips through JSON.
    #[dialog_common::test]
    async fn it_serializes_a_sync() {
        let (a, _b, _env) = two_channels().await;
        let pb = peer("b");
        a.join(pb.clone());
        write(&a, vec![fact("doc:1", "doc/title", "Notes")]);
        let sync = a.since(&pb);
        let encoded = serde_json::to_vec(&sync).expect("sync encodes");
        let decoded: Catchup = serde_json::from_slice(&encoded).expect("sync decodes");
        assert_eq!(decoded, sync);
    }
}
