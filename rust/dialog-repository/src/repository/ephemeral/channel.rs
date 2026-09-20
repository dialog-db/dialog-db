//! A channel: an ephemeral layer whose instants replicate to peers.
//!
//! A local ephemeral layer fans instants out to in-process observers and
//! keeps no log. A channel is the same layer with the one thing peers
//! need added: a bounded **log** of its instants with a per-peer
//! **offset** into it. Sync is "the instants past my offset"; retention
//! is the lowest peer offset, under a ring bound; a peer whose offset
//! fell off the ring resyncs from the fold. That is the shape presence
//! and awareness protocols already have, with the sequence standing in
//! for a per-peer clock, and it is what makes a replicated scope
//! meaningful without a tree: there is nothing to push but the log.
//!
//! What lands here is the log and its offsets, exercised with peers in
//! one process: a channel on each side and the [`Sync`] carried between
//! them by the test. Carrying it over the remote transport is the part
//! that remains; the [`Sync`] is serializable so that binding is a
//! transport concern, not a change to this shape.
//!
//! Echo is prevented by origin: an instant a channel applied on a
//! peer's behalf is logged as that peer's, and a sync to that peer skips
//! it. Two channels exchanging syncs therefore converge rather than
//! bouncing each other's writes.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use dialog_artifacts::{Artifact, Changes, Entity, Update as _};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use super::{Ephemeral, Instant, Observer};

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

/// What a peer receives when it syncs.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Sync {
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
        /// The offset the peer stands at once the fold is applied.
        sequence: u64,
    },
}

impl Sync {
    /// The offset a peer stands at after applying this.
    pub fn sequence(&self) -> u64 {
        match self {
            Sync::Instants { sequence, .. } | Sync::Resync { sequence, .. } => *sequence,
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
}

/// A replicated ephemeral layer. Cheap to clone: clones share the log.
#[derive(Clone, Debug)]
pub struct Channel {
    layer: Ephemeral,
    observer: Arc<Observer>,
    log: Arc<Mutex<Log>>,
}

impl Channel {
    /// Replicate `layer`, keeping at most `capacity` instants for peers
    /// that fall behind. Instants minted before this call are not in
    /// the log; a peer joining now starts from the fold.
    pub fn over(layer: Ephemeral, capacity: usize) -> Self {
        let observer = Arc::new(layer.observe_everything());
        Self {
            layer,
            observer,
            log: Arc::new(Mutex::new(Log {
                entries: VecDeque::new(),
                capacity: capacity.max(1),
                offsets: HashMap::new(),
                received: HashMap::new(),
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
    /// fold too takes it with [`fold`](Self::fold), or is admitted
    /// behind with [`join_at`](Self::join_at).
    pub fn join(&self, peer: Entity) -> u64 {
        let sequence = self.layer.revision().sequence;
        self.join_at(peer, sequence);
        sequence
    }

    /// Admit `peer` claiming to stand at `sequence`. A claim the log no
    /// longer covers makes the peer's next sync a resync.
    pub fn join_at(&self, peer: Entity, sequence: u64) {
        let mut log = self.log.lock();
        log.offsets.insert(peer, sequence);
    }

    /// Forget `peer`: its offset no longer holds instants back.
    pub fn leave(&self, peer: &Entity) -> bool {
        let mut log = self.log.lock();
        let left = log.offsets.remove(peer).is_some();
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

    /// What `peer` needs to be current: the instants past its offset
    /// that it did not itself send, or the fold if its offset fell off
    /// the log. Moves the peer's offset to the sequence the sync
    /// reaches; a peer not admitted is admitted by this.
    pub fn since(&self, peer: &Entity) -> Sync {
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
            let instants: Vec<Instant> = log
                .entries
                .iter()
                .filter(|entry| entry.instant.sequence > offset)
                .filter(|entry| entry.origin.as_ref() != Some(peer))
                .map(|entry| entry.instant.clone())
                .collect();
            Sync::Instants {
                instants,
                sequence: current,
            }
        } else {
            let (facts, sequence) = self.fold();
            Sync::Resync { facts, sequence }
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
    pub fn receive(&self, peer: &Entity, sync: Sync) -> usize {
        match sync {
            Sync::Instants { instants, .. } => {
                let mut applied = 0;
                for instant in instants {
                    let mut changes = Changes::new();
                    for fact in instant.retracted {
                        if !instant.transient || !instant.asserted.contains(&fact) {
                            changes.dissociate(fact.the, fact.of, fact.is);
                        }
                    }
                    for fact in instant.asserted {
                        changes.associate(fact.the, fact.of, fact.is);
                    }
                    let mut log = self.log.lock();
                    let minted = if instant.transient {
                        self.layer.witness(changes)
                    } else {
                        self.layer.apply(changes)
                    };
                    if let Some(minted) = minted {
                        log.received.insert(minted.sequence, peer.clone());
                        applied += 1;
                    }
                }
                self.absorb(&mut self.log.lock());
                applied
            }
            Sync::Resync { facts, .. } => {
                self.resync(peer, facts);
                1
            }
        }
    }

    /// Replace the layer's facts with `facts`, as `peer`'s fold.
    fn resync(&self, peer: &Entity, facts: Vec<Artifact>) {
        let held = self.layer.facts();
        let mut changes = Changes::new();
        for fact in held {
            changes.dissociate(fact.the, fact.of, fact.is);
        }
        for fact in facts {
            changes.associate(fact.the, fact.of, fact.is);
        }
        let mut log = self.log.lock();
        if let Some(minted) = self.layer.apply(changes) {
            log.received.insert(minted.sequence, peer.clone());
        }
        drop(log);
        self.absorb(&mut self.log.lock());
    }

    /// Drain the layer's instants into the log, tagging the ones that
    /// were applied on a peer's behalf, and bound the log.
    fn absorb(&self, log: &mut Log) -> u64 {
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
                // hole nothing can fill, so every peer resyncs.
                log.entries.clear();
                log.received.clear();
            }
        }
        Self::bound(log);
        sequence
    }

    /// Drop what every peer has and what the ring bound excludes.
    fn retain(&self) {
        let mut log = self.log.lock();
        Self::bound(&mut log);
    }

    fn bound(log: &mut Log) {
        let slowest = log.offsets.values().copied().min();
        while let Some(front) = log.entries.front() {
            let seen_by_all = slowest.is_some_and(|offset| front.instant.sequence <= offset);
            let over = log.entries.len() > log.capacity;
            if seen_by_all || over {
                log.entries.pop_front();
            } else {
                break;
            }
        }
        if log.offsets.is_empty() {
            // Nobody is waiting: keep only the ring, so a late joiner
            // can still be caught up from it.
            while log.entries.len() > log.capacity {
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
        changes.associate(fact.the, fact.of, fact.is);
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
    use crate::helpers::TestEnv;
    use dialog_artifacts::{ArtifactSelector, Value};
    use dialog_operator::helpers::test_operator_with_profile;

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
        channel.layer().apply(changes);
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

    async fn two_channels() -> (Channel, Channel, TestEnv) {
        let (operator, _profile) = test_operator_with_profile().await;
        let env = TestEnv::new(operator);
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
        let Sync::Instants { instants, sequence } = &sync else {
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
            Sync::Instants {
                instants: Vec::new(),
                sequence: 2
            }
        );
        assert_eq!(a.receive(&pb, back), 0);
        assert_eq!(a.since(&pb).sequence(), 2);
        assert!(matches!(a.since(&pb), Sync::Instants { instants, .. } if instants.is_empty()));
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
        assert!(matches!(&ab, Sync::Instants { instants, .. } if instants.is_empty()));
        assert!(matches!(&ba, Sync::Instants { instants, .. } if instants.is_empty()));
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
        a.layer().apply(changes);
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
        let Sync::Resync { facts, sequence } = &sync else {
            panic!("b fell off the log: {sync:?}");
        };
        assert_eq!(*sequence, 13);
        assert_eq!(facts.len(), 13);
        b.receive(&pa, sync);
        assert_eq!(titles(&b), titles(&a), "b holds exactly a's fold");
        assert!(!titles(&b).contains(&"Only on b".to_string()));
        assert_eq!(a.offset(&pb), Some(13));

        // b is current now: the next sync is an empty instants list.
        assert!(matches!(a.since(&pb), Sync::Instants { instants, .. } if instants.is_empty()));
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

        let Sync::Instants { instants, .. } = a.since(&late) else {
            panic!("late is covered");
        };
        assert_eq!(instants.len(), 1);
        assert_eq!(instants[0].asserted[0].of.to_string(), "doc:2");

        let Sync::Instants { instants, .. } = a.since(&early) else {
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
        assert!(b.layer().tombstones().is_empty());
        let super::super::Drained::Instants(instants) = observer.drain() else {
            panic!("one command cannot overflow the observer");
        };
        assert_eq!(instants.len(), 1);
        assert!(instants[0].transient);
        assert_eq!(instants[0].asserted, vec![command.clone()]);
        assert_eq!(instants[0].retracted, vec![command]);
        assert!(matches!(b.since(&pa), Sync::Instants { instants, .. } if instants.is_empty()));
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
        let decoded: Sync = serde_json::from_slice(&encoded).expect("sync decodes");
        assert_eq!(decoded, sync);
    }
}
