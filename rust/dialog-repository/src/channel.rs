//! A channel: a layer whose rows live one tick, with two ends.
//!
//! A channel is the simplest layer a stack holds. It keeps no table: a
//! row of it lives for one tick, the tick it is stimulus of, and is gone
//! after. What makes it a channel rather than a scratch space is that it
//! has **two ends**, and what is asserted on one end is a row of the
//! channel on the other end for one tick:
//!
//! - **Outside to inside.** [`Channel::send`] stages rows on the outside
//!   end. The next dispatch through the stack takes them at its first
//!   tick, as rows of the channel, and after that tick they are gone. A
//!   send while a dispatch runs is taken by the next dispatch, not the
//!   running one.
//! - **Inside to outside.** What a dispatch writes into the channel (a
//!   write `.at(channel)`, a rule head whose descriptor names the
//!   channel) is the next tick's stimulus inside the dispatch, read as a
//!   row of the channel, and, once the dispatch's publish has won, lands
//!   on the outside end as one tick of the channel's **log**: its
//!   sequence and its rows. A publish that loses its compare-and-swap
//!   lands nothing. A reader, [`Arrivals`](crate::Arrivals), reads the
//!   log from its position, so a reader that was not reading still gets
//!   every tick once, oldest first.
//!
//! The log is bounded by its readers: a tick every reader has passed is
//! dropped. While no reader exists, the channel keeps at most its latest
//! tick, which a reader built then reads first.
//!
//! [`CONTROL`](crate::CONTROL) is the built-in channel, the loopback:
//! every stack has it, [`Stack::invoke`](crate::Stack::invoke) sends its
//! command into it and runs a dispatch, and what a dispatch sends or
//! writes into it lands on its outside end, where the stack's providers
//! read their commands. Any other channel is any entity but a
//! [`MEMORY`](crate::MEMORY) or [`SESSION`](crate::SESSION) one: one the
//! primary's schema declares ([`LayerType::Channel`](crate::LayerType::Channel)),
//! or one [`Stack::channel`](crate::Stack::channel) creates. Its inbound
//! rows are read inside alone: only `control:` hands back what is sent
//! into it.
//!
//! ```no_run
//! # use dialog_repository::{Stack, StackEnv};
//! # async fn example(stack: Stack, env: impl StackEnv) -> anyhow::Result<()> {
//! let mailbox: dialog_corpus::Entity = "mail:box".parse()?;
//! let channel = stack.channel(&mailbox)?;
//! channel.send(
//!     dialog_query::the!("mail.message/subject")
//!         .of("message:1".parse::<dialog_corpus::Entity>()?)
//!         .is("Hello".to_string()),
//! );
//! // The next dispatch reads the message as a row of `mail:box`.
//! stack.assert(dialog_corpus::Changes::new()).perform(&env).await?;
//! # Ok(())
//! # }
//! ```

use std::collections::VecDeque;
use std::fmt::{Debug, Formatter, Result as FmtResult};
use std::mem::take;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};

use dialog_common::Blake3Hash;
use dialog_corpus::{Change, Changes, Entity, Fact, Statement};
use parking_lot::Mutex;
use tokio::sync::Notify;

/// A channel layer of a stack: rows that live one tick, with an outside
/// end that sends rows in ([`send`](Self::send)) and a log of the ticks
/// that landed rows out, read with [`Arrivals`](crate::Arrivals). See the
/// [module docs](self). Cheap to clone: clones share the channel.
#[derive(Clone)]
pub struct Channel {
    shared: Arc<Shared>,
}

struct Shared {
    entity: Entity,
    state: Mutex<State>,
    /// Woken each time a dispatch through the stack has published and
    /// landed what it wrote into the channel, whether or not it wrote
    /// anything: what [`Arrivals::next`](crate::Arrivals::next) waits on.
    landed: Notify,
}

#[derive(Default)]
struct State {
    /// What the outside end sent that no dispatch has taken yet.
    sent: Changes,
    /// The ticks that landed on the outside end and some reader has not
    /// passed, oldest first; while no reader exists, the latest alone.
    log: VecDeque<Tick>,
    /// The sequence of the last tick that landed; zero for none.
    sequence: u64,
    /// The read position of every reader, held weakly: a reader dropped
    /// no longer holds the log back.
    readers: Vec<Weak<AtomicU64>>,
}

/// One tick of a channel's log: the rows one published dispatch landed
/// on its outside end.
struct Tick {
    sequence: u64,
    rows: Vec<Fact>,
}

impl State {
    /// Drop the ticks no reader needs: those every live reader has
    /// passed, or, with no reader, all but the latest.
    fn prune(&mut self) {
        let mut least: Option<u64> = None;
        self.readers.retain(|reader| match reader.upgrade() {
            Some(position) => {
                let at = position.load(Ordering::Acquire);
                least = Some(least.map_or(at, |least| least.min(at)));
                true
            }
            None => false,
        });
        match least {
            Some(least) => {
                while self.log.front().is_some_and(|tick| tick.sequence <= least) {
                    self.log.pop_front();
                }
            }
            None => {
                while self.log.len() > 1 {
                    self.log.pop_front();
                }
            }
        }
    }
}

impl Channel {
    /// A channel named `entity`, with nothing sent and nothing landed.
    pub(crate) fn new(entity: Entity) -> Self {
        Self {
            shared: Arc::new(Shared {
                entity,
                state: Mutex::new(State::default()),
                landed: Notify::new(),
            }),
        }
    }

    /// The entity naming this channel: what a descriptor's `in:` names.
    pub fn entity(&self) -> &Entity {
        &self.shared.entity
    }

    /// Send `statement` from the outside end: its rows are read by the
    /// next dispatch through the stack, at its first tick, as rows of
    /// this channel, and by no tick after. A layer the statement names
    /// (`.at(..)`) is ignored: what is sent is a row of this channel.
    /// Chainable.
    pub fn send<S: Statement>(&self, statement: S) -> &Self {
        let mut changes = Changes::new();
        statement.assert(&mut changes);
        self.send_changes(changes)
    }

    /// Send every change of `changes` from the outside end, as
    /// [`send`](Self::send) does. Chainable.
    pub fn send_changes(&self, mut changes: Changes) -> &Self {
        for (_, share) in changes.take_shares() {
            changes.merge(share);
        }
        self.shared.state.lock().sent.merge(changes);
        self
    }

    /// The sequence of the last tick that landed on the outside end:
    /// how many dispatches have landed rows in this channel. Zero for a
    /// channel nothing has landed in.
    pub fn sequence(&self) -> u64 {
        self.shared.state.lock().sequence
    }

    /// The revision this channel stands at: a hash over the sequence of
    /// the last tick that landed on its outside end
    /// ([`sequence`](Self::sequence)). What the stack's
    /// `dialog.layer/revision` row of the channel names, in base58. A
    /// channel's revision has no entity form yet.
    pub fn revision(&self) -> Blake3Hash {
        Blake3Hash::hash(&self.sequence().to_be_bytes())
    }

    /// Take what the outside end sent, with `also` after it, as one
    /// batch bound for no layer: the first tick's stimulus of the
    /// dispatch taking it. A send after this is the next dispatch's.
    pub(crate) fn take(&self, also: Changes) -> Changes {
        let mut state = self.shared.state.lock();
        let mut taken = take(&mut state.sent);
        taken.merge(also);
        taken
    }

    /// Put `taken` back before what was sent since: a dispatch that took
    /// it failed, so the next one reads it.
    pub(crate) fn restore(&self, taken: Changes) {
        let mut state = self.shared.state.lock();
        let mut sent = taken;
        sent.merge(take(&mut state.sent));
        state.sent = sent;
    }

    /// Land what a published dispatch wrote into the channel on its
    /// outside end: the facts `changes` asserts, as one tick, its
    /// retractions landing nothing. A dispatch that asserted nothing here
    /// lands no tick. Wakes every reader waiting either way.
    pub(crate) fn land(&self, changes: &Changes) {
        let rows: Vec<Fact> = changes
            .iter()
            .filter_map(|(of, the, change)| match change {
                Change::Assert(is, _) => Some(Fact {
                    the: the.clone(),
                    of: of.clone(),
                    is: is.clone(),
                    cause: None,
                }),
                Change::Retract(_) => None,
            })
            .collect();
        if !rows.is_empty() {
            let mut state = self.shared.state.lock();
            state.sequence += 1;
            let sequence = state.sequence;
            state.log.push_back(Tick { sequence, rows });
            state.prune();
        }
        self.shared.landed.notify_waiters();
    }

    /// A new reader of the log, positioned before the oldest tick the
    /// log holds, so its first read yields every tick held.
    pub(crate) fn reader(&self) -> Arc<AtomicU64> {
        let mut state = self.shared.state.lock();
        let start = match state.log.front() {
            Some(tick) => tick.sequence - 1,
            None => state.sequence,
        };
        let position = Arc::new(AtomicU64::new(start));
        state.readers.push(Arc::downgrade(&position));
        position
    }

    /// The rows of every tick after `position`, oldest first; `position`
    /// then stands at the latest tick, and the ticks every reader has
    /// passed are dropped.
    pub(crate) fn read(&self, position: &AtomicU64) -> Vec<Fact> {
        let mut state = self.shared.state.lock();
        let at = position.load(Ordering::Acquire);
        let mut rows = Vec::new();
        for tick in state.log.iter().filter(|tick| tick.sequence > at) {
            rows.extend(tick.rows.iter().cloned());
        }
        position.store(state.sequence.max(at), Ordering::Release);
        state.prune();
        rows
    }

    /// What wakes a reader waiting for the next dispatch.
    pub(crate) fn landed(&self) -> &Notify {
        &self.shared.landed
    }

    /// How many ticks the log holds.
    #[cfg(test)]
    pub(crate) fn held(&self) -> usize {
        self.shared.state.lock().log.len()
    }

    /// Whether the outside end sent anything no dispatch has taken.
    pub fn is_waiting(&self) -> bool {
        !self.shared.state.lock().sent.is_empty()
    }
}

impl Debug for Channel {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("Channel")
            .field("entity", &self.shared.entity)
            .field("sequence", &self.sequence())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use dialog_corpus::{Cardinality, Update as _, Value};

    fn rows(subjects: &[&str]) -> Changes {
        let mut changes = Changes::new();
        for subject in subjects {
            changes.associate(
                "mail.message/subject".parse().expect("attribute"),
                subject.parse().expect("entity"),
                Value::String((*subject).into()),
                Cardinality::Many,
            );
        }
        changes
    }

    fn subjects(facts: Vec<Fact>) -> Vec<String> {
        facts.into_iter().map(|fact| fact.of.to_string()).collect()
    }

    fn mailbox() -> Channel {
        Channel::new("mail:box".parse().expect("entity"))
    }

    /// A reader built before the ticks landed reads each once, oldest
    /// first; a reader built late reads every tick the log still holds,
    /// once; and a tick both have passed is dropped.
    #[dialog_common::test]
    fn it_reads_every_tick_once_for_each_reader() -> anyhow::Result<()> {
        let channel = mailbox();
        let early = channel.reader();
        channel.land(&rows(&["message:1"]));
        channel.land(&rows(&["message:2"]));
        let late = channel.reader();
        assert_eq!(channel.held(), 2, "the early reader holds both");
        assert_eq!(
            subjects(channel.read(&late)),
            vec!["message:1".to_string(), "message:2".to_string()],
            "a late reader gets every tick held"
        );
        assert!(channel.read(&late).is_empty(), "once");
        assert_eq!(channel.held(), 2, "the early reader has passed none");
        assert_eq!(
            subjects(channel.read(&early)),
            vec!["message:1".to_string(), "message:2".to_string()]
        );
        assert_eq!(channel.held(), 0, "a tick both passed is dropped");
        channel.land(&rows(&["message:3"]));
        assert_eq!(
            subjects(channel.read(&early)),
            vec!["message:3".to_string()]
        );
        assert_eq!(subjects(channel.read(&late)), vec!["message:3".to_string()]);
        Ok(())
    }

    /// With no reader the log keeps its latest tick alone, and a reader
    /// dropped no longer holds the log back.
    #[dialog_common::test]
    fn it_keeps_the_latest_tick_while_no_reader_exists() -> anyhow::Result<()> {
        let channel = mailbox();
        channel.land(&rows(&["message:1"]));
        channel.land(&rows(&["message:2"]));
        assert_eq!(channel.held(), 1);
        let reader = channel.reader();
        channel.land(&rows(&["message:3"]));
        assert_eq!(
            subjects(channel.read(&reader)),
            vec!["message:2".to_string(), "message:3".to_string()]
        );
        channel.land(&rows(&["message:4"]));
        channel.land(&rows(&["message:5"]));
        assert_eq!(channel.held(), 2, "the reader holds what it has not read");
        drop(reader);
        channel.land(&rows(&["message:6"]));
        assert_eq!(channel.held(), 1, "the dropped reader holds nothing");
        Ok(())
    }

    /// A landing that asserts nothing lands no tick and moves no
    /// revision; one that asserts moves both.
    #[dialog_common::test]
    fn it_moves_its_revision_with_each_tick_that_landed() -> anyhow::Result<()> {
        let channel = mailbox();
        let before = channel.revision();
        let mut retraction = Changes::new();
        retraction.dissociate(
            "mail.message/subject".parse()?,
            "message:1".parse()?,
            Value::String("message:1".into()),
        );
        channel.land(&retraction);
        assert_eq!(channel.sequence(), 0);
        assert_eq!(channel.revision(), before);
        channel.land(&rows(&["message:1"]));
        assert_eq!(channel.sequence(), 1);
        assert_ne!(channel.revision(), before);
        Ok(())
    }

    /// What is sent is taken once, by the first take after it; a
    /// restored take is read again before what was sent since.
    #[dialog_common::test]
    fn it_hands_what_was_sent_to_one_take() -> anyhow::Result<()> {
        let channel = mailbox();
        channel.send_changes(rows(&["message:1"]));
        assert!(channel.is_waiting());
        let taken = channel.take(Changes::new());
        assert_eq!(taken.iter().count(), 1);
        assert!(!channel.is_waiting());
        assert!(channel.take(Changes::new()).is_empty(), "taken once");
        channel.send_changes(rows(&["message:2"]));
        channel.restore(taken);
        assert_eq!(channel.take(Changes::new()).iter().count(), 2);
        Ok(())
    }
}
