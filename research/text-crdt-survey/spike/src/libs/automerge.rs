use automerge::{transaction::Transactable, ActorId, AutoCommit, Change, ObjType, ReadDoc, ROOT};

use crate::{Lib, Ooo, Patch};

pub struct Am;

fn text_obj(doc: &AutoCommit) -> Option<automerge::ObjId> {
    doc.get(ROOT, "text").ok().flatten().map(|(_, id)| id)
}

fn text_of(doc: &AutoCommit) -> String {
    match text_obj(doc) {
        Some(o) => doc.text(&o).unwrap(),
        None => String::new(),
    }
}

fn decode(b: &[u8]) -> Change {
    Change::try_from(b).expect("decode change")
}

impl Lib for Am {
    type R = AutoCommit;
    const NAME: &'static str = "automerge";

    fn new(agent: usize) -> AutoCommit {
        AutoCommit::new().with_actor(ActorId::from(vec![agent as u8 + 1]))
    }

    fn apply(r: &mut AutoCommit, change: &[u8]) {
        r.apply_changes(vec![decode(change)]).expect("apply");
    }

    fn local(r: &mut AutoCommit, patches: &[Patch]) -> (Vec<u8>, String) {
        // The first transaction creates the shared text object; every later
        // one descends from it.
        let obj = match text_obj(r) {
            Some(o) => o,
            None => r.put_object(ROOT, "text", ObjType::Text).unwrap(),
        };
        for (pos, del, ins) in patches {
            r.splice_text(&obj, *pos, *del as isize, ins).unwrap();
        }
        r.commit();
        let mut c = r.get_last_local_change().expect("change");
        let id = format!(
            "hash {} = (actor {}, seq {}), deps {}",
            c.hash(),
            c.actor_id(),
            c.seq(),
            c.deps().len()
        );
        (c.bytes().to_vec(), id)
    }

    fn text(r: &mut AutoCommit) -> String {
        text_of(r)
    }

    fn snapshot(r: &mut AutoCommit) -> Vec<u8> {
        r.save()
    }

    fn batch(r: &mut AutoCommit, _changes: &[Vec<u8>]) -> Vec<u8> {
        let hashes: Vec<_> = r.get_changes(&[]).iter().map(|c| c.hash()).collect();
        r.bundle(hashes).expect("bundle").bytes().to_vec()
    }

    fn load_snapshot(bytes: &[u8]) -> String {
        text_of(&AutoCommit::load(bytes).unwrap())
    }

    fn load_each(changes: &[Vec<u8>]) -> String {
        let mut d = AutoCommit::new();
        for c in changes {
            d.apply_changes(vec![decode(c)]).unwrap();
        }
        text_of(&d)
    }

    fn load_one_call(changes: &[Vec<u8>]) -> String {
        let mut d = AutoCommit::new();
        d.apply_changes(changes.iter().map(|c| decode(c)).collect::<Vec<_>>())
            .unwrap();
        text_of(&d)
    }

    fn out_of_order(changes: &[Vec<u8>]) -> Ooo {
        let mut d = AutoCommit::new();
        let mut errors = 0;
        let mut first_error = None;
        let mut max_missing = 0;
        for (i, c) in changes.iter().enumerate() {
            if let Err(e) = d.apply_changes(vec![decode(c)]) {
                errors += 1;
                first_error.get_or_insert(e.to_string());
            }
            if i % 1000 == 0 {
                let heads = d.get_heads();
                max_missing = max_missing.max(d.get_missing_deps(&heads).len());
            }
        }
        Ooo {
            text: text_of(&d),
            errors,
            first_error,
            note: format!(
                "apply_changes queues changes whose deps are missing; get_missing_deps reported up to {max_missing} missing hashes at sampled points"
            ),
        }
    }
}
