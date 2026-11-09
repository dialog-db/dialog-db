use yrs::updates::decoder::Decode;
use yrs::{Doc, GetString, ReadTxn, StateVector, Text, Transact, Update};

use crate::{Lib, Ooo, Patch};

pub struct Y;

fn text_of(d: &Doc) -> String {
    let t = d.get_or_insert_text("t");
    let txn = d.transact();
    t.get_string(&txn)
}

fn apply_one(d: &Doc, b: &[u8]) -> Result<(), String> {
    let u = Update::decode_v1(b).map_err(|e| e.to_string())?;
    d.transact_mut().apply_update(u).map_err(|e| e.to_string())
}

impl Lib for Y {
    type R = Doc;
    const NAME: &'static str = "yrs";

    fn new(agent: usize) -> Doc {
        Doc::with_client_id(agent as u64 + 1)
    }

    fn apply(r: &mut Doc, change: &[u8]) {
        apply_one(r, change).expect("apply");
        let txn = r.transact();
        assert!(
            txn.store().pending_update().is_none(),
            "causal import left pending structs"
        );
    }

    fn local(r: &mut Doc, patches: &[Patch]) -> (Vec<u8>, String) {
        let t = r.get_or_insert_text("t");
        let mut txn = r.transact_mut();
        let before = txn.state_vector();
        for (pos, del, ins) in patches {
            if *del > 0 {
                t.remove_range(&mut txn, *pos as u32, *del as u32);
            }
            if !ins.is_empty() {
                t.insert(&mut txn, *pos as u32, ins);
            }
        }
        let bytes = txn.encode_update_v1();
        let after = txn.state_vector();
        let client = txn.doc().client_id();
        let id = format!(
            "(client {client}, clock {}..{}) + delete set",
            before.get(&client),
            after.get(&client)
        );
        (bytes, id)
    }

    fn text(r: &mut Doc) -> String {
        text_of(r)
    }

    fn snapshot(r: &mut Doc) -> Vec<u8> {
        r.transact()
            .encode_state_as_update_v1(&StateVector::default())
    }

    fn batch(_r: &mut Doc, changes: &[Vec<u8>]) -> Vec<u8> {
        yrs::merge_updates_v1(changes).expect("merge")
    }

    fn load_snapshot(bytes: &[u8]) -> String {
        let d = Doc::new();
        apply_one(&d, bytes).unwrap();
        text_of(&d)
    }

    fn load_each(changes: &[Vec<u8>]) -> String {
        let d = Doc::new();
        for c in changes {
            apply_one(&d, c).unwrap();
        }
        text_of(&d)
    }

    fn load_one_call(changes: &[Vec<u8>]) -> String {
        let d = Doc::new();
        {
            let mut txn = d.transact_mut();
            for c in changes {
                txn.apply_update(Update::decode_v1(c).unwrap()).unwrap();
            }
        }
        text_of(&d)
    }

    fn out_of_order(changes: &[Vec<u8>]) -> Ooo {
        let d = Doc::new();
        let mut errors = 0;
        let mut first_error = None;
        let mut pending_after = 0;
        for c in changes {
            if let Err(e) = apply_one(&d, c) {
                errors += 1;
                first_error.get_or_insert(e);
            }
            if d.transact().store().pending_update().is_some() {
                pending_after += 1;
            }
        }
        let still = {
            let txn = d.transact();
            txn.store().pending_update().is_some() || txn.store().pending_ds().is_some()
        };
        Ooo {
            text: text_of(&d),
            errors,
            first_error,
            note: format!(
                "apply_update stashes structs whose origins are missing; pending after {pending_after} of {} imports; pending at end: {still}",
                changes.len()
            ),
        }
    }
}
