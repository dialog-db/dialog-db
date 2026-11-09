use loro::{ExportMode, LoroDoc};

use crate::{Lib, Ooo, Patch};

pub struct Lo;

fn text_of(d: &LoroDoc) -> String {
    d.get_text("t").to_string()
}

impl Lib for Lo {
    type R = LoroDoc;
    const NAME: &'static str = "loro";

    fn new(agent: usize) -> LoroDoc {
        let d = LoroDoc::new();
        d.set_peer_id(agent as u64 + 1).unwrap();
        d
    }

    fn apply(r: &mut LoroDoc, change: &[u8]) {
        let s = r.import(change).expect("import");
        assert!(s.pending.is_none(), "causal import left pending ops");
    }

    fn local(r: &mut LoroDoc, patches: &[Patch]) -> (Vec<u8>, String) {
        let before = r.oplog_vv();
        let t = r.get_text("t");
        for (pos, del, ins) in patches {
            t.splice(*pos, *del, ins).unwrap();
        }
        r.commit();
        let after = r.oplog_vv();
        let bytes = r.export(ExportMode::updates(&before)).unwrap();
        let peer = r.peer_id();
        let id = format!(
            "(peer {peer}, counter {}..{})",
            before.get(&peer).copied().unwrap_or(0),
            after.get(&peer).copied().unwrap_or(0)
        );
        (bytes, id)
    }

    fn text(r: &mut LoroDoc) -> String {
        text_of(r)
    }

    fn snapshot(r: &mut LoroDoc) -> Vec<u8> {
        r.export(ExportMode::Snapshot).unwrap()
    }

    fn batch(r: &mut LoroDoc, _changes: &[Vec<u8>]) -> Vec<u8> {
        r.export(ExportMode::all_updates()).unwrap()
    }

    fn load_snapshot(bytes: &[u8]) -> String {
        text_of(&LoroDoc::from_snapshot(bytes).unwrap())
    }

    fn load_each(changes: &[Vec<u8>]) -> String {
        let d = LoroDoc::new();
        for c in changes {
            d.import(c).unwrap();
        }
        text_of(&d)
    }

    fn load_one_call(changes: &[Vec<u8>]) -> String {
        let d = LoroDoc::new();
        d.import_batch(changes).unwrap();
        text_of(&d)
    }

    fn out_of_order(changes: &[Vec<u8>]) -> Ooo {
        let d = LoroDoc::new();
        let mut errors = 0;
        let mut first_error = None;
        let mut pending_reports = 0;
        for c in changes {
            match d.import(c) {
                Ok(s) => {
                    if s.pending.is_some() {
                        pending_reports += 1;
                    }
                }
                Err(e) => {
                    errors += 1;
                    first_error.get_or_insert(e.to_string());
                }
            }
        }
        Ooo {
            text: text_of(&d),
            errors,
            first_error,
            note: format!(
                "import holds back ops whose deps are missing; {pending_reports} of {} imports returned ImportStatus.pending",
                changes.len()
            ),
        }
    }
}
