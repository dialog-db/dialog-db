//! Diamond Types at git main (package version 2.0.0, rev 89ae3a0a).
//! Same API shape as 1.0 (`ListCRDT`, `ListOpLog`, `ListBranch`,
//! `encode_from`, `decode_and_add`), with an Eg-walker merge underneath.

use diamond_types_git::list::encoding::{EncodeOptions, ENCODE_FULL, ENCODE_PATCH};
use diamond_types_git::list::{ListCRDT, ListOpLog};
use diamond_types_git::AgentId;

use crate::{Lib, Ooo, Patch};

pub struct Dt2;

pub struct Replica {
    doc: ListCRDT,
    agent: AgentId,
    name: String,
}

fn text_of_oplog(o: &ListOpLog) -> String {
    o.checkout_tip().content().to_string()
}

fn seq_end(o: &ListOpLog, name: &str) -> usize {
    o.iter_remote_mappings()
        .filter(|m| m.0 == name)
        .map(|m| m.1.end)
        .max()
        .unwrap_or(0)
}

impl Lib for Dt2 {
    type R = Replica;
    const NAME: &'static str = "diamond-types git";

    fn new(agent: usize) -> Replica {
        let mut doc = ListCRDT::new();
        let name = format!("agent{agent}");
        let agent = doc.get_or_create_agent_id(&name);
        Replica { doc, agent, name }
    }

    fn apply(r: &mut Replica, change: &[u8]) {
        r.doc.merge_data_and_ff(change).expect("merge");
    }

    fn local(r: &mut Replica, patches: &[Patch]) -> (Vec<u8>, String) {
        let before = r.doc.oplog.local_frontier();
        let seq_before = seq_end(&r.doc.oplog, &r.name);
        for (pos, del, ins) in patches {
            if *del > 0 {
                r.doc.delete(r.agent, *pos..*pos + *del);
            }
            if !ins.is_empty() {
                r.doc.insert(r.agent, *pos, ins);
            }
        }
        let bytes = r.doc.oplog.encode_from(&ENCODE_PATCH, before.as_ref());
        let seq_after = seq_end(&r.doc.oplog, &r.name);
        let id = format!("(agent \"{}\", seq {seq_before}..{seq_after})", r.name);
        (bytes, id)
    }

    fn text(r: &mut Replica) -> String {
        r.doc.branch.content().to_string()
    }

    fn snapshot(r: &mut Replica) -> Vec<u8> {
        // The experimental end-content chunk is written but this rev has
        // no decoder for it; report its size only.
        let with_end = EncodeOptions::full()
            .experimentally_store_end_branch_content(true)
            .build();
        eprintln!(
            "    dt git: snapshot with experimental end content = {} B (not loadable at this rev)",
            r.doc.oplog.encode(&with_end).len()
        );
        r.doc.oplog.encode(&ENCODE_FULL)
    }

    fn batch(r: &mut Replica, _changes: &[Vec<u8>]) -> Vec<u8> {
        r.doc.oplog.encode(&ENCODE_PATCH)
    }

    fn load_snapshot(bytes: &[u8]) -> String {
        // The saved form is the op log; the text is rebuilt from it.
        text_of_oplog(&ListOpLog::load_from(bytes).unwrap())
    }

    fn load_each(changes: &[Vec<u8>]) -> String {
        // One decode_and_add per change into the op log; the text (the
        // positional index) is built once at the end.
        let mut o = ListOpLog::new();
        for c in changes {
            o.decode_and_add(c).unwrap();
        }
        text_of_oplog(&o)
    }

    fn load_one_call(changes: &[Vec<u8>]) -> String {
        // No multi-chunk import; merge into a live branch per change, as an
        // open editor would.
        let mut d = ListCRDT::new();
        for c in changes {
            d.merge_data_and_ff(c).unwrap();
        }
        d.branch.content().to_string()
    }

    fn out_of_order(changes: &[Vec<u8>]) -> Ooo {
        let mut o = ListOpLog::new();
        let mut errors = 0;
        let mut first_error = None;
        let mut held: Vec<&Vec<u8>> = Vec::new();
        for c in changes {
            match o.decode_and_add(c) {
                Ok(_) => loop {
                    // A buffer the caller keeps: retry held changes until
                    // none applies.
                    let before = held.len();
                    held.retain(|h| o.decode_and_add(h).is_err());
                    if held.len() == before {
                        break;
                    }
                },
                Err(e) => {
                    errors += 1;
                    first_error.get_or_insert(format!("{e:?}"));
                    held.push(c);
                }
            }
        }
        Ooo {
            text: text_of_oplog(&o),
            errors,
            first_error,
            note: format!(
                "decode_and_add refuses a change whose parents are missing and rolls back; convergence shown is with a caller-side retry buffer ({} left at end)",
                held.len()
            ),
        }
    }
}
