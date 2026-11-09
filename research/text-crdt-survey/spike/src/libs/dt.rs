use diamond_types::list::encoding::{ENCODE_FULL, ENCODE_PATCH};
use diamond_types::list::{ListCRDT, OpLog};

use crate::{Lib, Ooo, Patch};

pub struct Dt;

pub struct Replica {
    doc: ListCRDT,
    agent: diamond_types::AgentId,
    name: String,
}

fn text_of_oplog(o: &OpLog) -> String {
    o.checkout_tip().content().to_string()
}

impl Lib for Dt {
    type R = Replica;
    const NAME: &'static str = "diamond-types";

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
        let before = r.doc.oplog.local_version();
        let seq_before = r
            .doc
            .oplog
            .iter_mappings()
            .filter(|m| m.agent == r.agent)
            .map(|m| m.seq_range.end)
            .max()
            .unwrap_or(0);
        for (pos, del, ins) in patches {
            if *del > 0 {
                r.doc.delete(r.agent, *pos..*pos + *del);
            }
            if !ins.is_empty() {
                r.doc.insert(r.agent, *pos, ins);
            }
        }
        let bytes = r.doc.oplog.encode_from(ENCODE_PATCH, &before);
        let seq_after = r
            .doc
            .oplog
            .iter_mappings()
            .filter(|m| m.agent == r.agent)
            .map(|m| m.seq_range.end)
            .max()
            .unwrap_or(0);
        let id = format!("(agent \"{}\", seq {seq_before}..{seq_after})", r.name);
        (bytes, id)
    }

    fn text(r: &mut Replica) -> String {
        r.doc.branch.content().to_string()
    }

    fn snapshot(r: &mut Replica) -> Vec<u8> {
        r.doc.oplog.encode(ENCODE_FULL)
    }

    fn batch(r: &mut Replica, _changes: &[Vec<u8>]) -> Vec<u8> {
        r.doc.oplog.encode(ENCODE_PATCH)
    }

    fn load_snapshot(bytes: &[u8]) -> String {
        // The saved form is the op log; the text is rebuilt from it.
        text_of_oplog(&OpLog::load_from(bytes).unwrap())
    }

    fn load_each(changes: &[Vec<u8>]) -> String {
        // Changes go into the op log one call each; the text (the
        // positional index) is built once at the end.
        let mut o = OpLog::new();
        for c in changes {
            o.decode_and_add(c).unwrap();
        }
        text_of_oplog(&o)
    }

    fn load_one_call(changes: &[Vec<u8>]) -> String {
        // No multi-chunk import in 1.0; merge into a live branch per change
        // instead, which is what an open editor would do.
        let mut d = ListCRDT::new();
        for c in changes {
            d.merge_data_and_ff(c).unwrap();
        }
        d.branch.content().to_string()
    }

    fn out_of_order(changes: &[Vec<u8>]) -> Ooo {
        let mut o = OpLog::new();
        let mut errors = 0;
        let mut first_error = None;
        let mut held: Vec<&Vec<u8>> = Vec::new();
        for c in changes {
            match o.decode_and_add(c) {
                Ok(_) => {
                    // A buffer the caller keeps: retry held changes until
                    // none applies.
                    loop {
                        let before = held.len();
                        held.retain(|h| o.decode_and_add(h).is_err());
                        if held.len() == before {
                            break;
                        }
                    }
                }
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
