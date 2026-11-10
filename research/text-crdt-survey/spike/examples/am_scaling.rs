//! Does the cost of one Automerge `apply_changes` call grow with document
//! size? Builds a document from a sequential trace, one change per edit,
//! then re-imports the changes one call each and reports time per chunk.

use std::io::Read;
use std::time::Instant;

use automerge::{transaction::Transactable, AutoCommit, ObjType, ROOT};

fn main() {
    let path = std::env::args().nth(1).expect("trace path");
    let mut s = String::new();
    flate2::read::GzDecoder::new(std::fs::File::open(path).unwrap())
        .read_to_string(&mut s)
        .unwrap();
    let v: serde_json::Value = serde_json::from_str(&s).unwrap();
    let mut doc = AutoCommit::new();
    let obj = doc.put_object(ROOT, "text", ObjType::Text).unwrap();
    doc.commit();
    for txn in v["txns"].as_array().unwrap() {
        for p in txn["patches"].as_array().unwrap() {
            let pos = p[0].as_u64().unwrap() as usize;
            let del = p[1].as_i64().unwrap() as isize;
            let ins = p[2].as_str().unwrap();
            doc.splice_text(&obj, pos, del, ins).unwrap();
        }
        doc.commit();
    }
    let changes = doc.get_changes(&[]);
    println!("{} changes", changes.len());
    let mut fresh = AutoCommit::new();
    for (k, chunk) in changes.chunks(2000).enumerate() {
        let t = Instant::now();
        for c in chunk {
            fresh.apply_changes(vec![c.clone()]).unwrap();
        }
        let us = t.elapsed().as_secs_f64() * 1e6 / chunk.len() as f64;
        println!(
            "changes {}..{}: {us:.0} us per apply_changes call",
            k * 2000,
            k * 2000 + chunk.len()
        );
    }
}
