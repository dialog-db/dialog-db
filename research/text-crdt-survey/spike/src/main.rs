//! E2 and E4 spike: replay concurrent editing traces through four text CRDT
//! libraries, one replica per agent, moving every transaction between
//! replicas as one exported change. Measures change export, out of order
//! import, snapshot and per change sizes, and load time.
//!
//! Usage: text-crdt-spike <trace.json.gz>... [--libs automerge,loro,yrs,dt] [--runs N]

mod libs;

use std::io::Read;
use std::time::Instant;

use serde::Deserialize;

#[derive(Deserialize)]
struct Txn {
    parents: Vec<usize>,
    agent: usize,
    patches: Vec<(usize, usize, String)>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Trace {
    end_content: String,
    num_agents: usize,
    txns: Vec<Txn>,
}

pub type Patch = (usize, usize, String);

/// What one library has to do for the spike.
pub trait Lib {
    type R;
    const NAME: &'static str;
    fn new(agent: usize) -> Self::R;
    /// Apply one remote change, delivered in causal order.
    fn apply(r: &mut Self::R, change: &[u8]);
    /// Apply a transaction locally, return its change bytes and a printable id.
    fn local(r: &mut Self::R, patches: &[Patch]) -> (Vec<u8>, String);
    fn text(r: &mut Self::R) -> String;
    fn snapshot(r: &mut Self::R) -> Vec<u8>;
    /// All history in one export, the library's own batch form.
    fn batch(r: &mut Self::R, changes: &[Vec<u8>]) -> Vec<u8>;
    fn load_snapshot(bytes: &[u8]) -> String;
    /// Fresh document, one import call per change, causal order.
    fn load_each(changes: &[Vec<u8>]) -> String;
    /// Fresh document, all individual changes handed over in one call.
    fn load_one_call(changes: &[Vec<u8>]) -> String;
    /// Fresh document, import each change of a shuffled list.
    fn out_of_order(changes: &[Vec<u8>]) -> Ooo;
}

pub struct Ooo {
    pub text: String,
    pub errors: usize,
    pub first_error: Option<String>,
    pub note: String,
}

struct Row {
    lib: &'static str,
    trace: String,
    converged: bool,
    changes: usize,
    snapshot: usize,
    per_change_sum: usize,
    batch: usize,
    sample_id: String,
    ooo_errors: usize,
    ooo_converged: bool,
    ooo_note: String,
    ooo_ms: f64,
    load_snapshot_ms: (f64, usize),
    load_each_ms: (f64, usize),
    load_one_call_ms: (f64, usize),
    replay_ms: f64,
}

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

/// Median of up to `runs` runs, stopping early once 30 s have been spent.
fn time<F: FnMut() -> String>(runs: usize, expect: &str, mut f: F) -> (f64, usize) {
    let mut out = Vec::new();
    let start = Instant::now();
    while out.len() < runs && (out.is_empty() || start.elapsed().as_secs() < 30) {
        let t = Instant::now();
        let s = f();
        out.push(t.elapsed().as_secs_f64() * 1000.0);
        assert_eq!(s.len(), expect.len(), "load produced wrong text");
        assert!(s == expect, "load produced wrong text");
    }
    let n = out.len();
    (median(out), n)
}

fn shuffled(changes: &[Vec<u8>], seed: u64) -> Vec<Vec<u8>> {
    let mut v = changes.to_vec();
    let mut x = seed;
    for i in (1..v.len()).rev() {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        let j = (x % (i as u64 + 1)) as usize;
        v.swap(i, j);
    }
    v
}

fn run<L: Lib>(name: &str, trace: &Trace, runs: usize) -> Row {
    let n = trace.num_agents;
    // Per agent, the txn indexes it authored, in order; seq of each txn.
    let mut by_agent: Vec<Vec<usize>> = vec![vec![]; n];
    let mut seq = vec![0usize; trace.txns.len()];
    for (i, t) in trace.txns.iter().enumerate() {
        seq[i] = by_agent[t.agent].len();
        by_agent[t.agent].push(i);
    }
    // Each agent's txns are totally ordered, so a causal closure is a
    // vector of per agent counts.
    let mut after: Vec<Vec<usize>> = Vec::with_capacity(trace.txns.len());
    let mut replicas: Vec<L::R> = (0..n).map(L::new).collect();
    let mut applied = vec![vec![0usize; n]; n];
    let mut changes: Vec<Vec<u8>> = Vec::with_capacity(trace.txns.len());
    let mut sample_id = String::new();

    let t0 = Instant::now();
    for (i, t) in trace.txns.iter().enumerate() {
        let mut before = vec![0usize; n];
        for &p in &t.parents {
            for b in 0..n {
                before[b] = before[b].max(after[p][b]);
            }
        }
        let a = t.agent;
        assert_eq!(before[a], seq[i], "agent's own history not in parents");
        // Bring the replica to exactly the parents' state.
        let mut missing = Vec::new();
        for b in 0..n {
            assert!(applied[a][b] <= before[b], "replica ahead of parents");
            missing.extend_from_slice(&by_agent[b][applied[a][b]..before[b]]);
        }
        missing.sort_unstable();
        for j in missing {
            L::apply(&mut replicas[a], &changes[j]);
        }
        applied[a] = before.clone();
        let (bytes, id) = L::local(&mut replicas[a], &t.patches);
        if i == trace.txns.len() / 2 {
            sample_id = id;
        }
        changes.push(bytes);
        applied[a][a] += 1;
        let mut me = before;
        me[a] += 1;
        after.push(me);
    }
    // Bring every replica to the full history and check convergence.
    let mut converged = true;
    for a in 0..n {
        let mut missing = Vec::new();
        for b in 0..n {
            missing.extend_from_slice(&by_agent[b][applied[a][b]..]);
        }
        missing.sort_unstable();
        for j in missing {
            L::apply(&mut replicas[a], &changes[j]);
        }
        let text = L::text(&mut replicas[a]);
        if text != trace.end_content {
            eprintln!(
                "{} agent {a}: diverged, len {} vs {}",
                L::NAME,
                text.len(),
                trace.end_content.len()
            );
            converged = false;
        }
    }
    let replay_ms = t0.elapsed().as_secs_f64() * 1000.0;

    let snapshot = L::snapshot(&mut replicas[0]);
    let batch = L::batch(&mut replicas[0], &changes);
    let expect = &trace.end_content;

    let shuffled = shuffled(&changes, 0x9e3779b97f4a7c15);
    let t1 = Instant::now();
    let ooo = L::out_of_order(&shuffled);
    let ooo_ms = t1.elapsed().as_secs_f64() * 1000.0;

    let load_snapshot_ms = time(runs, expect, || L::load_snapshot(&snapshot));
    let load_each_ms = time(runs, expect, || L::load_each(&changes));
    let load_one_call_ms = time(runs, expect, || L::load_one_call(&changes));

    Row {
        lib: L::NAME,
        trace: name.to_string(),
        converged,
        changes: changes.len(),
        snapshot: snapshot.len(),
        per_change_sum: changes.iter().map(Vec::len).sum(),
        batch: batch.len(),
        sample_id,
        ooo_errors: ooo.errors,
        ooo_converged: ooo.text == *expect,
        ooo_note: match ooo.first_error {
            Some(e) => format!("{}; first error: {e}", ooo.note),
            None => ooo.note,
        },
        ooo_ms,
        load_snapshot_ms,
        load_each_ms,
        load_one_call_ms,
        replay_ms,
    }
}

fn load(path: &str) -> Trace {
    let mut s = String::new();
    flate2::read::GzDecoder::new(std::fs::File::open(path).expect("open trace"))
        .read_to_string(&mut s)
        .expect("gunzip");
    serde_json::from_str(&s).expect("parse trace")
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut paths = Vec::new();
    let mut libs = "automerge,loro,yrs,dt".to_string();
    let mut runs = 7;
    let mut it = args.into_iter();
    while let Some(a) = it.next() {
        match a.as_str() {
            "--libs" => libs = it.next().unwrap(),
            "--runs" => runs = it.next().unwrap().parse().unwrap(),
            _ => paths.push(a),
        }
    }
    let mut rows = Vec::new();
    for p in &paths {
        let trace = load(p);
        let name = std::path::Path::new(p)
            .file_name()
            .unwrap()
            .to_string_lossy()
            .trim_end_matches(".json.gz")
            .to_string();
        eprintln!(
            "{name}: {} txns, {} agents",
            trace.txns.len(),
            trace.num_agents
        );
        for l in libs.split(',') {
            eprintln!("  {l} ...");
            let row = match l {
                "automerge" => run::<libs::automerge::Am>(&name, &trace, runs),
                "loro" => run::<libs::loro::Lo>(&name, &trace, runs),
                "yrs" => run::<libs::yrs::Y>(&name, &trace, runs),
                "dt" => run::<libs::dt::Dt>(&name, &trace, runs),
                other => panic!("unknown lib {other}"),
            };
            rows.push(row);
        }
    }
    println!("| library | trace | converged | changes | snapshot B | sum per-change B | batch B | out-of-order: errors / converged | ooo import ms | load snapshot ms | load changes, one call each ms | load changes, one call ms | replay ms | sample change id |");
    println!("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|");
    for r in &rows {
        println!(
            "| {} | {} | {} | {} | {} | {} | {} | {} / {} | {:.0} | {:.2} (n={}) | {:.1} (n={}) | {:.1} (n={}) | {:.0} | `{}` |",
            r.lib,
            r.trace,
            r.converged,
            r.changes,
            r.snapshot,
            r.per_change_sum,
            r.batch,
            r.ooo_errors,
            r.ooo_converged,
            r.ooo_ms,
            r.load_snapshot_ms.0,
            r.load_snapshot_ms.1,
            r.load_each_ms.0,
            r.load_each_ms.1,
            r.load_one_call_ms.0,
            r.load_one_call_ms.1,
            r.replay_ms,
            r.sample_id
        );
    }
    println!();
    for r in &rows {
        println!("- {} / {}: {}", r.lib, r.trace, r.ooo_note);
    }
}
