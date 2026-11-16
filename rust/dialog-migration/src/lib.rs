#![cfg_attr(not(target_arch = "wasm32"), warn(missing_docs))]
// Native-only: migrations read and write replicas on disk. The crate
// compiles to nothing on wasm32 so the workspace's wasm builds are
// unaffected.
#![cfg(not(target_arch = "wasm32"))]

//! Migrations that move a dialog replica from one representation to
//! another, as a library an application runs before it opens a replica, and
//! as the `dialog-migrate` binary.
//!
//! A migration is a [`Step`]. A step finds what it has to do by looking at
//! the replica itself, so running it twice does nothing the second time,
//! and an interrupted step picks up where it stopped. [`Migrations`] runs
//! steps in order: [`plan`](Migrations::plan) says what they would do,
//! [`run`](Migrations::run) does it.
//!
//! ```no_run
//! # fn main() -> std::io::Result<()> {
//! use dialog_migration::Migrations;
//! use dialog_migration::chronicle::IntoChronicle;
//!
//! // An application moving its spaces to the single-file archive, before
//! // it opens them as file-archive spaces.
//! let migrations = Migrations::new().with(IntoChronicle::default());
//! for pending in migrations.plan("path/to/profile".as_ref())? {
//!     println!("{}: {}", pending.step, pending.detail);
//! }
//! migrations.run("path/to/profile".as_ref())?;
//! # Ok(())
//! # }
//! ```
//!
//! Each migration is behind a feature of its own, so it is built, and its
//! dependencies pulled in, only by those who need to run it:
//!
//! - `chronicle` ([`chronicle`]): every space of a replica the `fs`
//!   provider wrote, a file per block, into the single-file archive. A
//!   conversion an application opts into, not an upgrade every replica
//!   needs.

use std::io;
use std::path::{Path, PathBuf};

#[cfg(feature = "chronicle")]
pub mod chronicle;

/// One migration: a change to a replica on disk, made before the replica
/// is opened.
pub trait Step {
    /// The step's name, as `dialog-migrate --step` takes it.
    fn id(&self) -> &'static str;

    /// What the step does, in a line.
    fn summary(&self) -> &'static str;

    /// What the step would do to the replica at `replica` (a profile
    /// directory): nothing, once it has done it.
    fn pending(&self, replica: &Path) -> io::Result<Vec<Pending>>;

    /// Does what `pending` says, one piece of what [`pending`](Self::pending)
    /// found.
    fn apply(&self, pending: &Pending) -> io::Result<Applied>;
}

/// A piece of work a step would do.
#[derive(Clone, Debug)]
pub struct Pending {
    /// The step's [`id`](Step::id).
    pub step: &'static str,
    /// What it applies to: a space's directory, say.
    pub at: PathBuf,
    /// What it would do there.
    pub detail: String,
}

/// A piece of work a step did.
#[derive(Clone, Debug)]
pub struct Applied {
    /// The step's [`id`](Step::id).
    pub step: &'static str,
    /// What it applied to.
    pub at: PathBuf,
    /// What it did there.
    pub detail: String,
}

/// Steps to run on a replica, in order.
#[derive(Default)]
pub struct Migrations {
    steps: Vec<Box<dyn Step>>,
}

impl Migrations {
    /// No steps.
    pub fn new() -> Self {
        Self::default()
    }

    /// These steps, and `step` after them.
    pub fn with(mut self, step: impl Step + 'static) -> Self {
        self.steps.push(Box::new(step));
        self
    }

    /// The steps, in order.
    pub fn steps(&self) -> impl Iterator<Item = &dyn Step> {
        self.steps.iter().map(|step| step.as_ref())
    }

    /// What the steps would do to the replica at `replica`, as each sees it
    /// now. A step that depends on an earlier one sees the replica before
    /// that one ran, so the plan is what the first run would start with.
    pub fn plan(&self, replica: &Path) -> io::Result<Vec<Pending>> {
        let mut plan = Vec::new();
        for step in &self.steps {
            plan.extend(step.pending(replica)?);
        }
        Ok(plan)
    }

    /// Runs every step on the replica at `replica`, in order, each finding
    /// its work after the steps before it ran. Stops at the first piece of
    /// work that fails; running again picks up from there.
    pub fn run(&self, replica: &Path) -> io::Result<Vec<Applied>> {
        let mut applied = Vec::new();
        for step in &self.steps {
            for pending in step.pending(replica)? {
                applied.push(step.apply(&pending)?);
            }
        }
        Ok(applied)
    }
}
