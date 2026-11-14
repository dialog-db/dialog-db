//! Builds the guide's grocery space in one file and prints its bytes: the
//! values the guide's chapter "One File per Space" draws.
//!
//! ```sh
//! cargo run -p dialog-chronicle --example guide_bytes
//! ```
//!
//! Alice's repository gets four commits: two facts, then *the
//! `demo.grocery/name` of `item:1` is "Oat milk"*, then that name changed
//! to "Oat milk, 2 L", then a fact committed while a helper process holds
//! the writer's turn, as another process in the middle of an append would.
//! After each commit the file's new bytes are walked record by record and
//! printed: the slots, every record's header, tokens with their offsets
//! and ids, frames with their header and rows, extents and cells. The
//! file is read here from its bytes alone, as any reader of the format
//! could, not through the archive's own code.

#[cfg(unix)]
fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.get(1).map(String::as_str) == Some("hold") {
        return walk::hold(&args[2]);
    }
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(walk::run())
}

#[cfg(not(unix))]
fn main() {
    println!("guide_bytes holds the writer's turn with a POSIX lock: Unix only");
}

#[cfg(unix)]
mod walk {
    use std::collections::BTreeMap;
    use std::io::{BufRead, BufReader, Write};
    use std::path::Path;
    use std::process::{Command, Stdio};
    use std::str::FromStr;

    use dialog_capability::Subject;
    use dialog_chronicle::{Chronicle, FILE_NAME};
    use dialog_common::Blake3Hash;
    use dialog_corpus::{Changes, Entity, Fact, Instruction, Pick, Relation, Value};
    use dialog_effects::storage::{Directory, Location};
    use dialog_peer::helpers::{Seed, open_peer_as, test_owned};
    use dialog_repository::RepositoryExt as _;
    use dialog_storage::provider::{FileSystem, Space, Storage};

    /// A space whose blocks and cells, branch heads among them, are in one
    /// file.
    type FileSpace = Space<Chronicle, Chronicle, FileSystem, FileSystem, FileSystem>;

    /// The byte of the writer's turn: past any file the archive grows to.
    const TURN: u64 = 1 << 62;
    const SLOT: u64 = 64 * 1024;
    /// The header: `DIALOG`, 0xB6, the format version, zeros to 16 bytes.
    const HEAD: u64 = 16;
    const DATA: u64 = HEAD + 4 * SLOT;
    const ALIGN: u64 = 16;
    const PAGE: u64 = 4096;

    /// Holds the writer's turn of the file at `path` until a line arrives
    /// on standard input.
    pub fn hold(path: &str) -> anyhow::Result<()> {
        use std::os::fd::AsRawFd;
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)?;
        // SAFETY: a plain fcntl record lock on an open descriptor.
        let mut lock: libc::flock = unsafe { std::mem::zeroed() };
        lock.l_type = libc::F_WRLCK as _;
        lock.l_whence = libc::SEEK_SET as _;
        lock.l_start = TURN as _;
        lock.l_len = 1;
        // SAFETY: as above.
        if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_SETLKW, &lock) } != 0 {
            anyhow::bail!(
                "could not take the turn: {}",
                std::io::Error::last_os_error()
            );
        }
        println!("held");
        std::io::stdout().flush()?;
        let mut line = String::new();
        std::io::stdin().read_line(&mut line)?;
        Ok(())
    }

    fn fact(entity: &str, attribute: &str, value: &str) -> anyhow::Result<Fact> {
        Ok(Fact {
            the: Relation::from_str(attribute)?,
            of: Entity::from_str(entity)?,
            is: Value::String(value.to_string()),
            cause: None,
        })
    }

    macro_rules! commit {
        ($branch:expr, $operator:expr, $changes:expr) => {{
            let changes: Changes = $changes;
            $branch
                .transact(|transaction| {
                    let changes = changes.clone();
                    async move {
                        Ok::<_, dialog_repository::CommitError>(transaction.integrate(changes))
                    }
                })
                .perform($operator)
                .await
        }};
    }

    pub async fn run() -> anyhow::Result<()> {
        let directory = tempfile::tempdir()?;
        let at = Directory::At(directory.path().to_string_lossy().into_owned());
        let (peer, name) = ("alice".to_string(), "groceries".to_string());
        let path = directory.path().join(&name).join(FILE_NAME);

        let storage = test_owned(Storage::<FileSpace>::new()).await;
        let credential = Seed::new(0).credential(&peer).await?;
        let profile =
            open_peer_as(storage, Location::new(at.clone(), peer.clone()), credential).await?;
        let operator = profile
            .session(b"guide")
            .space(profile.state())
            .allow(Subject::any())
            .await?;
        let repository = profile
            .space(name.clone())
            .open()
            .perform(&operator)
            .await?;
        let branch = repository.branch("main").open().perform(&operator).await?;

        let mut walker = Walker::default();
        println!("# opened: {}", path.display());
        walker.walk(&path)?;

        println!("\n# commit 1: item:2 is \"Rye bread\", item:1 has quantity \"1\"");
        commit!(
            branch,
            &operator,
            [
                Instruction::Assert(fact("item:2", "demo.grocery/name", "Rye bread")?, Pick::All),
                Instruction::Assert(fact("item:1", "demo.grocery/quantity", "1")?, Pick::All),
            ]
            .into_iter()
            .collect()
        )?;
        walker.walk(&path)?;

        println!("\n# commit 2: the demo.grocery/name of item:1 is \"Oat milk\"");
        commit!(
            branch,
            &operator,
            [Instruction::Assert(
                fact("item:1", "demo.grocery/name", "Oat milk")?,
                Pick::All
            )]
            .into_iter()
            .collect()
        )?;
        walker.walk(&path)?;

        println!("\n# commit 3: the name of item:1 becomes \"Oat milk, 2 L\"");
        commit!(
            branch,
            &operator,
            [Instruction::Assert(
                fact("item:1", "demo.grocery/name", "Oat milk, 2 L")?,
                Pick::Last
            )]
            .into_iter()
            .collect()
        )?;
        walker.walk(&path)?;

        println!("\n# a sync point");
        let archive = Chronicle::open(&path)?;
        archive.sync()?;
        walker.walk(&path)?;

        println!("\n# commit 4, while another process holds the turn");
        let mut holder = Command::new(std::env::current_exe()?)
            .arg("hold")
            .arg(&path)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()?;
        let mut said = String::new();
        BufReader::new(holder.stdout.take().expect("a pipe")).read_line(&mut said)?;
        anyhow::ensure!(said.trim() == "held", "the helper did not take the turn");
        // The commit's blocks go into an extent at once; its head waits for
        // the turn, which the helper lets go of a moment later.
        let mut release = holder.stdin.take().expect("a pipe");
        let watched = path.clone();
        let releaser = std::thread::spawn(move || -> anyhow::Result<()> {
            std::thread::sleep(std::time::Duration::from_millis(300));
            println!("\n## while the turn is held, before the head is written");
            Walker::default().walk_extents(&watched)?;
            release.write_all(b"go\n")?;
            Ok(())
        });
        commit!(
            branch,
            &operator,
            [Instruction::Assert(
                fact("item:3", "demo.grocery/name", "Eggs")?,
                Pick::All
            )]
            .into_iter()
            .collect()
        )?;
        releaser.join().expect("the releaser")?;
        holder.wait()?;
        walker.walk(&path)?;

        println!("\n# commit 5: folds the extent in");
        commit!(
            branch,
            &operator,
            [Instruction::Assert(
                fact("item:3", "demo.grocery/quantity", "12")?,
                Pick::All
            )]
            .into_iter()
            .collect()
        )?;
        walker.walk(&path)?;

        println!("\n# two handles on main: the second commits over a head it did not read");
        let other = repository.branch("main").open().perform(&operator).await?;
        commit!(
            branch,
            &operator,
            [Instruction::Assert(
                fact("item:2", "demo.grocery/quantity", "2")?,
                Pick::All
            )]
            .into_iter()
            .collect()
        )?;
        let tries = std::sync::atomic::AtomicUsize::new(0);
        let changes: Changes = [Instruction::Assert(
            fact("item:4", "demo.grocery/name", "Coffee")?,
            Pick::All,
        )]
        .into_iter()
        .collect();
        other
            .transact(|transaction| {
                tries.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let changes = changes.clone();
                async move { Ok::<_, dialog_repository::CommitError>(transaction.integrate(changes)) }
            })
            .perform(&operator)
            .await?;
        println!(
            "the second handle's transaction ran {} times",
            tries.load(std::sync::atomic::Ordering::Relaxed)
        );
        walker.walk(&path)?;
        walker.quiet = true;

        println!("\n# 2100 small blocks that are not tree nodes, appended at once");
        let blocks: Vec<dialog_common::Buffer> = (0..2100u32)
            .map(|n| dialog_common::Buffer::from(format!("imported block {n:04}").into_bytes()))
            .collect();
        archive.append("index", &blocks)?;
        walker.walk(&path)?;

        println!("\n# every block rebuilt from its frame, and its hash checked");
        for (catalog, hash) in archive.hashes() {
            if !archive.is_frame(&catalog, &hash) {
                continue;
            }
            let block = archive.get(&catalog, &hash)?.expect("a block it lists");
            let rebuilt = Blake3Hash::hash(&block);
            println!(
                "{catalog} {} {} bytes, frame: {}, hash matches: {}",
                short(hash.as_bytes()),
                block.len(),
                archive.is_frame(&catalog, &hash),
                rebuilt == hash
            );
        }
        let stats = archive.stats();
        println!("stats: {stats:?}");

        let open_len = std::fs::metadata(&path)?.len();
        drop(archive);
        drop(other);
        drop(branch);
        drop(repository);
        drop(operator);
        drop(profile);
        let closed_len = std::fs::metadata(&path)?.len();
        println!("\n# closed: {open_len} bytes while open, {closed_len} after the last close");
        walker.walk(&path)?;
        Ok(())
    }

    fn short(bytes: &[u8]) -> String {
        bytes.iter().take(6).map(|b| format!("{b:02x}")).collect()
    }

    fn hex(bytes: &[u8]) -> String {
        bytes
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<Vec<_>>()
            .join(" ")
    }

    /// Bytes as text where they are printable (a type byte before or a
    /// terminator after shown apart), else hex.
    fn shown(bytes: &[u8]) -> String {
        let printable =
            |bytes: &[u8]| !bytes.is_empty() && bytes.iter().all(|b| (0x20..0x7f).contains(b));
        let (tag, rest) = match bytes.first() {
            Some(first) if *first < 0x20 && bytes.len() > 1 => (Some(*first), &bytes[1..]),
            _ => (None, bytes),
        };
        let (text, end) = match rest.split_last() {
            Some((0, text)) => (text, Some(0u8)),
            _ => (rest, None),
        };
        if printable(text) {
            let mut out = String::new();
            if let Some(tag) = tag {
                out.push_str(&format!("{tag:02x} "));
            }
            out.push_str(&format!("{:?}", String::from_utf8_lossy(text)));
            if end.is_some() {
                out.push_str(" 00");
            }
            out
        } else {
            format!("[{}]", hex(bytes))
        }
    }

    /// A cursor over bytes, as the format reads them.
    struct Reader<'a> {
        bytes: &'a [u8],
        at: usize,
    }

    impl<'a> Reader<'a> {
        fn new(bytes: &'a [u8]) -> Self {
            Self { bytes, at: 0 }
        }
        fn take(&mut self, len: usize) -> anyhow::Result<&'a [u8]> {
            let taken = self
                .bytes
                .get(self.at..self.at + len)
                .ok_or_else(|| anyhow::anyhow!("ends early"))?;
            self.at += len;
            Ok(taken)
        }
        fn byte(&mut self) -> anyhow::Result<u8> {
            Ok(self.take(1)?[0])
        }
        fn u16(&mut self) -> anyhow::Result<u16> {
            Ok(u16::from_le_bytes(self.take(2)?.try_into()?))
        }
        fn u32(&mut self) -> anyhow::Result<u32> {
            Ok(u32::from_le_bytes(self.take(4)?.try_into()?))
        }
        fn u64(&mut self) -> anyhow::Result<u64> {
            Ok(u64::from_le_bytes(self.take(8)?.try_into()?))
        }
        fn varint(&mut self) -> anyhow::Result<u64> {
            let (mut value, mut shift) = (0u64, 0);
            loop {
                let byte = self.byte()?;
                value |= ((byte & 0x7f) as u64) << shift;
                if byte & 0x80 == 0 {
                    return Ok(value);
                }
                shift += 7;
            }
        }
        fn zigzag(&mut self) -> anyhow::Result<i64> {
            let raw = self.varint()?;
            Ok(((raw >> 1) as i64) ^ -((raw & 1) as i64))
        }
        /// `count` values written as runs: a signed count, then one value
        /// repeated or that many values one by one. Also the bytes read.
        fn runs(&mut self, count: usize) -> anyhow::Result<(Vec<u64>, Vec<u8>)> {
            let start = self.at;
            let mut values = Vec::new();
            while values.len() < count {
                let run = self.zigzag()?;
                let len = run.unsigned_abs() as usize;
                anyhow::ensure!(
                    len > 0 && values.len() + len <= count,
                    "a run past its column"
                );
                if run > 0 {
                    let value = self.varint()?;
                    values.resize(values.len() + len, value);
                } else {
                    for _ in 0..len {
                        values.push(self.varint()?);
                    }
                }
            }
            Ok((values, self.bytes[start..self.at].to_vec()))
        }
    }

    fn unzigzag(raw: u64) -> i64 {
        ((raw >> 1) as i64) ^ -((raw & 1) as i64)
    }

    /// How many token slots a key of `form` splits into.
    fn slots_of(form: u64) -> usize {
        match form {
            0..=2 => 3,
            3 | 5 => 4,
            _ => 1,
        }
    }

    const KINDS: [(u8, &str); 5] = [
        (4, "tokens"),
        (5, "frame"),
        (7, "pages"),
        (8, "extent"),
        (9, "cell"),
    ];

    fn kind_name(tag: u8) -> &'static str {
        KINDS
            .iter()
            .find(|(t, _)| *t == tag)
            .map_or("?", |(_, name)| name)
    }

    /// Walks the file, printing what is new since the last walk.
    #[derive(Default)]
    struct Walker {
        quiet: bool,
        whole_frames: usize,
        walked: u64,
        tokens: BTreeMap<u64, Vec<u8>>,
        frames: BTreeMap<u64, Vec<Vec<[u64; 6]>>>,
    }

    impl Walker {
        fn walk(&mut self, path: &Path) -> anyhow::Result<()> {
            let bytes = std::fs::read(path)?;
            let allocated = {
                use std::os::unix::fs::MetadataExt;
                std::fs::metadata(path)?.blocks() * 512
            };
            println!(
                "file: {} bytes long, {allocated} allocated on the disk; magic {}",
                bytes.len(),
                hex(&bytes[..HEAD as usize])
            );
            let mut newest = None;
            for (index, name) in ["rolling 0", "rolling 1", "durable 0", "durable 1"]
                .iter()
                .enumerate()
            {
                let at = HEAD + index as u64 * SLOT;
                let slot = self.slot(&bytes, at, name)?;
                if index >= 2 {
                    // The last 8 bytes of a durable copy's area: the
                    // sequence of the copy last synced there.
                    let marker = (at + SLOT - 8) as usize;
                    if let Some(synced) = bytes.get(marker..marker + 8) {
                        println!(
                            "  {name}: synced marker @{marker} = {}",
                            u64::from_le_bytes(synced.try_into()?)
                        );
                    }
                }
                if let Some(slot) = slot.filter(|_| index < 2)
                    && newest
                        .as_ref()
                        .is_none_or(|n: &SlotSeen| slot.sequence > n.sequence)
                {
                    newest = Some(slot);
                }
            }
            let Some(newest) = newest else {
                return Ok(());
            };
            let mut at = self.walked.max(DATA);
            while at < newest.end {
                let Some(next) = self.record(&bytes, at, &newest)? else {
                    println!("@{at}: not a whole record");
                    break;
                };
                at = next;
            }
            self.walked = newest.end;
            if self.whole_frames > 2 {
                println!("... and {} more frames", self.whole_frames - 2);
                self.whole_frames = 0;
            }
            Ok(())
        }

        /// Prints the extents the newest slot lists, and what they hold.
        fn walk_extents(&mut self, path: &Path) -> anyhow::Result<()> {
            let bytes = std::fs::read(path)?;
            for index in 0..2u64 {
                if let Some(slot) = self.slot(&bytes, HEAD + index * SLOT, "rolling")? {
                    for extent in &slot.extents {
                        self.extent(&bytes, *extent)?;
                    }
                }
            }
            Ok(())
        }

        fn slot(&self, bytes: &[u8], at: u64, name: &str) -> anyhow::Result<Option<SlotSeen>> {
            let mut reader = Reader::new(&bytes[at as usize..]);
            let len = reader.u32()? as usize;
            let crc = reader.u32()?;
            let Ok(body) = reader.take(len) else {
                println!("slot {name} @{at}: empty");
                return Ok(None);
            };
            if len == 0 || crc32fast::hash(body) != crc {
                println!("slot {name} @{at}: not whole (length {len})");
                return Ok(None);
            }
            let mut r = Reader::new(body);
            let (sequence, end, durable, boot) = (r.u64()?, r.u64()?, r.u64()?, r.u64()?);
            let counts: Vec<u64> = (0..7).map(|_| r.u64()).collect::<anyhow::Result<_>>()?;
            let catalogs: Vec<String> = (0..r.varint()?)
                .map(|_| -> anyhow::Result<String> {
                    let len = r.byte()? as usize;
                    Ok(String::from_utf8_lossy(r.take(len)?).into_owned())
                })
                .collect::<anyhow::Result<_>>()?;
            let root_len = r.u32()? as usize;
            let root = r.take(root_len)?;
            let pages: Vec<Vec<u32>> = (0..2)
                .map(|_| (0..r.varint()?).map(|_| r.u32()).collect())
                .collect::<anyhow::Result<_>>()?;
            let extents: Vec<Vec<u64>> = (0..3)
                .map(|_| (0..r.varint()?).map(|_| r.u64()).collect())
                .collect::<anyhow::Result<_>>()?;
            let cells_len = r.u32()? as usize;
            let cells = r.take(cells_len)?;
            println!(
                "slot {name} @{at}: length {len}, crc {crc:08x}; sequence {sequence}, end {end}, \
                 durable {durable}, boot {boot:016x}; counts [blocks, block bytes, frames, \
                 against, frame bytes, tokens, token bytes] {counts:?}; catalogs {catalogs:?}; \
                 root {root_len} bytes; reusable pages {:?}, pending {:?}; extents {:?}, free \
                 {:?}, pending {:?}; cells' root {cells_len} bytes",
                pages[0], pages[1], extents[0], extents[1], extents[2]
            );
            println!("  root: {}", index_node(root, false)?);
            println!(
                "  cells' root: {}",
                index_node(cells, false)?
                    .replace("lineage ", "")
                    .replace("block ", "")
                    .replace("token ", "")
            );
            Ok(Some(SlotSeen {
                sequence,
                end,
                extents: extents[0].clone(),
            }))
        }

        /// Prints the record at `at` and returns where the next starts.
        fn record(
            &mut self,
            bytes: &[u8],
            at: u64,
            slot: &SlotSeen,
        ) -> anyhow::Result<Option<u64>> {
            let mut reader = Reader::new(&bytes[at as usize..]);
            let tag = reader.byte()?;
            let len = reader.u32()? as u64;
            let crc = reader.u32()?;
            let body_at = at + 9;
            let Some(body) = bytes.get(body_at as usize..(body_at + len) as usize) else {
                return Ok(None);
            };
            let mut check = crc32fast::Hasher::new();
            check.update(&[tag]);
            check.update(body);
            let whole = check.finalize() == crc;
            if self.quiet && tag == 5 {
                self.whole_frames += 1;
                if self.whole_frames > 2 {
                    return Ok(Some(body_at + len));
                }
            }
            println!(
                "@{at} record: kind {tag} ({}), length {len}, crc {crc:08x} ({}); header bytes {}",
                kind_name(tag),
                if whole {
                    "matches"
                } else {
                    "not checked: written to since"
                },
                hex(&bytes[at as usize..body_at as usize])
            );
            match tag {
                4 => self.tokens(body, body_at)?,
                5 => self.frame(bytes, body_at, len)?,
                7 => self.pages(body, body_at)?,
                8 => {
                    let _ = slot;
                    self.extent(bytes, at)?;
                }
                9 => self.cell(body, body_at)?,
                _ => {}
            }
            Ok(Some(body_at + len))
        }

        fn tokens(&mut self, body: &[u8], body_at: u64) -> anyhow::Result<()> {
            let mut r = Reader::new(body);
            let count = r.varint()?;
            println!("  {count} tokens");
            for _ in 0..count {
                let pad_at = body_at + r.at as u64;
                let pad = r.byte()? as usize;
                r.take(pad)?;
                let len = r.u32()? as usize;
                let at = body_at + r.at as u64;
                let token = r.take(len)?.to_vec();
                println!(
                    "  token id {} @{at} (pad byte @{pad_at} = {pad}, length {len} = {}): {}",
                    at / ALIGN,
                    hex(&(len as u32).to_le_bytes()),
                    shown(&token)
                );
                self.tokens.insert(at / ALIGN, token);
            }
            Ok(())
        }

        fn name(&self, id: u64) -> String {
            match self.tokens.get(&id) {
                Some(token) => format!("{id}={}", shown(&token[..token.len().min(24)])),
                None => format!("{id}"),
            }
        }

        fn frame(&mut self, bytes: &[u8], body_at: u64, len: u64) -> anyhow::Result<()> {
            let pad = bytes[body_at as usize] as u64;
            let data_at = body_at + 1 + pad;
            let data = &bytes[data_at as usize..(body_at + len) as usize];
            let mut r = Reader::new(data);
            let hash = r.take(32)?;
            let kind = r.byte()?;
            let depth = r.byte()?;
            let catalog = r.varint()?;
            let base = r.varint()?;
            let manifest = r.varint()?;
            let header_len = r.at;
            println!(
                "  frame id {} (data @{data_at}, pad {pad}): hash {}, kind {kind:#04x}, depth \
                 {depth}, catalog {catalog}, base {}, manifest token {manifest}; header {} \
                 bytes: {}",
                data_at / ALIGN,
                short(hash),
                if base == 0 {
                    "none".to_string()
                } else {
                    format!("frame {}", base - 1)
                },
                header_len,
                hex(&data[..header_len])
            );
            if kind == 0xFF {
                let block_len = r.varint()?;
                println!("  whole block of {block_len} bytes");
                return Ok(());
            }
            let sections: &[Section] = if kind == 0 {
                &[Section::Entries]
            } else {
                &[Section::Links, Section::Buffers, Section::Ops]
            };
            let base_rows = (base > 0)
                .then(|| self.frames.get(&(base - 1)).cloned())
                .flatten();
            let mut all = Vec::new();
            for (index, section) in sections.iter().enumerate() {
                let against = base_rows.as_ref().map(|rows| rows[index].clone());
                let rows = self.section(&mut r, *section, against.as_deref())?;
                all.push(rows);
            }
            println!(
                "  frame data {} bytes ({} header, {} rows)",
                data.len(),
                header_len,
                data.len() - header_len
            );
            self.frames.insert(data_at / ALIGN, all);
            Ok(())
        }

        /// Reads and prints one section, against `base` (the section's rows
        /// in the frame this one is written against) when given.
        fn section(
            &self,
            r: &mut Reader,
            section: Section,
            base: Option<&[[u64; 6]]>,
        ) -> anyhow::Result<Vec<[u64; 6]>> {
            let start = r.at;
            let count = r.varint()? as usize;
            let mut ops = Vec::new();
            let mut literal = count;
            if base.is_some() {
                let steps = r.varint()? as usize;
                let (mut expected, mut sum) = (0i64, 0usize);
                for _ in 0..steps {
                    let head = r.varint()? as usize;
                    if head & 1 == 1 {
                        let from = expected + r.zigzag()?;
                        let n = head >> 1;
                        expected = from + n as i64;
                        ops.push(Step::Copy(from as usize, n));
                    } else {
                        sum += head >> 1;
                        ops.push(Step::New(head >> 1));
                    }
                }
                literal = sum;
            }
            println!(
                "  section {section:?}: {count} rows{}; count and steps bytes {}",
                if base.is_some() {
                    format!(
                        ", against its base: {}",
                        ops.iter()
                            .map(|op| match op {
                                Step::Copy(from, n) => format!("copy {n} from row {from}"),
                                Step::New(n) => format!("{n} new"),
                            })
                            .collect::<Vec<_>>()
                            .join(", ")
                    )
                } else {
                    ", written whole".to_string()
                },
                hex(&r.bytes[start..r.at])
            );
            let mut rows = vec![[0u64; 6]; literal];
            let delta = |r: &mut Reader,
                         rows: &mut Vec<[u64; 6]>,
                         field: usize,
                         name: &str,
                         when: &dyn Fn(&[u64; 6]) -> bool|
             -> anyhow::Result<()> {
                let used: Vec<usize> = (0..rows.len()).filter(|&i| when(&rows[i])).collect();
                let (deltas, bytes) = r.runs(used.len())?;
                let signed: Vec<i64> = deltas.iter().map(|d| unzigzag(*d)).collect();
                let mut previous = 0i64;
                for (i, delta) in used.iter().zip(&signed) {
                    previous += delta;
                    rows[*i][field] = previous as u64;
                }
                if !used.is_empty() {
                    println!(
                        "    column {name}: deltas {signed:?} as bytes {}",
                        hex(&bytes)
                    );
                }
                Ok(())
            };
            match section {
                Section::Entries | Section::Ops => {
                    let (forms, bytes) = r.runs(literal)?;
                    if literal > 0 {
                        println!("    column form: {forms:?} as bytes {}", hex(&bytes));
                    }
                    for (row, form) in rows.iter_mut().zip(&forms) {
                        row[0] = *form;
                    }
                    for slot in 0..4 {
                        delta(r, &mut rows, 1 + slot, &format!("slot {slot}"), &|row| {
                            slots_of(row[0]) > slot
                        })?;
                    }
                    if section == Section::Ops {
                        let (asserted, bytes) = r.runs(literal)?;
                        if literal > 0 {
                            println!("    column asserted: {asserted:?} as bytes {}", hex(&bytes));
                        }
                        for (row, asserted) in rows.iter_mut().zip(&asserted) {
                            row[5] = if *asserted == 1 { 0 } else { NONE };
                        }
                    }
                    delta(r, &mut rows, 5, "datum", &|row| row[5] != NONE)?;
                }
                Section::Links => {
                    delta(r, &mut rows, 0, "separator", &|_| true)?;
                    let (flags, bytes) = r.runs(literal)?;
                    if literal > 0 {
                        println!(
                            "    column scale and no-frame flag: {flags:?} as bytes {}",
                            hex(&bytes)
                        );
                    }
                    for (row, flag) in rows.iter_mut().zip(&flags) {
                        row[2] = flag >> 1;
                        if flag & 1 == 1 {
                            row[1] = NONE;
                        } else {
                            row[3] = NONE;
                        }
                    }
                    delta(r, &mut rows, 1, "child frame", &|row| row[1] != NONE)?;
                    delta(r, &mut rows, 3, "child hash token", &|row| row[1] == NONE)?;
                }
                Section::Buffers => {
                    for row in rows.iter_mut() {
                        row[0] = r.varint()?;
                        row[1] = r.varint()?;
                    }
                }
            }
            let rows = match base {
                Some(base) => {
                    let mut out = Vec::with_capacity(count);
                    let mut next = 0;
                    for op in &ops {
                        match op {
                            Step::Copy(from, n) => out.extend_from_slice(&base[*from..*from + *n]),
                            Step::New(n) => {
                                out.extend_from_slice(&rows[next..next + *n]);
                                next += *n;
                            }
                        }
                    }
                    out
                }
                None => rows,
            };
            for row in &rows {
                println!("    row: {}", self.row(section, row));
            }
            println!("    section bytes {}", r.at - start);
            Ok(rows)
        }

        fn row(&self, section: Section, row: &[u64; 6]) -> String {
            match section {
                Section::Entries | Section::Ops => {
                    let parts: Vec<String> = (0..slots_of(row[0]))
                        .map(|s| self.name(row[1 + s]))
                        .collect();
                    let datum = if row[5] == NONE {
                        "retracted".to_string()
                    } else {
                        format!("token {}", row[5])
                    };
                    format!(
                        "form {:02x} | {} | datum {datum}",
                        row[0],
                        parts.join(" | ")
                    )
                }
                Section::Links => format!(
                    "separator {} | child {} | scale {}",
                    self.name(row[0]),
                    if row[1] == NONE {
                        format!("by hash token {}", row[3])
                    } else {
                        format!("frame {}", row[1])
                    },
                    row[2]
                ),
                Section::Buffers => format!("child {} holds {} ops", row[0], row[1]),
            }
        }

        fn pages(&self, body: &[u8], body_at: u64) -> anyhow::Result<()> {
            let mut r = Reader::new(body);
            let pad = r.u16()? as usize;
            r.take(pad)?;
            let first = body_at + r.at as u64;
            let count = (body.len() - r.at) as u64 / PAGE;
            println!(
                "  padding {pad}, {count} pages from @{first} (page {})",
                first / PAGE
            );
            for page in 0..count {
                let bytes = r.take(PAGE as usize)?;
                let sequence = u32::from_le_bytes(bytes[0..4].try_into()?);
                let crc = u32::from_le_bytes(bytes[4..8].try_into()?);
                println!(
                    "  page {} written by append {sequence}, checksum {crc:08x}: {}",
                    first / PAGE + page,
                    index_node(&bytes[8..], true)?
                );
            }
            Ok(())
        }

        fn extent(&mut self, bytes: &[u8], record: u64) -> anyhow::Result<()> {
            let body = record + 9;
            let count_at = body + 1 + (8 - (body + 1) % 8) % 8;
            let mut r = Reader::new(&bytes[count_at as usize..]);
            let (used, inverse, epoch) = (r.u64()?, r.u64()?, r.u64()?);
            let len =
                u32::from_le_bytes(bytes[record as usize + 1..record as usize + 5].try_into()?);
            println!(
                "  extent @{record}: body {len} bytes; count @{count_at}: used {used}, inverse \
                 {inverse:016x} ({}), epoch {epoch}; bytes {}",
                if !used == inverse { "whole" } else { "torn" },
                hex(&bytes[count_at as usize..count_at as usize + 24])
            );
            let mut at = count_at + 24;
            let end = count_at + 24 + used;
            while at < end {
                let tag = bytes[at as usize];
                let len =
                    u32::from_le_bytes(bytes[at as usize + 1..at as usize + 5].try_into()?) as u64;
                println!(
                    "  in the extent @{at}: record kind {tag} ({}), length {len}",
                    kind_name(tag)
                );
                if tag == 5 {
                    let pad = bytes[at as usize + 9] as u64;
                    let data_at = at + 9 + 1 + pad;
                    let hash = &bytes[data_at as usize..data_at as usize + 32];
                    let kind = bytes[data_at as usize + 32];
                    println!(
                        "    frame id {}: hash {}, kind {kind:#04x}",
                        data_at / ALIGN,
                        short(hash)
                    );
                }
                at += 9 + len;
            }
            Ok(())
        }

        fn cell(&self, body: &[u8], body_at: u64) -> anyhow::Result<()> {
            let mut r = Reader::new(body);
            let pad = r.byte()? as usize;
            r.take(pad)?;
            let data_at = body_at + r.at as u64;
            let name_len = r.u16()? as usize;
            let name = r.take(name_len)?.to_vec();
            let present = r.byte()?;
            let value_len = r.u32()? as usize;
            let value = r.take(value_len)?.to_vec();
            let needs = r.u16()?;
            let mut needed = Vec::new();
            for _ in 0..needs {
                needed.push((r.u64()?, r.u64()?, r.u64()?, r.u32()?));
            }
            let readable: String = name
                .iter()
                .map(|b| if *b == 0 { '/' } else { *b as char })
                .collect();
            println!(
                "  cell id {} (data @{data_at}): name {readable:?} ({name_len} bytes), present \
                 {present}, value {value_len} bytes, first bytes {}; needs {needed:?} \
                 (extent record, epoch, bytes, CRC-32 of those bytes)",
                data_at / ALIGN,
                hex(&value[..value.len().min(16)])
            );
            Ok(())
        }
    }

    const NONE: u64 = u32::MAX as u64;

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum Section {
        Entries,
        Links,
        Buffers,
        Ops,
    }

    enum Step {
        Copy(usize, usize),
        New(usize),
    }

    struct SlotSeen {
        sequence: u64,
        end: u64,
        extents: Vec<u64>,
    }

    /// An index node: the root as the slot keeps it, or a page's node
    /// after its sequence and checksum.
    fn index_node(bytes: &[u8], page: bool) -> anyhow::Result<String> {
        let mut r = Reader::new(bytes);
        let kind = r.byte()?;
        let children = r.byte()? as usize;
        let entries = r.u16()? as usize;
        if page {
            r.take(4)?;
        }
        let space = |key: u32| match key >> 30 {
            0 => "block",
            1 => "token",
            2 => "lineage",
            _ => "?",
        };
        let entry = |r: &mut Reader| -> anyhow::Result<String> {
            let (key, value) = (r.u32()?, r.u32()?);
            Ok(format!("{} {key:08x}->{value}", space(key)))
        };
        if kind == 0 {
            let list: Vec<String> = (0..entries)
                .map(|_| entry(&mut r))
                .collect::<anyhow::Result<_>>()?;
            return Ok(format!("leaf, {entries} entries: {}", list.join(", ")));
        }
        let pivots: Vec<u32> = (0..children.saturating_sub(1))
            .map(|_| r.u32())
            .collect::<anyhow::Result<_>>()?;
        let kids: Vec<(u32, u32)> = (0..children)
            .map(|_| Ok((r.u32()?, r.u32()?)))
            .collect::<anyhow::Result<_>>()?;
        Ok(format!(
            "internal, {children} children {kids:?} (page, append), pivots {pivots:08x?}, {entries} buffered"
        ))
    }
}
