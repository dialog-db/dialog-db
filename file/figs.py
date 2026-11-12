"""The chapter's figures, from the values guide_bytes printed."""

from svg import Fig, hx

FIGS = {}


def fig(fn):
    FIGS[fn.__name__] = fn
    return fn


@fig
def why():
    f = Fig(760, 300, "Nine versions of the tree's nodes as nine files, and as frames and tokens in one file", "why")
    f.text(16, 22, "a file per block: archive/index/", "title")
    blocks = [
        ("b4ff23…", 360), ("35a1e8…", 2548), ("565412…", 4372), ("1a442d…", 6092),
        ("387ac8…", 7996), ("9afe62…", 9892), ("fc9fc5…", 11780), ("6c8933…", 11804),
        ("2c38c1…", 13692),
    ]
    y = 36
    for name, size in blocks:
        w = size / 13692 * 300
        f.rect(16, y, w, 20, "hash")
        f.text(16 + w + 8, y + 14, f"{name}  {size:,} bytes", "small")
        y += 25
    f.text(16, y + 14, "nine blocks, 68,536 bytes", "label small muted")
    x = 470
    f.text(x, 22, "one file: archive.dialog", "title")
    # Frames and tokens, drawn to the same scale.
    fw = 1431 / 13692 * 300
    tw = 5043 / 13692 * 300
    f.rect(x, 36, fw, 20, "value")
    f.text(x + fw + 8, 50, "9 frames, 1,431 bytes", "small")
    f.rect(x, 61, tw, 20, "attribute")
    f.text(x + tw + 8, 75, "49 tokens, 5,043 bytes", "small")
    f.text(x, 104, "6,474 bytes for the same nine", "label small muted")
    f.text(x, 120, "blocks, each rebuilt byte for byte", "label small muted")
    f.text(x, 136, "when asked for", "label small muted")
    f.text(380, 290, "Drawn to one scale. The nodes come from the example's seven commits.", "label small muted", "middle")
    return f


@fig
def opened():
    f = Fig(760, 190, "The file as Alice's peer created it: magic, three slots, an empty extent and one cell", "open")
    segs = [
        (8, "magic", "shade", "0"),
        (100, "rolling slot 0", "", "8"),
        (100, "rolling slot 1", "", "65,544"),
        (100, "durable slot", "", "131,080"),
        (270, "extent, empty: 1 MiB, never written", "ghost", "196,616"),
        (90, "cell", "hash", "1,245,232"),
    ]
    x = 16
    for w, name, cls, off in segs:
        if cls == "ghost":
            f.rect(x, 40, w, 44, "dashed")
        else:
            f.rect(x, 40, w, 44, cls)
        if w > 20:
            f.text(x + w / 2, 66, name, "small" if w < 120 else "label small", "middle")
        f.line(x, 84, x, 96, "")
        f.text(x, 110, off, "small muted", "middle" if x > 16 else "start")
        x += w + 4
    f.text(x - 4, 124, "1,245,272", "small muted", "end")
    f.text(16, 28, "archive.dialog, just created (offsets in bytes)", "label small muted")
    f.text(54, 66, "", "small")
    f.text(16, 140, "1,245,272 bytes long, 24,576 allocated: the extent is a hole the file system never stores.", "label small muted")
    f.text(16, 158, "The cell is the repository's dialog/version; slots are 64 KiB each, so data starts at 196,616.", "label small muted")
    return f


@fig
def magic():
    f = Fig(520, 92, "The eight bytes of magic: DIALOGA and the format version 0e", "mag")
    chars = ["D", "I", "A", "L", "O", "G", "A", ""]
    bytes_ = hx("44 49 41 4c 4f 47 41 0e")
    for i, (c, b) in enumerate(zip(chars, bytes_)):
        cls = "critical" if i == 7 else "shade"
        f.rect(16 + i * 40, 16, 40, 40, cls)
        f.text(36 + i * 40, 34, c if c else "14", "critical" if i == 7 else "", "middle")
        f.text(36 + i * 40, 50, b, "small muted", "middle")
    f.text(356, 34, "bytes 0 to 7", "label small muted")
    f.text(356, 50, "the last is the format: 14", "label small muted")
    f.text(16, 80, "A file whose first bytes differ is not opened; a format change bumps the last byte.", "label small muted")
    return f


@fig
def commit_tree():
    f = Fig(760, 250, "Commit 2 writes one new node: the root, its buffer grown from 10 to 16 operations; the leaf is reused", "ct")
    f.text(16, 20, "after commit 1", "title")
    f.text(440, 20, "after commit 2", "title")
    for (x, h, size, ops, cls) in [(16, "35a1e8…", "2,548 bytes", 10, "faded"), (440, "565412…", "4,372 bytes", 16, "")]:
        f.rect(x, 34, 300, 74, cls)
        f.text(x + 10, 54, f"root {h}", "hash")
        f.text(x + 10, 70, size, "small muted")
        f.rect(x + 150, 42, 140, 56, "value" if cls == "" else "shade")
        f.text(x + 220, 66, "buffer", "small", "middle")
        f.text(x + 220, 82, f"{ops} operations", "small", "middle")
    f.line(120, 108, 318, 162, "dashed", True)
    f.line(540, 108, 444, 162, "", True)
    f.rect(270, 164, 220, 46, "")
    f.text(380, 184, "leaf b4ff23…", "hash", "middle")
    f.text(380, 200, "360 bytes, unchanged", "small muted", "middle")
    f.text(380, 238, "Buffered writes (see The Search Tree): new facts wait in the root's buffer, so a small commit rewrites the root alone.", "label small muted", "middle")
    return f

@fig
def key_tokens():
    f = Fig(760, 230, "The EAV key of item:1's name split into three tokens: two the file already holds, one new", "kt")
    f.text(16, 20, "the EAV key, as the Keys chapter draws it", "label small muted")
    parts = [
        ("00", "shade", 100),
        ("item:1 00", "entity", 160),
        ("demo.grocery/name 00", "attribute", 230),
        ("03 \"Oat milk\" 00", "value", 200),
    ]
    below = [
        ("form 00", "in the row", "shade", False),
        ("token 77831", "found, from commit 1", "entity", False),
        ("token 77873", "found: item:2's, commit 1", "attribute", False),
        ("token 143526", "new: written now", "value", True),
    ]
    x = 16
    for (text, cls, w), (name, note, bcls, new) in zip(parts, below):
        f.rect(x, 30, w, 30, cls)
        f.text(x + w / 2, 50, text, cls if cls != "shade" else "", "middle")
        f.line(x + w / 2, 60, x + w / 2, 96, "" if new else "dashed", True)
        f.rect(x, 98, w, 44, bcls + (" solid" if new else ""))
        paper = "fill:var(--dg-paper)" if new else None
        f.text(x + w / 2, 116, name, "" if new else (bcls if bcls != "shade" else ""), "middle", style=paper)
        f.text(x + w / 2, 132, note, "small" if new else "small muted", "middle", style=paper)
        x += w + 4
    f.text(16, 176, "The writer looks each part up by the hash of its bytes in the file's index. Parts the file holds are only named:", "label small muted")
    f.text(16, 194, "of this fact, only \"Oat milk\" is written. The form byte is the key's tag and stays in the row.", "label small muted")
    return f

@fig
def token_bytes():
    f = Fig(760, 200, "A token in the file: a pad byte, padding, a four-byte length, then the bytes, starting on a multiple of 16", "tb")
    f.text(16, 20, "inside commit 2's tokens record, from offset 2,296,400", "label small muted")
    cw = 21
    x = 16
    x = f.bytes_row(x, 32, ["0b"], "shade", cw)
    x = f.bytes_row(x, 32, ["00"] * 11, "", cw, tcls="muted")
    x2 = f.bytes_row(x, 32, hx("0a 00 00 00"), "hash", cw)
    x3 = f.bytes_row(x2, 32, hx("03 4f 61 74 20 6d 69 6c 6b 00"), "value", cw)
    f.brace(16, 16 + cw, 58, "pad: 11", "small muted")
    f.brace(16 + cw, x, 58, "11 bytes of padding", "small muted")
    f.brace(x, x2, 58, "length 10", "small hash")
    f.brace(x2, x3, 58, "03 \"Oat milk\" 00", "small value")
    f.line(x2, 96, x2, 112, "critical")
    f.text(x2, 126, "offset 2,296,416 = 16 × 143,526", "critical", "middle")
    f.text(16, 156, "The id is the offset over 16. A reader holding only the id reads the length just before it and the bytes after:", "label small muted")
    f.text(16, 174, "nothing in memory says where a token is, and a token is never moved.", "label small muted")
    return f


@fig
def shared_tokens():
    f = Fig(760, 210, "The fact's four entries name the same three tokens; the history entry adds a version token", "st")
    heads = ["form", "slot 0", "slot 1", "slot 2", "slot 3"]
    xs = [16, 110, 250, 390, 530]
    for h, x in zip(heads, xs):
        f.text(x, 24, h, "label small muted")
    rows = [
        ("00 EAV", [("77831", "entity"), ("77873", "attribute"), ("143526", "value"), None]),
        ("01 AEV", [("77831", "entity"), ("77873", "attribute"), ("143526", "value"), None]),
        ("02 VAE", [("77831", "entity"), ("77873", "attribute"), ("143526", "value"), None]),
        ("03 history", [("143527", "hash"), ("77831", "entity"), ("77873", "attribute"), ("143526", "value")]),
    ]
    y = 34
    for form, cells in rows:
        f.rect(16, y, 86, 28, "shade")
        f.text(24, y + 19, form, "small")
        for (c, x) in zip(cells, xs[1:]):
            if c is None:
                f.rect(x, y, 130, 28, "ghost")
                continue
            t, cls = c
            f.rect(x, y, 130, 28, cls)
            f.text(x + 65, y + 19, t, cls, "middle")
        y += 34
    f.text(16, 192, "A row stores the entity, attribute and value in the same slots whatever its ordering; the form says which ordering it is.", "label small muted")
    return f


@fig
def record_header():
    f = Fig(760, 170, "A record's header: kind, length and CRC-32, then the body", "rh")
    f.text(16, 20, "commit 2's tokens record, at offset 2,295,767", "label small muted")
    x1 = f.bytes_row(16, 32, ["04"], "critical", 70)
    x2 = f.bytes_row(x1, 32, hx("b8 02 00 00"), "shade", 36)
    x3 = f.bytes_row(x2, 32, hx("e8 52 3a af"), "hash", 36)
    f.rect(x3, 32, 300, 30, "dashed")
    f.text(x3 + 150, 52, "body: 696 bytes, five tokens", "small", "middle")
    f.brace(16, x1, 66, "kind 4", "small critical")
    f.brace(x1, x2, 66, "length 696", "small muted")
    f.brace(x2, x3, 66, "crc32 af3a52e8", "small hash")
    f.text(16, 118, "1 + 4 + 4 bytes, little-endian. The checksum covers the kind byte and the body, so a reader walking the file stops at", "label small muted")
    f.text(16, 136, "the first record that is cut short or does not match: everything before it is whole.", "label small muted")
    return f

@fig
def frame_header():
    f = Fig(760, 220, "A frame's data starts on a multiple of 16 with the node's hash, then kind, depth, catalog, base and manifest", "fh")
    f.text(16, 20, "commit 2's frame: record at 2,296,472, data at 2,296,496 = 16 × 143,531", "label small muted")
    cw = 17.6
    x = 16
    hashb = hx("56 54 12 b9 09 7e 31 22 dd 82 98 73 1e f7 d4 5e 49 5b d6 24 60 c4 61 f6 6f ae 37 2b d2 cc 1a 54")
    x1 = f.bytes_row(x, 32, hashb[:16], "hash", cw)
    x1b = f.bytes_row(16, 64, hashb[16:], "hash", cw)
    f.brace(16, x1, 98, "the node's BLAKE3 hash: 32 bytes", "small hash")
    y = 130
    xa = f.bytes_row(16, y, ["01"], "", 100)
    xb = f.bytes_row(xa, y, ["01"], "", 80)
    xc = f.bytes_row(xb, y, ["00"], "", 80)
    xd = f.bytes_row(xc, y, hx("ca e0 04"), "shade", 56)
    xe = f.bytes_row(xd, y, hx("94 e0 04"), "attribute", 56)
    f.brace(16, xa, y + 28, "kind 1: index", "small muted")
    f.brace(xa, xb, y + 28, "depth 1", "small muted")
    f.brace(xb, xc, y + 28, "catalog 0", "small muted")
    f.brace(xc, xd, y + 28, "base: frame 77,897 (+1)", "small muted")
    f.brace(xd, xe, y + 28, "manifest: token 77,844", "small attribute")
    f.text(xe + 8, y + 17, "then rows", "label small muted")
    return f


@fig
def ops_against():
    f = Fig(760, 480, "The root's 16 buffered operations, written as 10 rows copied from the version before and 6 new rows", "oa")
    f.text(16, 20, "commit 1's root: 10 operations", "title")
    f.text(420, 20, "commit 2's root: 16 operations", "title")
    base = [
        "00 rev(old) revision", "00 item:2 name \"Rye bread\"", "01 item:2 name \"Rye bread\"",
        "01 item:1 quantity \"1\"", "01 rev(old) revision", "02 item:1 quantity \"1\"",
        "02 item:2 name \"Rye bread\"", "03 v1 item:1 quantity \"1\"", "03 v1 item:2 name \"Rye bread\"",
        "fe the revision's own key",
    ]
    new = [
        ("N", "00 rev(new) revision"), ("C0", ""), ("N", "00 item:1 name \"Oat milk\""), ("C1", ""),
        ("N", "01 item:1 name \"Oat milk\""), ("C2", ""), ("C3", ""), ("N", "01 rev(new) revision"),
        ("C4", ""), ("C5", ""), ("N", "02 item:1 name \"Oat milk\""), ("C6", ""), ("C7", ""), ("C8", ""),
        ("N", "03 v2 item:1 name \"Oat milk\""), ("C9", ""),
    ]
    rh = 24
    for i, b in enumerate(base):
        y = 34 + i * rh
        f.rect(16, y, 300, rh - 2, "shade")
        f.text(24, y + 16, f"{i:>2}  {b}", "small")
    for i, (kind, text) in enumerate(new):
        y = 34 + i * rh
        if kind == "N":
            f.rect(420, y, 324, rh - 2, "value")
            f.text(428, y + 16, f"{i:>2}  {text}", "small value")
        else:
            j = int(kind[1:])
            f.rect(420, y, 324, rh - 2, "")
            f.text(428, y + 16, f"{i:>2}  {base[j]}", "small muted")
            f.line(316, 34 + j * rh + 11, 418, y + 11, "dashed", True)
    f.text(16, 432, "steps: 1 new, copy 1 from row 0, 1 new, copy 1 from 1, 1 new, copy 2 from 2,", "small muted")
    f.text(16, 448, "       1 new, copy 2 from 4, 1 new, copy 3 from 6, 1 new, copy 1 from 9", "small muted")
    f.text(16, 466, "bytes: 10 0c 02 03 00 02 03 00 02 05 00 02 05 00 02 07 00 02 03 00", "small")
    return f


@fig
def columns():
    f = Fig(760, 420, "The six new rows column by column: ids, the deltas written, and their bytes", "col")
    heads = ["row 0", "row 2", "row 4", "row 7", "row 10", "row 14"]
    cols = [
        ("form", ["00", "00", "01", "01", "02", "03"], "runs: 2 × 00, 2 × 01, then 02 03", "04 00 04 01 03 02 03", ""),
        ("slot 0", ["143487", "77831", "77831", "143487", "77831", "143527"], "+143487 −65656 0 +65656 −65656 +65696", "0b fe c1 11 ef 81 08 00 f0 81 08 ef 81 08 c0 82 08", "entity"),
        ("slot 1", ["77850", "77873", "77873", "77850", "77873", "77831"], "+77850 +23 0 −23 +23 −42", "0b b4 c0 09 2e 00 2d 2e 53", "attribute"),
        ("slot 2", ["143491", "143526", "143526", "143491", "143526", "77873"], "+143491 +35 0 −35 +35 −65653", "0b 86 c2 11 46 00 45 46 e9 81 08", "value"),
        ("slot 3", ["", "", "", "", "", "143526"], "only the history row has a fourth part", "01 cc c2 11", "value"),
        ("datum", ["143517"] * 6, "+143517, then a run of five zeros", "01 ba c2 11 0a 00", "hash"),
    ]
    xs = [90, 200, 310, 420, 530, 640]
    for h, x in zip(heads, xs):
        f.text(x + 50, 20, h, "label small muted", "middle")
    y = 30
    for name, ids, deltas, bytes_, cls in cols:
        f.text(16, y + 16, name, "small")
        for v, x in zip(ids, xs):
            if v:
                f.rect(x, y, 100, 24, cls if cls else "shade")
                f.text(x + 50, y + 16, v, ("small " + cls) if cls else "small", "middle")
            else:
                f.rect(x, y, 100, 24, "ghost")
        f.text(90, y + 40, deltas, "small muted")
        f.text(740, y + 40, bytes_, "small", "end")
        y += 58
    f.text(16, 390, "Each column is zigzag deltas from the row above, as runs: a signed count, then one value repeated or values one by one.", "label small muted")
    f.text(16, 408, "Slot 0 of rows 0 and 7 is the revision's entity; a history row's slot 0 is its version. Ids are token offsets over 16.", "label small muted")
    return f

@fig
def append_layout():
    f = Fig(760, 260, "Commit 2's append in file order: tokens, the frame, then a slot; then two cell records, each followed by a slot", "al")
    recs = [
        ("tokens", 705, "attribute", "2,295,767"),
        ("frame", 150, "value", "2,296,472"),
        ("cell: revision", 347, "hash", "2,296,622"),
        ("cell: induction", 353, "hash", "2,296,969"),
    ]
    x = 16
    scale = 0.46
    xs = []
    for i, (name, size, cls, off) in enumerate(recs):
        w = size * scale
        f.rect(x, 40, w, 40, cls)
        f.text(x + w / 2, 58, name, "small", "middle")
        f.text(x + w / 2, 72, f"{size} B", "small muted", "middle")
        f.line(x, 80, x, 90 + (i % 2) * 14, "")
        f.text(x, 100 + (i % 2) * 14, off, "small muted", "start")
        xs.append((x, w))
        x += w
    f.line(x, 80, x, 90, "")
    f.text(x, 100, "2,297,322", "small muted", "end")
    f.text(16, 28, "after commit 1, the file ended at 2,295,767", "label small muted")
    slots = [("sequence 7 → rolling 1", xs[1][0] + xs[1][1]), ("sequence 8 → rolling 0", xs[2][0] + xs[2][1]), ("sequence 9 → rolling 1", xs[3][0] + xs[3][1])]
    y = 134
    for name, endx in slots:
        f.line(endx, 82, endx, y - 4, "critical", True)
        f.rect(endx - 170, y, 166, 24, "critical")
        f.text(endx - 87, y + 16, name, "small critical", "middle")
        y += 32
    f.text(16, 250, "Records go past the end first; the slot that names the new end is written last, over the older of the two rolling slots.", "label small muted")
    return f

@fig
def slot_layout():
    f = Fig(760, 360, "A slot's fields with the values of slot 9", "sl")
    rows = [
        ("length", "u32", "356", "shade"),
        ("crc32 of the body", "u32", "6624d565", "hash"),
        ("sequence", "u64", "9", ""),
        ("end", "u64", "2,297,322", ""),
        ("durable: the last sync point's sequence", "u64", "0", ""),
        ("boot it was written in", "u64", "1d7afa8f10579f35", "hash"),
        ("counts: blocks, bytes, frames, against, frame bytes, tokens, …", "7 × u64", "0, 0, 3, 1, 363, 20, 1,435", "shade"),
        ("catalogs", "count, names", "index", "attribute"),
        ("the file's index: its root", "u32, node", "a leaf of 25 entries", "value"),
        ("pages free; pages freed since the sync point", "lists of u32", "none; none", ""),
        ("extents held; free; freed since the sync point", "lists of u64", "196,616 and 1,246,469; –; –", "bob"),
        ("the cells' index: its root", "u32, node", "a leaf of 3 entries", "hash"),
    ]
    y = 16
    for name, size, value, cls in rows:
        f.rect(16, y, 410, 24, cls if cls else "")
        f.text(24, y + 16, name, "label small")
        f.text(436, y + 16, size, "small muted")
        f.text(536, y + 16, value, "small")
        y += 27
    f.text(16, 352, "Little-endian throughout. A slot has 64 KiB; this one uses 364 bytes. Its checksum is what makes it whole or not.", "label small muted")
    return f

@fig
def rolling():
    f = Fig(760, 230, "Two rolling slots take turns; a slot torn by a crash leaves the other, older one whole", "ro")
    f.text(16, 20, "every append writes the older slot", "title")
    seq = [(7, 1), (8, 0), (9, 1)]
    for i, (s, slot) in enumerate(seq):
        x = 16 + i * 150
        f.text(x, 46, f"append {s}", "label small")
        for k in range(2):
            y = 56 + k * 30
            cls = "critical" if k == slot else "shade"
            f.rect(x, y, 120, 24, cls)
            label = f"rolling {k}: " + (f"seq {s}" if k == slot else f"seq {s - 1}")
            f.text(x + 60, y + 16, label, "small", "middle")
    x = 500
    f.text(x, 46, "append 10, cut short", "label small")
    f.rect(x, 56, 120, 24, "ghost")
    f.text(x + 60, 72, "rolling 0: torn", "small critical", "middle")
    f.rect(x, 86, 120, 24, "value")
    f.text(x + 60, 102, "rolling 1: seq 9", "small", "middle")
    f.line(x + 124, 98, x + 170, 98, "", True)
    f.text(x + 176, 96, "opening", "label small")
    f.text(x + 176, 110, "takes this", "label small")
    f.text(16, 160, "Opening reads both rolling slots and takes the newer one whose checksum matches. The records an append wrote", "label small muted")
    f.text(16, 178, "before its slot was cut short are past that slot's end: no slot names them, and the next writer writes over them.", "label small muted")
    f.text(16, 206, "The third slot, the durable one, is written only at a sync point (below).", "label small muted")
    return f


@fig
def locks():
    f = Fig(760, 190, "The lock bytes sit at 2^62, far past any data, so locking them never blocks a read or a write", "lk")
    f.line(126, 60, 396, 60, "dashed")
    f.rect(16, 48, 110, 24, "value")
    f.text(71, 64, "the data, 3.5 MB", "small", "middle")
    f.text(261, 50, "4.6 × 10¹⁸ empty bytes", "small muted", "middle")
    names = [("sync", "2⁶²−3", "shade"), ("recovery", "2⁶²−2", "shade"), ("present", "2⁶²−1", "entity"), ("turn", "2⁶²", "critical"), ("owner(x)", "2⁶²+1+x", "bob")]
    x = 396
    for name, at, cls in names:
        w = 64 if name != "owner(x)" else 72
        f.rect(x, 48, w, 24, cls)
        f.text(x + w / 2, 64, name, "small", "middle")
        f.text(x + w / 2, 92, at, "small muted", "middle")
        x += w + 4
    f.text(16, 124, "turn: held by the writer laying out and writing an append.   present: held shared by every process with the file open.", "label small muted")
    f.text(16, 142, "recovery: held while a process opens the file.   sync: held while a sync point is made.   owner(x): held by extent x's writer.", "label small muted")
    f.text(16, 166, "POSIX record locks on Unix, LockFileEx on Windows: a process that dies lets go of every byte it held.", "label small muted")
    return f

@fig
def cell_record():
    f = Fig(760, 344, "The head's cell record: its name, its value naming the new root, and the extents it needs, none here", "cr")
    f.text(16, 20, "commit 2's head: record at 2,296,622, data at 2,296,640 = 16 × 143,540", "label small muted")
    cw = 26
    x1 = f.bytes_row(16, 32, hx("09 52 01 00 00 11 79 cd 1c"), "shade", cw)
    f.brace(16, x1, 60, "kind 9: cell, length 338, crc 1ccd7911", "small muted")
    x2 = f.bytes_row(16, 96, ["08"], "", 40)
    x2 = f.bytes_row(x2, 96, ["00"] * 8, "", 20, tcls="muted")
    x3 = f.bytes_row(x2, 96, hx("14 00"), "shade", 40)
    f.brace(16, 56, 124, "pad 8", "small muted")
    f.brace(x2, x3, 124, "name: 20 bytes", "small muted")
    f.rect(x3, 96, 300, 26, "attribute")
    f.text(x3 + 150, 114, "branch/main 00 revision", "attribute", "middle")
    y = 168
    x4 = f.bytes_row(16, y, ["01"], "", 64)
    x5 = f.bytes_row(x4, y, hx("2c 01 00 00"), "shade", 30)
    x6 = f.bytes_row(x5, y, hx("a7 64 74 72 65 65 98 20 18 56 18 54 12 18 b9 09"), "hash", 26)
    f.text(x6 + 8, y + 17, "…", "")
    f.brace(16, x4, y + 28, "present", "small muted")
    f.brace(x4, x5, y + 28, "300 bytes", "small muted")
    f.brace(x5 + 8 * 26, x6, y + 28, "the root's hash: 56 54 12 b9 09 …", "small hash")
    f.text(x5 + 4 * 26, y + 62, "CBOR: \"tree\" is the root's 32 bytes", "small muted", "middle")
    x7 = f.bytes_row(16, 262, hx("00 00"), "bob", 56)
    f.brace(16, x7, 290, "needs: 0 extents", "small bob")
    f.text(140, 278, "each needed extent would follow: record u64, epoch u64, bytes of frames u64", "label small muted")
    f.text(16, 336, "The cells' index then files the name's key, 1f2546b7, at 143,540. A cell read follows that and nothing else.", "label small muted")
    return f

@fig
def order():
    f = Fig(760, 250, "Why a head after its blocks survives any power loss: recovery keeps a prefix of the log, and a head is always behind what it names", "or")
    f.text(16, 20, "the log, in the order it was written", "title")
    recs = [("tokens", "attribute"), ("frame 565412…", "value"), ("head → 565412…", "hash")]
    x = 16
    for name, cls in recs:
        f.rect(x, 34, 170, 34, cls)
        f.text(x + 85, 56, name, "small", "middle")
        x += 174
    f.path("M 446 34 C 420 6, 300 6, 280 32", "hash", True)
    cases = [
        ("power lost after the frame", 360, "the head is not there: the branch is at commit 1, whole"),
        ("power lost after the head", 538, "everything is there: the branch is at commit 2, whole"),
    ]
    y = 100
    for n, (name, cut, result) in enumerate(cases, 1):
        f.line(cut, 30, cut, 76, "critical")
        f.text(cut, 90, str(n), "small critical", "middle")
        f.text(16, y + 14, f"{n}. {name}", "label small critical")
        f.text(250, y + 14, result, "label small")
        y += 22
    f.text(16, 168, "The disk can reorder unsynced writes, so a record is kept only if everything before it in the slot is whole:", "label small muted")
    f.text(16, 186, "opening walks every record since the durable slot by its checksum. What a power loss can lose is a suffix.", "label small muted")
    f.text(16, 212, "Like SQLite's synchronous=NORMAL: an update is durable at the next sync point, not at its own.", "label small muted")
    return f


@fig
def sync_point():
    f = Fig(760, 230, "A sync point: the file synced, the durable slot written as slot 12 and synced, then rolling slot 13 says durable 12", "sp")
    steps = [
        ("1", "fsync the file", "everything up to slot 12's end (2,299,109) is on the disk", "shade"),
        ("2", "write the durable slot", "a copy of slot 12, freed pages and extents now free", "critical"),
        ("3", "fsync again", "the durable slot is on the disk", "shade"),
        ("4", "write rolling slot 13", "durable = 12: pages and extents freed before 12 can be reused", "value"),
    ]
    y = 20
    for n, a, b, cls in steps:
        f.circle(32, y + 14, 12, cls)
        f.text(32, y + 19, n, "", "middle")
        f.text(56, y + 12, a, "label small")
        f.text(56, y + 28, b, "small muted")
        y += 44
    f.text(16, 210, "One follows every 64 appends, 2 seconds, or 2,048 freed index pages, and Chronicle::sync. It is made after the turn is let go.", "label small muted")
    return f


@fig
def lookup():
    f = Fig(760, 300, "Reading item:1's name back: the block's key in the slot's index, the frame, then each token by its id", "lu")
    steps = [
        (16, 30, 200, "hash 565412b9097e…", "hash", "a reader asks for the root"),
        (16, 100, 200, "key 39125456", "shade", "two bits of space (00: blocks), thirty of the hash"),
        (16, 170, 200, "frame 143531", "value", "the slot's index leaf: 39125456 → 143531"),
        (380, 170, 180, "offset 2,296,496", "value", "16 × 143531: the frame's data"),
        (380, 100, 180, "rows, decoded", "", "against frame 77,897, depth 1"),
        (380, 30, 180, "token 143526", "value", "row 2's slot 2"),
        (600, 30, 140, "03 \"Oat milk\" 00", "value", "at 16 × 143526"),
    ]
    prev = None
    for x, y, w, t, cls, note in steps:
        f.rect(x, y, w, 30, cls)
        f.text(x + w / 2, y + 20, t, "small " + (cls if cls in ("hash", "value") else ""), "middle")
        f.text(x, y + 46, note, "small muted")
        if prev:
            px, py, pw = prev
            if py == y:
                f.line(px + pw, py + 15, x - 2, y + 15, "", True)
            elif px == x:
                f.line(x + w / 2, py + 30 if py < y else py, x + w / 2, y - 2 if py < y else y + 32, "", True)
            else:
                f.line(px + pw, py + 15, x - 2, y + 15, "", True)
        prev = (x, y, w)
    f.text(16, 270, "Inside a leaf, a search compares keys part by part where their tokens sit, through the mapping: no key is copied out.", "label small muted")
    f.text(16, 288, "When opening trusts the newest slot, it reads the slots and nothing else; everything after is mapped and read on demand.", "label small muted")
    return f


@fig
def chain():
    f = Fig(760, 220, "The root's frames, each written against the one before: reading the newest decodes the chain back to a whole frame", "ch")
    frames = [("77897", 0, 117, "commit 1"), ("143531", 1, 126, "commit 2"), ("143642", 2, 159, "commit 3"), ("143847", 3, 128, "commit 4, folded"), ("209486", 4, 214, "commit 6"), ("209547", 5, 114, "a lost try")]
    x = 16
    for name, d, size, c in frames:
        cls = "value solid" if d == 0 else "value"
        f.rect(x, 40, 104, 44, cls)
        f.text(x + 52, 58, f"frame {name}", "small" if d else "small", "middle", style="fill:var(--dg-paper)" if d == 0 else None)
        f.text(x + 52, 74, f"depth {d}, {size} B", "small", "middle", style="fill:var(--dg-paper)" if d == 0 else None)
        f.text(x + 52, 100, c, "small muted", "middle")
        if d:
            f.line(x - 2, 62, x - 18, 62, "", True)
        x += 122
    f.text(16, 28, "base ←", "label small muted")
    f.text(16, 136, "Reading frame 209,547 decodes six frames: the whole one, then each frame's copies and new rows on top. A writer looks up the", "label small muted")
    f.text(16, 154, "node's lineage (its catalog, its kind and its first key) to find the frame to write against.", "label small muted")
    f.text(16, 180, "At depth 32 (CHAIN) the next version is written whole again, so no read decodes more than 32 frames.", "label small muted")
    f.text(16, 206, "Commit 5's root is frame 143,836, also written against 143,642: two versions can share a base.", "label small muted")
    return f


@fig
def edit_rows():
    f = Fig(760, 200, "Changing the name: the old value's entries are written again as retractions beside the new value's", "er")
    rows = [
        ("00 item:1 name \"Oat milk\"", "retracted", "critical"),
        ("00 item:1 name \"Oat milk, 2 L\"", "datum 143614", "value"),
        ("01 item:1 name \"Oat milk\"", "retracted", "critical"),
        ("01 item:1 name \"Oat milk, 2 L\"", "datum 143614", "value"),
        ("02 …, 03 …, and 05: a coverage entry for what the change replaced", "", "shade"),
    ]
    y = 20
    for t, d, cls in rows:
        f.rect(16, y, 460, 26, cls)
        f.text(24, y + 17, t, "small")
        f.text(484, y + 17, d, "small " + ("critical" if d == "retracted" else "muted"))
        y += 30
    f.text(16, 182, "Commit 3's root frame: 23 operations, 13 copied from commit 2's and 10 new, 159 bytes for a 6,092-byte block.", "label small muted")
    return f


@fig
def two_processes():
    f = Fig(760, 430, "Bob's process commits while Alice's holds the turn: his block goes into his extent at once, and his head waits for the turn", "tp")
    f.text(130, 20, "Alice's process", "alice", "middle")
    f.text(380, 20, "the file", "title", "middle")
    f.text(630, 20, "Bob's process", "bob", "middle")
    for x in (130, 380, 630):
        f.line(x, 28, x, 410, "dashed")
    f.line(130, 52, 376, 52, "alice", True)
    f.text(250, 46, "takes the turn", "small alice", "middle")
    f.rect(118, 58, 24, 248, "alice")
    f.line(630, 86, 384, 86, "bob", True)
    f.text(506, 80, "tries the turn: taken", "small bob", "middle")
    f.line(630, 134, 384, 134, "bob", True)
    f.text(506, 110, "writes the block whole into", "small bob", "middle")
    f.text(506, 126, "its extent at 1,246,469", "small bob", "middle")
    f.line(630, 168, 384, 168, "bob", True)
    f.text(506, 162, "count: 8,059 bytes", "small bob", "middle")
    f.text(392, 192, "readers in any process", "small muted")
    f.text(392, 206, "find the block now", "small muted")
    f.rect(618, 226, 24, 80, "bob")
    f.text(650, 256, "the head", "small bob")
    f.text(650, 270, "waits for", "small bob")
    f.text(650, 284, "the turn", "small bob")
    f.line(130, 306, 376, 306, "alice", True)
    f.text(250, 300, "lets go", "small alice", "middle")
    f.line(630, 350, 384, 350, "bob", True)
    f.text(506, 326, "takes the turn, appends the head:", "small bob", "middle")
    f.text(506, 342, "needs extent 1,246,469, epoch 4", "small bob", "middle")
    f.line(630, 396, 384, 396, "bob", True)
    f.text(506, 372, "its next commit, in the turn,", "small bob", "middle")
    f.text(506, 388, "folds the block in as frame 143,847", "small bob", "middle")
    f.text(16, 424, "In the example, a helper process holds the turn for 300 ms while the example's peer commits item:3.", "label small muted")
    return f


@fig
def extent_bytes():
    f = Fig(760, 260, "An extent's header: count, its inverse and the epoch, then whole frames", "eb")
    f.text(16, 20, "the extent at 1,246,469 while it holds Bob's block", "label small muted")
    cw = 26
    x1 = f.bytes_row(16, 32, hx("08 1a 00 10 00 6e a1 c1 2c"), "shade", cw)
    f.brace(16, x1, 60, "kind 8, length 1,048,602", "small muted")
    x2 = f.bytes_row(x1 + 8, 32, hx("01 00"), "", cw, tcls="muted")
    f.brace(x1 + 8, x2, 60, "pad to 8", "small muted")
    y = 104
    cw = 28
    x3 = f.bytes_row(16, y, hx("7b 1f 00 00 00 00 00 00"), "bob", cw)
    x4 = f.bytes_row(x3 + 8, y, hx("84 e0 ff ff ff ff ff ff"), "bob", cw, tcls="muted")
    x5 = f.bytes_row(x4 + 8, y, hx("04 00 00 00 00 00 00 00"), "shade", cw)
    f.brace(16, x3, y + 28, "count: 8,059 bytes of frames", "small bob")
    f.brace(x3 + 8, x4, y + 28, "its inverse: a torn count is none", "small muted")
    f.brace(x4 + 8, x5, y + 28, "epoch 4: claimed by append 4", "small muted")
    f.rect(16, 180, 620, 28, "value")
    f.text(24, 198, "frame record, 8,050 bytes: hash 387ac86c…, kind ff (whole), then the 7,996-byte block", "small")
    f.text(16, 238, "Only the count is written over, after the frames it counts. The epoch tells a new claim of the same space from an old one.", "label small muted")
    return f

@fig
def cas():
    f = Fig(760, 300, "Two handles commit to main: the second's compare-and-swap fails, and its transaction runs again over the winner", "cas")
    f.text(120, 20, "handle 1", "alice", "middle")
    f.text(380, 20, "main's head", "title", "middle")
    f.text(640, 20, "handle 2", "bob", "middle")
    for x in (120, 380, 640):
        f.line(x, 28, x, 280, "dashed")
    f.line(640, 50, 384, 50, "bob", True)
    f.text(510, 44, "reads head h1", "small bob", "middle")
    f.line(120, 80, 376, 80, "alice", True)
    f.text(250, 74, "publish h2, expecting h1: ok", "small alice", "middle")
    f.line(640, 120, 384, 120, "bob", True)
    f.text(510, 114, "publish h3, expecting h1", "small bob", "middle")
    f.line(380, 140, 636, 140, "critical", True)
    f.text(510, 156, "version mismatch: the head is h2", "small critical", "middle")
    f.rect(628, 170, 24, 40, "bob")
    f.text(600, 194, "the closure runs again over h2", "small bob", "end")
    f.line(640, 230, 384, 230, "bob", True)
    f.text(510, 224, "publish h3', expecting h2: ok", "small bob", "middle")
    f.text(16, 290, "Measured with the example: the second handle's Branch::transact closure ran 2 times.", "label small muted")
    return f


@fig
def reader():
    f = Fig(760, 250, "A reader holds a snapshot: the slot it took and a mapping up to that slot's end; a writer appends past it", "rd")
    f.rect(16, 50, 420, 36, "value")
    f.text(226, 72, "up to slot 9's end (2,297,322): never written again", "small", "middle")
    f.rect(436, 50, 160, 36, "attribute")
    f.text(516, 72, "commit 3's records", "small", "middle")
    f.line(436, 40, 436, 100, "critical")
    f.text(436, 116, "the reader's snapshot ends here", "small critical", "middle")
    f.text(16, 36, "the file, while commit 3 is written", "label small muted")
    f.text(16, 152, "Everything a slot names stays as it was, except index pages, which are reused. So a reader reads its snapshot without", "label small muted")
    f.text(16, 170, "a lock, and the writer never waits for it. A lookup that misses looks for a newer slot, once the file has grown.", "label small muted")
    f.text(16, 196, "An index page reused since is caught: each page records the append that wrote it and a checksum, and its parent names", "label small muted")
    f.text(16, 214, "both (page 860, append 29). A mismatch is stale: the reader takes the newest slot and looks again.", "label small muted")
    return f


@fig
def page():
    f = Fig(760, 220, "An index page: the append that wrote it, a checksum, a header, then entries; the slot's root names pages by page and append", "pg")
    f.text(16, 20, "page 860 (offset 3,522,560), written by append 29 when 2,100 blocks were appended at once", "label small muted")
    cw = 30
    x1 = f.bytes_row(16, 32, hx("1d 00 00 00"), "critical", cw)
    x2 = f.bytes_row(x1, 32, hx("2c 06 0e cd"), "hash", cw)
    x3 = f.bytes_row(x2, 32, hx("00 00 68 01"), "shade", cw)
    x4 = f.bytes_row(x3, 32, hx("00 00 00 00"), "", cw, tcls="muted")
    f.rect(x4, 32, 220, 30, "value")
    f.text(x4 + 110, 52, "360 entries: key, value", "small", "middle")
    f.brace(16, x1, 66, "append 29", "small critical")
    f.brace(x1, x2, 66, "crc32", "small hash")
    f.brace(x2, x3, 66, "leaf, 360 entries", "small muted")
    f.brace(x3, x4, 66, "unused", "small muted")
    f.text(16, 118, "The slot's root became internal: children (860, 29) … (865, 29),", "small muted")
    f.text(16, 134, "pivots 0ac5a1f9, 15a8af75, 21212262, 2c05ce66, 36b4584b.", "small muted")
    f.text(16, 166, "4,096 bytes a page, on a 4,096-byte boundary of the file. A leaf holds up to 510 entries; an internal node 8 children and a buffer.", "label small muted")
    f.text(16, 184, "Most appends write only the root, whose buffer holds up to 2,048 entries.", "label small muted")
    return f

@fig
def serve():
    f = Fig(760, 270, "Serving a block to a peer: the frame chain rebuilds the exact bytes, and their BLAKE3 hash is the name asked for", "sv")
    steps = [
        ("peer asks for 565412…", "hash"),
        ("frame 143531, depth 1", "value"),
        ("rows of frame 77,897, then copies and new rows", "value"),
        ("each token's bytes, by id", "attribute"),
        ("the block: 4,372 bytes, encoded", "shade"),
        ("BLAKE3 = 565412…  ✓", "hash"),
    ]
    y = 20
    for i, (t, cls) in enumerate(steps):
        f.rect(16, y, 380, 28, cls)
        f.text(206, y + 18, t, "small", "middle")
        if i:
            f.line(206, y - 10, 206, y - 2, "", True)
        y += 38
    f.text(420, 40, "The example rebuilt all nine nodes this way:", "label small")
    f.text(420, 58, "every hash matched.", "label small")
    f.text(420, 92, "A tree query never asks for bytes: it reads", "label small muted")
    f.text(420, 110, "the rows in place. Only sync and push need", "label small muted")
    f.text(420, 128, "blocks, since replicas exchange blocks.", "label small muted")
    f.text(420, 162, "Blocks that arrive are appended like a", "label small muted")
    f.text(420, 180, "commit's: tree nodes become frames, other", "label small muted")
    f.text(420, 198, "blocks are kept whole.", "label small muted")
    return f


@fig
def recovery():
    f = Fig(760, 330, "What opening trusts: the newest slot when the file is in use or the boot is the same, otherwise only what checks out since the durable slot", "rc")
    f.rect(260, 16, 240, 30, "shade")
    f.text(380, 36, "take the recovery byte", "small", "middle")
    f.line(380, 46, 380, 64, "", True)
    f.rect(240, 66, 280, 30, "")
    f.text(380, 86, "another process holds the turn?", "small", "middle")
    f.line(520, 81, 600, 81, "", True)
    f.text(560, 76, "yes", "small muted", "middle")
    f.rect(602, 66, 142, 30, "value")
    f.text(673, 86, "newest slot, as is", "small", "middle")
    f.line(380, 96, 380, 114, "", True)
    f.text(388, 110, "no", "small muted")
    f.rect(200, 116, 360, 30, "")
    f.text(380, 136, "another process present, or same boot?", "small", "middle")
    f.line(560, 131, 600, 131, "", True)
    f.text(580, 126, "yes", "small muted", "middle")
    f.rect(602, 116, 142, 30, "value")
    f.text(673, 136, "newest slot", "small", "middle")
    f.line(380, 146, 380, 164, "", True)
    f.text(388, 160, "no: maybe a power loss", "small muted")
    f.rect(160, 166, 440, 44, "critical")
    f.text(380, 184, "check every record since the durable slot by its checksum,", "small", "middle")
    f.text(380, 200, "the index pages it reaches, and each head's needed extents", "small", "middle")
    f.line(320, 210, 260, 228, "", True)
    f.line(440, 210, 500, 228, "", True)
    f.rect(130, 230, 240, 30, "value")
    f.text(250, 250, "newest rolling slot that passes", "small", "middle")
    f.rect(390, 230, 240, 30, "shade")
    f.text(510, 250, "else the durable slot", "small", "middle")
    f.text(16, 290, "A process that died holding the turn left its writes with the operating system, in order: a slot of this boot is trusted.", "label small muted")
    f.text(16, 308, "Its records past the slot's end are dropped by the next writer. A power loss can reorder writes, so only then is the file checked.", "label small muted")
    return f


@fig
def torn():
    f = Fig(760, 170, "A power-loss image: everything up to the last sync, then any subset of later writes, each torn at 512-byte sectors", "tn")
    f.text(16, 20, "what the test's disk keeps after a power loss", "title")
    f.rect(16, 34, 300, 30, "value")
    f.text(166, 54, "up to the last fsync: all kept", "small", "middle")
    xs = [(320, 60, "kept"), (384, 50, "lost"), (438, 80, "torn"), (522, 40, "kept"), (566, 70, "lost")]
    for x, w, k in xs:
        if k == "kept":
            f.rect(x, 34, w, 30, "value")
        elif k == "lost":
            f.rect(x, 34, w, 30, "ghost")
        else:
            for s in range(4):
                f.rect(x + s * w / 4, 34, w / 4, 30, "value" if s % 2 == 0 else "ghost")
        f.text(x + w / 2, 82, k, "small muted", "middle")
    f.text(16, 116, "The archive's tests replay their own writes onto such images, at every point and in 30 ways at each,", "label small muted")
    f.text(16, 134, "and open each as after a reboot. Each must open, keep every block synced before the loss, read no block as", "label small muted")
    f.text(16, 152, "other bytes, and hold heads whose every block is there.", "label small muted")
    return f


@fig
def close():
    f = Fig(760, 176, "The last close: extents are freed and a sync point made; free extents that end the file are cut off", "cl")
    f.text(16, 20, "the example's file after its last close: 3,547,136 bytes long, 241,664 allocated", "label small muted")
    segs = [(20, "shade", ""), (40, "ghost", "free"), (170, "value", ""), (40, "ghost", "free"), (120, "value", ""), (40, "ghost", "free"), (190, "value", "index pages last")]
    x = 16
    for w, cls, t in segs:
        f.rect(x, 34, w, 30, cls)
        if t:
            f.text(x + w / 2, 54, t, "small", "middle")
        x += w
    f.text(16, 92, "All three extents are free, but the file ends in records, so nothing is cut here. A file whose last", "label small muted")
    f.text(16, 110, "records are free extents is cut to their start. Until then, an extent that was never written is a hole the", "label small muted")
    f.text(16, 128, "file system does not store: about 3 MB of the length above.", "label small muted")
    f.text(16, 162, "The last close is the process that takes the present byte exclusive: no other has the file open.", "label small muted")
    return f
