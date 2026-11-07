#!/usr/bin/env python3
"""The 2026-11-07 charts of what a file holds and of index layouts, from
the numbers `breakdown` and `write_amplification_of_a_file` printed (see
the journal's entry "the index in the file, measured").

    research/single-file-format/bench/file-charts.py <out dir>

Writes `composition.svg` and `index-layouts.svg`; `render.js` makes PNGs.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from report import svg_bars  # noqa: E402

out = Path(sys.argv[1])
out.mkdir(parents=True, exist_ok=True)

# name, then MiB of slots, frames, tokens and index
FILES = [
    ("1×, 2.0 MiB", 0.13, 0.64, 0.80, 0.41),
    ("10×, 24 MiB", 0.13, 12.09, 2.94, 9.12),
    ("100×, 676 MiB", 0.13, 257.99, 23.27, 395.02),
    ("MusicBrainz, 2.7 GiB", 0.13, 798.84, 145.91, 1826.12),
]
values = []
for _, slots, frames, tokens, index in FILES:
    total = slots + frames + tokens + index
    values.append([(100 * v / total,) * 3 for v in (frames, tokens, index, slots)])
(out / "composition.svg").write_text(svg_bars(
    "What the file holds, format 9",
    "share of each file by record kind: tonk-shaped (a member at 1×, the owner at 10× and 100×) "
    "and MusicBrainz; index of 32 children a node",
    [f[0] for f in FILES], ["frames", "tokens", "index", "slots"], values, "% of the file",
    labels=True))

# children and buffer, then MiB written with lineage entries and without
LAYOUTS = [("32 and 256", 2165, 1590), ("8 and 512", 1083, 814), ("4 and 256", 838, 640)]
(out / "index-layouts.svg").write_text(svg_bars(
    "Index written over MusicBrainz's appends, by children a node and buffer",
    "MiB, replayed in memory (2,000 appends of 1,442 frames and 1,513 tokens); "
    "dashed: the frames and tokens it indexes, 945 MiB",
    [name for name, _, _ in LAYOUTS], ["lineages in the index", "no lineage entries"],
    [[(k, k, k), (d, d, d)] for _, k, d in LAYOUTS], "MiB written", reference=945, labels=True))

# The index with its pages reused (`index_layouts`, src/archive/model.rs):
# layout, then % of the data at MusicBrainz, at 100×, at 100× with pages
# held 64 appends, and an append's index writes against its data at
# MusicBrainz (944.8 MiB in 2,000 appends).
REUSED = [
    ("keys held, 16 B", 14.3, 12.5, 14.4, 749),
    ("4-byte fragments, 8 B", 6.9, 6.3, 7.1, 344),
    ("positions, tree", 3.8, 3.2, 3.6, 156),
    ("hashed, 4 KiB", 4.8, 3.4, 36.0, 8094),
    ("hashed, 1 KiB", 5.6, 4.2, 14.4, 4444),
    ("hashed, 512 B", 5.3, 5.3, 14.1, 2474),
]
(out / "index-reused.svg").write_text(svg_bars(
    "The index with its pages reused, as a share of the data",
    "modelled in pages of the size given (4 KiB for the trees); dashed: 10%",
    [row[0] for row in REUSED],
    ["MusicBrainz", "100×", "100×, pages held 64 appends"],
    [[(v, v, v) for v in row[1:4]] for row in REUSED], "% of the data", reference=10,
    labels=True))
per_append = 944.8 * 1024 / 2000
(out / "index-writes.svg").write_text(svg_bars(
    "What an append writes to the index, against the append's own data",
    "MusicBrainz's appends, modelled; dashed: as much as the data",
    [row[0] for row in REUSED], ["index writes ÷ data"],
    [[(row[4] / per_append,) * 3] for row in REUSED], "× the append's data", reference=1,
    labels=True))
