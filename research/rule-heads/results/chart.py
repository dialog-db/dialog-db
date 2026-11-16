"""Draw `<date>.svg` from `<date>.json`: instructions a commit retires in
each shape, a group a cell. No dependency; run as `python3 chart.py 2026-11-12`."""
import json
import sys

date = sys.argv[1]
data = json.load(open(f"{date}.json"))["scenarios"]

CELLS = [("light-2", "1 read, 2 heads"), ("light-4", "1 read, 4 heads"),
         ("heavy-2", "4 reads, 2 heads"), ("heavy-4", "4 reads, 4 heads")]
SHAPES = [("wide", "one rule, one wide head (stands in for several heads)", "#2a78d6"),
          ("named", "a named condition: one rule issues a command, a rule a head reads it", "#eb6834"),
          ("repeat", "a rule a head, the body written again", "#1baf7a")]
INK, MUTED, GRID, SURFACE = "#0b0b0b", "#52514e", "#e4e3df", "#fcfcfb"


def median(scenario):
    return sorted(data[scenario]["instructions"])[1] / 1e9


W, H, LEFT, TOP, BOTTOM, RIGHT = 920, 470, 64, 118, 46, 20
plot_w, plot_h = W - LEFT - RIGHT, H - TOP - BOTTOM
top = 4.0
group = plot_w / len(CELLS)
bar, gap = 44, 2

out = [f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" width="{W}" height="{H}" '
       f'font-family="system-ui, -apple-system, Segoe UI, sans-serif">',
       f'<rect width="{W}" height="{H}" fill="{SURFACE}"/>',
       f'<text x="{LEFT}" y="26" font-size="16" font-weight="600" fill="{INK}">'
       'Instructions one commit of 500 move commands retires, in billions</text>',
       f'<text x="{LEFT}" y="46" font-size="12" fill="{MUTED}">'
       'main dd19d6c, callgrind, median of three runs; the percentage is over the wide head</text>']
for at, (_, label, color) in enumerate(SHAPES):
    y = 64 + at * 17
    out.append(f'<rect x="{LEFT}" y="{y}" width="12" height="12" rx="2" fill="{color}"/>')
    out.append(f'<text x="{LEFT + 18}" y="{y + 10}" font-size="12" fill="{INK}">{label}</text>')
for tick in range(0, 5):
    y = TOP + plot_h - tick / top * plot_h
    out.append(f'<line x1="{LEFT}" x2="{W - RIGHT}" y1="{y:.1f}" y2="{y:.1f}" stroke="{GRID}" stroke-width="1"/>')
    out.append(f'<text x="{LEFT - 8}" y="{y + 4:.1f}" font-size="11" text-anchor="end" fill="{MUTED}">{tick}</text>')
for g, (cell, label) in enumerate(CELLS):
    x0 = LEFT + g * group + (group - len(SHAPES) * bar - (len(SHAPES) - 1) * gap) / 2
    wide = median(f"heads-wide-{cell}")
    for s, (shape, _, color) in enumerate(SHAPES):
        value = median(f"heads-{shape}-{cell}")
        h = value / top * plot_h
        x, y = x0 + s * (bar + gap), TOP + plot_h - h
        out.append(f'<path d="M{x:.1f},{TOP + plot_h} V{y + 4:.1f} Q{x:.1f},{y:.1f} {x + 4:.1f},{y:.1f} '
                   f'H{x + bar - 4:.1f} Q{x + bar:.1f},{y:.1f} {x + bar:.1f},{y + 4:.1f} V{TOP + plot_h} Z" fill="{color}">'
                   f'<title>{shape}, {label}: {value:.3f} billion</title></path>')
        out.append(f'<text x="{x + bar / 2:.1f}" y="{y - 6:.1f}" font-size="11" text-anchor="middle" fill="{INK}">{value:.2f}</text>')
        if shape != "wide":
            out.append(f'<text x="{x + bar / 2:.1f}" y="{y - 19:.1f}" font-size="10" text-anchor="middle" fill="{MUTED}">'
                       f'+{(value / wide - 1) * 100:.0f}%</text>')
    out.append(f'<text x="{LEFT + g * group + group / 2:.1f}" y="{H - 18}" font-size="12" text-anchor="middle" fill="{INK}">{label}</text>')
out.append(f'<line x1="{LEFT}" x2="{W - RIGHT}" y1="{TOP + plot_h}" y2="{TOP + plot_h}" stroke="{MUTED}" stroke-width="1"/>')
out.append("</svg>")
open(f"{date}.svg", "w").write("\n".join(out) + "\n")
