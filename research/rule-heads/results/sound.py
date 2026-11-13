"""Draw the charts of `2026-11-13-sound.json` and print its tables.
Run as `python3 sound.py`."""
import json

from bars import bars

NAME = "2026-11-13-sound"
data = json.load(open(f"{NAME}.json"))["variants"]
# Each variant with the main it was interleaved with.
SITTINGS = [("share", "main", "one body evaluated once (#790)", "#1baf7a"),
            ("subject", "main2", "a seed binds its entity (#791)", "#2a78d6"),
            ("plan", "main2", "a rule's plans are kept (#792)", "#c9a227"),
            ("both", "main3", "#790 and #791 together", "#eb6834")]
CELLS = [("light-2", "1 read, 2 heads"), ("heavy-4", "4 reads, 4 heads")]
SHAPES = ["wide", "named", "repeat"]


def value(variant, scenario):
    entry = data[variant][scenario]
    runs = sorted(entry["instructions"])
    median = runs[len(runs) // 2]
    return median / entry["size"] / 1e6 if scenario.startswith("heads-each") else median / 1e6


def spread():
    worst = 0
    for variant in data.values():
        for entry in variant.values():
            runs = entry["instructions"]
            worst = max(worst, (max(runs) - min(runs)) / min(runs) * 100)
    return worst


def row(name, variant, prefix, cell, label, digits):
    w, n, r = (value(variant, f"{prefix}-{shape}-{cell}") for shape in SHAPES)
    print(f"| {name} | {label} | {w:.{digits}f} | {n:.{digits}f} | {r:.{digits}f} "
          f"| {(n / w - 1) * 100:+.1f}% | {(r / w - 1) * 100:+.1f}% |")


def table(prefix, digits):
    print("| | body, heads | `wide` | `named` | `repeat` | `named` over `wide` | `repeat` over `wide` |")
    print("|---|---|---|---|---|---|---|")
    for cell, label in CELLS:
        row("main", "main", prefix, cell, label, digits)
        for variant, _, name, _ in SITTINGS:
            row(name, variant, prefix, cell, label, digits)


def against(prefix):
    print("| | body, heads | `wide` | `named` | `repeat` |")
    print("|---|---|---|---|---|")
    for cell, label in CELLS:
        for variant, main, name, _ in SITTINGS:
            cells = []
            for shape in SHAPES:
                off, on = value(main, f"{prefix}-{shape}-{cell}"), value(variant, f"{prefix}-{shape}-{cell}")
                cells.append(f"{(on / off - 1) * 100:+.1f}%")
            print(f"| {name} | {label} | " + " | ".join(cells) + " |")


def chart(prefix, path, title, top, ticks, digits):
    groups = [f"{shape}|{label}" for _, label in CELLS for shape in SHAPES]
    variants = [("main", "main at dd19d6c", "#8a8985")] + [(v, name, color) for v, _, name, color in SITTINGS]
    series = [{"name": name, "color": color,
               "values": [value(variant, f"{prefix}-{shape}-{cell}") for cell, _ in CELLS for shape in SHAPES]}
              for variant, name, color in variants]
    bars(path, {"title": title,
                "subtitle": "callgrind, median of three runs interleaved with main; "
                            "every number is in the tables under the chart",
                "groups": groups, "series": series, "top": top, "ticks": ticks, "digits": digits, "small": True})


chart("heads", f"{NAME}.svg", "Instructions one commit of 500 move commands retires, in millions", 4000, 4, 0)
chart("heads-each", f"{NAME}-one-command.svg", "Instructions one commit of one move command retires, in millions", 12, 4, 2)

print(f"widest spread between runs of one scenario: {spread():.2f}%\n")
print("main in its three sittings, 500 a commit, `repeat` 4 reads 4 heads: "
      + ", ".join(f"{value(m, 'heads-repeat-heavy-4'):.1f}" for m in ("main", "main2", "main3")) + "\n")
print("## 500\n")
table("heads", 1)
print()
against("heads")
print("\n## one\n")
table("heads-each", 2)
print()
against("heads-each")
print("\n## net of plain\n")
print("| | body, heads | `plain` | `wide` | `named` | `repeat` | `repeat` over `wide` |")
print("|---|---|---|---|---|---|---|")
for cell, label in CELLS:
    for variant, name in [("main", "main")] + [(v, name) for v, _, name, _ in SITTINGS]:
        plain = value(variant, f"heads-each-plain-{cell[-1]}")
        w, n, r = (value(variant, f"heads-each-{shape}-{cell}") - plain for shape in SHAPES)
        print(f"| {name} | {label} | {plain:.2f} | {w:.2f} | {n:.2f} | {r:.2f} | {(r / w - 1) * 100:+.1f}% |")
