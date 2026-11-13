"""Draw the charts of `2026-11-12-induce-cost.json` and print its tables.
Run as `python3 induce_cost.py`."""
import json

from bars import bars

NAME = "2026-11-12-induce-cost"
data = json.load(open(f"{NAME}.json"))["variants"]
VARIANTS = [("off", "as it is", "#8a8985"), ("plan", "plan: a rule planned once", "#c9a227"),
            ("subject", "subject: a seed binds its entity", "#2a78d6"),
            ("share", "share: one body evaluated once", "#1baf7a"),
            ("all", "plan, subject and share together", "#eb6834")]
CELLS = [("light-2", "1 read, 2 heads"), ("heavy-4", "4 reads, 4 heads")]
SHAPES = ["wide", "named", "repeat"]


def value(variant, scenario):
    entry = data[variant].get(scenario)
    if not entry:
        return None
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


def table(prefix, digits):
    print("| | body, heads | `wide` | `named` | `repeat` | `named` over `wide` | `repeat` over `wide` |")
    print("|---|---|---|---|---|---|---|")
    for cell, label in CELLS:
        for variant, _, _ in VARIANTS:
            w, n, r = (value(variant, f"{prefix}-{shape}-{cell}") for shape in SHAPES)
            name = "as it is" if variant == "off" else "all three" if variant == "all" else f"`{variant}`"
            print(f"| {name} | {label} | {w:.{digits}f} | {n:.{digits}f} | {r:.{digits}f} "
                  f"| {(n / w - 1) * 100:+.1f}% | {(r / w - 1) * 100:+.1f}% |")


def against(prefix):
    print("| | body, heads | `wide` | `named` | `repeat` |")
    print("|---|---|---|---|---|")
    for cell, label in CELLS:
        for variant, _, _ in VARIANTS[1:]:
            cells = []
            for shape in SHAPES:
                off, on = value("off", f"{prefix}-{shape}-{cell}"), value(variant, f"{prefix}-{shape}-{cell}")
                cells.append(f"{(on / off - 1) * 100:+.1f}%")
            name = "all three" if variant == "all" else f"`{variant}`"
            print(f"| {name} | {label} | " + " | ".join(cells) + " |")


def chart(prefix, path, title, top, ticks, digits):
    groups = [f"{shape}|{label}" for _, label in CELLS for shape in SHAPES]
    series = [{"name": name, "color": color,
               "values": [value(variant, f"{prefix}-{shape}-{cell}") for cell, _ in CELLS for shape in SHAPES]}
              for variant, name, color in VARIANTS]
    bars(path, {"title": title,
                "subtitle": "spike/induce-cost e13e838, one build, callgrind, median of three runs; "
                            "every number is in the tables under the chart",
                "groups": groups, "series": series, "top": top, "ticks": ticks, "digits": digits, "small": True})


chart("heads", f"{NAME}.svg", "Instructions one commit of 500 move commands retires, in millions", 4000, 4, 0)
chart("heads-each", f"{NAME}-one-command.svg", "Instructions one commit of one move command retires, in millions", 12, 4, 2)

print(f"widest spread between runs of one scenario: {spread():.2f}%\n")
print("## 500\n")
table("heads", 1)
print()
against("heads")
print("\n## one\n")
table("heads-each", 2)
print()
against("heads-each")
print("\n## net of plain\n")
print("| | body, heads | `plain` | `wide` | `named` | `repeat` | `named` over `wide` | `repeat` over `wide` |")
print("|---|---|---|---|---|---|---|---|")
for cell, label in CELLS:
    for variant in ("off", "all"):
        plain = value(variant, f"heads-each-plain-{cell[-1]}")
        w, n, r = (value(variant, f"heads-each-{shape}-{cell}") - plain for shape in SHAPES)
        name = "as it is" if variant == "off" else "all three"
        print(f"| {name} | {label} | {plain:.2f} | {w:.2f} | {n:.2f} | {r:.2f} "
              f"| {(n / w - 1) * 100:+.1f}% | {(r / w - 1) * 100:+.1f}% |")
print("\n## entity\n")
print("| body, heads | shape | as it is | `entity` | |")
print("|---|---|---|---|---|")
for cell, label in CELLS:
    for shape in SHAPES:
        off, on = value("off", f"heads-each-{shape}-{cell}"), value("entity", f"heads-each-{shape}-{cell}")
        if on is not None:
            print(f"| {label} | `{shape}` | {off:.3f} | {on:.3f} | {(on / off - 1) * 100:+.1f}% |")
