#!/usr/bin/env python3
"""Puts `tonk_bench` runs side by side, as Markdown with charts.

    report.py <runs dir> [<charts dir>] > report.md

With a charts directory, it also writes `time.svg` (every store's time
against the first, as grouped bars with a legend) and `storage.svg`.

<runs dir> holds `<store>-<n>.json`, one per run (or `<build>-<n>.json`,
reported as `<store> <build>`, to set builds of one store side by side), each the `report.json` a
run of `tonk_bench` wrote. Runs of one store against one dataset are
summarized together: a measure's value for a run is the median of its
samples, and the report gives the median of those across runs and their
spread (lowest to highest run), so a difference smaller than the spread
reads as noise. Every run must name the same dataset.

Each chart is a Mermaid bar chart, one series: the stores along the x axis.
Its numbers are in the table under it.
"""

import json
import statistics
import sys
from collections import defaultdict
from pathlib import Path

# The measures each axis charts, with the unit they are given in.
AXES = [
    ("Queries, cold (a replica that just joined)", "ms", [
        "query/v2/issue/cold", "query/v2/activity/cold", "query/issue-detail/cold",
        "query/memo/full/cold", "query/v2/issue-labels/cold",
    ]),
    ("Queries, warm", "ms", [
        "query/v2/issue/warm", "query/v2/activity/warm", "query/issue-detail/warm",
        "query/memo/full/warm", "query/v2/issue-labels/warm",
    ]),
    ("Writes: one command a commit, its rules included", "ms", [
        "command/all", "command/first-after-pull", "command/v2/issue-create",
        "command/memo/post", "command/v2/issue-post-comment", "command/v2/issue-set-status",
    ]),
    ("Sync", "ms", ["join/pull", "sync/pull", "sync/push", "setup/push"]),
    ("Subscriptions: a poll after each of the reader's pulls", "ms", [
        "subscription/v2/issue/poll", "subscription/v2/activity/poll",
        "subscription/memo/full/poll",
    ]),
]

SIZES = ["setup/owner-bytes", "bytes/member-0", "bytes/member-1", "bytes/remote"]

# The measures charted, one chart each.
CHARTS = [
    ("query/v2/issue/cold", "ms"), ("query/v2/issue/warm", "ms"),
    ("command/all", "ms"), ("command/first-after-pull", "ms"),
    ("sync/pull", "ms"), ("sync/push", "ms"),
    ("subscription/v2/issue/poll", "ms"), ("bytes/member-1", "MiB"),
]

ORDER = ["main/fs", "fs", "file"]


def load(directory):
    runs = defaultdict(list)
    datasets = set()
    for path in sorted(Path(directory).glob("*.json")):
        report = json.loads(path.read_text())
        datasets.add(report["dataset"])
        store = report["store"]
        if report.get("build") == "main":
            store = f"main/{store}"
        # `v9-1.json`, a run of the store `file`, is the build `v9`'s file.
        name = path.stem.rsplit("-", 1)[0]
        if name not in (report["store"], store):
            store = f"{store} {name}"
        runs[store].append(report["measures"])
    if len(datasets) != 1:
        sys.exit(f"runs name {len(datasets)} datasets: {sorted(datasets)}")
    stores = sorted(runs, key=lambda s: (ORDER.index(s) if s in ORDER else len(ORDER), s))
    return datasets.pop(), stores, runs


def per_run(runs, measure):
    """The measure's value in each run: its median sample, or its count."""
    values = []
    for run in runs:
        if measure in run["timings"]:
            values.append(statistics.median(run["timings"][measure]))
        elif measure in run["counts"]:
            values.append(run["counts"][measure])
    return values


def cell(values, scale=1.0, digits=2):
    if not values:
        return "–"
    middle = statistics.median(values) / scale
    if len(values) == 1:
        return f"{middle:.{digits}f}"
    low, high = min(values) / scale, max(values) / scale
    return f"{middle:.{digits}f} ({low:.{digits}f}–{high:.{digits}f})"


def table(stores, runs, measures, unit, scale=1.0, digits=2):
    base = stores[0]
    lines = ["| measure | " + " | ".join(stores) + f" | {stores[-1]} ÷ {base} |",
             "|---|" + "---|" * (len(stores) + 1)]
    for measure in measures:
        row = [per_run(runs[store], measure) for store in stores]
        if not any(row):
            continue
        ratio = "–"
        if row[0] and row[-1]:
            ratio = f"{statistics.median(row[-1]) / statistics.median(row[0]):.2f}"
        lines.append(f"| `{measure}` ({unit}) | "
                     + " | ".join(cell(values, scale, digits) for values in row)
                     + f" | {ratio} |")
    return "\n".join(lines)


def chart(stores, runs, measure, unit):
    scale = 2**20 if unit == "MiB" else 1.0
    values = [statistics.median(per_run(runs[s], measure) or [0]) / scale for s in stores]
    top = max(values) * 1.15 or 1
    labels = ", ".join(f'"{s}"' for s in stores)
    bars = ", ".join(f"{v:.2f}" for v in values)
    return (f"```mermaid\nxychart-beta\n  title \"{measure} ({unit})\"\n"
            f"  x-axis [{labels}]\n  y-axis \"{unit}\" 0 --> {top:.2f}\n  bar [{bars}]\n```")


# The measures the SVG overview draws, each as a ratio to the first store.
OVERVIEW = [
    ("v2/issue cold", "query/v2/issue/cold"),
    ("v2/issue warm", "query/v2/issue/warm"),
    ("memo/full cold", "query/memo/full/cold"),
    ("issue detail cold", "query/issue-detail/cold"),
    ("command", "command/all"),
    ("command after pull", "command/first-after-pull"),
    ("pull", "sync/pull"),
    ("push", "sync/push"),
    ("v2/issue poll", "subscription/v2/issue/poll"),
]

COLORS = ["#6b7280", "#2563eb", "#d97706", "#059669", "#db2777"]


def svg_bars(title, subtitle, groups, stores, values, unit, reference=None, labels=False):
    """Grouped bars: a group per label, a bar per store, with a legend.

    `values[g][s]` is (median, low, high) or None; the spread is drawn as a
    whisker. `reference` draws a dashed line at that value (1.0 for ratios).
    """
    width, height = 960, 460
    left, right, top, bottom = 70, 20, 70, 110
    plot_w, plot_h = width - left - right, height - top - bottom
    peak = max(high for row in values for v in row if v for high in [v[2]]) * 1.1
    step = plot_w / len(groups)
    bar = step * 0.8 / len(stores)
    out = [f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width} {height}" '
           f'font-family="system-ui, sans-serif" font-size="12">',
           f'<rect width="{width}" height="{height}" fill="#ffffff"/>',
           f'<text x="{left}" y="24" font-size="16" font-weight="600">{title}</text>',
           f'<text x="{left}" y="44" fill="#4b5563">{subtitle}</text>']
    for i in range(6):
        value = peak * i / 5
        y = top + plot_h - plot_h * value / peak
        out.append(f'<line x1="{left}" x2="{width - right}" y1="{y:.1f}" y2="{y:.1f}" stroke="#e5e7eb"/>')
        out.append(f'<text x="{left - 6}" y="{y + 4:.1f}" text-anchor="end" fill="#4b5563">{value:.2f}</text>')
    out.append(f'<text x="16" y="{top + plot_h / 2}" transform="rotate(-90 16 {top + plot_h / 2})" '
               f'text-anchor="middle" fill="#4b5563">{unit}</text>')
    if reference is not None:
        y = top + plot_h - plot_h * reference / peak
        out.append(f'<line x1="{left}" x2="{width - right}" y1="{y:.1f}" y2="{y:.1f}" '
                   f'stroke="#111827" stroke-dasharray="4 3"/>')
    for g, label in enumerate(groups):
        x0 = left + g * step + step * 0.1
        for s, store in enumerate(stores):
            v = values[g][s]
            if not v:
                continue
            middle, low, high = v
            x = x0 + s * bar
            y = top + plot_h - plot_h * middle / peak
            out.append(f'<rect x="{x:.1f}" y="{y:.1f}" width="{bar - 2:.1f}" '
                       f'height="{top + plot_h - y:.1f}" fill="{COLORS[s % len(COLORS)]}"/>')
            cx = x + (bar - 2) / 2
            y_low = top + plot_h - plot_h * low / peak
            y_high = top + plot_h - plot_h * high / peak
            out.append(f'<line x1="{cx:.1f}" x2="{cx:.1f}" y1="{y_low:.1f}" y2="{y_high:.1f}" stroke="#111827"/>')
            if labels:
                out.append(f'<text x="{cx:.1f}" y="{min(y, y_high) - 6:.1f}" text-anchor="middle" '
                           f'font-weight="600">{middle:.2f}</text>')
        cx = x0 + step * 0.4
        out.append(f'<text x="{cx:.1f}" y="{top + plot_h + 16}" text-anchor="end" '
                   f'transform="rotate(-30 {cx:.1f} {top + plot_h + 16})">{label}</text>')
    # Each legend entry as wide as its name (about 7 px a character at 12
    # px), right-aligned under the plot.
    widths = [18 + 7 * len(store) + 24 for store in stores]
    lx = width - right - sum(widths)
    for s, store in enumerate(stores):
        x = lx + sum(widths[:s])
        out.append(f'<rect x="{x}" y="{height - 24}" width="12" height="12" fill="{COLORS[s % len(COLORS)]}"/>')
        out.append(f'<text x="{x + 18}" y="{height - 14}">{store}</text>')
    out.append("</svg>")
    return "\n".join(out)


def overview(dataset, stores, runs):
    """Every overview measure, as each store's ratio to the first store."""
    base = stores[0]
    values = []
    for _, measure in OVERVIEW:
        reference = statistics.median(per_run(runs[base], measure) or [0]) or None
        row = []
        for store in stores:
            samples = per_run(runs[store], measure)
            if not samples or not reference:
                row.append(None)
                continue
            row.append((statistics.median(samples) / reference,
                        min(samples) / reference, max(samples) / reference))
        values.append(row)
    return svg_bars(
        f"Time against {base}, lower is better",
        f"dataset {dataset}; median of {len(runs[base])} runs, whiskers from the lowest to the highest run",
        [label for label, _ in OVERVIEW], stores, values, f"× {base}", reference=1.0)


def storage(dataset, stores, runs):
    measures = [("owner after setup", "setup/owner-bytes"),
                ("a member after the week", "bytes/member-1")]
    values = []
    for _, measure in measures:
        row = []
        for store in stores:
            samples = [v / 2**20 for v in per_run(runs[store], measure)]
            row.append((statistics.median(samples), min(samples), max(samples)) if samples else None)
        values.append(row)
    return svg_bars("A replica on disk", f"dataset {dataset}", [m for m, _ in measures],
                    stores, values, "MiB", labels=True)


def main():
    dataset, stores, runs = load(sys.argv[1])
    if len(sys.argv) > 2:
        charts = Path(sys.argv[2])
        charts.mkdir(parents=True, exist_ok=True)
        (charts / "time.svg").write_text(overview(dataset, stores, runs))
        (charts / "storage.svg").write_text(storage(dataset, stores, runs))
    counts = ", ".join(f"{s}: {len(runs[s])}" for s in stores)
    out = [f"Dataset `{dataset}`. Runs per store: {counts}. Each cell is the median "
           "across runs of each run's median, with the lowest and highest run in "
           "brackets.", ""]
    for title, unit, measures in AXES:
        out += [f"#### {title}", "", table(stores, runs, measures, unit), ""]
    out += ["#### Storage", "", table(stores, runs, SIZES, "MiB", 2**20), ""]
    out += ["#### Charts", ""]
    for measure, unit in CHARTS:
        out += [chart(stores, runs, measure, unit), ""]
    print("\n".join(out))


if __name__ == "__main__":
    main()
