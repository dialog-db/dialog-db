#!/usr/bin/env python3
"""Puts `tonk_bench` runs side by side, as Markdown with charts.

    report.py <runs dir> > report.md

<runs dir> holds `<store>-<n>.json`, one per run, each the `report.json` a
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


def main():
    dataset, stores, runs = load(sys.argv[1])
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
