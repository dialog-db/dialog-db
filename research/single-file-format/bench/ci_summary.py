#!/usr/bin/env python3
"""Tables for the "File archive against main" workflow.

    ci_summary.py <runs dir>

<runs dir> holds one directory per setting (`1x-disk`, `1x-shm`,
`10x-disk`), each with `main-<n>.err` and `branch-<n>.err`, the stderr of
`tonk_bench` runs. A measure's value for a run is the median the bench
printed; a cell is the median across runs, with the lowest and highest run
in brackets when there are several.
"""

import re
import statistics
import sys
from collections import defaultdict
from pathlib import Path

LINE = re.compile(r"^(\S+)\s+n=\d+\s+median\s+([\d.]+) ms\s+p95\s+([\d.]+) ms")
COUNT = re.compile(r"^((?:bytes|setup)/\S+-bytes|bytes/\S+)\s+(\d+)$")

MEASURES = [
    ("warm command", "steady/all"),
    ("warm command, p95", "steady/all#p95"),
    ("warm `v2/issue-create`", "steady/v2/issue-create"),
    ("a command in the week", "command/all"),
    ("first command after a pull", "command/first-after-pull"),
    ("the owner's setup", "setup/build"),
    ("`v2/issue`, cold", "query/v2/issue/cold"),
    ("`v2/issue`, warm", "query/v2/issue/warm"),
    ("`v2/activity`, cold", "query/v2/activity/cold"),
    ("`v2/activity`, warm", "query/v2/activity/warm"),
    ("`v2/issue` poll after a pull", "subscription/v2/issue/poll"),
    ("pull, in a round", "sync/pull"),
    ("push, in a round", "sync/push"),
    ("the owner's first push", "setup/push"),
    ("a member joining", "join/pull"),
    ("a member's replica (MiB)", "bytes/member-1"),
    ("the owner's replica after setup (MiB)", "setup/owner-bytes"),
]


def load(directory):
    runs = defaultdict(lambda: defaultdict(list))
    for err in sorted(directory.glob("*-*.err")):
        build = err.stem.rsplit("-", 1)[0]
        for line in err.read_text().splitlines():
            if match := LINE.match(line):
                runs[match[1]][build].append(float(match[2]))
                runs[match[1] + "#p95"][build].append(float(match[3]))
            elif match := COUNT.match(line.strip()):
                runs[match[1]][build].append(int(match[2]) / 2**20)
    return runs


def cell(values):
    if not values:
        return "-"
    middle = statistics.median(values)
    if len(values) == 1:
        return f"{middle:,.1f}"
    return f"{middle:,.1f} ({min(values):,.1f}–{max(values):,.1f})"


def table(title, runs):
    print(f"### {title}\n")
    print("| | main | branch | branch ÷ main |")
    print("|---|--:|--:|--:|")
    for label, key in MEASURES:
        if key not in runs:
            continue
        main, branch = runs[key].get("main", []), runs[key].get("branch", [])
        ratio = "-"
        if main and branch and statistics.median(main):
            ratio = f"{statistics.median(branch) / statistics.median(main):.2f}"
        print(f"| {label} | {cell(main)} | {cell(branch)} | {ratio} |")
    print()


def main():
    root = Path(sys.argv[1])
    titles = {
        "1x-disk": "tonk-shaped 1×, on the runner's disk (ms)",
        "1x-shm": "tonk-shaped 1×, on /dev/shm (ms)",
        "10x-disk": "tonk-shaped 10×, setup and views, on the runner's disk (ms)",
    }
    for name, title in titles.items():
        if (root / name).is_dir():
            table(title, load(root / name))


if __name__ == "__main__":
    main()
