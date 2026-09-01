#!/usr/bin/env python3
"""Compare two soak sweep directories (see scripts/soak.sh) and flag regressions.

Two noise sources shape the gate. The harness randomizes identities per
run, so tree shape — and with it which phase pays for which shared leaf —
wobbles a block or two between runs. And on *shaped* (delayed) profiles the
latency itself inflates request counts nondeterministically: while a block
fetch is in flight, concurrent misses for it each fetch again (the node
cache deliberately has no single-flight), so slower links produce a few
duplicate fetches. The `none` profile has no delays, which makes its
request counts deterministic protocol shape.

The gate therefore holds the `none` report tight (threshold, slack 2 — a
new round trip on `pull`/`probe`/`claim`/`requery` is a protocol
regression) and the shaped reports loose (2.5x threshold on requests, 2x
on bytes and modeled time, over lazy-join and download totals). Costs
migrating between phases cancel in the totals; real growth does not.

Exits non-zero when anything regressed, so the script can gate CI or a
recurring soak job.

Usage:
    scripts/soak-compare.py BASELINE_DIR NEW_DIR [--threshold PCT]
"""

import argparse
import glob
import json
import os
import sys


def load(directory):
    reports = {}
    for path in glob.glob(os.path.join(directory, "*.json")):
        with open(path) as f:
            reports[os.path.basename(path)] = json.load(f)
    return reports


def totals(report, download):
    picked = [p for p in report["phases"] if (p["name"] == "download") == download]
    return {
        "requests": sum(p["traffic"]["requests"] for p in picked),
        "bytes": sum(p["traffic"]["bytes"] for p in picked),
        "virtual_ms": sum(p["virtual_ms"] for p in picked),
    }


def regressed(base_value, new_value, slack, threshold_pct):
    if new_value - base_value <= slack:
        return False
    if base_value == 0:
        return True
    return (new_value - base_value) / base_value * 100.0 > threshold_pct


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("baseline")
    parser.add_argument("new")
    parser.add_argument("--threshold", type=float, default=10.0,
                        help="percent growth in a total that counts as a regression")
    args = parser.parse_args()

    baseline = load(args.baseline)
    new = load(args.new)
    shared = sorted(set(baseline) & set(new))
    if not shared:
        print("no overlapping reports to compare", file=sys.stderr)
        return 2

    regressions = []
    for name in shared:
        base_phases = {p["name"]: p for p in baseline[name]["phases"]}
        new_phases = {p["name"]: p for p in new[name]["phases"]}
        shaped = new[name].get("network") != "none"

        request_slack = 3 if shaped else 2
        request_threshold = args.threshold * (2.5 if shaped else 1.0)
        for phase_name in base_phases.keys() & new_phases.keys():
            base_reqs = base_phases[phase_name]["traffic"]["requests"]
            new_reqs = new_phases[phase_name]["traffic"]["requests"]
            if regressed(base_reqs, new_reqs, request_slack, request_threshold):
                regressions.append(
                    f"{name}:{phase_name}: requests {base_reqs} -> {new_reqs}")

        for scope, download in (("lazy-join", False), ("download", True)):
            base_total = totals(baseline[name], download)
            new_total = totals(new[name], download)
            for metric, slack, threshold in (
                ("requests", request_slack, request_threshold),
                ("bytes", 64 * 1024, args.threshold * (2.0 if shaped else 1.0)),
                ("virtual_ms", 100, args.threshold * (2.0 if shaped else 1.0)),
            ):
                if regressed(base_total[metric], new_total[metric], slack,
                             threshold):
                    growth = ((new_total[metric] - base_total[metric])
                              / base_total[metric] * 100.0
                              if base_total[metric] else float("inf"))
                    regressions.append(
                        f"{name}:{scope}: {metric} {base_total[metric]} -> "
                        f"{new_total[metric]} (+{growth:.1f}%)")

    for name in shared:
        base_lazy = totals(baseline[name], download=False)["virtual_ms"]
        new_lazy = totals(new[name], download=False)["virtual_ms"]
        base_dl = totals(baseline[name], download=True)["virtual_ms"]
        new_dl = totals(new[name], download=True)["virtual_ms"]
        print(f"{name}: lazy join {base_lazy} ms -> {new_lazy} ms | "
              f"download {base_dl} ms -> {new_dl} ms")

    if regressions:
        print(f"\n{len(regressions)} regression(s) over {args.threshold:.0f}% threshold:")
        for line in regressions:
            print(f"  {line}")
        return 1
    print("\nno regressions")
    return 0


if __name__ == "__main__":
    sys.exit(main())
