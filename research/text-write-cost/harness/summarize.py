"""Tabulate run.sh output: one row per (variant, flush, sync).

    python3 -I research/text-write-cost/harness/summarize.py <out.jsonl>...
"""

import json
import sys


def kb(n):
    return f"{n / 1000:,.1f}" if n is not None else "-"


def main(paths):
    rows = []
    for path in paths:
        with open(path) as f:
            rows += [json.loads(line) for line in f if line.strip()]
    print("| trace | variant | flush | commits | written KB | x raw | live KB | x raw | sync KB | x raw | ms/commit (mean / p99) |")
    print("|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|")
    for row in rows:
        raw = row["raw_bytes"]
        r = row["result"]
        if not r:
            print(f"| {row['trace']} | raw | - | - | {kb(raw)} | 1.0 | {kb(raw)} | 1.0 | - | - | - |")
            continue
        if "written" in r:
            written = r["written"]["bytes"]
        else:
            written = sum(w["bytes"] for w in r["written_per_replica"])
        live = r["live"]["bytes"]
        sync = r.get("sync", {}).get("bytes")
        timing = r["timing"]
        variant = row["variant"] + (" (sync)" if row["sync"] else "")
        print(
            f"| {row['trace']} | {variant} | {row['flush']} | {timing['commits']} "
            f"| {kb(written)} | {written / raw:.1f} | {kb(live)} | {live / raw:.1f} "
            f"| {kb(sync)} | {'-' if sync is None else f'{sync / raw:.1f}'} "
            f"| {timing['mean_ms']:.2f} / {timing['p99_ms']:.2f} |"
        )


if __name__ == "__main__":
    main(sys.argv[1:])
