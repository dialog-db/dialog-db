"""Grouped bar charts (SVG, no plotting dependency) from the answers
sweep and tonk_bench reports, with every number also written as a
Markdown table.

    python3 charts.py <sweep-dir> <tonk-bench-dir> <out-dir>
"""
import glob, json, os, statistics, sys

SERIES = {"main": "#8a8f98", "eager": "#e0a458", "lazy": "#3d7dd8", "branch": "#3d7dd8"}


def bars(title, unit, groups, series, values, path, note=""):
    """groups: x labels; series: names; values[group][series] -> number."""
    width, height, left, bottom, top = 760, 360, 70, 70, 50
    plot_w, plot_h = width - left - 20, height - top - bottom
    peak = max(v for g in groups for v in values[g].values()) or 1
    slot = plot_w / len(groups)
    bar = slot * 0.8 / len(series)
    out = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
           f'font-family="system-ui, sans-serif" font-size="12">',
           f'<rect width="100%" height="100%" fill="#ffffff"/>',
           f'<text x="{left}" y="22" font-size="15" font-weight="600">{title}</text>',
           f'<text x="{left}" y="40" fill="#555">{note}</text>']
    for tick in range(5):
        y = top + plot_h - plot_h * tick / 4
        out.append(f'<line x1="{left}" x2="{left + plot_w}" y1="{y}" y2="{y}" stroke="#e5e5e5"/>')
        out.append(f'<text x="{left - 6}" y="{y + 4}" text-anchor="end" fill="#555">{peak * tick / 4:,.0f}</text>')
    out.append(f'<text x="14" y="{top + plot_h / 2}" transform="rotate(-90 14 {top + plot_h / 2})" text-anchor="middle" fill="#555">{unit}</text>')
    for at, group in enumerate(groups):
        x0 = left + slot * at + slot * 0.1
        for index, name in enumerate(series):
            value = values[group].get(name, 0)
            h = plot_h * value / peak
            x = x0 + bar * index
            out.append(f'<rect x="{x:.1f}" y="{top + plot_h - h:.1f}" width="{bar - 2:.1f}" height="{h:.1f}" fill="{SERIES.get(name, "#999")}"/>')
        out.append(f'<text x="{left + slot * at + slot / 2:.1f}" y="{top + plot_h + 16}" text-anchor="middle">{group}</text>')
    for index, name in enumerate(series):
        x = left + index * 110
        out.append(f'<rect x="{x}" y="{height - 26}" width="12" height="12" fill="{SERIES.get(name, "#999")}"/>')
        out.append(f'<text x="{x + 18}" y="{height - 16}">{name}</text>')
    out.append("</svg>")
    open(path, "w").write("\n".join(out))
    table = [f"| {unit} | " + " | ".join(series) + " |", "|---|" + "---|" * len(series)]
    for group in groups:
        table.append(f"| {group} | " + " | ".join(f"{values[group].get(s, 0):,.1f}" for s in series) + " |")
    return "\n".join(table)


def main(sweep, bench, out):
    os.makedirs(out, exist_ok=True)
    reports = {}
    for path in glob.glob(f"{sweep}/*.json"):
        d = json.load(open(path))
        reports[os.path.basename(path)[:-5]] = d
    datasets = sorted({k.rsplit("-", 1)[0] for k in reports}, key=lambda k: (k.split("-")[0], int(k.rsplit("-", 1)[1])))
    variants = ["main", "eager", "lazy"]
    md = []

    def per_row(key, phase, what):
        d = reports[key]
        if what == "instructions":
            return d["instructions"].get(phase, 0) / d["rows"]
        if what == "allocations":
            return d["phases"][phase]["allocations"].get("alloc.calls", 0) / d["rows"]
        return statistics.median(d["phases"][phase]["micros"]) / 1000

    for what, unit in [("instructions", "instructions per row"), ("allocations", "allocations per row"), ("wall", "ms")]:
        values = {ds: {v: per_row(f"{ds}-{v}", "engine", what) for v in variants if f"{ds}-{v}" in reports} for ds in datasets}
        md.append(f"### Engine, {unit}\n\n![](engine-{what}.png)\n\n" + bars(f"Engine (query to conclusions), {unit}", unit, datasets, variants, values, f"{out}/engine-{what}.svg", "answers harness; main = 04965aa, branch eager and lazy, left merge on") + "\n")

    # Whole path: engine + encode + decode, JSON (tonk) against the frame.
    values = {}
    for ds in datasets:
        main_key, lazy_key = f"{ds}-main", f"{ds}-lazy"
        if main_key not in reports or lazy_key not in reports:
            continue
        m, l = reports[main_key], reports[lazy_key]
        n = m["rows"]
        values[ds] = {
            "main + JSON": (m["instructions"]["engine"] + m["instructions"]["json_encode"] + m["instructions"]["json_decode"]) / n,
            "branch + JSON": (l["instructions"]["engine"] + l["instructions"]["json_encode"] + l["instructions"]["json_decode"]) / n,
            "branch + frame": (l["instructions"]["engine"] + l["instructions"]["frame_encode"] + l["instructions"]["frame_read_two"]) / n,
        }
    SERIES.update({"main + JSON": "#8a8f98", "branch + JSON": "#e0a458", "branch + frame": "#3d7dd8"})
    md.append("### Query to a receiver holding the answer, instructions per row\n\n![](path-instructions.png)\n\n" + bars("Query to receiver, instructions per row", "instructions per row", list(values), ["main + JSON", "branch + JSON", "branch + frame"], values, f"{out}/path-instructions.svg", "engine + encode + decode (frame: reading two fields)") + "\n")

    # tonk_bench, 10x
    pools = {}
    for path in glob.glob(f"{bench}/10x-*-*.json"):
        variant = os.path.basename(path).split("-")[1]
        for key, samples in json.load(open(path))["measures"]["timings"].items():
            pools.setdefault(key, {}).setdefault(variant, []).extend(samples if isinstance(samples, list) else [samples])
    keys = ["query/v2/issue/warm", "query/v2/issue/cold", "query/v2/activity/warm", "subscription/v2/issue/first", "subscription/v2/issue/poll", "subscription/v2/activity/poll"]
    names = {k: k.replace("query/", "q ").replace("subscription/", "sub ").replace("v2/", "") for k in keys}
    values = {names[k]: {("branch" if v == "lm" else v): statistics.median(pools[k][v]) for v in pools.get(k, {})} for k in keys if k in pools}
    md.append("### tonk_bench at 10×, ms\n\n![](tonk-10x.png)\n\n" + bars("tonk_bench, tonk-team at 10×, median ms", "ms", list(values), ["main", "branch"], values, f"{out}/tonk-10x.svg", "fs store; branch = lazy bindings + left merge") + "\n")
    open(f"{out}/charts.md", "w").write("\n".join(md))


if __name__ == "__main__":
    main(*sys.argv[1:4])
