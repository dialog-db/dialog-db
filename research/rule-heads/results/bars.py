"""Draw grouped bars as an SVG, with no dependency: `bars(path, spec)`.

`spec` holds `title`, `subtitle`, `groups` (the labels along the x axis),
`series` (each a `name`, a `color` and a value a group), `top` (the y
axis's end), `ticks` (how many gridlines) and, optionally, `over`: the
index of the series the others' percentages are over."""

INK, MUTED, GRID, SURFACE = "#0b0b0b", "#52514e", "#e4e3df", "#fcfcfb"


def bars(path, spec):
    series, groups = spec["series"], spec["groups"]
    W, LEFT, BOTTOM, RIGHT = 920, 64, 46, 20
    TOP = 64 + len(series) * 17 + 14
    H = TOP + 306 + BOTTOM
    plot_w, plot_h = W - LEFT - RIGHT, H - TOP - BOTTOM
    top, ticks = spec["top"], spec.get("ticks", 4)
    group = plot_w / len(groups)
    gap = 2
    bar = min(44, (group - 24) / len(series) - gap)
    out = [f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" width="{W}" height="{H}" '
           f'font-family="system-ui, -apple-system, Segoe UI, sans-serif">',
           f'<rect width="{W}" height="{H}" fill="{SURFACE}"/>',
           f'<text x="{LEFT}" y="26" font-size="16" font-weight="600" fill="{INK}">{spec["title"]}</text>',
           f'<text x="{LEFT}" y="46" font-size="12" fill="{MUTED}">{spec["subtitle"]}</text>']
    for at, each in enumerate(series):
        y = 64 + at * 17
        out.append(f'<rect x="{LEFT}" y="{y}" width="12" height="12" rx="2" fill="{each["color"]}"/>')
        out.append(f'<text x="{LEFT + 18}" y="{y + 10}" font-size="12" fill="{INK}">{each["name"]}</text>')
    for tick in range(0, ticks + 1):
        value = top * tick / ticks
        y = TOP + plot_h - tick / ticks * plot_h
        out.append(f'<line x1="{LEFT}" x2="{W - RIGHT}" y1="{y:.1f}" y2="{y:.1f}" stroke="{GRID}" stroke-width="1"/>')
        out.append(f'<text x="{LEFT - 8}" y="{y + 4:.1f}" font-size="11" text-anchor="end" fill="{MUTED}">{value:g}</text>')
    for g, label in enumerate(groups):
        x0 = LEFT + g * group + (group - len(series) * bar - (len(series) - 1) * gap) / 2
        for s, each in enumerate(series):
            value = each["values"][g]
            h = value / top * plot_h
            x, y = x0 + s * (bar + gap), TOP + plot_h - h
            out.append(f'<path d="M{x:.1f},{TOP + plot_h} V{y + 4:.1f} Q{x:.1f},{y:.1f} {x + 4:.1f},{y:.1f} '
                       f'H{x + bar - 4:.1f} Q{x + bar:.1f},{y:.1f} {x + bar:.1f},{y + 4:.1f} V{TOP + plot_h} Z" '
                       f'fill="{each["color"]}"><title>{each["name"]}, {label}: {value:.3f}</title></path>')
            out.append(f'<text x="{x + bar / 2:.1f}" y="{y - 6:.1f}" font-size="10" text-anchor="middle" '
                       f'fill="{INK}">{value:.2f}</text>')
            over = spec.get("over")
            if over is not None and s > over:
                base = series[over]["values"][g]
                out.append(f'<text x="{x + bar / 2:.1f}" y="{y - 18:.1f}" font-size="9" text-anchor="middle" '
                           f'fill="{MUTED}">{(value / base - 1) * 100:+.0f}%</text>')
        out.append(f'<text x="{LEFT + g * group + group / 2:.1f}" y="{H - 18}" font-size="12" '
                   f'text-anchor="middle" fill="{INK}">{label}</text>')
    out.append(f'<line x1="{LEFT}" x2="{W - RIGHT}" y1="{TOP + plot_h}" y2="{TOP + plot_h}" stroke="{MUTED}" stroke-width="1"/>')
    out.append("</svg>")
    open(path, "w").write("\n".join(out) + "\n")
