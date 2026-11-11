"""Grouped bar charts, as SVG with a legend, of the 2026-11-11 spike's
numbers (the tables in results/2026-11-11-spike.md). No plotting library."""
import sys, pathlib

COLORS = ['#4e79a7', '#f28e2b', '#59a14f']

def chart(path, title, groups, series, values, unit, reference=None):
    w, h, left, top, bottom = 760, 380, 60, 60, 70
    plot_w, plot_h = w - left - 20, h - top - bottom
    peak = max(v for row in values for v in row if v is not None) * 1.15
    gw = plot_w / len(groups)
    bw = gw * 0.8 / len(series)
    out = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{w}" height="{h}" font-family="sans-serif" font-size="12">',
           f'<rect width="{w}" height="{h}" fill="white"/>',
           f'<text x="{w/2}" y="22" text-anchor="middle" font-size="15">{title}</text>']
    for i in range(6):
        v = peak * i / 5; y = top + plot_h - plot_h * v / peak
        out.append(f'<line x1="{left}" x2="{w-20}" y1="{y:.1f}" y2="{y:.1f}" stroke="#ddd"/>')
        out.append(f'<text x="{left-6}" y="{y+4:.1f}" text-anchor="end">{v:.2f}</text>' if peak < 10 else f'<text x="{left-6}" y="{y+4:.1f}" text-anchor="end">{v:.0f}</text>')
    if reference is not None:
        y = top + plot_h - plot_h * reference / peak
        out.append(f'<line x1="{left}" x2="{w-20}" y1="{y:.1f}" y2="{y:.1f}" stroke="#333" stroke-dasharray="5,4"/>')
        out.append(f'<text x="{left+4}" y="{y-5:.1f}" text-anchor="start">dag-cbor</text>')
    for g, name in enumerate(groups):
        x0 = left + g * gw + gw * 0.1
        for s in range(len(series)):
            v = values[g][s]
            if v is None:
                continue
            bh = plot_h * v / peak; x = x0 + s * bw
            out.append(f'<rect x="{x:.1f}" y="{top+plot_h-bh:.1f}" width="{bw-2:.1f}" height="{bh:.1f}" fill="{COLORS[s]}"/>')
            out.append(f'<text x="{x+bw/2-1:.1f}" y="{top+plot_h-bh-4:.1f}" text-anchor="middle" font-size="11">{v:g}</text>')
        out.append(f'<text x="{left+g*gw+gw/2:.1f}" y="{top+plot_h+18}" text-anchor="middle">{name}</text>')
    for s, name in enumerate(series):
        x = left + s * 230
        out.append(f'<rect x="{x}" y="{h-30}" width="12" height="12" fill="{COLORS[s]}"/>')
        out.append(f'<text x="{x+18}" y="{h-20}">{name}</text>')
    out.append(f'<text x="14" y="{top+plot_h/2}" transform="rotate(-90 14 {top+plot_h/2})" text-anchor="middle">{unit}</text>')
    out.append('</svg>')
    pathlib.Path(path).write_text('\n'.join(out))

out = pathlib.Path(sys.argv[1]); out.mkdir(parents=True, exist_ok=True)
chart(out / 'size.svg', 'Pairs ÷ dag-cbor (revisions-4000-11, tonk-bindings)',
      ['revisions, versions as records', 'revisions, versions as bytes', 'tonk view bindings'],
      ['on the wire', 'on the wire, gzip over 64', 'in the file, split (model)'],
      [[1.44, 1.05, 0.40], [1.25, 1.02, 0.47], [1.89, 1.00, 1.26]], '÷ dag-cbor', reference=1.0)
chart(out / 'reads.svg', 'Reading one field, median ns a record (7 rounds)',
      ['parents', "parents' editions", 'issuer', 'event names'],
      ['dag-cbor, decoded whole', 'pairs, versions as bytes', 'pairs (versions as records)'],
      [[745, 383, 612], [862, None, 285], [855, None, 697], [1578, None, 802]], 'ns')
