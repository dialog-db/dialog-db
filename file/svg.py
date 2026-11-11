"""Helpers drawing the guide's figures with its own classes."""

from html import escape

MONO = 7.8  # px per character, 13px monospace
SMALL = 6.6  # px per character, 11px monospace


def esc(s):
    return escape(str(s), quote=True)


class Fig:
    def __init__(self, w, h, label, prefix):
        self.w, self.h, self.label, self.p = w, h, label, prefix
        self.items = []

    def rect(self, x, y, w, h, cls="", rx=None):
        c = f' class="{cls}"' if cls else ""
        r = f' rx="{rx}"' if rx else ""
        self.items.append(f'<rect{c} x="{x:g}" y="{y:g}" width="{w:g}" height="{h:g}"{r}/>')

    def circle(self, x, y, r, cls=""):
        c = f' class="{cls}"' if cls else ""
        self.items.append(f'<circle{c} cx="{x:g}" cy="{y:g}" r="{r:g}"/>')

    def poly(self, points, cls=""):
        c = f' class="{cls}"' if cls else ""
        pts = " ".join(f"{x:g},{y:g}" for x, y in points)
        self.items.append(f'<polygon{c} points="{pts}"/>')

    def text(self, x, y, s, cls="", anchor="start", style=None):
        c = f' class="{cls}"' if cls else ""
        a = f' text-anchor="{anchor}"' if anchor != "start" else ""
        st = f' style="{style}"' if style else ""
        self.items.append(f'<text{c} x="{x:g}" y="{y:g}"{a}{st}>{esc(s)}</text>')

    def line(self, x1, y1, x2, y2, cls="", arrow=False):
        c = f' class="{cls}"' if cls else ""
        m = f' marker-end="url(#{self.p}-arrow)"' if arrow else ""
        self.items.append(f'<line{c} x1="{x1:g}" y1="{y1:g}" x2="{x2:g}" y2="{y2:g}"{m}/>')

    def path(self, d, cls="", arrow=False):
        c = f'wire {cls}'.strip()
        m = f' marker-end="url(#{self.p}-arrow)"' if arrow else ""
        self.items.append(f'<path class="{c}" d="{d}"{m}/>')

    def render(self):
        defs = (
            f'<defs><marker id="{self.p}-arrow" viewBox="0 0 8 8" refX="7" refY="4" '
            f'markerWidth="7" markerHeight="7" orient="auto"><path d="M0,0 L8,4 L0,8 z"/>'
            f"</marker></defs>"
        )
        body = "\n".join(self.items)
        return (
            f'<figure class="dg">\n<svg class="dg" viewBox="0 0 {self.w} {self.h}" '
            f'width="{self.w}" height="{self.h}" xmlns="http://www.w3.org/2000/svg" role="img" '
            f'aria-label="{esc(self.label)}">\n{defs}\n{body}\n</svg>\n</figure>'
        )

    # Compound shapes.

    def box(self, x, y, w, h, cls, title, sub=None, tcls=None, scls="small muted"):
        """A box with a centered title and an optional line below it."""
        self.rect(x, y, w, h, cls)
        tc = tcls if tcls is not None else (cls.split()[0] if cls and cls.split()[0] in ROLES else "")
        if sub is None:
            self.text(x + w / 2, y + h / 2 + 4.5, title, tc, "middle")
        else:
            self.text(x + w / 2, y + h / 2 - 2, title, tc, "middle")
            self.text(x + w / 2, y + h / 2 + 12, sub, scls, "middle")

    def bytes_row(self, x, y, hexes, cls="", cw=24, h=24, tcls=None):
        """One box per byte, its hex inside. Returns the x after."""
        for i, b in enumerate(hexes):
            self.rect(x + i * cw, y, cw, h, cls)
            tc = tcls if tcls is not None else (cls if cls in ROLES else "")
            self.text(x + i * cw + cw / 2, y + h / 2 + 4.5, b, tc, "middle")
        return x + len(hexes) * cw

    def brace(self, x1, x2, y, label, cls="small muted", up=False):
        """A bracket under (or over) x1..x2 with a label."""
        d = -6 if up else 6
        self.path(f"M{x1:g},{y:g} L{x1:g},{y + d:g} L{x2:g},{y + d:g} L{x2:g},{y:g}")
        ty = y + d + (-6 if up else 14)
        est = len(label) * (SMALL if "small" in cls else MONO)
        cx = (x1 + x2) / 2
        if cx - est / 2 < 4:
            self.text(max(x1, 4), ty, label, cls)
        elif cx + est / 2 > self.w - 4:
            self.text(min(x2, self.w - 4), ty, label, cls, "end")
        else:
            self.text(cx, ty, label, cls, "middle")


ROLES = {"entity", "attribute", "value", "hash", "critical", "cause", "alice", "bob"}


def hx(s):
    """'44 49 41' -> ['44', '49', '41']"""
    return s.split()
