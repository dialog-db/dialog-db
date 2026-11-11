"""Writes the guide's `file.md` from `chapter.md`, filling each `{{name}}`
with the figure `figs.py` draws under that name.

    python3 build.py path/to/dialog-db/guide/src/file.md
"""
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from figs import FIGS  # noqa: E402

out = Path(sys.argv[1] if len(sys.argv) > 1 else "file.md")
src = (Path(__file__).parent / "chapter.md").read_text()
used = set()


def sub(match):
    name = match.group(1)
    used.add(name)
    return FIGS[name]().render()


text = re.sub(r"\{\{(\w+)\}\}", sub, src)
missing = set(FIGS) - used
if missing:
    print("unused figures:", sorted(missing))
out.write_text(text)
print(len(used), "figures written to", out)
