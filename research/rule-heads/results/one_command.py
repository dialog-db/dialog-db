"""Draw `2026-11-12-one-command.svg` from its `.json`: what one commit of
one command retires, a group a cell. Run as `python3 one_command.py`."""
import json

from bars import bars

NAME = "2026-11-12-one-command"
data = json.load(open(f"{NAME}.json"))["scenarios"]
CELLS = [("light", 2, "1 read, 2 heads"), ("light", 4, "1 read, 4 heads"),
         ("heavy", 2, "4 reads, 2 heads"), ("heavy", 4, "4 reads, 4 heads")]


def commit(scenario):
    entry = data[scenario]
    return sorted(entry["instructions"])[1] / entry["size"] / 1e6


def cells(shape):
    if shape == "plain":
        return [commit(f"heads-each-plain-{heads}") for _, heads, _ in CELLS]
    return [commit(f"heads-each-{shape}-{body}-{heads}") for body, heads, _ in CELLS]


bars(f"{NAME}.svg", {
    "title": "Instructions one commit of one move command retires, in millions",
    "subtitle": "main dd19d6c, callgrind, 100 commits over 500 to-dos, median of three runs; "
                "the percentage is over the wide head",
    "groups": [label for _, _, label in CELLS],
    "series": [
        {"name": "no rule: the transaction asserts the facts itself", "color": "#8a8985", "values": cells("plain")},
        {"name": "one rule, one wide head (stands in for several heads)", "color": "#2a78d6", "values": cells("wide")},
        {"name": "a named condition: one rule issues a command, a rule a head reads it", "color": "#eb6834",
         "values": cells("named")},
        {"name": "a rule a head, the body written again", "color": "#1baf7a", "values": cells("repeat")},
    ],
    "top": 12, "ticks": 4, "over": 1,
})
