"""Census of the syntax trees the notation keeps for its .clj documents.

Run: python3 -I census.py <dialog-notation>/sketches/*.clj <dialog-notation>/examples/*.clj
<dialog-notation>/src/prelude.clj, with CENSUS_DUMP=<path> to write the trees
as JSON for `record_spike access`.

Reads each document with the notation's reader rules (form.rs), builds the
tree syntax.rs builds (Syntax, then Syntax::value), and counts the
composites (maps and lists) of each top-level form's value.
"""
import sys, statistics, math, re

OPERATOR = set("+-*/<>=!%&|^~")

def tokens(text):
    out = []; i = 0; n = len(text)
    while i < n:
        c = text[i]
        if c.isspace(): i += 1; continue
        if c == ';':
            while i < n and text[i] != '\n': i += 1
            continue
        if c in '()[]{}\'':
            out.append(c); i += 1; continue
        if c == '"':
            j = i + 1; lit = ''
            while text[j] != '"':
                if text[j] == '\\':
                    lit += {'n':'\n','t':'\t','r':'\r'}.get(text[j+1], text[j+1]); j += 2
                else:
                    lit += text[j]; j += 1
            out.append(('"', lit)); i = j + 1; continue
        j = i
        while j < n and not text[j].isspace() and text[j] not in '()[]{};"': j += 1
        out.append(text[i:j]); i = j
    return out

def label(tok):
    if isinstance(tok, tuple) or not tok.endswith(':'): return None
    name = tok[:-1]
    return None if name.endswith(':') else name

def is_number(t):
    d = t[1:] if t.startswith('-') else t
    parts = d.split('.', 1)
    ok = lambda p: p != '' and p.isdigit()
    return ok(parts[0]) and (len(parts) == 1 or ok(parts[1]))

def atom(t):
    if isinstance(t, tuple): return ('text', t[1])
    if t.startswith('?'): return ('var', t[1:])
    if t == '_': return ('blank',)
    if is_number(t): return ('num', t)
    if t.startswith(':'): return ('kw', t[1:])
    return ('sym', t)

def form(ts, at):
    t = ts[at[0]]; at[0] += 1
    if t == '[':
        items = []
        while ts[at[0]] != ']': items.append(form(ts, at))
        at[0] += 1
        return ('vec', items)
    if t == '{':
        entries = []
        while ts[at[0]] != '}':
            nx = ts[at[0]]
            if isinstance(nx, str) and (nx == ':' or (nx.startswith(':'))):
                at[0] += 1; key = ('sym', nx[1:])
            elif label(nx) is not None and label(nx) == '':
                at[0] += 1; key = ('sym', '')
            else:
                key = form(ts, at)
            if ts[at[0]] == '}':
                entries.append((key, ('blank',))); break
            entries.append((key, form(ts, at)))
        at[0] += 1
        return ('map', entries)
    if t == "'":
        q = form(ts, at)
        return ('app', ('sym', 'quote'), [('bare', q)])
    if t == '(':
        head = form(ts, at); parts = []
        while ts[at[0]] != ')':
            nx = ts[at[0]]; l = label(nx)
            if l is not None:
                at[0] += 1; parts.append(('lab', l, form(ts, at)))
            else:
                parts.append(('bare', form(ts, at)))
        at[0] += 1
        return ('app', head, parts)
    return atom(t)

def kind(name):
    if name and all(c in OPERATOR for c in name): return 'op'
    if ':' in name: return 'entity'
    return 'name'

# Values: ('map', {key: value}) with text keys, ('list', [..]), scalars.
def node(k, fields):
    d = dict(fields); d['type'] = ('entity', k + ':')
    return ('map', d)

def syntax_value(f):
    tag = f[0]
    if tag == 'text': return node('constant', [('value', ('text', f[1]))])
    if tag == 'num': return node('constant', [('value', ('num', f[1]))])
    if tag == 'kw': return node('constant', [('value', ('entity', f[1] + ':'))])
    if tag == 'sym':
        if f[1] in ('true', 'false'): return node('constant', [('value', ('bool',))])
        if kind(f[1]) == 'entity': return node('constant', [('value', ('entity', f[1]))])
        return node('reference', [('name', ('symbol', f[1] if '/' in f[1] and not f[1].startswith('/') else '/' + f[1]))])
    if tag == 'var': return node('variable', [('name', ('text', f[1]))])
    if tag == 'blank': return node('blank', [])
    if tag == 'vec': return node('tuple', [('items', ('list', [syntax_value(x) for x in f[1]]))])
    if tag == 'map':
        return node('map', [('entries', ('list', [('list', [syntax_value(k), syntax_value(v)]) for k, v in f[1]]))])
    if tag == 'app':
        parts = f[2]
        bare_before = []
        for p in parts:
            if p[0] == 'bare' and p[1] == ('sym', '...'): break
            if p[0] == 'bare': bare_before.append(p)
        primary = not bare_before and any(p[0] == 'lab' and p[1] == 'of' for p in parts)
        items = []; terms = {}
        for p in parts:
            if p[0] == 'lab':
                if primary and p[1] == 'of': items.insert(0, syntax_value(p[2]))
                else: terms[p[1]] = syntax_value(p[2])
        items += [syntax_value(p[1]) for p in parts if p[0] == 'bare']
        operand = node('parameters', [('items', ('list', items)), ('terms', ('map', terms))])
        return node('application', [('operator', syntax_value(f[1])), ('operand', operand)])
    raise ValueError(f)

def head(n):
    return 1 if n < 24 else 2 if n < 256 else 3 if n < 65536 else 5 if n < 2**32 else 9

def varint(n):
    b = 1
    while n >= 0x80: n >>= 7; b += 1
    return b

def link(held):
    inner = 7 + varint(held) + held
    return 2 + head(inner) + inner

def cbor(v):
    t = v[0]
    if t == 'map': return head(len(v[1])) + sum(head(len(k.encode())) + len(k.encode()) + cbor(x) for k, x in v[1].items())
    if t == 'list': return head(len(v[1])) + sum(cbor(x) for x in v[1])
    if t == 'text': return head(len(v[1].encode())) + len(v[1].encode())
    if t == 'bool': return 1
    if t == 'num':
        s = v[1]
        if '.' in s and not re.fullmatch(r'-?\d+\.0*', s): return 9
        n = abs(int(float(s)))
        return head(n)
    if t in ('entity', 'symbol'): return link(len(v[1].encode()))
    raise ValueError(v)

def walk(v, depth, stats):
    t = v[0]
    if t in ('map', 'list'):
        stats['composites'] += 1
        stats['depth'] = max(stats['depth'], depth)
        if t == 'map':
            stats['map_fields'].append(len(v[1]))
            for x in v[1].values(): walk(x, depth + 1, stats)
        else:
            stats['list_items'].append(len(v[1]))
            for x in v[1]: walk(x, depth + 1, stats)

def dist(xs):
    xs = sorted(xs)
    p95 = xs[min(len(xs) - 1, math.ceil(0.95 * len(xs)) - 1)]
    return f"{xs[0]} / {statistics.median(xs):g} / {p95} / {xs[-1]}"

def to_json(v):
    t = v[0]
    if t == 'map': return {'map': [[k, to_json(x)] for k, x in v[1].items()]}
    if t == 'list': return {'list': [to_json(x) for x in v[1]]}
    if t == 'bool': return {'bool': True}
    return {t: v[1]}

import json, os
dump = os.environ.get('CENSUS_DUMP')
dumped = []
per_doc = []
all_maps = []; all_lists = []; all_depth = []; all_bytes = []; all_comps = []
for path in sys.argv[1:]:
    ts = tokens(open(path).read())
    at = [0]; trees = []
    while at[0] < len(ts): trees.append(syntax_value(form(ts, at)))
    comps = 0; bytes_ = []; depths = []
    for tree in trees:
        s = {'composites': 0, 'depth': 0, 'map_fields': [], 'list_items': []}
        walk(tree, 1, s)
        comps += s['composites']; all_comps.append(s['composites'])
        all_maps += s['map_fields']; all_lists += s['list_items']
        depths.append(s['depth']); all_depth.append(s['depth'])
        b = 1 + cbor(tree); bytes_.append(b); all_bytes.append(b)
        dumped.append({'document': path.split('dialog-notation/')[-1], 'cbor': b, 'tree': to_json(tree)})
    per_doc.append((path.split('dialog-notation/')[-1], len(trees), comps, sum(bytes_), dist(bytes_), dist(depths)))

print("| document | forms | composites | dag-cbor bytes | bytes per form (min / median / p95 / max) | depth per form |")
print("|---|--:|--:|--:|---|---|")
for row in per_doc:
    print("| `%s` | %d | %d | %d | %s | %s |" % row)
print()
print("all forms:", len(all_bytes))
print("composites per form:", dist(all_comps))
print("fields per map:", dist(all_maps), "count", len(all_maps))
print("items per list:", dist(all_lists), "count", len(all_lists))
print("depth:", dist(all_depth))
print("bytes per form:", dist(all_bytes), "total", sum(all_bytes))
print("maps with > 8 fields:", sum(1 for x in all_maps if x > 8), "lists > 8:", sum(1 for x in all_lists if x > 8))

if dump:
    json.dump(dumped, open(dump, 'w'))
