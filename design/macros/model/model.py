"""A throwaway model of the kernel, to check that the prelude's core forms
expand and run as written. It is not dialog and shares no code with it.

Kernel: rule (heads, when, unless), application of a relation (a domain,
one entity, labelled fields), application of a formula (:name or an
operator), the write relations assert! and retract!, assert for what
holds one round, the report relation,
and data with quoted forms.
"""
import hashlib
import itertools
import re
import sys

# ---------------------------------------------------------------- forms
# atom:  ('sym', s) ('var', n) ('blank',) ('kw', s) ('str', s) ('num', n)
#        ('ell',) ('rest', n)
# form:  ('app', head, parts)   part: ('b', item) | ('l', label, item)
#        ('quote', form) ('vec', items)

TOKEN = re.compile(r'''\s+|;[^\n]*|(\(|\)|\[|\]|'|"(?:[^"\\]|\\.)*"|[^\s()\[\]';]+)''')


def tokens(text):
    out, at = [], 0
    while at < len(text):
        m = TOKEN.match(text, at)
        if not m:
            raise SyntaxError(f'cannot read at {text[at:at + 20]!r}')
        if m.group(1):
            out.append(m.group(1))
        at = m.end()
    return out


def is_label(tok):
    return tok.endswith(':') and not tok.startswith('"') and tok != '::'


def atom(tok):
    if tok == '...':
        return ('ell',)
    if tok.startswith('...?'):
        return ('rest', tok[4:])
    if tok.startswith('?'):
        return ('var', tok[1:])
    if tok == '_':
        return ('blank',)
    if tok.startswith('"'):
        return ('str', tok[1:-1])
    if re.fullmatch(r'-?\d+(\.\d+)?', tok):
        return ('num', tok)
    if tok.startswith(':'):
        return ('kw', tok[1:])
    return ('sym', tok)


def read(toks, at):
    tok = toks[at]
    if tok == "'":
        form, at = read(toks, at + 1)
        return ('quote', form), at
    if tok == '[':
        items, at = [], at + 1
        while toks[at] != ']':
            item, at = read(toks, at)
            items.append(item)
        return ('vec', tuple(items)), at + 1
    if tok == '(':
        head, at = read(toks, at + 1)
        parts = []
        while toks[at] != ')':
            if is_label(toks[at]):
                label = toks[at][:-1]
                item, at = read(toks, at + 1)
                parts.append(('l', label, item))
            else:
                item, at = read(toks, at)
                parts.append(('b', item))
        return ('app', head, tuple(parts)), at + 1
    return atom(tok), at + 1


def read_all(text):
    toks, at, forms = tokens(text), 0, []
    while at < len(toks):
        form, at = read(toks, at)
        forms.append(form)
    return forms


def show(f):
    kind = f[0]
    if kind == 'sym':
        return f[1]
    if kind == 'var':
        return '?' + f[1]
    if kind == 'blank':
        return '_'
    if kind == 'kw':
        return ':' + f[1]
    if kind == 'str':
        return '"' + f[1] + '"'
    if kind == 'num':
        return f[1]
    if kind == 'ell':
        return '...'
    if kind == 'rest':
        return '...?' + f[1]
    if kind == 'quote':
        return "'" + show(f[1])
    if kind == 'vec':
        return '[' + ' '.join(show(i) for i in f[1]) + ']'
    if kind == 'holes':
        return '{' + ' '.join(k + ' ' + show_binding(v) for k, v in sorted(f[1].items())) + '}'
    parts = [show(f[1])]
    for p in f[2]:
        parts.append(show(p[1]) if p[0] == 'b' else (p[1] + ': ' + show(p[2])))
    return '(' + ' '.join(parts) + ')'


def show_binding(v):
    if v[0] == 'seq':
        return '[' + ' '.join(show(i) for i in v[1]) + ']'
    if v[0] == 'row':
        return '{' + ' '.join(l + ': ' + show(i) for l, i in v[1]) + '}'
    return show(v)


def pretty(f, indent=0):
    """A rule on several lines, everything else on one."""
    if f[0] == 'app' and f[1] == ('sym', 'rule'):
        pad = ' ' * (indent + 2)
        lines = ['(rule']
        for p in f[2]:
            item = p[1]
            if item[0] == 'app' and item[1] in (('sym', 'when'), ('sym', 'unless')):
                inner = [show(q[1]) for q in item[2]]
                word = item[1][1]
                gap = ' ' * (len(word) + 2)
                lines.append(pad + '(' + word + ' ' + ('\n' + pad + gap).join(inner) + ')')
            else:
                lines.append(pad + show(item))
        return ' '.join(lines[:2]).replace('(rule   ', '(rule ') + ''.join('\n' + l for l in lines[2:]) + ')'
    return show(f)


# ------------------------------------------------------- match and fill
def split(parts):
    bare = [p[1] for p in parts if p[0] == 'b' and p[1][0] != 'rest']
    rest = [p[1][1] for p in parts if p[0] == 'b' and p[1][0] == 'rest']
    labelled = [(p[1], p[2]) for p in parts if p[0] == 'l']
    return bare, labelled, (rest[0] if rest else None)


def holes_in(pattern, into=None):
    into = [] if into is None else into
    if pattern[0] == 'var':
        into.append(pattern[1])
    elif pattern[0] == 'quote':
        holes_in(pattern[1], into)
    elif pattern[0] == 'vec':
        for i in pattern[1]:
            holes_in(i, into)
    elif pattern[0] == 'app':
        holes_in(pattern[1], into)
        for p in pattern[2]:
            holes_in(p[-1], into)
    return into


def bind(env, name, value):
    if name in env:
        return env if env[name] == value else None
    env = dict(env)
    env[name] = value
    return env


def match(form, pattern, env):
    """The holes `pattern` binds against `form`, or None. A hole takes any
    one part. Bare parts match in order and in number; `p ...` takes any
    number of them. A labelled part of the pattern the form leaves out
    stays unbound; `...?rest` takes the labelled parts the pattern does
    not name, and without it the form may have none."""
    kind = pattern[0]
    if kind == 'var':
        return bind(env, pattern[1], form)
    if kind == 'blank':
        return env
    if kind == 'quote':
        return match(form[1], pattern[1], env) if form[0] == 'quote' else None
    if kind == 'vec':
        if form[0] != 'vec' or len(form[1]) != len(pattern[1]):
            return None
        for f, p in zip(form[1], pattern[1]):
            env = match(f, p, env)
            if env is None:
                return None
        return env
    if kind != 'app':
        return env if form == pattern else None
    if form[0] != 'app':
        return None
    env = match(form[1], pattern[1], env)
    if env is None:
        return None
    fbare, flabelled, _ = split(form[2])
    pbare, plabelled, rest = split(pattern[2])
    if ('ell',) in pbare:
        at = pbare.index(('ell',))
        before, repeated, after = pbare[:at - 1], pbare[at - 1], pbare[at + 1:]
        if len(fbare) < len(before) + len(after):
            return None
        middle = fbare[len(before):len(fbare) - len(after)]
        pairs = list(zip(fbare, before)) + list(zip(fbare[len(fbare) - len(after):], after))
        names = holes_in(repeated)
        columns = {n: [] for n in names}
        for item in middle:
            one = match(item, repeated, {})
            if one is None:
                return None
            for n in names:
                columns[n].append(one[n])
        for n in names:
            env = bind(env, n, ('seq', tuple(columns[n])))
            if env is None:
                return None
    else:
        if len(fbare) != len(pbare):
            return None
        pairs = list(zip(fbare, pbare))
    for f, p in pairs:
        env = match(f, p, env)
        if env is None:
            return None
    given = dict(flabelled)
    for label, p in plabelled:
        if label in given:
            env = match(given[label], p, env)
            if env is None:
                return None
    named = {label for label, _ in plabelled}
    extra = tuple((l, f) for l, f in flabelled if l not in named)
    if rest is not None:
        return bind(env, rest, ('row', extra))
    return env if not extra else None


def fill(template, env):
    """`template` with its holes filled. A hole with no binding is `_`."""
    kind = template[0]
    if kind == 'var':
        return env.get(template[1], ('blank',))
    if kind == 'quote':
        return ('quote', fill(template[1], env))
    if kind == 'vec':
        return ('vec', tuple(fill(i, env) for i in template[1]))
    if kind != 'app':
        return template
    parts, items = [], list(template[2])
    for at, p in enumerate(items):
        if p[0] == 'l':
            parts.append(('l', p[1], fill(p[2], env)))
            continue
        item = p[1]
        if item == ('ell',):
            continue
        if item[0] == 'rest':
            row = env.get(item[1], ('row', ()))
            parts.extend(('l', l, f) for l, f in row[1])
            continue
        repeats = at + 1 < len(items) and items[at + 1] == ('b', ('ell',))
        if repeats:
            names = [n for n in holes_in(item) if env.get(n, ('',))[0] == 'seq']
            count = len(env[names[0]][1]) if names else 0
            for k in range(count):
                each = dict(env)
                for n in names:
                    each[n] = env[n][1][k]
                parts.append(('b', fill(item, each)))
            continue
        value = fill(item, env)
        if value[0] == 'seq':
            parts.extend(('b', v) for v in value[1])
        else:
            parts.append(('b', value))
    return ('app', fill(template[1], env), tuple(parts))


# ------------------------------------------------------------- formulas
class Unbound(Exception):
    pass


TYPES = {
    'text': lambda v: v[0] == 'str',
    'natural': lambda v: v[0] == 'num' and not v[1].startswith('-') and '.' not in v[1],
    'entity': lambda v: v[0] == 'sym' and ':' in v[1],
    'symbol': lambda v: v[0] == 'sym',
    'value': lambda v: True,
}
PICKS = {'last', 'top', 'all', 'max', 'min'}
OPERATORS = set('+-*/<>=')


def is_formula(form):
    return form[0] == 'app' and (form[1][0] == 'kw' or (form[1][0] == 'sym' and set(form[1][1]) <= OPERATORS))


def is_relation(form):
    return form[0] == 'app' and form[1][0] == 'sym' and '.' in form[1][1]


def entity_of(*parts):
    digest = hashlib.blake2s(' '.join(parts).encode(), digest_size=4).hexdigest()
    return ('sym', 'id:' + digest)


def value(term, env):
    """What a term stands for. A quoted form is itself. An application of a
    formula in a term's place stands for its `is:` value."""
    kind = term[0]
    if kind == 'var':
        if term[1] not in env:
            raise Unbound(term[1])
        return env[term[1]]
    if kind == 'quote':
        return term[1]
    if kind == 'blank':
        raise Unbound('_')
    if is_formula(term):
        rows = apply_formula(term, env, want_is=True)
        if len(rows) != 1:
            raise ValueError(f'{show(term)} has {len(rows)} values')
        return rows[0]
    return term


def unwrap(term):
    """A term under its type and pick: (:last (:text ?x)) is ?x, text, last."""
    checks = []
    while (term[0] == 'app' and term[1][0] == 'kw' and len(term[2]) == 1 and term[2][0][0] == 'b'
           and (term[1][1] in TYPES or term[1][1] in PICKS)):
        if term[1][1] in TYPES:
            checks.append(term[1][1])
        term = term[2][0][1]
    return term, checks


def apply_formula(form, env, want_is=False):
    """The environments a formula application leaves, or with `want_is` its
    values. Inputs must be bound; `is:` is the output."""
    name = form[1][1]
    bare, labelled, _ = split(form[2])
    labels = dict(labelled)
    out = labels.pop('is', None)

    def done(result):
        if want_is:
            return [result]
        if out is None:
            return [env]
        bound = unify(out, result, env)
        return [bound] if bound is not None else []

    if name in TYPES:
        subject = value(bare[0], env)
        return done(subject) if TYPES[name](subject) else []
    if name == 'match':
        holes = match(value(bare[0], env), value(labels[''], env), {})
        return done(('holes', holes)) if holes is not None else []
    if name == 'fill':
        return done(fill(value(bare[0], env), value(labels[''], env)[1]))
    if name == 'get':
        holes = value(bare[0], env)[1]
        key = value(labels[''], env)[1]
        return done(holes[key]) if key in holes else []
    if name == 'bind':
        holes = dict(value(bare[0], env)[1])
        holes[value(labels[''], env)[1]] = value(labels['to'], env)
        return done(('holes', holes))
    if name == 'id':
        parts = [show(value(bare[0], env))] if bare else []
        parts += [l + ': ' + show(value(unwrap(t)[0], env)) for l, t in sorted(labels.items())]
        return done(entity_of(*parts))
    if name == 'command':
        form_ = value(bare[0], env)
        inner = split(form_[2])[0][0]
        return done(('sym', 'dialog.command.' + form_[1][1] + '.' + inner[1][1]))
    if name == '+':
        return done(('num', str(int(value(bare[0], env)[1]) + int(value(labels[''], env)[1]))))
    raise ValueError(f'no formula {show(form[1])}')


def unify(term, val, env):
    term, checks = unwrap(term)
    if any(not TYPES[c](val) for c in checks):
        return None
    if term[0] == 'var':
        return bind(env, term[1], val)
    if term[0] == 'blank':
        return env
    if term[0] == 'quote':
        return env if term[1] == val else None
    return env if term == val else None


# ---------------------------------------------------------------- rules
def clauses(rule):
    heads, when, unless = [], [], []
    for p in rule[2]:
        item = p[1]
        if item[0] == 'app' and item[1] == ('sym', 'when'):
            when += [q[1] for q in item[2]]
        elif item[0] == 'app' and item[1] == ('sym', 'unless'):
            unless += [q[1] for q in item[2]]
        else:
            heads.append(item)
    return heads, when, unless


def read_relation(form, env, facts):
    """Every environment in which one entity holds every field the
    application names."""
    domain = form[1][1]
    bare, labelled, _ = split(form[2])
    attributes = [(domain + '/' + label, term) for label, term in labelled]
    if not attributes:
        raise ValueError(f'{show(form)} names no field')
    entities = {e for (e, a) in facts if a == attributes[0][0]}
    out = []
    for entity in sorted(entities):
        first = unify(bare[0], entity, env) if bare else env
        if first is None:
            continue
        if any((entity, a) not in facts for a, _ in attributes):
            continue
        for values in itertools.product(*[sorted(facts[(entity, a)]) for a, _ in attributes]):
            bound = first
            for (_, term), val in zip(attributes, values):
                bound = unify(term, val, bound)
                if bound is None:
                    break
            if bound is not None:
                out.append(bound)
    return out


def solve(premises, env, facts):
    if not premises:
        return [env]
    for at, premise in enumerate(premises):
        if is_formula(premise):
            try:
                found = apply_formula(premise, env)
            except Unbound:
                continue
        elif is_relation(premise):
            found = read_relation(premise, env, facts)
        else:
            raise ValueError(f'not a kernel premise: {show(premise)}')
        rest = premises[:at] + premises[at + 1:]
        return [e for one in found for e in solve(rest, one, facts)]
    raise ValueError('no premise can run: ' + ' '.join(show(p) for p in premises))


def fire(rule, facts):
    heads, when, unless = clauses(rule)
    out = []
    for env in solve(when, {}, facts):
        if any(solve([absent], env, facts) for absent in unless):
            continue
        out.append((heads, env))
    return out


def ground(application, env):
    """The facts an application stands for once its terms have values."""
    domain = application[1][1]
    bare, labelled, _ = split(application[2])
    entity = value(bare[0], env)
    out = []
    for label, term in labelled:
        term, checks = unwrap(term)
        val = value(term, env)
        if any(not TYPES[c](val) for c in checks):
            raise ValueError(f'{show(val)} is not {checks} in {show(application)}')
        out.append(((entity, domain + '/' + label), val))
    return out


# ------------------------------------------------------------ the walker
HEAD_WORDS = {('sym', 'assert!'), ('sym', 'retract!'), ('sym', 'assert'), ('sym', 'report')}


class Replica:
    def __init__(self):
        self.facts = {}          # (entity, attribute) -> set of values
        self.deductive = []      # rules that derive a relation
        self.inductive = []      # rules that write or report
        self.commands = []       # issued this run, held for one round
        self.diagnostics = []    # what the rules say about a form without refusing it
        self.trace = []

    def write(self, key, val, into=None):
        into = self.facts if into is None else into
        fresh = val not in into.get(key, ())
        into.setdefault(key, set()).add(val)
        return fresh

    def ask(self, form, position):
        """What the rules say `form` expands to in `position`."""
        site = entity_of(show(form), position)
        facts = dict(self.facts)
        facts[(site, 'dialog.syntax/form')] = {form}
        facts[(site, 'dialog.syntax/position')] = {('kw', position)}
        found = []
        for rule in self.deductive:
            for heads, env in fire(rule, facts):
                for head in heads:
                    for (entity, attribute), val in ground(head, env):
                        if entity == site and attribute == 'dialog.syntax/expansion' and val not in found:
                            found.append(val)
                        if entity == site and attribute == 'dialog.syntax/warning' and (form, val) not in self.diagnostics:
                            self.diagnostics.append((form, val))
        if not found:
            raise ValueError(f'nothing says what {show(form)} is')
        if len(found) > 1:
            raise ValueError(f'{show(form)} is ambiguous: ' + ' | '.join(show(f) for f in found))
        return found[0]

    def application(self, form, position):
        while not (is_relation(form) or is_formula(form)):
            if position == 'head' and form[0] == 'app' and form[1] in HEAD_WORDS:
                inner = self.application(form[2][0][1], 'head')
                return ('app', form[1], (('b', inner),))
            form = self.ask(form, position)
        return form

    def top(self, form):
        """A top-level form, rewritten until it is a kernel rule."""
        while form[1] != ('sym', 'rule'):
            form = self.ask(form, 'head')
        parts = []
        for p in form[2]:
            item = p[1]
            if item[0] == 'app' and item[1] in (('sym', 'when'), ('sym', 'unless')):
                inner = tuple(('b', self.application(q[1], 'premise')) for q in item[2])
                parts.append(('b', ('app', item[1], inner)))
            else:
                parts.append(('b', self.application(item, 'head')))
        return ('app', form[1], tuple(parts))

    def install(self, rule):
        heads, when, unless = clauses(rule)
        writes = [h for h in heads if h[1] in HEAD_WORDS]
        if not writes:
            self.deductive.append(rule)
        elif when or unless or any(h[1] == ('sym', 'report') for h in heads):
            self.inductive.append(rule)
            # the index of what each rule reads, as dialog keeps `dialog.rule/on`
            for premise in when:
                if is_relation(premise):
                    self.write((entity_of(show(rule)), 'dialog.rule/on'), premise[1])
        else:
            for head in heads:
                for key, val in ground(head[2][0][1], {}):
                    if head[1] == ('sym', 'assert'):
                        self.commands.append((key, val))
                    else:
                        self.write(key, val)

    def submit(self, text, quiet=False):
        for form in read_all(text):
            kernel = self.top(form)
            if not quiet:
                self.trace.append((form, kernel))
            self.install(kernel)

    def run(self, rounds=16):
        """Rounds until nothing new lands. What `assert` says holds one round."""
        held, reports = {}, []
        for key, val in self.commands:
            self.write(key, val, held)
        for _ in range(rounds):
            view = {k: set(v) for k, v in self.facts.items()}
            for k, v in held.items():
                view.setdefault(k, set()).update(v)
            landed, issued = [], {}
            for rule in self.inductive:
                for heads, env in fire(rule, view):
                    for head in heads:
                        word, inner = head[1][1], head[2][0][1]
                        if word == 'report':
                            continue
                        for key, val in ground(inner, env):
                            if word == 'retract!':
                                landed.append(('retract', key, val))
                            elif word == 'assert':
                                self.write(key, val, issued)
                            else:
                                landed.append(('assert', key, val))
            changed = False
            for word, key, val in landed:
                if word == 'assert':
                    # an electing pick replaces what the cell held
                    if key in self.facts and val not in self.facts[key] and key[1].startswith('xyz.'):
                        self.facts[key] = set()
                    changed |= self.write(key, val)
                elif val in self.facts.get(key, ()):
                    self.facts[key].discard(val)
                    changed = True
            held = issued
            if not changed and not held:
                break
        else:
            raise ValueError('still producing after 16 rounds')
        for rule in self.inductive:
            for heads, env in fire(rule, self.facts):
                for head in heads:
                    if head[1] == ('sym', 'report'):
                        row = ' '.join('?' + n + ' = ' + show(v) for n, v in sorted(env.items()))
                        if row not in reports:
                            reports.append(row)
        return reports


if __name__ == '__main__':
    here = sys.argv[1]
    replica = Replica()
    replica.submit(open(here + '/prelude.clj').read(), quiet=True)
    for path in sys.argv[2:]:
        replica.submit(open(path).read())
    for form, kernel in replica.trace:
        print('; ' + show(form))
        print(pretty(kernel))
        print()
    print('; report')
    for row in replica.run():
        print(row)
