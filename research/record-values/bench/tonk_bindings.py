"""Extract, from tonk's library YAML, each view's `on:` bindings and the
event declarations they resolve to, as the analyzer's Bindings would hold
them. Output: JSON {"views": [[event names...]], "events": {name: {...}}}."""
import json, re, sys, pathlib, yaml

root = pathlib.Path(sys.argv[1])
events, views = {}, []
head = re.compile(r'^([a-z-]+)!:\s*(&(\S+))?')
for path in sorted(root.glob('*.yaml')):
    lines = path.read_text().splitlines()
    blocks, cur = [], None
    for line in lines:
        m = head.match(line)
        if m:
            cur = [m.group(1), m.group(3), []]
            blocks.append(cur)
        elif cur is not None:
            cur[2].append(line)
    for kind, name, body in blocks:
        text = '\n'.join(body)
        if kind == 'event' and name:
            try:
                spec = yaml.safe_load(text) or {}
            except yaml.YAMLError:
                continue
            sources = {}
            for field, raw in (spec.get('where') or {}).items():
                raw = str(raw)
                if raw.startswith('.'):
                    sources[field] = {'property': [s for s in raw.split('.') if s]}
                elif raw.startswith('{') and raw.endswith('}'):
                    sources[field] = {'field': raw[1:-1]}
                elif raw.startswith('"') or ' ' in raw:
                    sources[field] = {'literal': raw.strip('"')}
                else:
                    sources[field] = {'reference': raw}
            events[name] = {
                'event-type': str(spec.get('type', name.split('/')[-1])),
                'prevent-default': bool(spec.get('prevent-default', False)),
                'stop-propagation': bool(spec.get('stop-propagation', False)),
                'sources': sources,
            }
        elif kind == 'view':
            names = sorted({'on/' + n for n in re.findall(r'on:([a-z0-9-]+)=', text)})
            if names:
                views.append(names)
views = [[n for n in v if n in events] for v in views]
views = [v for v in views if v]
json.dump({'views': views, 'events': events}, sys.stdout, indent=1, sort_keys=True)
