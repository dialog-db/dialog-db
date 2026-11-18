"""gzip -6 over groups of records, as a tree node's block would carry them."""
import gzip, sys, pathlib, struct
d = pathlib.Path(sys.argv[1])
def records(path):
    b = path.read_bytes(); out = []; at = 0
    while at < len(b):
        n = struct.unpack('>I', b[at:at+4])[0]; out.append(b[at+4:at+4+n]); at += 4+n
    return out
names = ['revisions, versions as records', 'revisions, versions as bytes', 'tonk view bindings']
print('| dataset | form | group | raw | gzip -6 | ÷ dag-cbor raw |')
print('|---|---|--:|--:|--:|--:|')
for i, name in enumerate(names):
    base = None
    for form in ['cbor', 'pairs']:
        rs = records(d / f'set{i}-{form}.bin')
        for group in ([1, 64] if i < 2 else [1, len(rs)]):
            raw = sum(len(r) for r in rs)
            z = sum(len(gzip.compress(b''.join(rs[k:k+group]), 6)) for k in range(0, len(rs), group))
            if form == 'cbor' and group == 1: base = raw
            print(f'| {name} | {"dag-cbor" if form=="cbor" else "pairs"} | {group} | {raw} | {z} | {z/base:.2f} |')
