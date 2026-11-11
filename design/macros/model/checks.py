import sys; sys.path.insert(0, "."); import model

def fresh():
    r = model.Replica()
    r.submit(open('prelude.clj').read(), quiet=True)
    r.submit(open('todo.clj').read(), quiet=True)
    r.run(); r.commands = []
    return r

def attempt(title, text, run=True):
    r = fresh()
    print('--', title)
    try:
        r.submit(text)
        for form, kernel in r.trace:
            print(model.pretty(kernel))
        if run:
            for row in r.run():
                print('  ', row)
    except Exception as error:
        print('   refused:', error)

attempt('a later document finishes one to-do; absence is tested on the concept as a unit', '''
(finish (todo id:7a6a542c))
''', run=True)

r = fresh()
r.submit('(finish (todo id:7a6a542c))'); r.run(); r.commands = []; r.trace = []
r.submit('(query (todo ?t text: ?x) (when (todo ?t text: ?x)) (unless (todo ?t state: :done)))')
print('-- then: open to-dos only')
print(model.pretty(r.trace[0][1]))
for row in r.run(): print('  ', row)

attempt('an add, and a finish of an earlier to-do, in one document', '''
(add (todo list: list:home text: "Walk dog"))
(finish (todo id:7a6a542c))
(query (todo ?t text: ?x state: ?s))
''')
attempt('an unknown form in a premise', '(query (todo ?t text: ?x) (when (frobnicate ?t)))', run=False)
attempt('a second concept under the same name', '''
(concept (todo ?t title: ?x) (xyz.note ?t title: (:last (:text ?x))))
(query (todo ?t))
''', run=False)
attempt('a write through a concept that leaves a field out', '(assert! (todo todo:1 text: "x"))', run=False)
attempt('a complete write through a concept', '''
(assert! (todo todo:1 list: list:home text: "x" state: :open))
(query (todo todo:1 text: ?x))
''')
attempt('a write whose value is the wrong type', '(assert! (todo todo:1 list: list:home text: 7 state: :open))', run=False)
attempt('retract by entity alone', '(retract! (todo id:7a6a542c))', run=False)
attempt('a parameter the concept does not have', '(query (todo ?t colour: ?c))', run=False)

print('-- an add and the finish of what it adds, in one document: the finish is lost')
r = model.Replica()
r.submit(open('prelude.clj').read(), quiet=True)
r.submit(open('todo.clj').read().split('\n(add ')[0], quiet=True)
r.submit('''
(add (todo list: list:home text: "Buy milk"))
(finish (todo id:7a6a542c))
(query (todo ?t text: ?x state: ?s))
''')
for row in r.run():
    print('  ', row)
