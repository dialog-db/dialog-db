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

print('-- a command defined only by the rule that reads it, around a domain')
r = model.Replica()
r.submit(open('prelude.clj').read(), quiet=True)
r.submit('''
(rule (assert! (xyz.counter ?c count: (+ ?n : 1)))
  (when (increment (xyz.counter ?c))
        (xyz.counter ?c count: (:natural ?n))))
(assert! (xyz.counter counter:a count: 0))
''')
r.run(); r.commands = []
r.submit('''
(increment (xyz.counter counter:a))
(query (xyz.counter ?c count: ?n))
''')
for form, kernel in r.trace:
    print(model.pretty(kernel))
for row in r.run():
    print('  ', row)
print('   warnings:', len(r.diagnostics))

print('-- the same command misspelled: it is issued, with a warning')
r.commands = []; r.trace = []
r.submit('(incremnet (xyz.counter counter:a))')
print(model.pretty(r.trace[0][1]))
for row in r.run():
    print('  ', row)
for form, said in r.diagnostics:
    print('   warning:', model.show(form), model.show(said))

print('-- descriptions: on a concept, on a rule, on an attribute, and found by search')
r = model.Replica()
r.submit(open('prelude.clj').read(), quiet=True)
r.submit('''
(concept (todo ?t list: ?l text: ?x state: ?s)
  description: "Something to do, on a list."
  (xyz.todo ?t list: (:last (:entity ?l)) text: (:last (:text ?x)) state: (:top ?s)))

(rule description: "Adding the same text to the same list twice makes one to-do."
  (assert! (todo ?t list: ?l text: ?x state: :open))
  (when (add (todo list: ?l text: ?x))
        (:id xyz.todo list: ?l text: ?x is: ?t)))

(assert! (dialog.meta xyz.todo/text description: "What is to be done."))
''')
print(model.pretty(r.trace[0][1]))
r.trace = []
r.submit('''
(query (dialog.meta ?thing description: ?text))
''')
for row in r.run():
    print('  ', row)
print('   search for "list", with the name where there is one')
r.inductive = []
r.submit('''
(query (dialog.meta ?thing description: ?text)
  (when (dialog.meta ?thing description: ?text)
        (:like ?text : "%list%")))
(query (dialog.name ?thing is: ?name)
  (when (dialog.name ?thing is: ?name)
        (dialog.meta ?thing description: ?text)
        (:like ?text : "%list%")))
''')
for row in r.run():
    print('  ', row)

def scene(title, setup, *documents):
    print('--', title)
    r = model.Replica()
    r.submit(open('prelude.clj').read(), quiet=True)
    try:
        r.submit(setup)
        r.run(); r.commands = []
        for text in documents:
            r.trace = []; r.inductive = [x for x in r.inductive if not any(h[1] == ('sym', 'report') for h in model.clauses(x)[0])]
            r.submit(text)
            for form, kernel in r.trace:
                print(model.pretty(kernel))
            for row in r.run():
                print('  ', row)
            r.commands = []
    except Exception as error:
        print('   refused:', error)

scene('a parameter named differently from the field it reads',
      '''
(concept (task ?t title: ?x) (xyz.todo ?t text: (:last (:text ?x))))
(assert! (task task:1 title: "Buy milk"))
''',
      '(query (task ?t title: ?what))')

scene('a concept over two domains: read, written, and absent as a whole',
      '''
(concept (author ?p name: ?n email: ?e)
  (io.person ?p name: (:last (:text ?n)))
  (io.contact ?p email: (:last (:text ?e))))
(assert! (author person:ada name: "Ada" email: "ada@example.com"))
(assert! (io.person person:bob name: "Bob"))
''',
      '(query (author ?p name: ?n email: ?e))',
      '(query (io.person ?p name: ?n) (when (io.person ?p name: ?n)) (unless (author ?p)))')

scene('a concept with a check in it is refused: a check has no inverse',
      '''
(concept (cheap ?i price: ?n)
  (shop.item ?i price: (:last (:natural ?n)))
  (< ?n : 100))
''')

scene('the same as a rule that derives a relation, which is read and never written',
      '''
(rule (shop.cheap ?i price: ?n)
  (when (shop.item ?i price: (:last (:natural ?n)))
        (< ?n : 100)))
(assert! (shop.item item:tea price: 5))
(assert! (shop.item item:car price: 9000))
''',
      '(query (shop.item ?i price: ?n) (when (shop.item ?i price: ?n) (< ?n : 100)))')

scene('a concept over two entities is refused',
      '''
(concept (membership ?m person: ?p team: ?t)
  (org.member ?m person: ?p)
  (org.team ?t lead: ?p))
''')
