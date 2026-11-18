# What a view takes from where it is rendered

**Decision:** how a view gets what tonk's templates read off their host element.
**For:** the notation's `view` is a function of a concept and its conditions, and tonk's views also read where they are mounted.

Met porting `table.yaml` on 2026-11-18. The maintainer, of `model={dom.host/model}`: "I think this is a wart we need to consider alternatives for."

## What tonk does

`{dom.host/<name>}` in a template is an attribute of the element the view is rendered into (`tonk-template/src/lib.rs:290`). The library's views use it five times:

```
 model={dom.host/model}   core.yaml 48        the view of any model, tonk:_
                          core.yaml 966       a view that wraps the model's own
                          prose-editor 182    the directory of prose
                          table.yaml 548      the directory of table
 with={dom.host/with}     core.yaml 2374      the branch and repository, for an element that builds a URL
```

`core.yaml` says why the first is there: a directory renders each instance in a nested display, and "an instance carries no pointer to its own model". And why the second: "subscriptions inherit the site's pinned context without it, but URL builders have nothing to inherit."

So one spelling carries two things:

```
 which model was asked for     known when the view is resolved; four uses
 which branch is being read    the renderer's own context; one use
```

## Two things that cannot both be true

The design has a view be a rule that concludes a markup value, with nested views as references the renderer resolves. A rule is evaluated over facts. There is no element to read when it runs. So a designed view cannot read its host, whatever the spelling: what it needs has to be in the rule's head or be the renderer's to supply.

## Approaches

| criterion \ approach | Status quo: read the host | The enclosing view's concept is the default | The default directory is the renderer's | A view names what it takes |
|---|---|---|---|---|
| in one line | a template reads any attribute of the element it is mounted in | a nested reference with no `:model` means the concept of the view it stands in; the renderer knows it, as it resolved that view by concept and name | no document holds the view of any model; a model with no directory gets the renderer's | a view's head lists inputs beside its entity, and a reference passes them |
| a directory of one concept (table, prose) | reads the host | 🟢 nothing to write: the reference's model is the view's own | the same as the second column | passes the model by name |
| the view of any model (`tonk:_`) | reads the host | ? needs a view whose concept is a variable, and a condition over "every instance of it"; the design has neither | 🟢 nothing to write | ? the same variable concept |
| the branch being read | reads the host | not addressed | not addressed | 🟡 every view between the root and the element that needs it passes it along |
| a view as a rule that derives markup | 🔴 a rule has no host | a rule; the default is resolved outside it | a rule | a rule with more head fields |
| what a space can replace | the view of any model, by installing another | the same, once a variable concept exists | 🟡 the default directory is no longer data | the same |
| what it adds to the notation | a path namespace in templates, unbounded | a rule of resolution, no form | nothing | a form for inputs, and their types |

The second and third columns are not alternatives: the second covers a view of a named concept, the third the one view that names none.

The branch is not a view's business in any column but the last. A subscription inherits it already; an element that builds a URL could be given it the same way, by the renderer.

Open: whether a space ever replaces the view of any model, which decides whether a variable concept is needed at all.
