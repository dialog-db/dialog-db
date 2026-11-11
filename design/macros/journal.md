# Macros as rules: journal

## Reflective inquiry

|  | Understanding | Activity |
|---|---|---|
| **Status** | The kernel is small and the core forms are 65 lines over it. They run on a real branch through `rust/dialog-notation`, which compiles the kernel to descriptors Dialog-DB has ([poc.md](poc.md)). Syntax is held by the crate, not stored. | **Designing.** Direction decided by the maintainer. The kernel is built as a proof of concept; storing syntax is not. |
| **Agenda** | Whether a form can be a value a premise takes apart, so syntax is stored. Whether `report` takes a template. Whether a write through a concept may leave fields out. What a read with no pick reads. What makes an entity from a rule's bindings. What expansion costs. | Decide the open findings of [kernel.md](kernel.md) and [poc.md](poc.md), then design forms as values. |

## Candidate problems

"Macros as rules" is one possible answer. These are the problems it could be an answer to. Each is a hypothesis in the [ledger](claims.md) until evidence or the maintainer says otherwise.

1. **A form costs a release.** Adding `view` or `event`, or changing what `concept` takes, means changing a parser and an analyzer in Rust and shipping them. A space cannot add a form of its own.
2. **A definition's meaning is in code, not in data.** What `concept` or `rule` takes is known to the analyzer and to an 831-line guide. An agent or a tool cannot ask the database what forms exist and what each one takes.
3. **A definition's meaning can move after it is written.** A stored form that refers to something by name means something else once the name is re-pointed.
4. **Two builds may read one document two ways.** What a stored form means depends on the analyzer that reads it, and replicas do not all run the same build.

## Approaches parked for Direction

Not examined yet. They are columns, not recommendations.

- **Status quo.** A parser and compiler in Rust know every form (tonk-notation today; the `dialog-notation` crate #728 plans).
- **Everything is a rule** (Irakli). Macros are rules, and the compiler is a rule too.
- **Macros are rules, compiling is a formula** (Irakli). Rules turn a surface form into kernel forms; one built-in formula turns a kernel form into what dialog stores.
- **Macros are data, expanding is built in** (the 2026-05-16 journal). A macro is a stored pattern and templates; a native expander applies them. No rule takes part.

## 2026-11-10, Diagnosing: from "macros as rules" to what is in the way

**Asked.**

- Irakli brought the list in the [topic's description](../macros.md#description), and asked: is the compiler itself a rule, or is there a built-in formula that takes syntax and compiles it?
- The agent's answer, as a hypothesis (H1 in the ledger): both, split at the kernel form. Three reasons the compiler cannot be a rule all the way down. Something has to compile the first rule. A rule's identity comes from a canonical labeling that searches (colour refinement, then individualisation), which is not a deduction. And a rule that does not match says nothing, where a compiler has to say why a form was refused.
- Open, asked of Irakli: which of the four candidate problems is in the way today, and what happens because of it.

**Learned.**

- "All primitive forms are expressed through macro rules" and "macros are rules" cannot both hold all the way down: `rule` would be defined by a rule. Irakli's own follow-up, the built-in formula, is where the regress stops. The word "primitive" was carrying two meanings; the [glossary](glossary.md) splits it into surface form and kernel form.
- Earlier readings by the agent in this conversation were off the mark and are withdrawn: conduct as the host language, and a meta-circular evaluator with bindings threaded through premises. Neither is in the maintainer's notes.
- The 2026-05-16 design had no problem assembling a form from rows, because its macros expanded to triples. The problem appeared on #728 when a definition became one value under its hash. H4 says a set-collecting `reduce` closes it.

**Decided.** Not decided. What would decide the phase: Irakli's answer to the open question, written into the Problem section.

## 2026-11-11, Designing: the kernel, and the core forms run in a model

**Asked.**

- Irakli declined to state a problem and asked for a design and a plan, not an implementation. Direction: macros as rules, a minimal kernel, hash-addressed definitions, nothing built on conduct or wasm, and as fast as the system is today.
- He asked that claims be confirmed in the code and not left as hypotheses, and that notes not be trusted. The planner, the rule evaluator and fixpoint, commit-time induction, the value and type model, resolvers and the write instructions were read.
- He set the spellings: a formula is applied by keyword, `(:text ?x)`, or by a bare operator with `:` as the second operand's label, `(+ ?a : ?b is: ?c)`; equality is `(:value ?a is: ?b)`; `when` and `unless` are siblings in a rule; "application", not "call"; `query` is a rule marked for the report; a head that persists is `assert!`.
- He asked for the core forms defined in the slim kernel, to see whether it works before the kernel is built.

**Learned.**

- From the code: `reduce` is refused in deductive rules; a recursive group is evaluated bottom-up in full and non-recursive rules get the caller's bindings; a rule installed in a commit fires in that commit; `Record` is opaque bytes; formulas have fixed cells; a resolver may read content-addressed storage only; the store has two write instructions, `Assert(fact, pick)` and `Retract(fact)`; a rule's kind is its descriptor's head field, `deduce`, `assert!` or `retract!`; `unless` negates one proposition.
- So: `assert!` and `retract!` cannot both be macros; expansion cannot be a resolver, since names are read through the mutable head, and a query at a pinned revision is a `Snapshot`; a read has to be one entity and several fields.
- Withdrawn by the agent: conduct as the host, a meta-circular evaluator with threaded bindings, building forms with `reduce`, dag-cbor for forms, shared subforms, "a macro applies from the next document", and the walker as a resolver.
- The core forms work in the model with one rule applying every macro. `concept` is one line: a macro that defines a macro.
- With `!` marking what stays, issuing a command is a plain `assert`, and the model no longer tells a command by its domain.

**Decided.**

- The direction, by the maintainer, on an opinion. No problem statement.
- The kernel and its spellings as in [kernel.md](kernel.md), except the pick wrapper, which is proposed.
- `!` marks what stays, which departs from #728's design as of R25.
- Forms are fully inlined, with a canonical encoding (bijoux for integers). The #733 tokenization is for storage. A source map keeps, for each reference, its name, the revision and the entity.
- Not decided: findings 3 to 8 of [kernel.md](kernel.md).

## 2026-11-11, Designing: syntax is a rule

**Asked.** Irakli found the first definitions unreadable: a top-level write defined as a macro of itself, and `concept` defined as a macro that writes a macro. He asked for Racket's style, then asked why syntax is not a regular rule, with `(expand pattern)` in the premise and `(syntax template)` in the head, and whether those two should be the quote and unquote. He asked that a misspelled command be a warning and not a refusal, as tonk's language server reports.

**Learned.**

- A macro written as a rule needs no stored pattern and template, no rule that applies them, and no formulas to match, fill, get and bind. The prelude went from 104 lines to 64 and every check holds.
- `expand` and `syntax` are the quoting forms: the walker does not go into them, a variable the pattern binds is replaced, and any other variable in a template is renamed at each expansion.
- A concept definition is one rule: `(todo …)` stands for that read of `xyz.todo`. Shown as that rule it is plain; shown as the syntax that writes the rule it was not.
- A diagnostic is something a rule derives about a form.

**Decided.** Syntax is a rule with `expand` and `syntax`. A top-level write is kernel. A misspelled command is a warning. Not decided: whether `concept` stays a word of its own, since it only shortens a rule.

## 2026-11-11, Designing: the kernel on Dialog-DB

**Asked.** Irakli asked for a proof of concept in dialog itself: concepts, rules and queries defined in the notation and evaluated. He asked that domains be told from other names, since syntax needs them. He corrected a concept the agent wrote with a check in its body: a concept maps facts to itself and back, and a check has no inverse. He then said a concept may read several entities, a backlink being one, and a rule may have several heads of different kinds; what dialog lacks for these it can be given.

**Learned.**

- The kernel compiles to descriptors Dialog-DB already has, and the to-do, the counter, a backlink, a recursive relation and a query of several premises run on a real branch. See [poc.md](poc.md).
- A rule and the command it reads can share a document: the rule fires in the transaction that installs it.
- Dialog-DB has no formula that makes an entity, so a rule cannot issue a command yet.
- `unless` takes one proposition whose own variables are blank. A formula inside it, a concept spanning entities under it, and a variable repeated inside it are each out of reach.
- The rules about syntax are held and matched by the crate, not stored. That part of the direction is not shown.
- A report comes back in relations, not in the words the query used.
- Two commands in one document share a round. Where both write one `last` field, the cell holds both claims.

**Decided.** Nothing new. The proof of concept is evidence for the kernel table, not a decision about it. Open for the maintainer: whether `report` takes a template; whether a write through a concept may leave fields out; what a read with no pick reads.
