# Introduction

## What is Dialog?

Dialog is a database that stores **facts** instead of documents or rows. Each fact is a small, immutable piece of knowledge that can be queried, combined, and reasoned about.

Think of it like building with LEGO blocks instead of carving monolithic sculptures. Instead of storing a complete "employee" document, you store individual facts:

- The name of employee #123 is "Alice"
- The salary of employee #123 is 60000
- The manager of employee #123 is employee #456

This granular approach gives you flexibility: the same data can be interpreted through different lenses, queried in ways you didn't anticipate, and evolved without breaking existing code.

## Facts, Not Documents

Traditional databases force you to decide upfront: "Is this a SQL table? A JSON document? A graph?" Dialog doesn't ask you to choose. Facts naturally express:

- **Structured data** (like SQL rows)
- **Nested data** (like JSON documents)
- **Connected data** (like graph edges)

All at the same time, using the same primitives.

Dialog is **schema-on-read**, not schema-on-write. You don't need migrations when your model evolves. Multiple applications can interpret the same facts differently. Your data remains flexible as requirements change.

## Built for Collaboration

Dialog is designed for **local-first, privacy-preserving collaboration**:

- Queries run against your **local database**
- Changes **synchronize** using content-addressed storage
- **Conflict-free replication** through causal timestamps
- You control your data and who sees it

## Let's Build Something

In the next chapter, we'll explore the paradigm that makes Dialog different: how modeling information as immutable facts changes everything.

Then we'll jump straight into code - defining attributes, modeling data, and running queries.
