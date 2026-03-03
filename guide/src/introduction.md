# The Dialog Guide

Dialog is a database for local-first, collaborative applications. It stores data as immutable claims, queries them with pattern matching and rules, and synchronizes across peers automatically.

If you need an app that works offline, syncs later, lets multiple users edit shared data, derives computed views, and keeps full history — Dialog is designed for that.

## What makes Dialog different?

Most databases separate storage from sync. Dialog treats sync as first-class: every claim is content-addressed and causally tracked, so two peers exchange exactly what they're missing without a central coordinator.

Schema works differently too. Instead of fixed table schemas, you describe **attributes** and compose them into **concepts** at query time.

## Who is this guide for?

Primarily Rust developers. No prior knowledge of Datalog or databases required. We also cover JavaScript/TypeScript usage via browser service workers.

## How to read this guide

1. **Getting Started** — what a claim is, entities, attributes, and why this model is useful
2. **Modeling Data** — defining your domain: attributes, concepts, cardinality
3. **Working with Data** — sessions, queries, rules, formulas
4. **Building an App** — a complete Recipe Book application
5. **Sync and Replication** — how built-in sync works
6. **Beyond Rust** — platform-agnostic notation and running Dialog on the web

Chapters build on each other. If you're familiar with entity-attribute-value models, skip ahead.
