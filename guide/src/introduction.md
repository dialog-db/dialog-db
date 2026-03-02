# The Dialog Guide

Dialog is a database for building local-first, collaborative applications. It stores data as immutable facts, lets you query them with pattern matching and rules, and synchronizes across peers automatically.

If you have ever built an app that needs to:

- Work offline and sync later
- Let multiple users edit shared data
- Derive computed views from raw data
- Keep a full history of changes

...then Dialog is designed for you.

## What makes Dialog different?

Most databases separate storage from sync. You pick a database, then bolt on a sync layer. Dialog treats sync as a first-class concern. Every fact you store is content-addressed and causally tracked, so two peers can exchange exactly the data they're missing without a central coordinator.

Dialog also takes a different approach to schema. Instead of defining table schemas up front, you describe **attributes** (individual things you can say about an entity) and compose them into **concepts** at query time. An entity can participate in as many concepts as you like without migration.

## Who is this guide for?

This guide is primarily for Rust developers who want to use Dialog in their applications. You don't need to know anything about Datalog or databases to follow along. We introduce concepts incrementally, starting from the basics.

We'll also touch on how Dialog can be used from JavaScript/TypeScript, particularly in browser service workers, for web developers interested in local-first architectures.

## How to read this guide

The guide is structured in layers:

1. **Getting Started** introduces the core ideas: what a fact is, how entities and attributes work, and why this model is useful.

2. **Modeling Data** shows you how to define your domain using Rust types: attributes, concepts, and cardinality.

3. **Working with Data** covers the read/write API: sessions, queries, rules, and formulas.

4. **Building an App** walks through a complete Recipe Book application, applying everything from earlier chapters.

5. **Sync and Replication** explains how Dialog's built-in sync works and what it means for your application architecture.

6. **Beyond Rust** covers the platform-agnostic notation and running Dialog on the web.

Each chapter builds on the previous ones, so reading in order will give you the smoothest experience. But if you're already familiar with entity-attribute-value models, feel free to skip ahead.
