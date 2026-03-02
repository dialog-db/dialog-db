# A Recipe Book

Let's put everything together by building a Recipe Book application. This isn't a complete, runnable application, but it walks through how you'd model a real domain with Dialog, showing how the concepts from earlier chapters combine.

We'll build up in stages:

1. [Defining the domain](./recipe-domain.md) - attributes and concepts for recipes, ingredients, and tags
2. [Adding recipes](./recipe-writes.md) - writing data through sessions and transactions
3. [Searching and filtering](./recipe-queries.md) - querying with patterns and joins
4. [Derived data with rules](./recipe-rules.md) - classifications and computed views

Each section introduces a new aspect of the Dialog API while keeping the domain consistent.

## What we're building

A household recipe book where:

- Recipes have a name, a serving count, a prep time, and an author
- Recipes have multiple ingredients, each with a quantity and unit
- Recipes can be tagged (e.g., "breakfast", "vegetarian", "quick")
- Family members can each add and edit recipes from their own devices
- The book stays in sync across devices

The first few sections focus on the single-peer experience (modeling, writing, querying). The sync aspect comes for free from Dialog's architecture, as discussed in the [Sync chapter](./sync.md).
